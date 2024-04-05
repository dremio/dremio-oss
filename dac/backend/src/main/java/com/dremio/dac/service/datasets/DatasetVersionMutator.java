/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.dac.service.datasets;

import static com.dremio.dac.service.datasets.DatasetDownloadManager.DATASET_DOWNLOAD_STORAGE_PLUGIN;
import static com.dremio.dac.util.DatasetsUtil.isTemporaryPath;
import static com.dremio.dac.util.DatasetsUtil.printVersionViewInfo;
import static com.dremio.dac.util.DatasetsUtil.toVirtualDatasetUI;
import static com.dremio.dac.util.DatasetsUtil.toVirtualDatasetVersion;
import static com.dremio.exec.ExecConstants.VERSIONED_VIEW_ENABLED;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_ALLPARENTS;
import static com.dremio.service.namespace.dataset.DatasetVersion.MAX_VERSION;
import static com.dremio.service.namespace.dataset.DatasetVersion.MIN_VERSION;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static java.lang.String.format;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.dac.daemon.DACDaemonModule;
import com.dremio.dac.explore.QueryParser;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.VersionContextUtils;
import com.dremio.dac.model.common.RootEntity.RootType;
import com.dremio.dac.proto.model.dataset.DatasetVersionOrigin;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.service.datasets.DatasetDownloadManager.DownloadDataResponse;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStore.LegacyFindByRange;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.Views;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.exec.util.OptionUtil;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.options.OptionManager;
import com.dremio.service.job.JobCountsRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.DownloadInfo;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** For creating/updating/deleting of dataset and dataset versions. */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class DatasetVersionMutator {
  private static final Logger logger = LoggerFactory.getLogger(DatasetVersionMutator.class);

  private static final int MAX_VERSIONS_TO_DELETE = 10000;

  protected final JobsService jobsService;
  protected final CatalogService catalogService;
  private final ContextService contextService;
  private final LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> datasetVersions;
  protected final OptionManager optionManager;

  @Inject
  public DatasetVersionMutator(
      final LegacyKVStoreProvider kv,
      final JobsService jobsService,
      final CatalogService catalogService,
      final OptionManager optionManager,
      final ContextService contextService) {
    this.jobsService = jobsService;
    this.datasetVersions = kv.getStore(VersionStoreCreator.class);
    this.catalogService = catalogService;
    this.contextService = contextService;
    this.optionManager = optionManager;
  }

  public DatasetDownloadManager downloadManager() {
    final FileSystemPlugin<?> downloadPlugin =
        catalogService.getSource(DATASET_DOWNLOAD_STORAGE_PLUGIN);
    final FileSystemPlugin<InternalFileConf> jobResultsPlugin =
        catalogService.getSource(DACDaemonModule.JOBS_STORAGEPLUGIN_NAME);
    logger.debug(
        "Job results plugin \"{}\" rooted at \"{}\" is {}.",
        jobResultsPlugin.getName(),
        jobResultsPlugin.getRootLocation(),
        jobResultsPlugin.getConfig().isPdfsBased() ? "PDFS based" : "non-PDFS based");
    return new DatasetDownloadManager(
        jobsService,
        downloadPlugin.getConfig().getPath(),
        downloadPlugin.getSystemUserFS(),
        jobResultsPlugin.getConfig().isPdfsBased(),
        optionManager);
  }

  public static void validate(DatasetPath path, VirtualDatasetUI ds) {
    if (ds.getSqlFieldsList() == null || ds.getSqlFieldsList().isEmpty()) {
      throw new IllegalArgumentException("SqlFields can't be null for " + path);
    }
    if (ds.getState() == null) {
      throw new IllegalArgumentException("state can't be null for " + path);
    }
  }

  private void putVersion(
      VirtualDatasetUI ds, @Nullable VirtualDatasetUI baseVirtualDataset, boolean doValidation)
      throws DatasetNotFoundException {
    DatasetPath path = new DatasetPath(ds.getFullPathList());
    if (doValidation) {
      validate(path, ds);
    }
    if (baseVirtualDataset == null) {
      ds.setCreatedAt(System.currentTimeMillis());
    } else {
      ds.setCreatedAt(baseVirtualDataset.getCreatedAt());
    }
    final VersionDatasetKey datasetKey = new VersionDatasetKey(path, ds.getVersion());
    datasetVersions.put(datasetKey, toVirtualDatasetVersion(ds));
  }

  public void putVersion(VirtualDatasetUI ds) throws DatasetNotFoundException {
    putVersion(ds, null, true);
  }

  public void putVersion(VirtualDatasetUI ds, @Nullable VirtualDatasetUI baseVirtualDataset)
      throws DatasetNotFoundException {
    putVersion(ds, baseVirtualDataset, true);
  }

  public void putTempVersionWithoutValidation(VirtualDatasetUI ds) throws DatasetNotFoundException {
    Preconditions.checkArgument(
        isTemporaryPath(ds.getFullPathList()), "Only temp untitled dataset can bypass validation.");
    putVersion(ds, null, false);
  }

  public VirtualDatasetUI createDatasetFrom(
      DatasetPath existingDatasetPath, DatasetPath newDatasetPath, String ownerName)
      throws NamespaceException {
    // Not supported for versioned plugin.
    if (isVersionedPluginSource(existingDatasetPath) || isVersionedPluginSource(newDatasetPath)) {
      throw UserException.unsupportedError()
          .message("Copy of entity is not allowed within Versioned source")
          .buildSilently();
    }

    // Cannot copy to itself.
    if (existingDatasetPath.equals(newDatasetPath)) {
      throw UserException.validationError()
          .message(String.format("%s already exists", newDatasetPath.toUnescapedString()))
          .buildSilently();
    }

    VirtualDatasetUI ds = get(existingDatasetPath);
    // Set a new version, name.
    ds.setFullPathList(newDatasetPath.toPathList());
    ds.setVersion(DatasetVersion.newVersion());
    ds.setName(newDatasetPath.getLeaf().getName());
    ds.setSavedTag(null);
    ds.setId(null);
    ds.setPreviousVersion(null);
    ds.setOwner(ownerName);
    putVersion(ds);

    try {
      put(ds);
    } catch (NamespaceNotFoundException nfe) {
      throw new ClientErrorException(
          format("Parent folder %s doesn't exist", existingDatasetPath.toParentPath()), nfe);
    }
    return ds;
  }

  public void put(VirtualDatasetUI ds, NamespaceAttribute... attributes)
      throws DatasetNotFoundException, NamespaceException {
    DatasetPath path = new DatasetPath(ds.getFullPathList());
    validatePaths(path, null);
    validate(path, ds);
    final SqlQuery query = new SqlQuery(ds.getSql(), ds.getState().getContextList(), ds.getOwner());
    validateVersions(query, null);
    DatasetConfig datasetConfig = toVirtualDatasetVersion(ds).getDataset();
    getCatalog().addOrUpdateDataset(path.toNamespaceKey(), datasetConfig, attributes);
    ds.setId(datasetConfig.getId().getId());
    ds.setSavedTag(datasetConfig.getTag());
    // Update this version of dataset with new occ version of dataset config from namespace.
    putVersion(ds);
  }

  public void putWithVersionedSource(
      VirtualDatasetUI ds, DatasetPath path, String branchName, String savedTag)
      throws DatasetNotFoundException, IOException, NamespaceException {
    DatasetConfig datasetConfig = toVirtualDatasetVersion(ds).getDataset();
    Preconditions.checkNotNull(path);
    VersionContext versionContext =
        (branchName == null
            ? VersionContext.NOT_SPECIFIED
            : VersionContextUtils.parse(VersionContext.Type.BRANCH.toString(), branchName));
    Map<String, VersionContext> contextMap =
        ImmutableMap.of(path.getRoot().toString(), versionContext);
    Catalog catalog = getCatalog().resolveCatalog(contextMap);
    final SqlQuery query = new SqlQuery(ds.getSql(), ds.getState().getContextList(), ds.getOwner());

    // TODO: Once DX-65418 is fixed, injected catalog will validate the right entity accordingly
    catalog.validatePrivilege(new NamespaceKey(path.getRoot().getName()), SqlGrant.Privilege.ALTER);
    validateVersions(query, contextMap);
    BatchSchema schema = CalciteArrowHelper.fromDataset(datasetConfig);
    View view =
        Views.fieldTypesToView(
            Iterables.getLast(datasetConfig.getFullPathList()),
            datasetConfig.getVirtualDataset().getSql(),
            ViewFieldsHelper.getViewFields(datasetConfig),
            datasetConfig.getVirtualDataset().getContextList(),
            null);
    DremioTable exist = catalog.getTable(new NamespaceKey(path.toPathList()));
    if (exist != null && !(exist instanceof ViewTable)) {
      throw UserException.validationError()
          .message(
              "Expecting getting a view but returns a entity type of %s",
              exist.getDatasetConfig().getType())
          .buildSilently();
    } else if (exist != null
        && (savedTag == null || !savedTag.equals(exist.getDatasetConfig().getTag()))) {
      throw UserException.concurrentModificationError()
          .message(
              "The specified location already contains a view named \"%s\". Please provide a unique view name or open the existing view, edit and then save.",
              path.toPathList().get(path.toPathList().size() - 1))
          .build(logger);
    }
    final boolean viewExists = (exist != null);

    ResolvedVersionContext resolvedVersionContext =
        CatalogUtil.resolveVersionContext(catalog, path.getRoot().getName(), versionContext);
    ViewOptions viewOptions =
        new ViewOptions.ViewOptionsBuilder()
            .version(resolvedVersionContext)
            .batchSchema(schema)
            .actionType(
                viewExists
                    ? ViewOptions.ActionType.UPDATE_VIEW
                    : ViewOptions.ActionType.CREATE_VIEW)
            .build();
    if (viewExists) {
      catalog.updateView(new NamespaceKey(path.toPathList()), view, viewOptions);
    } else {
      catalog.createView(new NamespaceKey(path.toPathList()), view, viewOptions);
    }

    catalog = getCatalog().resolveCatalog(contextMap);

    DremioTable table = catalog.getTable(new NamespaceKey(path.toPathList()));

    ds.setId(table.getDatasetConfig().getId().getId());
    ds.setSavedTag(table.getDatasetConfig().getTag());
    ds.setDatasetVersionOrigin(DatasetVersionOrigin.SAVE);

    logger.debug(
        "Putting VirtualDatasetUI {} in datasetVersions store for versioned view {}",
        printVersionViewInfo(ds),
        path.toUnescapedString());
    putVersion(ds);
  }

  public Catalog getCatalog() {
    // TODO - Why are we using the System User when interacting with Catalog when most of the
    // DatasetTool should be in the context of a user?
    return catalogService.getCatalog(
        MetadataRequestOptions.of(
            SchemaConfig.newBuilder(CatalogUser.from(SYSTEM_USERNAME)).build()));
  }

  public boolean checkIfVersionedViewEnabled() {
    return optionManager.getOption(VERSIONED_VIEW_ENABLED);
  }

  private void validatePaths(DatasetPath toPath, DatasetPath fromPath) {
    if (toPath.getRoot().getRootType() == RootType.TEMP) {
      throw new IllegalArgumentException("can not save dataset in tmp space");
    }

    if (fromPath != null && fromPath.equals(toPath)) {
      throw new IllegalArgumentException(
          format(
              "A dataset named '%s' already exists in [%s]",
              fromPath.getLeaf(), toPath.toParentPath()));
    }
  }

  public VirtualDatasetUI renameDataset(final DatasetPath oldPath, final DatasetPath newPath)
      throws NamespaceException, DatasetNotFoundException, DatasetVersionNotFoundException {
    try {
      if (isVersionedPluginSource(newPath) || isVersionedPluginSource(oldPath)) {
        throw UserException.unsupportedError()
            .message("Moving or Renaming of entity is not allowed in Versioned source")
            .buildSilently();
      }
      validatePaths(newPath, oldPath);
      VirtualDatasetVersion latestVersion = null; // the one that matches in the namespace
      final DatasetConfig datasetConfig =
          getCatalog().renameDataset(oldPath.toNamespaceKey(), newPath.toNamespaceKey());
      final List<VirtualDatasetUI> allVersions =
          FluentIterable.from(getAllVersions(oldPath)).toList();
      final Map<NameDatasetRef, NameDatasetRef> newPrevLinks = Maps.newHashMap();
      for (final VirtualDatasetUI ds : allVersions) {
        final NameDatasetRef previousVersion = ds.getPreviousVersion();
        if (previousVersion != null) {
          // Only rewrite the path if its equal to the old path.
          final String path =
              previousVersion.getDatasetPath().equals(oldPath.toString())
                  ? newPath.toString()
                  : previousVersion.getDatasetPath();
          newPrevLinks.put(
              previousVersion,
              new NameDatasetRef(path).setDatasetVersion(previousVersion.getDatasetVersion()));
        }
      }
      // rename all old versions, link the previous version correctly
      for (final VirtualDatasetUI ds : allVersions) {
        datasetVersions.delete(new VersionDatasetKey(oldPath, ds.getVersion()));
        ds.setName(newPath.getDataset().getName());
        ds.setFullPathList(newPath.toPathList());
        final VirtualDatasetVersion vvds = toVirtualDatasetVersion(ds);
        // get returns null for the first
        vvds.setPreviousVersion(newPrevLinks.get(ds.getPreviousVersion()));
        datasetVersions.put(new VersionDatasetKey(newPath, ds.getVersion()), vvds);
        if (datasetConfig
            .getVirtualDataset()
            .getVersion()
            .equals(vvds.getDataset().getVirtualDataset().getVersion())) {
          latestVersion = vvds;
        }
      }
      if (latestVersion == null) {
        throw new DatasetNotFoundException(
            newPath,
            format(
                "Missing version %s after rename.",
                datasetConfig.getVirtualDataset().getVersion().toString()));
      }
      return toVirtualDatasetUI(latestVersion);
    } catch (NamespaceNotFoundException nfe) {
      throw new DatasetNotFoundException(oldPath, nfe);
    }
  }

  public boolean isVersionedPluginSource(DatasetPath datasetPath) throws NamespaceException {
    String sourceName = datasetPath.getRoot().getName();
    NameSpaceContainer nameSpaceContainer =
        getCatalog().getEntityByPath(new NamespaceKey(sourceName));
    if (nameSpaceContainer != null
        && nameSpaceContainer.getType() == NameSpaceContainer.Type.SOURCE) {
      // catalogService does not store the spaces, home etc
      return catalogService.getSource(sourceName).isWrapperFor(VersionedPlugin.class);
    }
    return false;
  }

  public VirtualDatasetUI getVersion(DatasetPath path, DatasetVersion version) {
    return getVersion(path, version, false);
  }

  /**
   * For the given path and given version, get the entry from dataset versions store, and transform
   * to {@link VirtualDatasetUI}. If the entry is available in namespace, dataset id and saved
   * version are also set in the returned object. Note that this method does not throw if an entry
   * is found in dataset version store, but not in namespace (entry is not saved).
   *
   * @param path dataset path
   * @param version dataset version
   * @param isVersionedSource
   * @return virtual dataset UI
   * @throws DatasetVersionNotFoundException if dataset is not found in namespace and dataset
   *     versions store
   * @throws DatasetNotFoundException if dataset is found in namespace, but not in dataset versions
   *     store
   */
  public VirtualDatasetUI getVersion(
      DatasetPath path, DatasetVersion version, boolean isVersionedSource)
      throws DatasetVersionNotFoundException {
    VirtualDatasetUI virtualDatasetUI = null;
    try {
      VirtualDatasetVersion datasetVersion = getVirtualDatasetVersion(path, version);
      virtualDatasetUI = toVirtualDatasetUI(datasetVersion);
      DatasetConfig datasetConfig;
      if (isVersionedSource && virtualDatasetUI != null) {
        datasetConfig = datasetVersion.getDataset();
        logger.debug(
            "For versioned view {} got datasetConfig {} from datasetVersion store",
            path.toUnescapedString(),
            datasetConfig);
      } else {
        datasetConfig = getCatalog().getDataset(path.toNamespaceKey());
      }

      if (virtualDatasetUI == null) {
        // entry exists in namespace but not in dataset versions; very likely an invalid request
        throw new DatasetNotFoundException(path, String.format("version [%s]", version));
      }

      virtualDatasetUI.setId(datasetConfig.getId().getId()).setSavedTag(datasetConfig.getTag());
    } catch (final NamespaceException ex) {
      logger.debug("dataset error for {}", path, ex);
    }

    if (virtualDatasetUI == null) {
      throw new DatasetVersionNotFoundException(path, version);
    }
    return virtualDatasetUI;
  }

  public DatasetVersion getLatestVersionByOrigin(
      DatasetPath path, DatasetVersion version, DatasetVersionOrigin datasetVersionOrigin) {
    VersionDatasetKey datasetKey = new VersionDatasetKey(path, version);
    do {
      VirtualDatasetVersion virtualDatasetVersion = datasetVersions.get(datasetKey);

      if (virtualDatasetVersion.getDatasetVersionOrigin() == datasetVersionOrigin) {
        return datasetKey.getVersion();
      }
      NameDatasetRef nameDatasetRef = virtualDatasetVersion.getPreviousVersion();
      datasetKey = createDatasetKeyFromNameDatasetRef(nameDatasetRef);
    } while (datasetKey != null);
    // we return null for saved version when we are not able to find saved version.
    return null;
  }

  private VersionDatasetKey createDatasetKeyFromNameDatasetRef(NameDatasetRef nameDatasetRef) {
    if (nameDatasetRef == null) {
      return null;
    }
    DatasetVersion version = new DatasetVersion(nameDatasetRef.getDatasetVersion());
    DatasetPath path = new DatasetPath(nameDatasetRef.getDatasetPath());
    return new VersionDatasetKey(path, version);
  }

  private DatasetPath getDatasetPathInOriginalCase(DatasetPath path) {
    // namespaceService key is by default case-insensitive, but datasetVersions is case-sensitive.
    // Here we use the Dataset Path in original case preserved in namespaceService value.
    DatasetPath datasetPath = path;

    // Temporary VDS are not saved in namespace.
    if (!isTemporaryPath(path.toPathList())) {
      final DremioTable dremioTable = getCatalog().getTableForQuery(path.toNamespaceKey());
      datasetPath =
          dremioTable != null
              ? new DatasetPath(dremioTable.getDatasetConfig().getFullPathList())
              : datasetPath;
    }

    return datasetPath;
  }

  public VirtualDatasetVersion getVirtualDatasetVersion(DatasetPath path, DatasetVersion version) {
    DatasetPath datasetPath = getDatasetPathInOriginalCase(path);
    VirtualDatasetVersion vdsVersion =
        datasetVersions.get(new VersionDatasetKey(datasetPath, version));
    if (vdsVersion == null) {
      tryFixVersionHistory(path, version);
      vdsVersion = datasetVersions.get(new VersionDatasetKey(datasetPath, version));
    }
    return vdsVersion;
  }

  public Iterable<VirtualDatasetUI> getAllVersions(DatasetPath path)
      throws DatasetVersionNotFoundException {
    DatasetPath datasetPath = getDatasetPathInOriginalCase(path);
    return Iterables.transform(
        datasetVersions.find(
            new LegacyFindByRange<>(
                new VersionDatasetKey(datasetPath, MIN_VERSION),
                false,
                new VersionDatasetKey(datasetPath, MAX_VERSION),
                false)),
        new Function<Entry<VersionDatasetKey, VirtualDatasetVersion>, VirtualDatasetUI>() {
          @Override
          public VirtualDatasetUI apply(Entry<VersionDatasetKey, VirtualDatasetVersion> input) {
            return toVirtualDatasetUI(input.getValue());
          }
        });
  }

  /**
   * @param path the id of the dataset
   * @return the latest saved version of the corresponding dataset
   * @throws DatasetNotFoundException if the path was not found
   */
  public VirtualDatasetUI get(DatasetPath path)
      throws DatasetNotFoundException, NamespaceException {
    try {
      final DatasetConfig datasetConfig = getCatalog().getDataset(path.toNamespaceKey());
      final VirtualDatasetVersion virtualDatasetVersion =
          getVirtualDatasetVersion(path, datasetConfig.getVirtualDataset().getVersion());
      if (virtualDatasetVersion == null) {
        throw new DatasetVersionNotFoundException(
            path, datasetConfig.getVirtualDataset().getVersion());
      }
      final VirtualDatasetUI virtualDatasetUI =
          toVirtualDatasetUI(virtualDatasetVersion)
              .setId(datasetConfig.getId().getId())
              .setSavedTag(datasetConfig.getTag());
      return virtualDatasetUI;
    } catch (NamespaceNotFoundException nsnf) {
      throw new DatasetNotFoundException(path, nsnf);
    }
  }

  public VirtualDatasetUI get(DatasetPath path, DatasetVersion version)
      throws DatasetNotFoundException, NamespaceException {
    try {
      final DatasetConfig datasetConfig = getCatalog().getDataset(path.toNamespaceKey());
      final VirtualDatasetVersion virtualDatasetVersion = getVirtualDatasetVersion(path, version);
      if (virtualDatasetVersion == null) {
        throw new DatasetVersionNotFoundException(
            path, datasetConfig.getVirtualDataset().getVersion());
      }
      final VirtualDatasetUI virtualDatasetUI =
          toVirtualDatasetUI(virtualDatasetVersion)
              .setId(datasetConfig.getId().getId())
              .setSavedTag(datasetConfig.getTag());
      return virtualDatasetUI;
    } catch (NamespaceNotFoundException e) {
      throw new DatasetNotFoundException(
          path,
          format(
              "Some path not found while looking for dataset %s, version %s.",
              path.toPathString(), version.toString()),
          e);
    }
  }

  public void deleteDataset(DatasetPath datasetPath, String namespaceEntityVersion)
      throws DatasetNotFoundException, NamespaceException {
    try {
      getCatalog().deleteDataset(datasetPath.toNamespaceKey(), namespaceEntityVersion);
    } catch (NamespaceNotFoundException nsnf) {
      throw new DatasetNotFoundException(datasetPath, nsnf);
    }
  }

  /**
   * Get count of datasets depending on given dataset
   *
   * @param path path of saved dataset
   * @return count of all descendants.
   * @throws NamespaceException
   */
  public int getDescendantsCount(NamespaceKey path) {
    try (TimedBlock b = Timer.time("getDescendantCounts")) {
      return getCatalog()
          .getCounts(SearchQueryUtils.newTermQuery(DATASET_ALLPARENTS, path.toString()))
          .get(0);
    } catch (NamespaceException e) {
      logger.error("Failed to get descendant counts for path " + path);
      return 0;
    }
  }

  /**
   * Get list of dataset paths depending on given dataset
   *
   * @param path path of saved dataset
   * @return dataset paths of descendants.
   * @throws NamespaceException
   */
  public Iterable<DatasetPath> getDescendants(DatasetPath path) throws NamespaceException {
    LegacyFindByCondition condition =
        new LegacyFindByCondition()
            .setCondition(
                SearchQueryUtils.newTermQuery(DATASET_ALLPARENTS, path.toNamespaceKey().toString()))
            .setLimit(1000);
    return Iterables.transform(
        getCatalog().find(condition),
        new Function<Entry<NamespaceKey, NameSpaceContainer>, DatasetPath>() {
          @Override
          public DatasetPath apply(Entry<NamespaceKey, NameSpaceContainer> input) {
            return new DatasetPath(input.getKey().getPathComponents());
          }
        });
  }

  public int getJobsCount(NamespaceKey path, OptionManager optionManager) {
    int jobCount = 0;
    if ((optionManager == null)
        || (optionManager.getOption(ExecConstants.CATALOG_JOB_COUNT_ENABLED))) {
      jobCount = getJobsCount(path);
    }
    return jobCount;
  }

  public int getJobsCount(NamespaceKey path) {
    JobCountsRequest.Builder builder = JobCountsRequest.newBuilder();
    builder.addDatasets(VersionedDatasetPath.newBuilder().addAllPath(path.getPathComponents()));
    builder.setJobCountsAgeInDays(
        OptionUtil.getJobCountsAgeInDays(
            optionManager.getOption(ExecConstants.JOB_MAX_AGE_IN_DAYS)));
    return jobsService.getJobCounts(builder.build()).getCountList().get(0);
  }

  public long getJobsCount(List<NamespaceKey> datasetPaths) {
    long jobCount = 0;
    final JobCountsRequest.Builder builder = JobCountsRequest.newBuilder();
    datasetPaths.forEach(
        datasetPath ->
            builder.addDatasets(
                VersionedDatasetPath.newBuilder().addAllPath(datasetPath.getPathComponents())));
    builder.setJobCountsAgeInDays(
        OptionUtil.getJobCountsAgeInDays(
            optionManager.getOption(ExecConstants.JOB_MAX_AGE_IN_DAYS)));
    for (Integer count : jobsService.getJobCounts(builder.build()).getCountList()) {
      if (count != null) {
        jobCount += count;
      }
    }
    return jobCount;
  }

  public DownloadDataResponse downloadData(
      DownloadInfo downloadInfo, List<ArrowFileMetadata> resultMetadataList, String userName)
      throws IOException {
    // TODO check if user can access this dataset.
    return downloadManager().getDownloadData(downloadInfo, resultMetadataList);
  }

  /**
   * Deletes a dataset version using the path and version.
   *
   * <p>This method is static to be easily accessible from maintenance tasks. <br>
   *
   * <p><b><em>Be aware that this method can delete any dataset version without verifying if a
   * history will be broken.</em></b>
   *
   * @param provider the {@link LegacyKVStoreProvider} from where the store of type {@link
   *     VersionStoreCreator} will be obtained.
   * @param path the dataset path
   * @param version the version of the dataset
   */
  public static void deleteDatasetVersion(
      LegacyKVStoreProvider provider, List<String> path, String version) {
    LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> store =
        provider.getStore(VersionStoreCreator.class);
    deleteDatasetVersion(store, path, version);
  }

  private static void deleteDatasetVersion(
      LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> store,
      List<String> path,
      String version) {
    final VersionDatasetKey key =
        new VersionDatasetKey(new DatasetPath(path), new DatasetVersion(version));
    deleteDatasetVersion(store, key);
  }

  private static void deleteDatasetVersion(
      LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> store, VersionDatasetKey datasetKey) {
    store.delete(datasetKey);
  }

  /**
   * Delete orphan dataset versions.
   *
   * <p>This method is static to be easily accessible from maintenance tasks.
   *
   * <p>A dataset version is considered an orphan when:
   *
   * <ul>
   *   <li>if the dataset version is temporary, no Jobs are referencing the dataset version
   * </ul>
   *
   * @param optionManagerProvider Jobs KVStore.
   * @param datasetStore Dataset versions KVStore
   * @param daysThreshold Dataset versions older than the threshold days will be deleted. This
   *     threshold is limited to be greater than {@link ExecConstants#JOB_MAX_AGE_IN_DAYS}.
   * @return The number of dataset version entries that were deleted.
   */
  public static long deleteOrphans(
      Provider<OptionManager> optionManagerProvider,
      KVStore<VersionDatasetKey, VirtualDatasetVersion> datasetStore,
      int daysThreshold) {
    return deleteOrphans(optionManagerProvider, datasetStore, daysThreshold, false);
  }

  @VisibleForTesting
  @WithSpan
  public static long deleteOrphans(
      Provider<OptionManager> optionManagerProvider,
      KVStore<VersionDatasetKey, VirtualDatasetVersion> datasetVersionsStore,
      int daysThreshold,
      boolean unsafe) {

    final OptionManager optionManager = optionManagerProvider.get();
    final long maxJobAgeInDays =
        unsafe ? daysThreshold : optionManager.getOption(ExecConstants.JOB_MAX_AGE_IN_DAYS);

    // Dremio has a background process that cleans Jobs older than 30 days (configurable).
    // To prevent deleting dataset versions still associated to existing jobs, the threshold is
    // limited to be at least 30 days.
    long threshold = Math.max(maxJobAgeInDays, daysThreshold);

    ImmutableList.Builder<VersionDatasetKey> keysToDeleteBuilder = ImmutableList.builder();
    long now = System.currentTimeMillis();
    long deleteCount = 0;
    for (Document<VersionDatasetKey, VirtualDatasetVersion> entry : datasetVersionsStore.find()) {
      if (withinThreshold(threshold, now, entry.getValue())) {
        continue;
      }

      VersionDatasetKey datasetKey = entry.getKey();
      if (isTemporaryPath(datasetKey.getPath().toPathList())) {
        keysToDeleteBuilder.add(datasetKey);
        ++deleteCount;
      }
    }

    if (deleteCount > 0) {
      logger.info("Deleting {} orphaned dataset versions", deleteCount);
      for (List<VersionDatasetKey> keys :
          Lists.partition(keysToDeleteBuilder.build(), MAX_VERSIONS_TO_DELETE)) {
        logger.info("Deleting batch of {} orphaned dataset versions", keys.size());
        datasetVersionsStore.bulkDelete(keys);
      }
    }

    return deleteCount;
  }

  private static boolean withinThreshold(
      long daysThreshold, long now, VirtualDatasetVersion version) {
    final long daysThresholdMillis = TimeUnit.DAYS.toMillis(daysThreshold);
    final Long dsCreation = version.getDataset().getCreatedAt();
    return dsCreation != null && now - dsCreation < daysThresholdMillis;
  }

  private void validateVersions(SqlQuery query, Map<String, VersionContext> sourceVersionMapping) {
    try {
      QueryParser.validateVersions(query, contextService.get(), sourceVersionMapping);
    } catch (ValidationException | RelConversionException e) {
      // Calcite exception could wrap exceptions in layers.  Find the root cause to get the original
      // error message.
      Throwable rootCause = e;
      while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
        rootCause = rootCause.getCause();
      }
      throw UserException.validationError().message(rootCause.getMessage()).buildSilently();
    } catch (Exception e) {
      throw UserException.validationError()
          .message("Validation of view sql failed. %s ", e.getMessage())
          .buildSilently();
    }
  }

  private void tryFixVersionHistory(DatasetPath path, DatasetVersion version) {
    if (isTemporaryPath(path.toPathList())) {
      return;
    }

    logger.info("Try to fix history for dataset {}", path.toUnescapedString());
    DatasetPath originalPath = getDatasetPathInOriginalCase(path);
    VersionDatasetKey foundKey = new VersionDatasetKey(path, version);
    VirtualDatasetVersion foundValue = datasetVersions.get(foundKey);

    if (foundValue != null) {
      NameDatasetRef previousVersion = foundValue.getPreviousVersion();
      if (!foundKey.getPath().equals(originalPath)
          || !foundValue.getDataset().getFullPathList().equals(originalPath.toPathList())) {
        datasetVersions.delete(foundKey);
        VersionDatasetKey newKey = new VersionDatasetKey(originalPath, foundKey.getVersion());
        VirtualDatasetVersion newValue = foundValue;
        newValue.getDataset().setFullPathList(originalPath.toPathList());
        datasetVersions.put(newKey, newValue);
        logger.info("Rewrite history {} to {}", foundKey, newKey);
      }
    }
  }

  /** Storage creator for dataset versions. */
  public static class VersionStoreCreator
      implements LegacyKVStoreCreationFunction<VersionDatasetKey, VirtualDatasetVersion> {

    @Override
    public LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> build(
        LegacyStoreBuildingFactory factory) {
      return factory
          .<VersionDatasetKey, VirtualDatasetVersion>newStore()
          .name("datasetVersions")
          .keyFormat(
              Format.wrapped(
                  VersionDatasetKey.class,
                  VersionDatasetKey::toString,
                  VersionDatasetKey::new,
                  Format.ofString()))
          .valueFormat(Format.ofProtostuff(VirtualDatasetVersion.class))
          .build();
    }
  }

  /** key for versioned dataset store. */
  public static class VersionDatasetKey {
    private final DatasetPath path;
    private final DatasetVersion version;

    VersionDatasetKey(DatasetPath path, DatasetVersion version) {
      this.path = path;
      this.version = version;
    }

    public VersionDatasetKey(String s) {
      final int pos = s.lastIndexOf('/');
      Preconditions.checkArgument(
          pos != -1, "version dataset key should include path and version separated by '/'");
      this.path = new DatasetPath(s.substring(0, pos));
      this.version = new DatasetVersion(s.substring(pos + 1));
    }

    @Override
    public String toString() {
      return getPath().toString() + "/" + version;
    }

    public DatasetPath getPath() {
      return path;
    }

    public DatasetVersion getVersion() {
      return version;
    }

    @Override
    public int hashCode() {
      return (this.path.hashCode() << 8) + this.version.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      VersionDatasetKey other = (VersionDatasetKey) obj;
      return path.equals(other.path) && version.equals(other.version);
    }
  }
}

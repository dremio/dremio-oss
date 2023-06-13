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
package com.dremio.dac.service.source;

import static com.dremio.dac.api.MetadataPolicy.ONE_MINUTE_IN_MS;
import static com.dremio.dac.model.namespace.ExternalNamespaceTreeUtils.namespaceTreeOf;
import static com.dremio.dac.service.source.ExternalResourceTreeUtils.generateResourceTreeEntityList;
import static com.dremio.dac.util.DatasetsUtil.toDatasetConfig;
import static com.dremio.dac.util.DatasetsUtil.toPhysicalDatasetConfig;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SOURCE;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.api.Source;
import com.dremio.dac.explore.model.VersionContextUtils;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.model.resourcetree.ResourceTreeEntity;
import com.dremio.dac.model.sources.PhysicalDataset;
import com.dremio.dac.model.sources.PhysicalDatasetName;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.sources.PhysicalDatasetResourcePath;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.collaboration.TagsSearchResult;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.PhysicalDatasetNotFoundException;
import com.dremio.dac.service.errors.ResourceExistsException;
import com.dremio.dac.service.errors.SourceFolderNotFoundException;
import com.dremio.dac.service.errors.SourceNotFoundException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.NessieNamespaceAlreadyExistsException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SchemaEntity;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.file.File;
import com.dremio.file.SourceFilePath;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.physicaldataset.proto.PhysicalDatasetConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * Source service.
 */
public class SourceService {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SourceService.class);
  public static final String LIST_SOURCE_TOTAL_COUNT_SPAN_ATTRIBUTE_NAME = "dremio.source_service.list_source_total_count";
  public static final String IS_VERSIONED_PLUGIN_SPAN_ATTRIBUTE_NAME = "dremio.source_service.isVersionedPlugin";
  public static final String IS_FILE_SYSTEM_PLUGIN_SPAN_ATTRIBUTE_NAME = "dremio.source_service.isFileSystemPlugin";

  private final SabotContext sabotContext;
  private final NamespaceService namespaceService;
  private final DatasetVersionMutator datasetService;
  private final CatalogService catalogService;
  private final ReflectionServiceHelper reflectionServiceHelper;
  private final ConnectionReader connectionReader;
  private final SecurityContext security;
  private final CollaborationHelper collaborationService;

  @Inject
  public SourceService(
    SabotContext sabotContext,
    NamespaceService namespaceService,
    DatasetVersionMutator datasetService,
    CatalogService catalogService,
    ReflectionServiceHelper reflectionHelper,
    CollaborationHelper collaborationService,
    ConnectionReader connectionReader,
    SecurityContext security) {
    this.sabotContext = sabotContext;
    this.namespaceService = namespaceService;
    this.datasetService = datasetService;
    this.catalogService = catalogService;
    this.reflectionServiceHelper = reflectionHelper;
    this.connectionReader = connectionReader;
    this.security = security;
    this.collaborationService = collaborationService;
  }

  private Catalog createCatalog() {
    return createCatalog(null);
  }

  private Catalog createCatalog(String userName) {
    return catalogService.getCatalog(MetadataRequestOptions.of(
      SchemaConfig.newBuilder(StringUtils.isBlank(userName)
          ? CatalogUser.from(security.getUserPrincipal().getName())
          : CatalogUser.from(userName))
        .build()));
  }

  public ConnectionConf<?, ?> getConnectionConf(SourceConfig config){
    return connectionReader.getConnectionConf(config);
  }

  public ConnectionReader getConnectionReader() {
    return connectionReader;
  }

  public SourceConfig registerSourceWithRuntime(SourceUI source) throws ExecutionSetupException, NamespaceException {
    return registerSourceWithRuntimeInternal(source.asSourceConfig(), createCatalog(), source.getNamespaceAttributes());
  }

  public SourceConfig registerSourceWithRuntime(SourceConfig sourceConfig) throws ExecutionSetupException, NamespaceException {
    return registerSourceWithRuntimeInternal(sourceConfig, createCatalog());
  }

  public SourceConfig registerSourceWithRuntime(SourceConfig sourceConfig, String userName, NamespaceAttribute... attributes) throws NamespaceException {
    return registerSourceWithRuntimeInternal(sourceConfig, createCatalog(userName), attributes);
  }

  private SourceConfig registerSourceWithRuntimeInternal(SourceConfig sourceConfig, SourceCatalog sourceCatalog, NamespaceAttribute... attributes) throws NamespaceException {
    validateConnectionConf(getConnectionConf(sourceConfig));

    if(sourceConfig.getTag() == null) {
      sourceCatalog.createSource(sourceConfig, attributes);
    } else {
      sourceCatalog.updateSource(sourceConfig, attributes);
    }

    final NamespaceKey key = new NamespaceKey(sourceConfig.getName());
    reflectionServiceHelper.getReflectionSettings().setReflectionSettings(key, new AccelerationSettings()
      .setMethod(RefreshMethod.FULL)
      .setRefreshPeriod(sourceConfig.getAccelerationRefreshPeriod())
      .setGracePeriod(sourceConfig.getAccelerationGracePeriod())
      .setNeverExpire(sourceConfig.getAccelerationNeverExpire())
      .setNeverRefresh(sourceConfig.getAccelerationNeverRefresh()));
    return namespaceService.getSource(key);
  }

  /**
   * Solely exists to allow clean-up of some test code.
   */
  @VisibleForTesting
  public void unregisterSourceWithRuntime(SourceName sourceName) {
    final NamespaceKey key = new NamespaceKey(sourceName.getName());
    try {
      SourceConfig config = namespaceService.getSource(key);
      validateConnectionConf(getConnectionConf(config));
      createCatalog().deleteSource(config);
      reflectionServiceHelper.getReflectionSettings().removeSettings(key);
    } catch (NamespaceException e) {
      throw Throwables.propagate(e);
    }
  }

  @WithSpan
  public SourceConfig createSource(SourceConfig sourceConfig, NamespaceAttribute... attributes) throws ExecutionSetupException, NamespaceException, ResourceExistsException {
    validateSourceConfig(sourceConfig);
    validateConnectionConf(getConnectionConf(sourceConfig));

    Preconditions.checkArgument(sourceConfig.getId() == null || Strings.isNullOrEmpty(sourceConfig.getId().getId()), "Source id is immutable.");
    Preconditions.checkArgument(sourceConfig.getTag() == null, "Source tag is immutable.");

    // check if source already exists with the given name.
    if (namespaceService.exists(new SourcePath(new SourceName(sourceConfig.getName())).toNamespaceKey(), SOURCE)) {
      throw new ResourceExistsException(String.format("A source with the name [%s] already exists.", sourceConfig.getName()));
    }

    sourceConfig.setCtime(System.currentTimeMillis());

    return registerSourceWithRuntimeInternal(sourceConfig, createCatalog(), attributes);
  }

  @WithSpan
  public SourceConfig updateSource(String id, SourceConfig sourceConfig, NamespaceAttribute... attributes) throws NamespaceException, SourceNotFoundException {
    validateSourceConfig(sourceConfig);
    validateConnectionConf(getConnectionConf(sourceConfig));

    SourceConfig oldSourceConfig = getById(id);

    Preconditions.checkNotNull(sourceConfig.getId(), "Source id cannot be null.");
    Preconditions.checkArgument(id.equals(sourceConfig.getId().getId()), "Source id is immutable.");
    Preconditions.checkArgument(oldSourceConfig.getName().equals(sourceConfig.getName()), "Source name is immutable.");
    Preconditions.checkArgument(oldSourceConfig.getType().equals(sourceConfig.getType()), "Source type is immutable.");

    return registerSourceWithRuntimeInternal(sourceConfig, createCatalog(), attributes);
  }

  public void deleteSource(SourceConfig sourceConfig) {
    validateSourceConfig(sourceConfig);

    createCatalog().deleteSource(sourceConfig);
    reflectionServiceHelper.getReflectionSettings().removeSettings(new NamespaceKey(sourceConfig.getName()));
  }

  private String formatErrorMsg(String errMsg) {
    return String.format("%s %d.", errMsg, ONE_MINUTE_IN_MS);
  }

  private void validateSourceConfig(SourceConfig sourceConfig) {
    // TODO: move this further down to the namespace or catalog service.  For some reason InputValidation does not work on SourceConfig.
    Preconditions.checkNotNull(sourceConfig);
    Preconditions.checkNotNull(sourceConfig.getName(), "Source name is missing.");
    Preconditions.checkArgument(!sourceConfig.getName().contains("."), "Source names can not contain periods.");
    Preconditions.checkArgument(!sourceConfig.getName().contains("\""), "Source names can not contain double quotes.");
    Preconditions.checkArgument(!sourceConfig.getName().startsWith(HomeName.HOME_PREFIX), "Source names can not start with the '%s' character.", HomeName.HOME_PREFIX);
    // TODO: add more specific numeric limits here, we never want to allow a 0 ms refresh.
    Preconditions.checkNotNull(sourceConfig.getMetadataPolicy(), "Source metadata policy is missing.");
    Preconditions.checkNotNull(sourceConfig.getMetadataPolicy().getAuthTtlMs(), "Source metadata policy values can not be null.");
    Preconditions.checkNotNull(sourceConfig.getMetadataPolicy().getDatasetDefinitionExpireAfterMs(), "Source metadata policy values can not be null.");
    Preconditions.checkNotNull(sourceConfig.getMetadataPolicy().getDatasetDefinitionRefreshAfterMs(), "Source metadata policy values can not be null.");
    Preconditions.checkNotNull(sourceConfig.getMetadataPolicy().getDatasetUpdateMode(), "Source metadata policy values can not be null.");
    Preconditions.checkNotNull(sourceConfig.getMetadataPolicy().getNamesRefreshMs(), "Source metadata policy values can not be null.");

    // Add validations as per definitions in MetadataPolicy class.
    Preconditions.checkArgument(sourceConfig.getMetadataPolicy().getAuthTtlMs() >= ONE_MINUTE_IN_MS,
      formatErrorMsg("Source metadata policy authTTLMs must be greater than or equal to"));
    Preconditions.checkArgument(sourceConfig.getMetadataPolicy().getDatasetDefinitionExpireAfterMs() >= ONE_MINUTE_IN_MS,
      formatErrorMsg("Source metadata policy datasetExpireAfterMs must be greater than or equal to"));
    Preconditions.checkArgument(sourceConfig.getMetadataPolicy().getDatasetDefinitionRefreshAfterMs() >= ONE_MINUTE_IN_MS,
      formatErrorMsg("Source metadata policy datasetRefreshAfterMs must be greater than or equal to"));
    Preconditions.checkArgument(sourceConfig.getMetadataPolicy().getNamesRefreshMs() >= ONE_MINUTE_IN_MS,
      formatErrorMsg("Source metadata policy namesRefreshMs must be greater than or equal to"));
  }

  public void checkSourceExists(SourceName sourceName) throws SourceNotFoundException, NamespaceException {
    try {
      namespaceService.getSource(new SourcePath(sourceName).toNamespaceKey());
    } catch (NamespaceNotFoundException nfe) {
      throw new SourceNotFoundException(sourceName.getName(), nfe);
    }
  }

  protected void addFileToNamespaceTree(NamespaceTree ns, SourceFilePath path, String owner) throws NamespaceNotFoundException {
    final File file = File.newInstance(
      path.toUrlPath(),
      path,
      getUnknownFileFormat(path),
      0, // files should not have any jobs, no need to check
      false,
      false,
      false,
      null
    );
    file.getFileFormat().getFileFormat().setOwner(owner);
    ns.addFile(file);
  }

  protected FileFormat getUnknownFileFormat(SourceFilePath sourceFilePath) {
    final FileConfig config = new FileConfig();
    config.setCtime(System.currentTimeMillis());
    config.setFullPathList(sourceFilePath.toPathList());
    config.setName(sourceFilePath.getFileName().getName());
    config.setType(FileType.UNKNOWN);
    config.setTag(null);
    return FileFormat.getForFile(config);
  }

  protected void addFolderToNamespaceTree(NamespaceTree ns, SourceFolderPath path, FolderConfig folderConfig) throws NamespaceNotFoundException {
    Folder folder = Folder.newInstance(path, folderConfig, null, null, false, true, 0);
    ns.addFolder(folder);
  }

  protected void addFolderTableToNamespaceTree(NamespaceTree ns, SourceFolderPath folderPath, FolderConfig folderConfig, FileFormat fileFormat, boolean isQueryable, int jobCount) throws NamespaceNotFoundException {
    final Folder folder = Folder.newInstance(folderPath, folderConfig, fileFormat, null, isQueryable,  true, jobCount);
    ns.addFolder(folder);
  }

  protected void addTableToNamespaceTree(NamespaceTree ns, PhysicalDatasetResourcePath path, PhysicalDatasetName name,
      PhysicalDatasetConfig datasetConfig, int jobsCount) throws NamespaceNotFoundException {
    ns.addPhysicalDataset(new PhysicalDataset(path, name, datasetConfig, jobsCount, null));
  }

  private void addToNamespaceTree(NamespaceTree ns, List<SchemaEntity> entities, SourceName sourceName, String prefix)
    throws IOException, PhysicalDatasetNotFoundException, NamespaceException {
    for (SchemaEntity entity:  entities) {
      switch (entity.getType()) {
        case SUBSCHEMA:
        case FOLDER:
        {
          SourceFolderPath path = new SourceFolderPath(prefix + "." + entity.getPath());
          FolderConfig folderConfig = new FolderConfig();
          folderConfig.setFullPathList(path.toPathList());
          folderConfig.setName(path.getFolderName().getName());
          folderConfig.setTag("0");
          addFolderToNamespaceTree(ns, path, folderConfig);
        }
        break;

        case TABLE:
        {
          PhysicalDatasetPath path = new PhysicalDatasetPath(prefix + "." + entity.getPath());
          PhysicalDatasetConfig datasetConfig = new PhysicalDatasetConfig();
          datasetConfig.setName(path.getFileName().getName());
          datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
          datasetConfig.setTag("0");
          datasetConfig.setFullPathList(path.toPathList());
          addTableToNamespaceTree(ns,
              new PhysicalDatasetResourcePath(sourceName, path),
              new PhysicalDatasetName(path.getFileName().getName()),
              datasetConfig,
              datasetService.getJobsCount(path.toNamespaceKey(), sabotContext.getOptionManager()));
        }
        break;

        case FILE_TABLE:
        {
          // TODO(Amit H): Should we ignore exceptions from getFilesystemPhysicalDataset?
          // Dataset could be marked as deleted by the time we come here.
          final SourceFilePath filePath = new SourceFilePath(prefix + '.' + entity.getPath());
          final File file = getFileDataset(filePath, entity.getOwner());
          ns.addFile(file);
        }
        break;

        case FOLDER_TABLE: {
          final SourceFolderPath folderPath = new SourceFolderPath(prefix + "." + entity.getPath());

          // TODO(Amit H): Should we ignore exceptions from getFilesystemPhysicalDataset?
          // Dataset could be marked as deleted by the time we come here.
          final PhysicalDatasetConfig physicalDatasetConfig = getFilesystemPhysicalDataset(folderPath);
          final FileConfig fileConfig = physicalDatasetConfig.getFormatSettings();
          fileConfig.setOwner(entity.getOwner());

          final FolderConfig folderConfig = new FolderConfig();
          folderConfig.setId(new EntityId(physicalDatasetConfig.getId()));
          folderConfig.setFullPathList(folderPath.toPathList());
          folderConfig.setName(folderPath.getFolderName().getName());

          // use version from physical dataset.
          folderConfig.setTag(physicalDatasetConfig.getTag());
          fileConfig.setTag(physicalDatasetConfig.getTag());

          addFolderTableToNamespaceTree(ns, folderPath, folderConfig, FileFormat.getForFolder(fileConfig), fileConfig.getType() != FileType.UNKNOWN, datasetService.getJobsCount(folderPath.toNamespaceKey(), sabotContext.getOptionManager()));
        }
        break;

        case FILE:
        {
          final SourceFilePath path = new SourceFilePath(prefix + '.' + entity.getPath());
          addFileToNamespaceTree(ns, path, entity.getOwner());
        }
        break;

        default:
          throw new IllegalArgumentException("Invalid SchemaEntity type " + entity.getType());
      }
    }
  }

  public File getFileDataset(final SourceFilePath filePath, String owner)
      throws PhysicalDatasetNotFoundException, NamespaceException {
    final PhysicalDatasetConfig physicalDatasetConfig = getFilesystemPhysicalDataset(filePath, DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
    final FileConfig fileConfig = physicalDatasetConfig.getFormatSettings();
    fileConfig.setOwner(owner);
    fileConfig.setTag(physicalDatasetConfig.getTag());

    return File.newInstance(physicalDatasetConfig.getId(), filePath, FileFormat.getForFile(fileConfig),
      datasetService.getJobsCount(filePath.toNamespaceKey(), sabotContext.getOptionManager()),
      false, false, fileConfig.getType() != FileType.UNKNOWN, null
    );
  }

  @WithSpan
  public NamespaceTree listSource(
      SourceName sourceName,
      SourceConfig sourceConfig,
      String userName,
      String refType,
      String refValue)
      throws IOException, PhysicalDatasetNotFoundException, NamespaceException {
    try {
      final NamespaceKey sourceKey = new NamespaceKey(sourceName.getName());
      final NamespaceTree namespaceTree;
      final StoragePlugin plugin = checkNotNull(catalogService.getSource(sourceName.getName()), "storage plugin %s not found", sourceName);
      if (plugin instanceof VersionedPlugin) {
        List<ExternalNamespaceEntry> entries = versionedPluginListEntriesHelper(
          (VersionedPlugin) plugin,
          sourceKey,
          refType,
          refValue);
        namespaceTree = namespaceTreeOf(sourceName, entries);
      } else if (plugin instanceof FileSystemPlugin) {
        namespaceTree = new NamespaceTree();
        namespaceTree.setIsFileSystemSource(true);
        namespaceTree.setIsImpersonationEnabled(((FileSystemPlugin<?>) plugin).getConfig().isImpersonationEnabled());
        addToNamespaceTree(namespaceTree, ((FileSystemPlugin<?>) plugin).list(sourceKey.getPathComponents(), userName), sourceName, sourceName.getName());
        fillInTags(namespaceTree);
      } else {
        namespaceTree = newNamespaceTree(namespaceService.list(sourceKey), false, false);
      }

      Span.current().setAttribute(LIST_SOURCE_TOTAL_COUNT_SPAN_ATTRIBUTE_NAME, namespaceTree.totalCount());
      return namespaceTree;
    } catch (IOException | DatasetNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public NamespaceTree listSource(SourceName sourceName, SourceConfig sourceConfig, String userName)
      throws NamespaceException, IOException {
    return listSource(sourceName, sourceConfig, userName, null, null);
  }

  /**
   * Get properties for folder in source.
   * @param sourceName source name
   * @param folderPath folder path
   * @return folder properties
   */
  public Folder getFolder(
      SourceName sourceName,
      SourceFolderPath folderPath,
      boolean includeContents,
      String userName,
      String refType,
      String refValue)
      throws SourceFolderNotFoundException, NamespaceException, PhysicalDatasetNotFoundException, IOException {
    final StoragePlugin plugin = catalogService.getSource(sourceName.getName());
    if(plugin == null) {
      throw new SourceFolderNotFoundException(sourceName, folderPath, null);
    }
    final boolean isFileSystemPlugin = (plugin instanceof FileSystemPlugin);
    Span.current().setAttribute(IS_FILE_SYSTEM_PLUGIN_SPAN_ATTRIBUTE_NAME, isFileSystemPlugin);

    FolderConfig folderConfig;
    if (isFileSystemPlugin) {
      // this could be a physical dataset folder
      DatasetConfig datasetConfig;
      try {
        datasetConfig = namespaceService.getDataset(folderPath.toNamespaceKey());
        if (datasetConfig.getType() != DatasetType.VIRTUAL_DATASET) {
          folderConfig = new FolderConfig()
            .setId(datasetConfig.getId())
            .setFullPathList(folderPath.toPathList())
            .setName(folderPath.getFolderName().getName())
            .setIsPhysicalDataset(true)
            .setTag(datasetConfig.getTag());
        } else {
          throw new SourceFolderNotFoundException(sourceName, folderPath,
            new IllegalArgumentException(folderPath.toString() + " is a virtual dataset"));
        }
      } catch (NamespaceNotFoundException nfe) {
        // folder on filesystem
        folderConfig = new FolderConfig()
          .setFullPathList(folderPath.toPathList())
          .setName(folderPath.getFolderName().getName());
      }
    } else {
      folderConfig = namespaceService.getFolder(folderPath.toNamespaceKey());
    }

    // TODO: why do we need to look up the dataset again in isPhysicalDataset?
    NamespaceTree contents = includeContents ? listFolder(sourceName, folderPath, userName, refType, refValue) : null;
    return newFolder(folderPath, folderConfig, contents, isPhysicalDataset(sourceName, folderPath), isFileSystemPlugin);
  }

  public void deleteFolder(SourceFolderPath folderPath, SourceName sourceName,
                           String refType, String refValue) {
    final StoragePlugin plugin =
      checkNotNull(
        catalogService.getSource(sourceName.getName()),
        "storage plugin %s not found",
        sourceName);
    final boolean isVersionedPlugin = plugin instanceof VersionedPlugin;
    Span.current().setAttribute(IS_VERSIONED_PLUGIN_SPAN_ATTRIBUTE_NAME, isVersionedPlugin);
    if (isVersionedPlugin) {
      final VersionContext version = VersionContextUtils.parse(refType, refValue);
      deleteFolderForVersionedPlugin(folderPath, (VersionedPlugin) plugin, version);
    } else {
      throw new UnsupportedOperationException("Deleting a folder in a source is not supported.");
    }
  }

  public void deleteFolderForVersionedPlugin(SourceFolderPath folderPath, VersionedPlugin plugin, VersionContext version) {
    plugin.deleteFolder(folderPath.toNamespaceKey(), version);
  }

  public Folder createFolder(
      SourceName sourceName,
      SourceFolderPath folderPath,
      String userName,
      String refType,
      String refValue) {
    final StoragePlugin plugin =
      checkNotNull(
        catalogService.getSource(sourceName.getName()),
        "storage plugin %s not found",
        sourceName);
    final boolean isVersionedPlugin = plugin instanceof VersionedPlugin;
    Span.current().setAttribute(IS_VERSIONED_PLUGIN_SPAN_ATTRIBUTE_NAME, isVersionedPlugin);

    if (!isVersionedPlugin) {
      throw new UnsupportedOperationException("Creating folder is not supported");
    }

    final VersionContext version = getVersionContext(refType, refValue);
    FolderConfig folderConfig = getFolderConfig(folderPath);

    try {
      ((VersionedPlugin) plugin).createNamespace(folderPath.toNamespaceKey(), version);
      return Folder.newInstance(
        sourceName,
        folderConfig,
        null);
    } catch (NessieNamespaceAlreadyExistsException e) {
      throw UserException.validationError(e)
        .message(
          "Nessie namespace %s already exists on source %s.",
          folderPath.getPathWithoutRoot().toPathString(), sourceName.getName())
        .buildSilently();
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
        .message("Requested %s not found on source %s.", version, sourceName.getName())
        .buildSilently();
    } catch (NoDefaultBranchException e) {
      throw UserException.validationError(e)
        .message(
          "Unable to resolve source version. Version was not specified and Source %s does not"
            + " have a default branch set.",
          sourceName.getName())
        .buildSilently();
    } catch (ReferenceTypeConflictException e) {
      throw UserException.validationError(e)
        .message(
          "Requested %s in source %s is not the requested type.", version, sourceName.getName())
        .buildSilently();
    }
  }

  protected Folder newFolder(SourceFolderPath folderPath, FolderConfig folderConfig, NamespaceTree contents, boolean isQueryable, boolean isFileSystemPlugin)
      throws NamespaceNotFoundException {
    // TODO: why do we need to look up the dataset again in isPhysicalDataset?
    return Folder.newInstance(folderPath, folderConfig, null, contents, isQueryable, isFileSystemPlugin, 0);
  }

  protected NamespaceTree newNamespaceTree(List<NameSpaceContainer> children, boolean isFileSystemSource, boolean isImpersonationEnabled) throws DatasetNotFoundException, NamespaceException {
    return NamespaceTree.newInstance(datasetService, children, SOURCE, collaborationService, isFileSystemSource, isImpersonationEnabled, null);
  }

  public NamespaceTree listFolder(
      SourceName sourceName,
      SourceFolderPath folderPath,
      String userName,
      String refType,
      String refValue)
    throws IOException, PhysicalDatasetNotFoundException, NamespaceException {
    final String name = sourceName.getName();
    final String prefix = folderPath.toPathString();
    try {
      final StoragePlugin plugin = checkNotNull(catalogService.getSource(name), "storage plugin %s not found", sourceName);
      if (plugin instanceof VersionedPlugin) {
        final NamespaceKey folderKey = folderPath.toNamespaceKey();
        List<ExternalNamespaceEntry> entries = versionedPluginListEntriesHelper(
          (VersionedPlugin) plugin,
          folderKey,
          refType,
          refValue);

        return namespaceTreeOf(sourceName, entries);
      } else if (plugin instanceof FileSystemPlugin) {
        final NamespaceTree ns = new NamespaceTree();
        ns.setIsFileSystemSource(true);
        ns.setIsImpersonationEnabled(((FileSystemPlugin<?>) plugin).getConfig().isImpersonationEnabled());
        addToNamespaceTree(ns, ((FileSystemPlugin) plugin).list(folderPath.toPathList(), userName), sourceName, prefix);

        fillInTags(ns);

        return ns;
      } else {
        return newNamespaceTree(namespaceService.list(folderPath.toNamespaceKey()), false, false);
      }
    } catch (IOException | DatasetNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public NamespaceTree listFolder(SourceName sourceName, SourceFolderPath folderPath, String userName)
      throws NamespaceException, IOException {
    return listFolder(sourceName, folderPath, userName, null, null);
  }

  @WithSpan
  public List<ResourceTreeEntity> listPath(
      NamespaceKey path,
      boolean showDatasets,
      String refType,
      String refValue)
      throws NamespaceException, UnsupportedEncodingException {
    final List<ResourceTreeEntity> resources = Lists.newArrayList();
    final String sourceName = path.getRoot();
    final StoragePlugin plugin =
      checkNotNull(
        catalogService.getSource(sourceName),
        "storage plugin %s not found",
        sourceName);
    final boolean isVersionedPlugin = plugin instanceof VersionedPlugin;
    Span.current().setAttribute(IS_VERSIONED_PLUGIN_SPAN_ATTRIBUTE_NAME, isVersionedPlugin);

    if (isVersionedPlugin) {
      List<ExternalNamespaceEntry> entries = versionedPluginListEntriesHelper(
        (VersionedPlugin) plugin,
        path,
        refType,
        refValue);

      return generateResourceTreeEntityList(path, entries);
    }

    for (NameSpaceContainer container : namespaceService.list(path)) {
      if (container.getType() == Type.FOLDER) {
        resources.add(new ResourceTreeEntity(container.getFolder()));
      } else if (showDatasets && container.getType() == Type.DATASET) {
        resources.add(new ResourceTreeEntity(container.getDataset()));
      }
    }

    return resources;
  }

  protected VersionContext getVersionContext(String refType, String refValue) {
    return VersionContextUtils.parse(refType, refValue);
  }

  protected FolderConfig getFolderConfig(SourceFolderPath folderPath) {
    return new FolderConfig()
      .setFullPathList(folderPath.toPathList())
      .setName(folderPath.getFolderName().getName());
  }

  protected List<ExternalNamespaceEntry> versionedPluginListEntriesHelper(
    VersionedPlugin plugin,
    NamespaceKey namespaceKey,
    String refType,
    String refValue) {
    VersionContext version = VersionContextUtils.parse(refType, refValue);
    String sourceName = namespaceKey.getRoot();
    try {
      return plugin.listEntries(
          namespaceKey.getPathWithoutRoot(),
          version)
        .collect(Collectors.toList());
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
        .message("Requested %s not found on source %s.", version, sourceName)
        .buildSilently();
    } catch (NoDefaultBranchException e) {
      throw UserException.validationError(e)
        .message("Unable to resolve source version. Version was not specified and Source %s does not have a default branch set.",
          sourceName)
        .buildSilently();
    } catch (ReferenceTypeConflictException e) {
      throw UserException.validationError(e)
        .message("Requested %s in source %s is not the requested type.", version, sourceName)
        .buildSilently();
    }
  }

  // Process all items in the namespacetree and get their tags in one go
  private void fillInTags(NamespaceTree ns) {
    List<File> files = ns.getFiles();
    TagsSearchResult tagsInfo = collaborationService.getTagsForIds(files.stream().map(File::getId).
      collect(Collectors.toSet()));
    Map<String, CollaborationTag> tags = tagsInfo.getTags();

    //we populate tags not for all files
    ns.setCanTagsBeSkipped(tagsInfo.getCanTagsBeSkipped());

    for (File input : files) {
      CollaborationTag collaborationTag = tags.get(input.getId());
      if (collaborationTag != null) {
        input.setTags(collaborationTag.getTagsList());
      }
    }
  }

  @Deprecated
  public FileFormat getDefaultFileFormat(SourceName sourceName, SourceFilePath sourceFilePath) {
    final FileConfig config = new FileConfig();
    config.setCtime(System.currentTimeMillis());
    config.setFullPathList(sourceFilePath.toPathList());
    config.setName(sourceFilePath.getFileName().getName());
    config.setType(FileFormat.getFileFormatType(singletonList(FilenameUtils.getExtension(config.getName()))));
    config.setTag(null);
    return FileFormat.getForFile(config);
  }

  /**
   * Get default file format for a directory in filesystem
   * @param sourceName name of source
   * @param sourceFolderPath path to directory
   * @param user user name
   * @return {@code FileFormat} format settings
   * @throws IOException on filesystem related errors
   * @throws NamespaceException on invalid namespace operation
   * @throws PhysicalDatasetNotFoundException if file/folder is marked as physical dataset but is missing from namespace
   */
  @Deprecated
  public FileFormat getDefaultFileFormat(SourceName sourceName, SourceFolderPath sourceFolderPath, String user)
    throws IOException, NamespaceException, PhysicalDatasetNotFoundException {
    final FileConfig config = new FileConfig();
    config.setCtime(System.currentTimeMillis());
    config.setFullPathList(sourceFolderPath.toPathList());
    config.setName(sourceFolderPath.getFolderName().getName());
    NamespaceTree ns = listFolder(sourceName, sourceFolderPath, user);
    if (!ns.getFiles().isEmpty()) {
      config.setType(FileFormat.getFileFormatType(singletonList(FilenameUtils.getExtension(ns.getFiles().get(0).getName()))));
    } else {
      config.setType(FileType.UNKNOWN);
    }
    config.setTag(null);
    return FileFormat.getForFolder(config);
  }


  /** A file or folder in source could be defined as a physical dataset.
   * Store physical dataset properties in namespace.
   */
  public void createPhysicalDataset(SourceFilePath filePath, PhysicalDatasetConfig datasetConfig)
      throws NamespaceException {
    createCatalog().createOrUpdateDataset(
      namespaceService,
      new NamespaceKey(filePath.getSourceName().getName()),
      new PhysicalDatasetPath(filePath).toNamespaceKey(),
      toDatasetConfig(datasetConfig, security.getUserPrincipal().getName()));
  }

  public void createPhysicalDataset(SourceFolderPath folderPath, PhysicalDatasetConfig datasetConfig)
      throws NamespaceException {
    createCatalog().createOrUpdateDataset(
      namespaceService,
      new NamespaceKey(folderPath.getSourceName().getName()),
      new PhysicalDatasetPath(folderPath).toNamespaceKey(),
      toDatasetConfig(datasetConfig, security.getUserPrincipal().getName()));
  }

  public PhysicalDatasetConfig getFilesystemPhysicalDataset(NamespacePath path, DatasetType type) throws NamespaceException {
    try {
      return toPhysicalDatasetConfig(namespaceService.getDataset(path.toNamespaceKey()));
    } catch (NamespaceNotFoundException nse) {
      throw new PhysicalDatasetNotFoundException(path, type, nse);
    }
  }

  public PhysicalDatasetConfig getFilesystemPhysicalDataset(SourceFolderPath path) throws NamespaceException {
    return getFilesystemPhysicalDataset(path, DatasetType.PHYSICAL_DATASET_HOME_FOLDER);
  }

  public PhysicalDatasetConfig getFilesystemPhysicalDataset(SourceFilePath path) throws NamespaceException {
    return getFilesystemPhysicalDataset(path, DatasetType.PHYSICAL_DATASET_HOME_FILE);
  }


  // For all tables including filesystem tables.
  // Physical datasets may be missing
  public PhysicalDataset getPhysicalDataset(PhysicalDatasetPath physicalDatasetPath) throws NamespaceException {
    final int jobsCount =  datasetService.getJobsCount(physicalDatasetPath.toNamespaceKey());
    try {
      final DatasetConfig datasetConfig = namespaceService.getDataset(physicalDatasetPath.toNamespaceKey());
      return newPhysicalDataset(
        new PhysicalDatasetResourcePath(physicalDatasetPath.getSourceName(), physicalDatasetPath),
        physicalDatasetPath.getDatasetName(),
        toPhysicalDatasetConfig(datasetConfig),
        jobsCount);
    } catch (NamespaceNotFoundException nse) {
      return newPhysicalDataset(
        new PhysicalDatasetResourcePath(physicalDatasetPath.getSourceName(), physicalDatasetPath),
        physicalDatasetPath.getDatasetName(),
        new PhysicalDatasetConfig()
          .setName(physicalDatasetPath.getLeaf().getName())
          .setType(DatasetType.PHYSICAL_DATASET)
          .setTag("0")
          .setFullPathList(physicalDatasetPath.toPathList()),
        jobsCount);
    }
  }

  protected PhysicalDataset newPhysicalDataset(PhysicalDatasetResourcePath resourcePath,
      PhysicalDatasetName datasetName, PhysicalDatasetConfig datasetConfig, Integer jobsCount) throws NamespaceNotFoundException {
    return new PhysicalDataset(
      resourcePath,
      datasetName,
      datasetConfig,
      jobsCount,
      null
    );
  }

  public boolean isPhysicalDataset(SourceName sourceName, SourceFolderPath folderPath) {
    try {
      DatasetConfig ds = namespaceService.getDataset(new PhysicalDatasetPath(folderPath).toNamespaceKey());
      return DatasetHelper.isPhysicalDataset(ds.getType());
    } catch (NamespaceException nse) {
      logger.debug("Error while checking physical dataset in source {} for folder {}, error {}",
        sourceName.getName(), folderPath.toPathString(), nse.toString());
      return false;
    }
  }

  public void deletePhysicalDataset(SourceName sourceName, PhysicalDatasetPath datasetPath, String version ,NamespaceService.DeleteCallback deleteCallback) throws PhysicalDatasetNotFoundException {
    try {
      DatasetConfig datasetConfig = namespaceService.getDataset(datasetPath.toNamespaceKey());
      deleteCallback.onDatasetDelete(datasetConfig);
      namespaceService.deleteDataset(datasetPath.toNamespaceKey(), version);
    } catch (NamespaceException nse) {
      throw new PhysicalDatasetNotFoundException(sourceName, datasetPath, nse);
    }
  }

  public SourceState getSourceState(String sourceName) {
    return catalogService.getSourceState(sourceName);
  }

  @VisibleForTesting
  public StoragePlugin getStoragePlugin(String sourceName) throws SourceNotFoundException, ExecutionSetupException {
    StoragePlugin plugin = catalogService.getSource(sourceName);
    if (plugin == null) {
      throw new SourceNotFoundException(sourceName);
    }
    return plugin;
  }

  @WithSpan
  public List<SourceConfig> getSources() {
    final List<SourceConfig> sources = new ArrayList<>();

    for (SourceConfig sourceConfig : namespaceService.getSources()) {
      if (SourceUI.isInternal(sourceConfig, connectionReader)) {
        continue;
      }

      sources.add(sourceConfig);
    }

    return sources;
  }

  public SourceConfig getById(String id) throws SourceNotFoundException, NamespaceException {
    try {
      return namespaceService.getSourceById(id);
    } catch (NamespaceNotFoundException e) {
      throw new SourceNotFoundException(id);
    }
  }

  public Source fromSourceConfig(SourceConfig sourceConfig) {
    return fromSourceConfig(sourceConfig, null);
  }

  public Source fromSourceConfig(SourceConfig sourceConfig, List<CatalogItem> children) {
    final AccelerationSettings settings = reflectionServiceHelper.getReflectionSettings().getReflectionSettings(new NamespaceKey(sourceConfig.getName()));
    Source source = new Source(sourceConfig, settings, getConnectionReader(), children);

    SourceState state = getStateForSource(sourceConfig);
    source.setState(state);

    return source;
  }

  public SourceState getStateForSource(SourceConfig sourceConfig) {
    return getSourceState(sourceConfig.getName());
  }

  public boolean isSourceConfigMetadataImpacting(SourceConfig sourceConfig) {
    return catalogService.isSourceConfigMetadataImpacting(sourceConfig);
  }

  /**
   * This method is public such that certain (test) code paths can mock this method to ignore this
   * validation.
   */
  @VisibleForTesting
  public void validateConnectionConf(ConnectionConf<?, ?> connectionConf) {
    if (connectionConf.isInternal()) {
      throw UserExceptionMapper.withStatus(UserException.unsupportedError(), Response.Status.BAD_REQUEST)
        .message("Source with connection type %s cannot be created nor modified.", connectionConf.getType())
        .buildSilently();
    }
  }
}

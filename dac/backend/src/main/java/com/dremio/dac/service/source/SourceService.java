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

import com.dremio.catalog.exception.CatalogEntityAlreadyExistsException;
import com.dremio.catalog.exception.CatalogEntityForbiddenException;
import com.dremio.catalog.exception.CatalogEntityNotFoundException;
import com.dremio.catalog.exception.CatalogException;
import com.dremio.catalog.exception.CatalogFolderNotEmptyException;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.CatalogFolder;
import com.dremio.catalog.model.ImmutableCatalogFolder;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.api.Source;
import com.dremio.dac.explore.model.VersionContextUtils;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.folder.FolderModel;
import com.dremio.dac.model.folder.FolderPath;
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
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.collaboration.TagsSearchResult;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.PhysicalDatasetNotFoundException;
import com.dremio.dac.service.errors.ResourceExistsException;
import com.dremio.dac.service.errors.ResourceForbiddenException;
import com.dremio.dac.service.errors.SourceFolderNotFoundException;
import com.dremio.dac.service.errors.SourceNotFoundException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.ImmutableVersionedListOptions;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.catalog.SourceRefreshOption;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SchemaEntity;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.file.File;
import com.dremio.file.SourceFilePath;
import com.dremio.options.OptionManager;
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
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.physicaldataset.proto.PhysicalDatasetConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.SourceNamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

/** Source service. */
public class SourceService {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(SourceService.class);
  public static final String LIST_SOURCE_TOTAL_COUNT_SPAN_ATTRIBUTE_NAME =
      "dremio.source_service.list_source_total_count";
  public static final String IS_VERSIONED_PLUGIN_SPAN_ATTRIBUTE_NAME =
      "dremio.source_service.isVersionedPlugin";
  public static final String IS_FILE_SYSTEM_PLUGIN_SPAN_ATTRIBUTE_NAME =
      "dremio.source_service.isFileSystemPlugin";
  public static final int NO_PAGINATION_MAX_RESULTS = Integer.MAX_VALUE;

  private final Clock clock;
  private final OptionManager optionManager;
  private final NamespaceService namespaceService;
  private final DatasetVersionMutator datasetService;
  private final CatalogService catalogService;
  private final ReflectionServiceHelper reflectionServiceHelper;
  private final ConnectionReader connectionReader;
  private final SecurityContext security;
  private final CollaborationHelper collaborationService;

  @Inject
  public SourceService(
      Clock clock,
      OptionManager optionManager,
      NamespaceService namespaceService,
      DatasetVersionMutator datasetService,
      CatalogService catalogService,
      ReflectionServiceHelper reflectionHelper,
      CollaborationHelper collaborationService,
      ConnectionReader connectionReader,
      SecurityContext security) {
    this.clock = clock;
    this.optionManager = optionManager;
    this.namespaceService = namespaceService;
    this.datasetService = datasetService;
    this.catalogService = catalogService;
    this.reflectionServiceHelper = reflectionHelper;
    this.connectionReader = connectionReader;
    this.security = security;
    this.collaborationService = collaborationService;
  }

  protected Catalog createCatalog() {
    return createCatalog(null);
  }

  Catalog createCatalog(String userName) {
    return catalogService.getCatalog(
        MetadataRequestOptions.of(
            SchemaConfig.newBuilder(
                    StringUtils.isBlank(userName)
                        ? CatalogUser.from(security.getUserPrincipal().getName())
                        : CatalogUser.from(userName))
                .build()));
  }

  public SourceConfig registerSourceWithRuntime(
      SourceUI source, SourceRefreshOption sourceRefreshOption)
      throws ExecutionSetupException, NamespaceException {
    return registerSourceWithRuntimeInternal(
        source.asSourceConfig(),
        createCatalog(),
        sourceRefreshOption,
        source.getNamespaceAttributes());
  }

  public SourceConfig registerSourceWithRuntime(
      SourceConfig sourceConfig, SourceRefreshOption sourceRefreshOption)
      throws ExecutionSetupException, NamespaceException {
    return registerSourceWithRuntimeInternal(sourceConfig, createCatalog(), sourceRefreshOption);
  }

  public SourceConfig registerSourceWithRuntime(
      SourceConfig sourceConfig,
      String userName,
      SourceRefreshOption sourceRefreshOption,
      NamespaceAttribute... attributes)
      throws NamespaceException {
    return registerSourceWithRuntimeInternal(
        sourceConfig, createCatalog(userName), sourceRefreshOption, attributes);
  }

  private SourceConfig registerSourceWithRuntimeInternal(
      SourceConfig sourceConfig,
      SourceCatalog sourceCatalog,
      SourceRefreshOption sourceRefreshOption,
      NamespaceAttribute... attributes)
      throws NamespaceException {
    validateConnectionConf(connectionReader.getConnectionConf(sourceConfig));

    if (sourceConfig.getTag() == null) {
      sourceCatalog.createSource(sourceConfig, sourceRefreshOption, attributes);
    } else {
      sourceCatalog.updateSource(sourceConfig, sourceRefreshOption, attributes);
    }

    final NamespaceKey key = new NamespaceKey(sourceConfig.getName());
    reflectionServiceHelper
        .getReflectionSettings()
        .setReflectionSettings(key, reflectionServiceHelper.fromSourceConfig(sourceConfig));
    return namespaceService.getSource(key);
  }

  /** Solely exists to allow clean-up of some test code. */
  @VisibleForTesting
  public void unregisterSourceWithRuntime(SourceName sourceName) {
    final NamespaceKey key = new NamespaceKey(sourceName.getName());
    try {
      SourceConfig config = namespaceService.getSource(key);
      validateConnectionConf(connectionReader.getConnectionConf(config));
      createCatalog().deleteSource(config, SourceRefreshOption.WAIT_FOR_DATASETS_CREATION);
      reflectionServiceHelper.getReflectionSettings().removeSettings(key);
    } catch (NamespaceException e) {
      throw Throwables.propagate(e);
    }
  }

  @WithSpan
  public SourceConfig createSource(
      SourceConfig sourceConfig,
      SourceRefreshOption sourceRefreshOption,
      NamespaceAttribute... attributes)
      throws ExecutionSetupException, NamespaceException, ResourceExistsException {
    validateSourceConfig(sourceConfig);
    validateConnectionConf(connectionReader.getConnectionConf(sourceConfig));

    Preconditions.checkArgument(
        sourceConfig.getId() == null || Strings.isNullOrEmpty(sourceConfig.getId().getId()),
        "Source id is immutable.");
    Preconditions.checkArgument(sourceConfig.getTag() == null, "Source tag is immutable.");

    // check if source already exists with the given name.
    if (namespaceService.exists(
        new SourcePath(new SourceName(sourceConfig.getName())).toNamespaceKey(), SOURCE)) {
      throw new ResourceExistsException(
          String.format("A source with the name [%s] already exists.", sourceConfig.getName()));
    }

    return registerSourceWithRuntimeInternal(
        sourceConfig, createCatalog(), sourceRefreshOption, attributes);
  }

  @WithSpan
  public SourceConfig updateSource(
      String id, SourceConfig sourceConfig, NamespaceAttribute... attributes)
      throws NamespaceException, SourceNotFoundException {
    validateSourceConfig(sourceConfig);
    validateConnectionConf(connectionReader.getConnectionConf(sourceConfig));

    SourceConfig oldSourceConfig = getById(id);

    Preconditions.checkNotNull(sourceConfig.getId(), "Source id cannot be null.");
    Preconditions.checkArgument(id.equals(sourceConfig.getId().getId()), "Source id is immutable.");
    Preconditions.checkArgument(
        oldSourceConfig.getName().equals(sourceConfig.getName()), "Source name is immutable.");
    Preconditions.checkArgument(
        oldSourceConfig.getType().equals(sourceConfig.getType()), "Source type is immutable.");
    Preconditions.checkArgument(
        sourceConfig.getCtime() == null
            || sourceConfig.getCtime() == 0L
            || sourceConfig.getCtime().equals(oldSourceConfig.getCtime()),
        "Creation time is immutable.");

    return registerSourceWithRuntimeInternal(
        sourceConfig,
        createCatalog(),
        SourceRefreshOption.BACKGROUND_DATASETS_CREATION,
        attributes);
  }

  public void deleteSource(SourceConfig sourceConfig, SourceRefreshOption sourceRefreshOption) {
    validateSourceConfig(sourceConfig);
    createCatalog().deleteSource(sourceConfig, sourceRefreshOption);
    reflectionServiceHelper
        .getReflectionSettings()
        .removeSettings(new NamespaceKey(sourceConfig.getName()));
  }

  private String formatErrorMsg(String errMsg) {
    return String.format("%s %d.", errMsg, ONE_MINUTE_IN_MS);
  }

  private void validateSourceConfig(SourceConfig sourceConfig) {
    // TODO: move this further down to the namespace or catalog service.  For some reason
    // InputValidation does not work on SourceConfig.
    Preconditions.checkNotNull(sourceConfig);
    Preconditions.checkNotNull(sourceConfig.getName(), "Source name is missing.");
    // TODO: add more specific numeric limits here, we never want to allow a 0 ms refresh.
    Preconditions.checkNotNull(
        sourceConfig.getMetadataPolicy(), "Source metadata policy is missing.");
    Preconditions.checkNotNull(
        sourceConfig.getMetadataPolicy().getAuthTtlMs(),
        "Source metadata policy values can not be null.");
    Preconditions.checkNotNull(
        sourceConfig.getMetadataPolicy().getDatasetDefinitionExpireAfterMs(),
        "Source metadata policy values can not be null.");
    Preconditions.checkNotNull(
        sourceConfig.getMetadataPolicy().getDatasetDefinitionRefreshAfterMs(),
        "Source metadata policy values can not be null.");
    Preconditions.checkNotNull(
        sourceConfig.getMetadataPolicy().getDatasetUpdateMode(),
        "Source metadata policy values can not be null.");
    Preconditions.checkNotNull(
        sourceConfig.getMetadataPolicy().getNamesRefreshMs(),
        "Source metadata policy values can not be null.");

    // Add validations as per definitions in MetadataPolicy class.
    Preconditions.checkArgument(
        sourceConfig.getMetadataPolicy().getAuthTtlMs() >= ONE_MINUTE_IN_MS,
        formatErrorMsg("Source metadata policy authTTLMs must be greater than or equal to"));
    Preconditions.checkArgument(
        sourceConfig.getMetadataPolicy().getDatasetDefinitionExpireAfterMs() >= ONE_MINUTE_IN_MS,
        formatErrorMsg(
            "Source metadata policy datasetExpireAfterMs must be greater than or equal to"));
    Preconditions.checkArgument(
        sourceConfig.getMetadataPolicy().getDatasetDefinitionRefreshAfterMs() >= ONE_MINUTE_IN_MS,
        formatErrorMsg(
            "Source metadata policy datasetRefreshAfterMs must be greater than or equal to"));
    Preconditions.checkArgument(
        sourceConfig.getMetadataPolicy().getNamesRefreshMs() >= ONE_MINUTE_IN_MS,
        formatErrorMsg("Source metadata policy namesRefreshMs must be greater than or equal to"));
  }

  public void checkSourceExists(SourceName sourceName)
      throws SourceNotFoundException, NamespaceException {
    try {
      namespaceService.getSource(new SourcePath(sourceName).toNamespaceKey());
    } catch (NamespaceNotFoundException nfe) {
      throw new SourceNotFoundException(sourceName.getName(), nfe);
    }
  }

  protected void addFileToNamespaceTree(NamespaceTree ns, SourceFilePath path, String owner)
      throws NamespaceNotFoundException {
    final File file =
        File.newInstance(
            path.toUrlPath(),
            path,
            getUnknownFileFormat(path),
            0, // files should not have any jobs, no need to check
            false,
            false,
            false,
            null);
    file.getFileFormat().getFileFormat().setOwner(owner);
    ns.addFile(file);
  }

  protected FileFormat getUnknownFileFormat(SourceFilePath sourceFilePath) {
    final FileConfig config = new FileConfig();
    config.setCtime(clock.millis());
    config.setFullPathList(sourceFilePath.toPathList());
    config.setName(sourceFilePath.getFileName().getName());
    config.setType(FileType.UNKNOWN);
    config.setTag(null);
    return FileFormat.getForFile(config);
  }

  protected void addFolderToNamespaceTreeForFileSystemPlugin(
      NamespaceTree ns, SourceFolderPath path, FolderConfig folderConfig)
      throws NamespaceNotFoundException {
    String id = folderConfig.getId() == null ? path.toUrlPath() : folderConfig.getId().getId();
    FolderModel folder =
        new FolderModel(
            id,
            folderConfig.getName(),
            path.toUrlPath(),
            folderConfig.getIsPhysicalDataset(),
            true,
            false,
            folderConfig.getExtendedConfig(),
            folderConfig.getTag(),
            null,
            null,
            null,
            0,
            folderConfig.getStorageUri(),
            folderConfig.getTag());
    ns.addFolder(folder);
  }

  protected void addFolderTableToNamespaceTreeForFileSystemPlugin(
      NamespaceTree ns,
      SourceFolderPath folderPath,
      FolderConfig folderConfig,
      FileFormat fileFormat,
      boolean isQueryable,
      int jobCount)
      throws NamespaceNotFoundException {
    String id =
        folderConfig.getId() == null ? folderPath.toUrlPath() : folderConfig.getId().getId();
    FolderModel folder =
        new FolderModel(
            id,
            folderConfig.getName(),
            folderPath.toUrlPath(),
            folderConfig.getIsPhysicalDataset(),
            true,
            isQueryable,
            folderConfig.getExtendedConfig(),
            folderConfig.getTag(),
            fileFormat,
            null,
            null,
            jobCount,
            folderConfig.getStorageUri(),
            folderConfig.getTag());
    ns.addFolder(folder);
  }

  protected void addTableToNamespaceTree(
      NamespaceTree ns,
      PhysicalDatasetResourcePath path,
      PhysicalDatasetName name,
      PhysicalDatasetConfig datasetConfig,
      int jobsCount)
      throws NamespaceNotFoundException {
    ns.addPhysicalDataset(new PhysicalDataset(path, name, datasetConfig, jobsCount, null));
  }

  private void addToNamespaceTreeForFileSystemPlugin(
      NamespaceTree ns, List<SchemaEntity> entities, String prefix)
      throws IOException, PhysicalDatasetNotFoundException, NamespaceException {
    for (SchemaEntity entity : entities) {
      switch (entity.getType()) {
        case SUBSCHEMA:
        case FOLDER:
          {
            SourceFolderPath path = new SourceFolderPath(prefix + "." + entity.getPath());
            FolderConfig folderConfig = new FolderConfig();
            folderConfig.setFullPathList(path.toPathList());
            folderConfig.setName(path.getFolderName().getName());
            folderConfig.setTag("0");
            addFolderToNamespaceTreeForFileSystemPlugin(ns, path, folderConfig);
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
            addTableToNamespaceTree(
                ns,
                new PhysicalDatasetResourcePath(path),
                new PhysicalDatasetName(path.getFileName().getName()),
                datasetConfig,
                datasetService.getJobsCount(path.toNamespaceKey(), optionManager));
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

        case FOLDER_TABLE:
          {
            final SourceFolderPath folderPath =
                new SourceFolderPath(prefix + "." + entity.getPath());

            // TODO(Amit H): Should we ignore exceptions from getFilesystemPhysicalDataset?
            // Dataset could be marked as deleted by the time we come here.
            // TODO: DX-101924 - Investigate why PHYSICAL_DATASET_HOME_FOLDER is being used here.
            final PhysicalDatasetConfig physicalDatasetConfig =
                getFilesystemPhysicalDataset(folderPath, DatasetType.PHYSICAL_DATASET_HOME_FOLDER);
            final FileConfig fileConfig = physicalDatasetConfig.getFormatSettings();
            fileConfig.setOwner(entity.getOwner());

            final FolderConfig folderConfig = new FolderConfig();
            folderConfig.setId(new EntityId(physicalDatasetConfig.getId()));
            folderConfig.setFullPathList(folderPath.toPathList());
            folderConfig.setName(folderPath.getFolderName().getName());

            // use version from physical dataset.
            folderConfig.setTag(physicalDatasetConfig.getTag());
            fileConfig.setTag(physicalDatasetConfig.getTag());

            addFolderTableToNamespaceTreeForFileSystemPlugin(
                ns,
                folderPath,
                folderConfig,
                FileFormat.getForFolder(fileConfig),
                fileConfig.getType() != FileType.UNKNOWN,
                datasetService.getJobsCount(folderPath.toNamespaceKey(), optionManager));
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
    final PhysicalDatasetConfig physicalDatasetConfig =
        getFilesystemPhysicalDataset(filePath, DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
    final FileConfig fileConfig = physicalDatasetConfig.getFormatSettings();
    fileConfig.setOwner(owner);
    fileConfig.setTag(physicalDatasetConfig.getTag());

    return File.newInstance(
        physicalDatasetConfig.getId(),
        filePath,
        FileFormat.getForFile(fileConfig),
        datasetService.getJobsCount(filePath.toNamespaceKey(), optionManager),
        false,
        false,
        fileConfig.getType() != FileType.UNKNOWN,
        null);
  }

  @WithSpan
  public NamespaceTree listSource(
      SourceName sourceName,
      SourceConfig sourceConfig,
      String userName,
      String refType,
      String refValue,
      @Nullable String pageToken,
      int maxResults,
      boolean includeUDFChildren)
      throws PhysicalDatasetNotFoundException, NamespaceException {
    try {
      final NamespaceKey sourceKey = new NamespaceKey(sourceName.getName());
      final NamespaceTree namespaceTree;
      final StoragePlugin plugin =
          checkNotNull(
              catalogService.getSource(sourceName.getName()),
              "storage plugin %s not found",
              sourceName);
      checkPluginState(sourceName.getName());
      if (plugin.isWrapperFor(VersionedPlugin.class)) {
        ExternalListResponse response =
            versionedPluginListEntriesHelper(
                plugin.unwrap(VersionedPlugin.class),
                sourceKey,
                refType,
                refValue,
                pageToken,
                maxResults);
        namespaceTree = namespaceTreeOf(sourceName, response, includeUDFChildren);
      } else if (plugin instanceof FileSystemPlugin) {
        // TODO: limit maximum number of files to get.
        namespaceTree = new NamespaceTree();
        namespaceTree.setIsFileSystemSource(true);
        namespaceTree.setIsImpersonationEnabled(
            ((FileSystemPlugin<?>) plugin).getConfig().isImpersonationEnabled());
        addToNamespaceTreeForFileSystemPlugin(
            namespaceTree,
            ((FileSystemPlugin<?>) plugin).list(sourceKey.getPathComponents(), userName),
            sourceName.getName());
        fillInTags(namespaceTree);
      } else {
        namespaceTree =
            newNamespaceTree(namespaceService.list(sourceKey, pageToken, maxResults), false, false);
      }

      Span.current()
          .setAttribute(LIST_SOURCE_TOTAL_COUNT_SPAN_ATTRIBUTE_NAME, namespaceTree.totalCount());
      return namespaceTree;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (DatasetNotFoundException e) {
      throw new NotFoundException(e);
    }
  }

  /**
   * Get properties for folder in source.
   *
   * @param sourceName source name
   * @param folderPath folder path
   * @return folder properties
   */
  public FolderModel getFolder(
      SourceName sourceName,
      SourceFolderPath folderPath,
      boolean includeContents,
      boolean includeUDFChildren,
      String userName,
      String refType,
      String refValue)
      throws SourceFolderNotFoundException,
          NamespaceException,
          PhysicalDatasetNotFoundException,
          IOException {
    try {
      final StoragePlugin plugin = catalogService.getSource(sourceName.getName());
      if (plugin == null) {
        throw new SourceFolderNotFoundException(sourceName, folderPath, null);
      }
      final boolean isFileSystemPlugin = (plugin instanceof FileSystemPlugin);
      Span.current().setAttribute(IS_FILE_SYSTEM_PLUGIN_SPAN_ATTRIBUTE_NAME, isFileSystemPlugin);

      NamespaceTree contents =
          includeContents
              ? listFolder(
                  sourceName,
                  folderPath,
                  userName,
                  refType,
                  refValue,
                  null,
                  Integer.MAX_VALUE,
                  includeUDFChildren)
              : null;

      return newFolder(
          folderPath,
          getFolderConfig(sourceName, folderPath, refType, refValue),
          contents,
          isPhysicalDataset(sourceName, folderPath),
          isFileSystemPlugin,
          plugin,
          refType,
          refValue);
    } catch (NamespaceNotFoundException e) {
      String errorMessage =
          Strings.isNullOrEmpty(e.getMessage())
              ? String.format("Folder [%s] not found", folderPath)
              : e.getMessage();
      throw new NotFoundException(errorMessage, e);
    }
  }

  private FolderConfig getFolderConfig(
      SourceName sourceName, SourceFolderPath folderPath, String refType, String refValue)
      throws SourceFolderNotFoundException, NamespaceException, PhysicalDatasetNotFoundException {
    final StoragePlugin plugin = catalogService.getSource(sourceName.getName());
    if (plugin == null) {
      throw new SourceFolderNotFoundException(sourceName, folderPath, null);
    }
    final boolean isFileSystemPlugin = plugin instanceof FileSystemPlugin;
    Span.current().setAttribute(IS_FILE_SYSTEM_PLUGIN_SPAN_ATTRIBUTE_NAME, isFileSystemPlugin);
    FolderConfig folderConfig;
    if (!isFileSystemPlugin) {
      folderConfig = namespaceService.getFolder(folderPath.toNamespaceKey());
    } else {
      folderConfig = getFolderConfigForFileSystemPlugin(sourceName, folderPath);
    }
    checkPluginState(sourceName.getName());
    if (!plugin.isWrapperFor(VersionedPlugin.class)) {
      return folderConfig;
    }
    return getFolderConfigForVersionedPlugin(sourceName, folderPath, refType, refValue, plugin);
  }

  private FolderConfig getFolderConfigForVersionedPlugin(
      SourceName sourceName,
      SourceFolderPath folderPath,
      String refType,
      String refValue,
      StoragePlugin plugin) {
    FolderConfig folderConfig;
    final VersionContext version = getVersionContext(refType, refValue);
    CatalogEntityKey folderKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(folderPath.toPathList())
            .tableVersionContext(TableVersionContext.of(version))
            .build();
    folderConfig =
        plugin
            .unwrap(VersionedPlugin.class)
            .getFolder(folderKey)
            .orElseThrow(() -> new SourceFolderNotFoundException(sourceName, folderPath, null));
    return folderConfig;
  }

  private @NotNull FolderConfig getFolderConfigForFileSystemPlugin(
      SourceName sourceName, SourceFolderPath folderPath) {
    FolderConfig folderConfig;
    // this could be a physical dataset folder
    DatasetConfig datasetConfig;
    try {
      datasetConfig = namespaceService.getDataset(folderPath.toNamespaceKey());
      if (datasetConfig.getType() != DatasetType.VIRTUAL_DATASET) {
        folderConfig =
            new FolderConfig()
                .setId(datasetConfig.getId())
                .setFullPathList(folderPath.toPathList())
                .setName(folderPath.getFolderName().getName())
                .setIsPhysicalDataset(true)
                .setTag(datasetConfig.getTag());
      } else {
        throw new SourceFolderNotFoundException(
            sourceName,
            folderPath,
            new IllegalArgumentException(folderPath + " is a virtual dataset"));
      }
    } catch (NamespaceNotFoundException nfe) {
      // folder on filesystem
      folderConfig =
          new FolderConfig()
              .setFullPathList(folderPath.toPathList())
              .setName(folderPath.getFolderName().getName());
    }
    return folderConfig;
  }

  public void deleteFolder(SourceFolderPath folderPath, String refType, String refValue) {
    final VersionContext versionContext = VersionContextUtils.parse(refType, refValue);
    CatalogEntityKey folderKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(folderPath.toPathList())
            .tableVersionContext(TableVersionContext.of(versionContext))
            .build();
    try {
      createCatalog().deleteFolder(folderKey, null);
    } catch (CatalogEntityNotFoundException e) {
      throw UserException.validationError()
          .message("Folder %s does not exist.", folderPath)
          .buildSilently();
    } catch (CatalogFolderNotEmptyException e) {
      throw UserException.validationError()
          .message("Folder %s contains items within the folder and cannot be deleted.", folderPath)
          .buildSilently();
    } catch (CatalogException e) {
      throw e.toRestApiException();
    }
  }

  public Optional<FolderModel> createFolder(
      SourceName sourceName,
      SourceFolderPath folderPath,
      String userName,
      String refType,
      String refValue,
      String storageUri) {
    final VersionContext versionContext = getVersionContext(refType, refValue);
    CatalogEntityKey folderKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(folderPath.toPathList())
            .tableVersionContext(TableVersionContext.of(versionContext))
            .build();
    CatalogFolder inputCatalogFolder =
        new ImmutableCatalogFolder.Builder()
            .setFullPath(folderKey.getKeyComponents())
            .setVersionContext(versionContext)
            .setStorageUri(storageUri)
            .build();
    try {
      Optional<CatalogFolder> returnedCatalogFolder =
          createCatalog().createFolder(inputCatalogFolder);
      if (returnedCatalogFolder.isEmpty()) {
        throw new RuntimeException(String.format("Failed to create folder %s.", folderPath));
      }
      return Optional.of(
          new FolderModel(
              (returnedCatalogFolder.get().id() == null)
                  ? UUID.randomUUID().toString()
                  : returnedCatalogFolder.get().id(),
              folderPath.getFolderName().getName(),
              new FolderPath(returnedCatalogFolder.get().fullPath()).toUrlPath(),
              false,
              false,
              false,
              null,
              null,
              null,
              new NamespaceTree(),
              List.of(),
              0,
              returnedCatalogFolder.get().storageUri(),
              returnedCatalogFolder.get().tag()));
    } catch (CatalogEntityAlreadyExistsException e) {
      throw new ResourceExistsException(String.format("Folder %s already exists.", folderPath));
    } catch (CatalogEntityForbiddenException e) {
      throw new ResourceForbiddenException(e.getMessage());
    } catch (CatalogException e) {
      throw e.toRestApiException();
    }
  }

  public Optional<FolderModel> updateFolder(
      SourceName sourceName,
      SourceFolderPath folderPath,
      String refType,
      String refValue,
      String storageUri) {
    final VersionContext versionContext = getVersionContext(refType, refValue);
    CatalogEntityKey folderKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(folderPath.toPathList())
            .tableVersionContext(TableVersionContext.of(versionContext))
            .build();
    CatalogFolder inputCatalogFolder =
        new ImmutableCatalogFolder.Builder()
            .setFullPath(folderKey.getKeyComponents())
            .setVersionContext(versionContext)
            .setStorageUri(storageUri)
            .build();
    try {
      Optional<CatalogFolder> returnedCatalogFolder =
          createCatalog().updateFolder(inputCatalogFolder);
      if (returnedCatalogFolder.isEmpty()) {
        throw new RuntimeException(String.format("Failed to update folder %s.", folderPath));
      }
      return Optional.of(
          new FolderModel(
              (returnedCatalogFolder.get().id() == null)
                  ? UUID.randomUUID().toString()
                  : returnedCatalogFolder.get().id(),
              folderPath.getFolderName().getName(),
              new FolderPath(returnedCatalogFolder.get().fullPath()).toUrlPath(),
              false,
              false,
              false,
              null,
              null,
              null,
              new NamespaceTree(),
              List.of(),
              0,
              returnedCatalogFolder.get().storageUri(),
              returnedCatalogFolder.get().tag()));
    } catch (CatalogEntityNotFoundException e) {
      throw UserException.validationError()
          .message("Folder %s does not exist.", folderPath)
          .buildSilently();
    } catch (CatalogEntityForbiddenException e) {
      throw new ResourceForbiddenException(e.getMessage());
    } catch (CatalogException e) {
      throw e.toRestApiException();
    }
  }

  protected FolderModel newFolder(
      SourceFolderPath folderPath,
      FolderConfig folderConfig,
      NamespaceTree contents,
      boolean isQueryable,
      boolean isFileSystemPlugin,
      StoragePlugin plugin,
      String refType,
      String refValue)
      throws NamespaceNotFoundException {
    String id =
        folderConfig.getId() == null ? folderPath.toUrlPath() : folderConfig.getId().getId();
    return new FolderModel(
        id,
        folderConfig.getName(),
        folderPath.toUrlPath(),
        folderConfig.getIsPhysicalDataset(),
        isFileSystemPlugin,
        isQueryable,
        folderConfig.getExtendedConfig(),
        folderConfig.getTag(),
        null,
        contents,
        null,
        0,
        folderConfig.getStorageUri(),
        folderConfig.getTag());
  }

  protected NamespaceTree newNamespaceTree(
      List<NameSpaceContainer> children, boolean isFileSystemSource, boolean isImpersonationEnabled)
      throws DatasetNotFoundException, NamespaceException {
    return NamespaceTree.newInstance(
        datasetService,
        children,
        SOURCE,
        collaborationService,
        isFileSystemSource,
        isImpersonationEnabled,
        optionManager);
  }

  public NamespaceTree listFolder(
      SourceName sourceName,
      SourceFolderPath folderPath,
      String userName,
      String refType,
      String refValue,
      @Nullable String pageToken,
      int maxResults,
      boolean includeUDFChildren)
      throws PhysicalDatasetNotFoundException, NamespaceException {
    final String name = sourceName.getName();
    final String prefix = folderPath.toPathString();
    try {
      final StoragePlugin plugin =
          checkNotNull(catalogService.getSource(name), "storage plugin %s not found", sourceName);
      if (plugin.isWrapperFor(VersionedPlugin.class)) {
        final NamespaceKey folderKey = folderPath.toNamespaceKey();
        ExternalListResponse response =
            versionedPluginListEntriesHelper(
                plugin.unwrap(VersionedPlugin.class),
                folderKey,
                refType,
                refValue,
                pageToken,
                maxResults);

        return namespaceTreeOf(sourceName, response, includeUDFChildren);
      } else if (plugin.isWrapperFor(FileSystemPlugin.class)) {
        final NamespaceTree ns = new NamespaceTree();
        ns.setIsFileSystemSource(true);
        ns.setIsImpersonationEnabled(
            ((FileSystemPlugin<?>) plugin).getConfig().isImpersonationEnabled());
        addToNamespaceTreeForFileSystemPlugin(
            ns,
            plugin.unwrap(FileSystemPlugin.class).list(folderPath.toPathList(), userName),
            prefix);

        fillInTags(ns);

        return ns;
      } else {
        return newNamespaceTree(
            namespaceService.list(folderPath.toNamespaceKey(), pageToken, maxResults),
            false,
            false);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (DatasetNotFoundException e) {
      throw new NotFoundException(e);
    }
  }

  @WithSpan
  public ResourceTreeListResponse listPath(
      NamespaceKey path,
      boolean showDatasets,
      boolean showFunctions,
      String refType,
      String refValue,
      @Nullable String pageToken,
      int maxResults)
      throws NamespaceException, UnsupportedEncodingException {
    final List<ResourceTreeEntity> resources = Lists.newArrayList();
    final String sourceName = path.getRoot();
    final StoragePlugin plugin =
        checkNotNull(
            catalogService.getSource(sourceName), "storage plugin %s not found", sourceName);
    final SupportsListingDatasets listingPlugin = getListingPlugin(plugin);
    final boolean isVersionedPlugin = plugin.isWrapperFor(VersionedPlugin.class);
    Span.current().setAttribute(IS_VERSIONED_PLUGIN_SPAN_ATTRIBUTE_NAME, isVersionedPlugin);

    if (isVersionedPlugin) {
      ExternalListResponse response =
          versionedPluginListEntriesHelper(
              plugin.unwrap(VersionedPlugin.class), path, refType, refValue, pageToken, maxResults);

      return generateResourceTreeEntityList(
          path, response, ResourceTreeEntity.ResourceType.SOURCE, showFunctions);
    }

    try {
      // Since we're listing path in a source, the rootType should be SOURCE
      for (NameSpaceContainer container : namespaceService.list(path, pageToken, maxResults)) {
        if (container.getType() == Type.FOLDER) {
          resources.add(
              new ResourceTreeEntity(container.getFolder(), ResourceTreeEntity.ResourceType.SOURCE));
        } else if (showDatasets && container.getType() == Type.DATASET) {
          resources.add(
              new ResourceTreeEntity(container.getDataset(), ResourceTreeEntity.ResourceType.SOURCE));
        }
      }
    } catch (NamespaceNotFoundException e) {
      if (!(showDatasets && listingPlugin != null)) {
        throw e;
      }
      logger.debug("Namespace path {} not found, falling back to plugin listing", path, e);
    }

    if (resources.isEmpty() && showDatasets && listingPlugin != null) {
      resources.addAll(listPathFromPlugin(path, listingPlugin));
    }

    return new ImmutableResourceTreeListResponse.Builder().setEntities(resources).build();
  }

  private List<ResourceTreeEntity> listPathFromPlugin(
      NamespaceKey path, SupportsListingDatasets plugin) throws UnsupportedEncodingException {
    final Map<List<String>, ResourceTreeEntity> entities = new LinkedHashMap<>();
    final List<String> requestedPath = path.getPathComponents();

    try (var listing = plugin.listDatasetHandles()) {
      var iterator = listing.iterator();
      while (iterator.hasNext()) {
        DatasetHandle handle = iterator.next();
        EntityPath datasetPath = handle.getDatasetPath();
        List<String> fullPath = datasetPath.getComponents();
        if (fullPath.isEmpty()) {
          continue;
        }

        if (!requestedPath.equals(fullPath.subList(0, Math.min(requestedPath.size(), fullPath.size())))) {
          continue;
        }

        if (fullPath.size() <= requestedPath.size()) {
          continue;
        }

        if (fullPath.size() == requestedPath.size() + 1) {
          entities.computeIfAbsent(
              fullPath,
              key ->
                  new ResourceTreeEntity(
                      ResourceTreeEntity.ResourceType.PHYSICAL_DATASET,
                      key.get(key.size() - 1),
                      key,
                      null,
                      null,
                      String.join(".", key),
                      ResourceTreeEntity.ResourceType.SOURCE,
                      false));
          continue;
        }

        List<String> folderPath = fullPath.subList(0, requestedPath.size() + 1);
        entities.computeIfAbsent(
            folderPath,
            key ->
                new ResourceTreeEntity(
                    ResourceTreeEntity.ResourceType.FOLDER,
                    key.get(key.size() - 1),
                    key,
                    "/resourcetree/" + new NamespaceKey(key).toUrlEncodedString(),
                    null,
                    String.join(".", key),
                    ResourceTreeEntity.ResourceType.SOURCE,
                    null));
      }
    } catch (Exception e) {
      logger.warn("Failed to list path {} directly from source plugin", path, e);
    }

    return new ArrayList<>(entities.values());
  }

  private SupportsListingDatasets getListingPlugin(StoragePlugin plugin) {
    if (plugin instanceof SupportsListingDatasets) {
      return (SupportsListingDatasets) plugin;
    }

    if (plugin.isWrapperFor(SupportsListingDatasets.class)) {
      return plugin.unwrap(SupportsListingDatasets.class);
    }

    return null;
  }

  protected VersionContext getVersionContext(String refType, String refValue) {
    return VersionContextUtils.parse(refType, refValue);
  }

  protected ExternalListResponse versionedPluginListEntriesHelper(
      VersionedPlugin plugin,
      NamespaceKey namespaceKey,
      String refType,
      String refValue,
      @Nullable String pageToken,
      int maxResults) {
    VersionContext version = VersionContextUtils.parse(refType, refValue);
    String sourceName = namespaceKey.getRoot();
    try {
      if (maxResults == NO_PAGINATION_MAX_RESULTS) {
        return ExternalListResponse.ofStream(
            plugin.listEntries(
                namespaceKey.getPathWithoutRoot(),
                plugin.resolveVersionContext(version),
                VersionedPlugin.NestingMode.IMMEDIATE_CHILDREN_ONLY,
                VersionedPlugin.ContentMode.ENTRY_METADATA_ONLY));
      } else {
        return ExternalListResponse.ofVersionedListPage(
            plugin.listEntriesPage(
                namespaceKey.getPathWithoutRoot(),
                plugin.resolveVersionContext(version),
                VersionedPlugin.NestingMode.IMMEDIATE_CHILDREN_ONLY,
                VersionedPlugin.ContentMode.ENTRY_METADATA_ONLY,
                new ImmutableVersionedListOptions.Builder()
                    .setPageToken(pageToken)
                    .setMaxResultsPerPage(maxResults)
                    .build()));
      }
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
          .message("Requested %s not found on source %s.", version, sourceName)
          .buildSilently();
    } catch (NoDefaultBranchException e) {
      throw UserException.validationError(e)
          .message(
              "Unable to resolve source version. Version was not specified and Source %s does not"
                  + " have a default branch set.",
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
    TagsSearchResult tagsInfo =
        collaborationService.getTagsForIds(
            files.stream().map(File::getId).collect(Collectors.toSet()));
    Map<String, CollaborationTag> tags = tagsInfo.getTags();

    // we populate tags not for all files
    ns.setCanTagsBeSkipped(tagsInfo.getCanTagsBeSkipped());

    for (File input : files) {
      CollaborationTag collaborationTag = tags.get(input.getId());
      if (collaborationTag != null) {
        input.setTags(collaborationTag.getTagsList());
      }
    }
  }

  @Deprecated
  public FileFormat getDefaultFileFormat(SourceFilePath sourceFilePath) {
    final FileConfig config = new FileConfig();
    config.setCtime(clock.millis());
    config.setFullPathList(sourceFilePath.toPathList());
    config.setName(sourceFilePath.getFileName().getName());
    config.setType(
        FileFormat.getFileFormatType(singletonList(FilenameUtils.getExtension(config.getName()))));
    config.setTag(null);
    return FileFormat.getForFile(config);
  }

  /**
   * Get default file format for a directory in filesystem
   *
   * @param sourceName name of source
   * @param sourceFolderPath path to directory
   * @param user user name
   * @return {@code FileFormat} format settings
   * @throws NamespaceException on invalid namespace operation
   * @throws PhysicalDatasetNotFoundException if file/folder is marked as physical dataset but is
   *     missing from namespace
   */
  @Deprecated
  public FileFormat getDefaultFileFormat(
      SourceName sourceName, SourceFolderPath sourceFolderPath, String user)
      throws NamespaceException, PhysicalDatasetNotFoundException {
    final FileConfig config = new FileConfig();
    config.setCtime(clock.millis());
    config.setFullPathList(sourceFolderPath.toPathList());
    config.setName(sourceFolderPath.getFolderName().getName());
    NamespaceTree ns =
        listFolder(sourceName, sourceFolderPath, user, null, null, null, Integer.MAX_VALUE, false);
    if (!ns.getFiles().isEmpty()) {
      config.setType(
          FileFormat.getFileFormatType(
              singletonList(FilenameUtils.getExtension(ns.getFiles().get(0).getName()))));
    } else {
      config.setType(FileType.UNKNOWN);
    }
    config.setTag(null);
    return FileFormat.getForFolder(config);
  }

  /**
   * A file or folder in source could be defined as a physical dataset. Store physical dataset
   * properties in namespace.
   */
  public void createPhysicalDataset(SourceFilePath filePath, PhysicalDatasetConfig datasetConfig)
      throws NamespaceException {
    createCatalog()
        .createOrUpdateDataset(
            new NamespaceKey(filePath.getSourceName().getName()),
            new PhysicalDatasetPath(filePath).toNamespaceKey(),
            toDatasetConfig(datasetConfig, security.getUserPrincipal().getName()));
  }

  public void createPhysicalDataset(
      SourceFolderPath folderPath, PhysicalDatasetConfig datasetConfig) throws NamespaceException {
    createCatalog()
        .createOrUpdateDataset(
            new NamespaceKey(folderPath.getSourceName().getName()),
            new PhysicalDatasetPath(folderPath).toNamespaceKey(),
            toDatasetConfig(datasetConfig, security.getUserPrincipal().getName()));
  }

  public PhysicalDatasetConfig getFilesystemPhysicalDataset(NamespacePath path, DatasetType type)
      throws PhysicalDatasetNotFoundException {
    try {
      return toPhysicalDatasetConfig(namespaceService.getDataset(path.toNamespaceKey()));
    } catch (NamespaceNotFoundException nse) {
      throw new PhysicalDatasetNotFoundException(path, type, nse);
    }
  }

  // For all tables including filesystem tables.
  // Physical datasets may be missing
  public PhysicalDataset getPhysicalDataset(PhysicalDatasetPath physicalDatasetPath)
      throws NamespaceException {
    final int jobsCount = datasetService.getJobsCount(physicalDatasetPath.toNamespaceKey());
    try {
      final DatasetConfig datasetConfig =
          namespaceService.getDataset(physicalDatasetPath.toNamespaceKey());
      return newPhysicalDataset(
          new PhysicalDatasetResourcePath(physicalDatasetPath),
          physicalDatasetPath.getDatasetName(),
          toPhysicalDatasetConfig(datasetConfig),
          jobsCount);
    } catch (NamespaceNotFoundException nse) {
      return newPhysicalDataset(
          new PhysicalDatasetResourcePath(physicalDatasetPath),
          physicalDatasetPath.getDatasetName(),
          new PhysicalDatasetConfig()
              .setName(physicalDatasetPath.getLeaf().getName())
              .setType(DatasetType.PHYSICAL_DATASET)
              .setTag("0")
              .setFullPathList(physicalDatasetPath.toPathList()),
          jobsCount);
    }
  }

  protected PhysicalDataset newPhysicalDataset(
      PhysicalDatasetResourcePath resourcePath,
      PhysicalDatasetName datasetName,
      PhysicalDatasetConfig datasetConfig,
      Integer jobsCount)
      throws NamespaceNotFoundException {
    return new PhysicalDataset(resourcePath, datasetName, datasetConfig, jobsCount, null);
  }

  private boolean isPhysicalDataset(SourceName sourceName, SourceFolderPath folderPath) {
    try {
      DatasetConfig ds =
          namespaceService.getDataset(new PhysicalDatasetPath(folderPath).toNamespaceKey());
      return DatasetHelper.isPhysicalDataset(ds.getType());
    } catch (NamespaceException nse) {
      logger.debug(
          "Error while checking physical dataset in source {} for folder {}, error {}",
          sourceName.getName(),
          folderPath.toPathString(),
          nse.toString());
      return false;
    }
  }

  public void deletePhysicalDataset(
      SourceName sourceName,
      PhysicalDatasetPath datasetPath,
      String version,
      SourceNamespaceService.DeleteCallback deleteCallback)
      throws PhysicalDatasetNotFoundException {
    try {
      final StoragePlugin plugin =
          checkNotNull(
              catalogService.getSource(sourceName.getName()),
              "storage plugin %s not found",
              sourceName);
      if (plugin.isWrapperFor(VersionedPlugin.class)) {
        throw UserException.unsupportedError()
            .message("Deletion of entity is not allowed for Versioned source")
            .buildSilently();
      }
      DatasetConfig datasetConfig = namespaceService.getDataset(datasetPath.toNamespaceKey());
      deleteCallback.onDatasetDelete(datasetConfig);
      namespaceService.deleteDataset(datasetPath.toNamespaceKey(), version);
    } catch (NamespaceException nse) {
      throw new PhysicalDatasetNotFoundException(datasetPath, nse);
    }
  }

  @WithSpan
  public List<SourceConfig> getSources() {
    final List<SourceConfig> sources = new ArrayList<>();

    for (SourceConfig sourceConfig : createCatalog().getSourceConfigs()) {
      if (SourceUI.isInternal(sourceConfig, connectionReader)) {
        continue;
      }

      sources.add(sourceConfig);
    }

    return sources;
  }

  public SourceConfig getById(String id) throws SourceNotFoundException, NamespaceException {
    try {
      return namespaceService.getSourceById(new EntityId(id));
    } catch (NamespaceNotFoundException e) {
      throw new SourceNotFoundException(id);
    }
  }

  public Source fromSourceConfig(
      SourceConfig sourceConfig, List<CatalogItem> children, @Nullable String nextPageToken) {
    final AccelerationSettings settings =
        reflectionServiceHelper
            .getReflectionSettings()
            .getReflectionSettings(new NamespaceKey(sourceConfig.getName()));
    return new Source(
        sourceConfig,
        settings,
        connectionReader,
        children,
        nextPageToken,
        catalogService.getSourceState(sourceConfig.getName()));
  }

  /**
   * This method is public such that certain (test) code paths can mock this method to ignore this
   * validation.
   */
  @VisibleForTesting
  public void validateConnectionConf(ConnectionConf<?, ?> connectionConf) {
    if (connectionConf.isInternal()) {
      throw UserExceptionMapper.withStatus(
              UserException.unsupportedError(), Response.Status.BAD_REQUEST)
          .message(
              "Source with connection type %s cannot be created nor modified.",
              connectionConf.getType())
          .buildSilently();
    }
  }

  protected void checkPluginState(String name) {
    SourceState state =
        catalogService.getSystemUserCatalog().refreshSourceStatus(new NamespaceKey(name));
    if (state.getStatus() == SourceState.SourceStatus.bad) {
      String message =
          String.format("Cannot connect to [%s]. %s", name, state.getSuggestedUserAction());
      logger.error(message);
      throw UserException.connectionError().message(message).buildSilently();
    }
  }
}

/*
 * Copyright (C) 2017 Dremio Corporation
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

import static com.dremio.dac.util.DatasetsUtil.toDatasetConfig;
import static com.dremio.dac.util.DatasetsUtil.toPhysicalDatasetConfig;
import static com.dremio.exec.store.StoragePluginRegistryImpl.isInternal;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SOURCE;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.io.FilenameUtils;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.model.sources.PhysicalDataset;
import com.dremio.dac.model.sources.PhysicalDatasetName;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.sources.PhysicalDatasetResourcePath;
import com.dremio.dac.model.sources.Source;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.server.SourceToStoragePluginConfig;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.PhysicalDatasetNotFoundException;
import com.dremio.dac.service.errors.ResourceExistsException;
import com.dremio.dac.service.errors.SourceFolderNotFoundException;
import com.dremio.dac.service.errors.SourceNotFoundException;
import com.dremio.dac.util.JSONUtil;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaEntity;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.file.File;
import com.dremio.file.SourceFilePath;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.Message;
import com.dremio.service.namespace.SourceState.MessageLevel;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.physicaldataset.proto.AccelerationSettingsDescriptor;
import com.dremio.service.namespace.physicaldataset.proto.PhysicalDatasetConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceType;
import com.dremio.service.namespace.space.proto.ExtendedConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Mocked source service.
 */
public class SourceService {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SourceService.class);

  private final NamespaceService namespaceService;
  private final SourceToStoragePluginConfig configurator;
  private final StoragePluginRegistry plugins;
  private final DatasetVersionMutator datasetService;
  private final CatalogService catalogService;

  @Inject
  public SourceService(
    StoragePluginRegistry plugins,
    NamespaceService namespaceService,
    SourceToStoragePluginConfig configurator,
    DatasetVersionMutator datasetService,
    CatalogService catalogService) {
    this.plugins = plugins;
    this.namespaceService = namespaceService;
    this.configurator = configurator;
    this.datasetService = datasetService;
    this.catalogService = catalogService;
  }

  public SourceConfig registerSourceWithRuntime(SourceUI source) throws ExecutionSetupException, NamespaceException {
    return registerSourceWithRuntime(source.asSourceConfig(), source.getConfig());
  }

  public SourceConfig registerSourceWithRuntime(SourceConfig sourceConfig, com.dremio.dac.model.sources.Source source) throws ExecutionSetupException, NamespaceException {
    StoragePluginConfig config = configurator.configure(source);
    if (logger.isDebugEnabled()) {
      logger.debug("Connection Object: \n{}\n", JSONUtil.toString(config));
    }
    plugins.createOrUpdate(sourceConfig.getName(), config, sourceConfig, true);
    return namespaceService.getSource(new NamespaceKey(sourceConfig.getName()));
  }

  public void unregisterSourceWithRuntime(SourceName sourceName) {
    plugins.deletePlugin(sourceName.getName());
  }

  public SourceConfig createSource(SourceConfig sourceConfig, com.dremio.dac.model.sources.Source source) throws ExecutionSetupException, NamespaceException, ResourceExistsException {
    validateSourceConfig(sourceConfig);

    Preconditions.checkArgument(sourceConfig.getId().getId() == null, "Source id is immutable.");
    Preconditions.checkArgument(sourceConfig.getVersion() == null, "Source tag is immutable.");

    // check if source already exists with the given name.
    if (namespaceService.exists(new SourcePath(new SourceName(sourceConfig.getName())).toNamespaceKey(), SOURCE)) {
      throw new ResourceExistsException(String.format("A source with the name [%s] already exists.", sourceConfig.getName()));
    }

    sourceConfig.setCtime(System.currentTimeMillis());

    return registerSourceWithRuntime(sourceConfig, source);
  }

  public SourceConfig updateSource(String id, SourceConfig sourceConfig, Source source) throws NamespaceException, ExecutionSetupException, SourceNotFoundException {
    validateSourceConfig(sourceConfig);

    SourceConfig oldSourceConfig = getById(id);

    Preconditions.checkArgument(id.equals(sourceConfig.getId().getId()), "Source id is immutable.");
    Preconditions.checkArgument(oldSourceConfig.getName().equals(sourceConfig.getName()), "Source name is immutable.");
    Preconditions.checkArgument(oldSourceConfig.getType().equals(sourceConfig.getType()), "Source type is immutable.");

    return registerSourceWithRuntime(sourceConfig, source);
  }

  public void deleteSource(SourceConfig sourceConfig) throws NamespaceException {
    namespaceService.deleteSource(new NamespaceKey(sourceConfig.getName()), sourceConfig.getVersion());
    plugins.deletePlugin(sourceConfig.getName());
  }

  private void validateSourceConfig(SourceConfig sourceConfig) {
    // TODO: move this further down to the namespace or catalog service.  For some reason InputValidation does not work on SourceConfig.
    Preconditions.checkNotNull(sourceConfig);
    Preconditions.checkNotNull(sourceConfig.getName(), "Source name is missing.");
    Preconditions.checkArgument(sourceConfig.getName().length() > 2, "Source names need to be at least 3 characters long.");
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
  }

  public void checkSourceExists(SourceName sourceName) throws SourceNotFoundException, NamespaceException {
    try {
      namespaceService.getSource(new SourcePath(sourceName).toNamespaceKey());
    } catch (NamespaceNotFoundException nfe) {
      throw new SourceNotFoundException(sourceName.getName(), nfe);
    }
  }

  protected void addFileToNamespaceTree(NamespaceTree ns, SourceName source, SourceFilePath path, String owner) throws NamespaceNotFoundException {
    final File file = File.newInstance(
        path.toUrlPath(),
        path,
        getDefaultFileFormat(source, path),
        0, // files should not have any jobs or descendants, no need to check
        0,
        false,
        false,
        false
      );
      file.getFileFormat().getFileFormat().setOwner(owner);
      ns.addFile(file);
  }

  protected void addFolderToNamespaceTree(NamespaceTree ns, SourceFolderPath path, FolderConfig folderConfig) throws NamespaceNotFoundException {
    Folder folder = Folder.newInstance(path, folderConfig, null, null, false, true);
    ns.addFolder(folder);
  }

  protected void addFolderTableToNamespaceTree(NamespaceTree ns, SourceFolderPath folderPath, FolderConfig folderConfig, FileFormat fileFormat, boolean isQueryable) throws NamespaceNotFoundException {
    final Folder folder = Folder.newInstance(folderPath, folderConfig, fileFormat, null, isQueryable,  true);
    ns.addFolder(folder);
  }

  protected void addTableToNamespaceTree(NamespaceTree ns, PhysicalDatasetResourcePath path, PhysicalDatasetName name,
      PhysicalDatasetConfig datasetConfig, int jobsCount, int descendants) throws NamespaceNotFoundException {
    ns.addPhysicalDataset(new PhysicalDataset(path, name, datasetConfig, jobsCount, descendants));
  }

  private void addToNamespaceTree(NamespaceTree ns, List<SchemaEntity> entities, SourceName source, String prefix)
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
          folderConfig.setVersion(0L);
          final List<NamespaceKey> datasetPaths = namespaceService.getAllDatasets(new FolderPath(folderConfig.getFullPathList()).toNamespaceKey());
          final ExtendedConfig extendedConfig = new ExtendedConfig()
            .setDatasetCount((long) datasetPaths.size())
            .setJobCount(datasetService.getJobsCount(datasetPaths));
          folderConfig.setExtendedConfig(extendedConfig);
          addFolderToNamespaceTree(ns, path, folderConfig);
        }
        break;

        case TABLE:
        {
          PhysicalDatasetPath path = new PhysicalDatasetPath(prefix + "." + entity.getPath());
          PhysicalDatasetConfig datasetConfig = new PhysicalDatasetConfig();
          datasetConfig.setName(path.getFileName().getName());
          datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
          datasetConfig.setVersion(0L);
          datasetConfig.setFullPathList(path.toPathList());
          addTableToNamespaceTree(ns,
              new PhysicalDatasetResourcePath(source, path),
              new PhysicalDatasetName(path.getFileName().getName()),
              datasetConfig,
              datasetService.getJobsCount(path.toNamespaceKey()),
              datasetService.getDescendantsCount(path.toNamespaceKey()));
        }
        break;

        case FILE_TABLE:
        {
          // TODO(Amit H): Should we ignore exceptions from getFilesystemPhysicalDataset?
          // Dataset could be marked as deleted by the time we come here.
          final SourceFilePath filePath = new SourceFilePath(prefix + '.' + entity.getPath());
          final File file = getFileDataset(source, filePath, entity.getOwner());
          ns.addFile(file);
        }
        break;

        case FOLDER_TABLE: {
          final SourceFolderPath folderPath = new SourceFolderPath(prefix + "." + entity.getPath());

          // TODO(Amit H): Should we ignore exceptions from getFilesystemPhysicalDataset?
          // Dataset could be marked as deleted by the time we come here.
          final PhysicalDatasetConfig physicalDatasetConfig = getFilesystemPhysicalDataset(source, folderPath);
          final FileConfig fileConfig = physicalDatasetConfig.getFormatSettings();
          fileConfig.setOwner(entity.getOwner());

          final FolderConfig folderConfig = new FolderConfig();
          folderConfig.setId(new EntityId(physicalDatasetConfig.getId()));
          folderConfig.setFullPathList(folderPath.toPathList());
          folderConfig.setName(folderPath.getFolderName().getName());

          // use version from physical dataset.
          folderConfig.setVersion(physicalDatasetConfig.getVersion());
          fileConfig.setVersion(physicalDatasetConfig.getVersion());

          // For physical dataset from folder set descendants, job count and datasets count.
          // job count is count of all datasets inside this folder + jobs directyly on this folder
          final List<NamespaceKey> datasetPaths = namespaceService.getAllDatasets(new FolderPath(folderConfig.getFullPathList()).toNamespaceKey());
          final ExtendedConfig extendedConfig = new ExtendedConfig()
            .setDatasetCount((long) datasetPaths.size())
            .setJobCount(datasetService.getJobsCount(datasetPaths) + datasetService.getJobsCount(folderPath.toNamespaceKey()));
          extendedConfig.setDescendants((long)datasetService.getDescendantsCount(folderPath.toNamespaceKey()));
          folderConfig.setExtendedConfig(extendedConfig);

          addFolderTableToNamespaceTree(ns, folderPath, folderConfig, FileFormat.getForFolder(fileConfig), fileConfig.getType() != FileType.UNKNOWN);
        }
        break;

        case FILE:
        {
          final SourceFilePath path = new SourceFilePath(prefix + '.' + entity.getPath());
          addFileToNamespaceTree(ns, source, path, entity.getOwner());
        }
        break;

        default:
          throw new IllegalArgumentException("Invalid SchemaEntity type " + entity.getType());
      }
    }
  }

  public File getFileDataset(SourceName source, final SourceFilePath filePath, String owner)
      throws PhysicalDatasetNotFoundException, NamespaceException {
    final PhysicalDatasetConfig physicalDatasetConfig = getFilesystemPhysicalDataset(source, filePath);
    final FileConfig fileConfig = physicalDatasetConfig.getFormatSettings();
    fileConfig.setOwner(owner);
    fileConfig.setVersion(physicalDatasetConfig.getVersion());
    final File file = File.newInstance(physicalDatasetConfig.getId(), filePath, FileFormat.getForFile(fileConfig),
      datasetService.getJobsCount(filePath.toNamespaceKey()), datasetService.getDescendantsCount(filePath.toNamespaceKey()),
      false, false, fileConfig.getType() != FileType.UNKNOWN
    );
    return file;
  }

  public NamespaceTree listSource(SourceName sourceName, SourceConfig sourceConfig, String userName)
    throws IOException, PhysicalDatasetNotFoundException, NamespaceException {
    try {
      final StoragePlugin plugin = checkNotNull(plugins.getPlugin(sourceName.getName()), "storage plugin %s not found", sourceName);
      if (plugin instanceof FileSystemPlugin) {
        final NamespaceTree ns = new NamespaceTree();
        addToNamespaceTree(ns, ((FileSystemPlugin) plugin).list(singletonList(sourceName.getName()), userName), sourceName, sourceName.getName());
        return ns;
      } else {
        return newNamespaceTree(namespaceService.list(new NamespaceKey(singletonList(sourceConfig.getName()))));
      }
    } catch (ExecutionSetupException | IOException | DatasetNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get properties for folder in source.
   * @param sourceName source name
   * @param folderPath folder path
   * @return folder properties
   */
  public Folder getFolder(SourceName sourceName, SourceFolderPath folderPath, boolean includeContents, String userName) throws SourceFolderNotFoundException, NamespaceException, PhysicalDatasetNotFoundException, IOException {
    try {
      final StoragePlugin plugin = checkNotNull(plugins.getPlugin(sourceName.getName()), "storage plugin %s not found", sourceName);
      final boolean isFileSystemPlugin = (plugin instanceof FileSystemPlugin);
      FolderConfig folderConfig;
      if (isFileSystemPlugin) {
        // this could be a physical dataset folder
        DatasetConfig datasetConfig = null;
        try {
          datasetConfig = namespaceService.getDataset(folderPath.toNamespaceKey());
          if (datasetConfig.getType() != DatasetType.VIRTUAL_DATASET) {
            folderConfig = new FolderConfig()
              .setId(datasetConfig.getId())
              .setFullPathList(folderPath.toPathList())
              .setName(folderPath.getFolderName().getName())
              .setIsPhysicalDataset(true)
              .setVersion(datasetConfig.getVersion());
          } else {
            throw new SourceFolderNotFoundException(sourceName, folderPath,
              new IllegalArgumentException(folderPath.toString() + " is a virtual dataset"));
          }
        } catch (NamespaceNotFoundException nfe) {
          // folder on fileystem
          folderConfig = new FolderConfig()
            .setFullPathList(folderPath.toPathList())
            .setName(folderPath.getFolderName().getName());
        }
      } else {
        folderConfig = namespaceService.getFolder(folderPath.toNamespaceKey());
      }

      final List<NamespaceKey> datasetPaths = namespaceService.getAllDatasets(folderPath.toNamespaceKey());
      final ExtendedConfig extendedConfig = new ExtendedConfig()
        .setDatasetCount((long) datasetPaths.size())
        .setJobCount(datasetService.getJobsCount(datasetPaths));

      folderConfig.setExtendedConfig(extendedConfig);
      // TODO: why do we need to look up the dataset again in isPhysicalDataset?
      NamespaceTree contents = includeContents ? listFolder(sourceName, folderPath, userName) : null;
      return newFolder(folderPath, folderConfig, contents, isPhysicalDataset(sourceName, folderPath), isFileSystemPlugin);
    } catch (ExecutionSetupException e) {
      throw new SourceFolderNotFoundException(sourceName, folderPath, e);
    }
  }

  protected Folder newFolder(SourceFolderPath folderPath, FolderConfig folderConfig, NamespaceTree contents, boolean isQueryable, boolean isFileSystemPlugin)
      throws NamespaceNotFoundException {
    // TODO: why do we need to look up the dataset again in isPhysicalDataset?
    Folder folder = Folder.newInstance(folderPath, folderConfig, null, contents, isQueryable, isFileSystemPlugin);
    return folder;
  }
  protected NamespaceTree newNamespaceTree(List<NameSpaceContainer> children) throws DatasetNotFoundException, NamespaceException {
    return NamespaceTree.newInstance(datasetService, namespaceService, children, SOURCE);
  }

  public NamespaceTree listFolder(SourceName sourceName, SourceFolderPath folderPath, String userName)
    throws IOException, PhysicalDatasetNotFoundException, NamespaceException {
    final String name = sourceName.getName();
    final String prefix = folderPath.toPathString();
    try {
      final StoragePlugin plugin = checkNotNull(plugins.getPlugin(name), "storage plugin %s not found", sourceName);
      if (plugin instanceof FileSystemPlugin) {
        final NamespaceTree ns = new NamespaceTree();
        addToNamespaceTree(ns, ((FileSystemPlugin) plugin).list(folderPath.toPathList(), userName), sourceName, prefix);
        return ns;
      } else {
        return newNamespaceTree(namespaceService.list(folderPath.toNamespaceKey()));
      }
    } catch (ExecutionSetupException | IOException | DatasetNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public FileFormat getDefaultFileFormat(SourceName sourceName, SourceFilePath sourceFilePath) {
    final FileConfig config = new FileConfig();
    config.setCtime(System.currentTimeMillis());
    config.setFullPathList(sourceFilePath.toPathList());
    config.setName(sourceFilePath.getFileName().getName());
    config.setType(FileFormat.getFileFormatType(singletonList(FilenameUtils.getExtension(config.getName()))));
    config.setVersion(null);
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
    config.setVersion(null);
    return FileFormat.getForFolder(config);
  }

  /** A file or folder in source could be defined as a physical dataset.
   * Store physical dataset properties in namespace.
   */
  public void createPhysicalDataset(SourceFilePath filePath, PhysicalDatasetConfig datasetConfig)
      throws NamespaceException {
    catalogService.createOrUpdateDataset(
      namespaceService,
      new NamespaceKey(filePath.getSourceName().getName()),
      new PhysicalDatasetPath(filePath).toNamespaceKey(),
      toDatasetConfig(datasetConfig, null));
  }

  public void createPhysicalDataset(SourceFolderPath folderPath, PhysicalDatasetConfig datasetConfig)
      throws NamespaceException {
    catalogService.createOrUpdateDataset(
      namespaceService,
      new NamespaceKey(folderPath.getSourceName().getName()),
      new PhysicalDatasetPath(folderPath).toNamespaceKey(),
      toDatasetConfig(datasetConfig, null));
  }

  public PhysicalDatasetConfig getFilesystemPhysicalDataset(SourceName sourceName, SourceFilePath filePath)
    throws PhysicalDatasetNotFoundException, NamespaceException {
    final PhysicalDatasetPath datasetPath = new PhysicalDatasetPath(filePath);
    try {
      return toPhysicalDatasetConfig(namespaceService.getDataset(datasetPath.toNamespaceKey()));
    } catch (NamespaceNotFoundException nse) {
      throw new PhysicalDatasetNotFoundException(sourceName, datasetPath, nse);
    }
  }

  /**
   * @param sourceName name of source
   * @param folderPath path of folder to get physical dataset properties.
   * @return {@code PhysicalDatasetConfig}
   * @throws PhysicalDatasetNotFoundException if folder is not marked as physical dataset
   * @throws NamespaceException on invalid namespace operation.
   */
  public PhysicalDatasetConfig getFilesystemPhysicalDataset(SourceName sourceName, SourceFolderPath folderPath)
    throws PhysicalDatasetNotFoundException, NamespaceException {
    final PhysicalDatasetPath datasetPath = new PhysicalDatasetPath(folderPath);
    try {
      return toPhysicalDatasetConfig(namespaceService.getDataset(datasetPath.toNamespaceKey()));
    } catch (NamespaceNotFoundException nse) {
      throw new PhysicalDatasetNotFoundException(sourceName, datasetPath, nse);
    }
  }

  // For all tables including filesystem tables.
  // Physical datasets may be missing
  public PhysicalDataset getPhysicalDataset(SourceName sourceName, PhysicalDatasetPath physicalDatasetPath) throws NamespaceException {
    final int jobsCount =  datasetService.getJobsCount(physicalDatasetPath.toNamespaceKey());
    final int descendants = datasetService.getDescendantsCount(physicalDatasetPath.toNamespaceKey());
    try {
      final DatasetConfig datasetConfig = namespaceService.getDataset(physicalDatasetPath.toNamespaceKey());
      return newPhysicalDataset(
        new PhysicalDatasetResourcePath(physicalDatasetPath.getSourceName(), physicalDatasetPath),
        physicalDatasetPath.getDatasetName(),
        toPhysicalDatasetConfig(datasetConfig),
        jobsCount,
        descendants);
    } catch (NamespaceNotFoundException nse) {
      final SourceConfig config = namespaceService.getSource(new SourcePath(sourceName).toNamespaceKey());

      return newPhysicalDataset(
        new PhysicalDatasetResourcePath(physicalDatasetPath.getSourceName(), physicalDatasetPath),
        physicalDatasetPath.getDatasetName(),
        new PhysicalDatasetConfig()
          .setName(physicalDatasetPath.getLeaf().getName())
          .setType(DatasetType.PHYSICAL_DATASET)
          .setVersion(0L)
          .setFullPathList(physicalDatasetPath.toPathList())
          .setAccelerationSettings(
              new AccelerationSettingsDescriptor()
                .setMethod(RefreshMethod.FULL)
                .setAccelerationRefreshPeriod(config.getAccelerationRefreshPeriod())
                .setAccelerationGracePeriod(config.getAccelerationGracePeriod())
          ),
        jobsCount,
        descendants);
    }
  }

  protected PhysicalDataset newPhysicalDataset(PhysicalDatasetResourcePath resourcePath,
      PhysicalDatasetName datasetName, PhysicalDatasetConfig datasetConfig, Integer jobsCount, Integer descendants) throws NamespaceNotFoundException {
    return new PhysicalDataset(
        resourcePath,
        datasetName,
        datasetConfig,
        jobsCount,
        descendants);
  }

  private static boolean isPhysicalDataset(DatasetType t) {
    return (t == DatasetType.PHYSICAL_DATASET || t == DatasetType.PHYSICAL_DATASET_SOURCE_FILE || t == DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER);
  }

  public boolean isPhysicalDataset(SourceName sourceName, SourceFolderPath folderPath) {
    try {
      DatasetConfig ds = namespaceService.getDataset(new PhysicalDatasetPath(folderPath).toNamespaceKey());
      return isPhysicalDataset(ds.getType());
    } catch (NamespaceException nse) {
      logger.debug("Error while checking physical dataset in source {} for folder {}, error {}",
        sourceName.getName(), folderPath.toPathString(), nse.toString());
      return false;
    }
  }

  public void deletePhysicalDataset(SourceName sourceName, SourceFolderPath folderPath, long version) throws PhysicalDatasetNotFoundException {
    final PhysicalDatasetPath datasetPath = new PhysicalDatasetPath(folderPath);
    try {
      namespaceService.deleteDataset(datasetPath.toNamespaceKey(), version);
    } catch (NamespaceException nse) {
      throw new PhysicalDatasetNotFoundException(sourceName, datasetPath, nse);
    }
  }

  public void deletePhysicalDataset(SourceName sourceName, SourceFilePath filePath, long version) throws PhysicalDatasetNotFoundException {
    final PhysicalDatasetPath datasetPath = new PhysicalDatasetPath(filePath);
    try {
      namespaceService.deleteDataset(datasetPath.toNamespaceKey(), version);
    } catch (NamespaceException nse) {
      throw new PhysicalDatasetNotFoundException(sourceName, datasetPath, nse);
    }
  }

  public SourceState getSourceState(String sourceName) throws SourceNotFoundException {
    try {
      StoragePlugin plugin = plugins.getPlugin(sourceName);
      if (plugin != null) {
        return plugin.getState();
      } else {
        throw new SourceNotFoundException(sourceName);
      }
    } catch (ExecutionSetupException e) {
      return new SourceState(SourceStatus.bad, ImmutableList.of(new Message(MessageLevel.ERROR, e.getMessage())));
    }
  }

  @VisibleForTesting
  public StoragePlugin getStoragePlugin(String sourceName) throws SourceNotFoundException, ExecutionSetupException {
    StoragePlugin plugin = plugins.getPlugin(sourceName);
    if (plugin == null) {
      throw new SourceNotFoundException(sourceName);
    }
    return plugin;
  }

  public List<SourceConfig> getSources() {
    final List<SourceConfig> sources = new ArrayList<>();

    for (SourceConfig sourceConfig : namespaceService.getSources()) {
      String name = sourceConfig.getName();
      if (isInternal(sourceConfig)
          || sourceConfig.getType() == SourceType.SYS
          || sourceConfig.getType() == SourceType.INFORMATION_SCHEMA

          // the test harness blows away storage plugin types.
          || "sys".equals(name)
          || "INFORMATION_SCHEMA".equals(name)) {
        continue;
      }

      sources.add(sourceConfig);
    }

    return sources;
  }

  public SourceConfig getById(String id) throws SourceNotFoundException {
    try {
      SourceConfig config = namespaceService.getSourceById(id);
      return config;
    } catch (NamespaceNotFoundException e) {
      throw new SourceNotFoundException(id);
    }
  }

  public SourceState getStateForSource(SourceConfig sourceConfig) {
    SourceState state;
    try {
      state = getSourceState(sourceConfig.getName());
    } catch (SourceNotFoundException e) {
      // if state is null, that means the source is registered in namespace, but the plugin is not yet available
      // we should ignore the source in this case
      logger.debug(String.format("%s not found. Possibly still loading schema info", sourceConfig.getName()));
      state = SourceState.badState(e);
    } catch (RuntimeException e) {
      logger.debug("Failed to get the state of source {}", sourceConfig.getName(), e);
      state = SourceState.badState(e);
    }

    return state;
  }
}

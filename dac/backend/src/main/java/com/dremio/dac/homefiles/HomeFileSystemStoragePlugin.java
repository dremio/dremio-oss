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
package com.dremio.dac.homefiles;

import static java.lang.String.format;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetNotFoundException;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.extensions.ValidateMetadataOption;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.exec.catalog.CurrentSchemaOption;
import com.dremio.exec.catalog.FileConfigOption;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.catalog.SortColumnsOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.MayBeDistFileSystemPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FileSystemUtils;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import javax.inject.Provider;

/** New storage plugin for home files */
public class HomeFileSystemStoragePlugin extends MayBeDistFileSystemPlugin<HomeFileConf> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(HomeFileSystemStoragePlugin.class);

  static final Set<PosixFilePermission> DEFAULT_PERMISSIONS =
      Sets.immutableEnumSet(
          PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE,
          PosixFilePermission.OWNER_EXECUTE);

  public static final String HOME_PLUGIN_NAME = "__home";
  private static final String UPLOADS = "_uploads";
  private static final String STAGING = "_staging";

  private final Path stagingDir;
  private final Path uploadsDir;
  private Object deleteHookKey;

  public HomeFileSystemStoragePlugin(
      final HomeFileConf config,
      final SabotContext context,
      final String name,
      Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
    this.stagingDir =
        config.getPath().resolve(STAGING + "." + context.getDremioConfig().getThisNode());
    this.uploadsDir = config.getPath().resolve(UPLOADS);
  }

  @Override
  public HomeFileConf getConfig() {
    return super.getConfig();
  }

  @Override
  public void start() throws IOException {
    super.start();
    FileSystem fs = getSystemUserFS();
    fs.mkdirs(getConfig().getPath(), DEFAULT_PERMISSIONS);
    fs.mkdirs(stagingDir, DEFAULT_PERMISSIONS);
    fs.mkdirs(uploadsDir, DEFAULT_PERMISSIONS);
    deleteHookKey = FileSystemUtils.deleteOnExit(fs, stagingDir);
  }

  @Override
  public void close() {
    if (deleteHookKey != null) {
      try {
        if (getSystemUserFS().exists(stagingDir)) {
          getSystemUserFS().delete(stagingDir, true);
          FileSystemUtils.cancelDeleteOnExit(deleteHookKey);
        }
      } catch (IOException | RejectedExecutionException ex) {
        logger.warn("Unable to delete staging directory when closing HomeFileSystemPlugin.", ex);
      }
      deleteHookKey = null;
    }
    super.close();
  }

  @Override
  public MetadataValidity validateMetadata(
      BytesOutput signature,
      DatasetHandle datasetHandle,
      DatasetMetadata metadata,
      ValidateMetadataOption... options)
      throws DatasetNotFoundException {
    return MetadataValidity.VALID;
  }

  @Override
  public BytesOutput provideSignature(DatasetHandle datasetHandle, DatasetMetadata metadata)
      throws ConnectorException {
    return BytesOutput.NONE;
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(
      EntityPath datasetPath, GetDatasetOption... options) throws ConnectorException {
    if (datasetPath.size() <= 1) {
      return Optional.empty();
    }
    NamespaceKey namespaceKey = MetadataObjectsUtils.toNamespaceKey(datasetPath);
    DatasetRetrievalOptions retrievalOptions = DatasetRetrievalOptions.of(options);

    FileConfig fileConfig = FileConfigOption.getFileConfig(options);
    PreviousDatasetInfo oldConfig =
        new PreviousDatasetInfo(
            fileConfig,
            CurrentSchemaOption.getSchema(options),
            SortColumnsOption.getSortColumns(options),
            null,
            null,
            true);
    FormatPluginConfig pluginConfig = null;
    try {
      final FileSystem fs = getSystemUserFS();

      if (fileConfig == null) {
        final DatasetConfig datasetConfig =
            getContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getDataset(namespaceKey);
        if (datasetConfig.getPhysicalDataset() == null) {
          throw UserException.validationError()
              .message("not a valid physical dataset")
              .buildSilently();
        } else {
          fileConfig = datasetConfig.getPhysicalDataset().getFormatSettings();
        }
      }

      pluginConfig =
          PhysicalDatasetUtils.toFormatPlugin(fileConfig, Collections.<String>emptyList());
      final FormatPlugin formatPlugin = formatCreator.newFormatPlugin(pluginConfig);
      return Optional.ofNullable(
          getDataset(
              namespaceKey,
              oldConfig,
              formatPlugin,
              fs,
              fileConfig,
              retrievalOptions.maxMetadataLeafColumns(),
              retrievalOptions.getTimeTravelRequest()));
    } catch (NamespaceNotFoundException nfe) {
      return Optional.empty();
    } catch (NamespaceException | IOException e) {
      throw new ConnectorException(e);
    }
  }

  @Override
  protected FileDatasetHandle getDatasetWithFormat(
      NamespaceKey datasetPath,
      PreviousDatasetInfo oldConfig,
      FormatPluginConfig formatPluginConfig,
      DatasetRetrievalOptions retrievalOptions,
      String user)
      throws Exception {
    try {
      final FileSystem fs = getSystemUserFS();
      final DatasetConfig datasetConfig =
          getContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getDataset(datasetPath);

      if (!(datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE
          || datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER)) {
        throw new IllegalArgumentException(
            format("Table %s does not belong to home space", datasetPath.toString()));
      }

      final FormatPlugin formatPlugin = formatCreator.newFormatPlugin(formatPluginConfig);

      return getDataset(
          datasetPath,
          oldConfig,
          formatPlugin,
          fs,
          datasetConfig.getPhysicalDataset().getFormatSettings(),
          retrievalOptions.maxMetadataLeafColumns(),
          retrievalOptions.getTimeTravelRequest());
    } catch (NamespaceNotFoundException nfe) {
      if (formatPluginConfig == null) {
        // a home file can only be read from the namespace or using a format options. Without
        // either, it is invalid, return nothing.
        return null;
      }

      // check that the user owns the home path
      final HomeName userHomePath = HomeName.getUserHomePath(user);
      final String stagingHome =
          PathUtils.toDottedPath(Path.mergePaths(Path.of(HOME_PLUGIN_NAME), this.stagingDir));

      final Path path = PathUtils.toFSPath(datasetPath.getPathComponents());
      if (PathUtils.toDottedPath(path).startsWith(stagingHome)) {
        final String userStaging =
            PathUtils.toDottedPath(
                Path.mergePaths(
                    Path.of(HOME_PLUGIN_NAME),
                    Path.mergePaths(this.stagingDir, Path.of(userHomePath.getName()))));

        if (!PathUtils.toDottedPath(path).startsWith(userStaging)) {
          return null;
        }
      }

      return super.getDatasetWithFormat(
          new NamespaceKey(relativePath(datasetPath.getPathComponents(), getConfig().getPath())),
          oldConfig,
          formatPluginConfig,
          retrievalOptions,
          SystemUser.SYSTEM_USERNAME);
    }
  }

  @Override
  public boolean containerExists(EntityPath key, GetMetadataOption... options) {
    List<String> folderPath = key.getComponents();
    try {
      return getSystemUserFS()
          .exists(
              PathUtils.toFSPath(
                  ImmutableList.<String>builder()
                      .addAll(folderPath.subList(1, folderPath.size()))
                      .build()));
    } catch (IOException e) {
      logger.info("IOException while trying to retrieve home files.", e);
      return false;
    }
  }

  private FileDatasetHandle getDataset(
      NamespaceKey datasetPath,
      PreviousDatasetInfo oldConfig,
      FormatPlugin formatPlugin,
      FileSystem fs,
      FileConfig fileConfig,
      int maxLeafColumns,
      TimeTravelOption.TimeTravelRequest travelRequest)
      throws IOException {

    final FileUpdateKey.Builder updateKey = FileUpdateKey.newBuilder();
    final FileAttributes rootAttributes = fs.getFileAttributes(Path.of(fileConfig.getLocation()));
    final Path combined =
        Path.of("/").resolve(PathUtils.removeLeadingSlash(fileConfig.getLocation()));
    final FileSelection fileSelection =
        FileSelection.create(datasetPath.getName(), fs, combined, formatPlugin.getMaxFilesLimit());

    if (fileSelection == null) {
      return null;
    }

    // first entity is always a root
    if (rootAttributes.isDirectory()) {
      updateKey.addCachedEntities(fromFileAttributes(rootAttributes));
    }

    boolean hasDirectories = false;
    for (FileAttributes dirAttributes : fileSelection.getFileAttributesList()) {
      if (dirAttributes.isDirectory()) {
        hasDirectories = true;
        updateKey.addCachedEntities(fromFileAttributes(dirAttributes));
      }
    }

    if (updateKey.getCachedEntitiesCount() == 0) {
      // this is a single file.
      updateKey.addCachedEntities(fromFileAttributes(rootAttributes));
    }
    // Expand selection by copying it first used to check extensions of files in directory.
    final FileSelection fileSelectionWithoutDir =
        hasDirectories ? fileSelection.minusDirectories() : fileSelection;

    if (fileSelectionWithoutDir.isEmpty()) {
      // no files in the found directory, not a table.
      return null;
    }

    return formatPlugin.getDatasetAccessor(
        DatasetType.PHYSICAL_DATASET_HOME_FILE,
        oldConfig,
        fs,
        fileSelectionWithoutDir,
        this,
        datasetPath,
        updateKey.build(),
        maxLeafColumns,
        travelRequest);
  }

  protected FileProtobuf.FileSystemCachedEntity fromFileAttributes(FileAttributes attributes) {
    return FileProtobuf.FileSystemCachedEntity.newBuilder()
        .setPath(attributes.getPath().toString())
        .setLastModificationTime(attributes.lastModifiedTime().toMillis())
        .build();
  }

  private static List<String> relativePath(List<String> tableSchemaPath, Path rootPath) {
    List<String> rootPathComponents = PathUtils.toPathComponents(rootPath);
    List<String> tablePathComponents =
        PathUtils.toPathComponents(PathUtils.toFSPathSkipRoot(tableSchemaPath, null));
    return tablePathComponents.subList(rootPathComponents.size(), tablePathComponents.size());
  }

  @Override
  public boolean supportsIcebergTables() {
    return false;
  }
}

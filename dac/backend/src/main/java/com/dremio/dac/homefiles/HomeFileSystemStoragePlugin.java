/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.inject.Provider;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetNotFoundException;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.extensions.ValidateMetadataOption;
import com.dremio.exec.catalog.CurrentSchemaOption;
import com.dremio.exec.catalog.FileConfigOption;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.catalog.SortColumnsOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;

/**
 * New storage plugin for home files
 */
public class HomeFileSystemStoragePlugin extends FileSystemPlugin<HomeFileConf> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HomeFileSystemStoragePlugin.class);

  static final FsPermission DEFAULT_PERMISSIONS = new FsPermission(FsAction.ALL, FsAction.READ, FsAction.EXECUTE);
  public static final String HOME_PLUGIN_NAME = "__home";
  private static final String UPLOADS = "_uploads";
  private static final String STAGING = "_staging";

  private final Path stagingDir;
  private final Path uploadsDir;

  public HomeFileSystemStoragePlugin(final HomeFileConf config, final SabotContext context, final String name, Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
    this.stagingDir = new Path(config.getPath(), STAGING + "." + context.getDremioConfig().getThisNode());
    this.uploadsDir = new Path(config.getPath(), UPLOADS);
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
    fs.deleteOnExit(stagingDir);
  }

  @Override
  public MetadataValidity validateMetadata(BytesOutput signature, DatasetHandle datasetHandle, DatasetMetadata metadata,
      ValidateMetadataOption... options) throws DatasetNotFoundException {
    return MetadataValidity.VALID;
  }

  @Override
  public BytesOutput provideSignature(DatasetHandle datasetHandle, DatasetMetadata metadata) throws ConnectorException {
    return BytesOutput.NONE;
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) throws ConnectorException {
    if (datasetPath.size() <= 1) {
      return Optional.empty();
    }
    NamespaceKey namespaceKey = MetadataObjectsUtils.toNamespaceKey(datasetPath);
    DatasetRetrievalOptions retrievalOptions = DatasetRetrievalOptions.of(options);

    FileConfig fileConfig = FileConfigOption.getFileConfig(options);
    PreviousDatasetInfo oldConfig = new PreviousDatasetInfo(fileConfig, CurrentSchemaOption.getSchema(options), SortColumnsOption.getSortColumns(options));
    FormatPluginConfig pluginConfig = null;
    try {
      final FileSystemWrapper fs = getSystemUserFS();

      if (fileConfig == null) {
        final DatasetConfig datasetConfig = getContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getDataset(namespaceKey);
        fileConfig = datasetConfig.getPhysicalDataset().getFormatSettings();
      }

      pluginConfig = PhysicalDatasetUtils.toFormatPlugin(fileConfig, Collections.<String>emptyList());
      final FormatPlugin formatPlugin = formatCreator.newFormatPlugin(pluginConfig);
      return Optional.ofNullable(getDataset(namespaceKey, oldConfig, formatPlugin, fs, fileConfig, retrievalOptions.maxMetadataLeafColumns()));
    } catch (NamespaceNotFoundException nfe) {
      return Optional.empty();
    } catch (NamespaceException | IOException e) {
      throw new ConnectorException(e);
    }
  }

  @Override
  protected FileDatasetHandle getDatasetWithFormat(NamespaceKey datasetPath, PreviousDatasetInfo oldConfig, FormatPluginConfig formatPluginConfig, DatasetRetrievalOptions retrievalOptions, String user) throws Exception {
    try{
      final FileSystemWrapper fs = getSystemUserFS();
      final DatasetConfig datasetConfig = getContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getDataset(datasetPath);

      if (!(datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE || datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER)) {
        throw new IllegalArgumentException(format("Table %s does not belong to home space", datasetPath.toString()));
      }

      final FormatPlugin formatPlugin = formatCreator.newFormatPlugin(formatPluginConfig);


      return getDataset(datasetPath, oldConfig, formatPlugin, fs, datasetConfig.getPhysicalDataset().getFormatSettings(), retrievalOptions.maxMetadataLeafColumns());
    } catch (NamespaceNotFoundException nfe){
      if(formatPluginConfig == null) {
        // a home file can only be read from the namespace or using a format options. Without either, it is invalid, return nothing.
        return null;
      }
      return super.getDatasetWithFormat(new NamespaceKey(relativePath(datasetPath.getPathComponents(), getConfig().getPath())), oldConfig, formatPluginConfig, retrievalOptions, null);
    }
  }


  @Override
  public boolean containerExists(EntityPath key) {
    List<String> folderPath = key.getComponents();
    try {
    return getSystemUserFS().exists(PathUtils.toFSPath(
      ImmutableList.<String>builder()
        .addAll(folderPath.subList(1, folderPath.size()))
        .build()));
    }catch(IOException e) {
      logger.info("IOException while trying to retrieve home files.", e);
      return false;
    }
  }

  private FileDatasetHandle getDataset(
      NamespaceKey datasetPath,
      PreviousDatasetInfo oldConfig,
      FormatPlugin formatPlugin,
      FileSystemWrapper fs,
      FileConfig fileConfig,
      int maxLeafColumns
  ) throws IOException {

    final FileUpdateKey.Builder updateKey = FileUpdateKey.newBuilder();
    final FileStatus rootStatus = fs.getFileStatus(new Path(fileConfig.getLocation()));
    final Path combined = new Path("/", PathUtils.removeLeadingSlash(fileConfig.getLocation()));
    final FileSelection fileSelection = FileSelection.create(fs, combined);

    if (fileSelection == null) {
      return null;
    }

    // first entity is always a root
    if (rootStatus.isDirectory()) {
      updateKey.addCachedEntities(fromFileStatus(rootStatus));
    }

    for (FileStatus dirStatus: fileSelection.getAllDirectories()) {
      updateKey.addCachedEntities(fromFileStatus(dirStatus));
    }

    if(updateKey.getCachedEntitiesCount() == 0){
      // this is a single file.
      updateKey.addCachedEntities(fromFileStatus(rootStatus));
    }
    final boolean hasDirectories = fileSelection.containsDirectories();
    // Expand selection by copying it first used to check extensions of files in directory.
    final FileSelection fileSelectionWithoutDir =  hasDirectories? fileSelection.minusDirectories(): fileSelection;

    if(fileSelectionWithoutDir == null){
      // no files in the found directory, not a table.
      return null;
    }

    FileDatasetHandle.checkMaxFiles(datasetPath.getName(), fileSelectionWithoutDir.getStatuses().size(), getContext(),
      getConfig().isInternal());
    return formatPlugin.getDatasetAccessor(DatasetType.PHYSICAL_DATASET_HOME_FILE, oldConfig, fs,
      fileSelectionWithoutDir, this, datasetPath, updateKey.build(), maxLeafColumns);
  }

  private static List<String> relativePath(List<String> tableSchemaPath, Path rootPath) {
    List<String> rootPathComponents = PathUtils.toPathComponents(rootPath);
    List<String> tablePathComponents = PathUtils.toPathComponents(PathUtils.toFSPathSkipRoot(tableSchemaPath, null));
    return tablePathComponents.subList(rootPathComponents.size(), tablePathComponents.size());
  }
}

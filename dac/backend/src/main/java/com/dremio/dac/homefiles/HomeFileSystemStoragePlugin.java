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

import javax.inject.Provider;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileSystemCachedEntity;
import com.dremio.service.namespace.file.proto.FileUpdateKey;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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
  public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldConfig, DatasetRetrievalOptions retrievalOptions) throws Exception {
    if (datasetPath.size() <= 1) {
      return null;
    }

    PhysicalDataset physicalDataset = oldConfig == null ? null : oldConfig.getPhysicalDataset();
    FormatPluginConfig pluginConfig = null;
    try {
      final FileSystemWrapper fs = getSystemUserFS();
      final FileConfig fileConfig;

      if(physicalDataset != null && physicalDataset.getFormatSettings() != null){
        fileConfig = physicalDataset.getFormatSettings();
      }else{
        final DatasetConfig datasetConfig =getContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getDataset(datasetPath);

        if (!(datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE || datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER)) {
          throw new IllegalArgumentException(format("Table %s does not belong to home space", datasetPath.toString()));
        }
        fileConfig = datasetConfig.getPhysicalDataset().getFormatSettings();
      }

      pluginConfig = PhysicalDatasetUtils.toFormatPlugin(fileConfig, Collections.<String>emptyList());
      final FormatPlugin formatPlugin = formatCreator.newFormatPlugin(pluginConfig);
      return getDataset(datasetPath, oldConfig, formatPlugin, fs, fileConfig,
        retrievalOptions.maxMetadataLeafColumns());
    } catch (NamespaceNotFoundException nfe){
      FormatPluginConfig formatPluginConfig = null;
      if(physicalDataset != null && physicalDataset.getFormatSettings() != null){
        formatPluginConfig = PhysicalDatasetUtils.toFormatPlugin(physicalDataset.getFormatSettings(), Collections.<String>emptyList());
      }
      return getDatasetWithFormat(datasetPath, oldConfig, formatPluginConfig, retrievalOptions, null);
    }
  }

  @Override
  protected SourceTableDefinition getDatasetWithFormat(NamespaceKey datasetPath, DatasetConfig oldConfig, FormatPluginConfig formatPluginConfig,
                                                       DatasetRetrievalOptions retrievalOptions, String user) throws Exception {
    try{
      final FileSystemWrapper fs = getSystemUserFS();
      final DatasetConfig datasetConfig = getContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getDataset(datasetPath);

      if (!(datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE || datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER)) {
        throw new IllegalArgumentException(format("Table %s does not belong to home space", datasetPath.toString()));
      }

      final FormatPlugin formatPlugin = formatCreator.newFormatPlugin(formatPluginConfig);

      return getDataset(datasetPath, oldConfig, formatPlugin, fs,
        datasetConfig.getPhysicalDataset().getFormatSettings(), retrievalOptions.maxMetadataLeafColumns());
    } catch (NamespaceNotFoundException nfe){
      if(formatPluginConfig == null) {
        // a home file can only be read from the namespace or using a format options. Without either, it is invalid, return nothing.
        return null;
      }
      return super.getDatasetWithFormat(new NamespaceKey(relativePath(datasetPath.getPathComponents(), getConfig().getPath())), oldConfig, formatPluginConfig, retrievalOptions, null);
    }
  }


  @Override
  public boolean containerExists(NamespaceKey key) {
    List<String> folderPath = key.getPathComponents();
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

  private SourceTableDefinition getDataset(
    NamespaceKey datasetPath,
    DatasetConfig oldConfig,
    FormatPlugin formatPlugin,
    FileSystemWrapper fs,
    FileConfig fileConfig,
    int maxLeafColumns
  ) throws IOException {

    final List<FileSystemCachedEntity> cachedEntities = Lists.newArrayList();
    final FileStatus rootStatus = fs.getFileStatus(new Path(fileConfig.getLocation()));
    final Path combined = new Path("/", PathUtils.removeLeadingSlash(fileConfig.getLocation()));
    final FileSelection fileSelection = FileSelection.create(fs, combined);

    if (fileSelection == null) {
      return null;
    }

    // first entity is always a root
    if (rootStatus.isDirectory()) {
      cachedEntities.add(fromFileStatus(rootStatus));
    }

    for (FileStatus dirStatus: fileSelection.getAllDirectories()) {
      cachedEntities.add(fromFileStatus(dirStatus));
    }

    if(cachedEntities.isEmpty()){
      // this is a single file.
      cachedEntities.add(fromFileStatus(rootStatus));
    }
    final FileUpdateKey updateKey = new FileUpdateKey().setCachedEntitiesList(cachedEntities);
    final boolean hasDirectories = fileSelection.containsDirectories();
    // Expand selection by copying it first used to check extensions of files in directory.
    final FileSelection fileSelectionWithoutDir =  hasDirectories? fileSelection.minusDirectories(): fileSelection;

    if(fileSelectionWithoutDir == null){
      // no files in the found directory, not a table.
      return null;
    }
    return formatPlugin.getDatasetAccessor(oldConfig, fs, fileSelectionWithoutDir, this, datasetPath, updateKey, maxLeafColumns);
  }

  private static List<String> relativePath(List<String> tableSchemaPath, Path rootPath) {
    List<String> rootPathComponents = PathUtils.toPathComponents(rootPath);
    List<String> tablePathComponents = PathUtils.toPathComponents(PathUtils.toFSPathSkipRoot(tableSchemaPath, null));
    return tablePathComponents.subList(rootPathComponents.size(), tablePathComponents.size());
  }
}

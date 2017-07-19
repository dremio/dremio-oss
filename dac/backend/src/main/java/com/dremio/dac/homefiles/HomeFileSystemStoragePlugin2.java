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
package com.dremio.dac.homefiles;

import static java.lang.String.format;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemStoragePlugin2;
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
import com.google.common.collect.Lists;

/**
 * New storage plugin for home files
 */
public class HomeFileSystemStoragePlugin2 extends FileSystemStoragePlugin2 {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HomeFileSystemStoragePlugin2.class);

  public HomeFileSystemStoragePlugin2(String name, FileSystemConfig fileSystemConfig, FileSystemPlugin fsPlugin) {
    super(name, fileSystemConfig, fsPlugin);
  }

  @Override
  public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldConfig, boolean ignoreAuthErrors) throws Exception {
    if (datasetPath.size() <= 1) {
      return null;
    }

    PhysicalDataset physicalDataset = oldConfig == null ? null : oldConfig.getPhysicalDataset();
    FormatPluginConfig pluginConfig = null;
    try {
      final FileSystemWrapper fs = getFs();
      final FileConfig fileConfig;

      if(physicalDataset != null && physicalDataset.getFormatSettings() != null){
        fileConfig = physicalDataset.getFormatSettings();
      }else{
        final DatasetConfig datasetConfig = getFsPlugin().getContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getDataset(datasetPath);

        if (!(datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE || datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER)) {
          throw new IllegalArgumentException(format("Table %s does not belong to home space", datasetPath.toString()));
        }
        fileConfig = datasetConfig.getPhysicalDataset().getFormatSettings();
      }

      pluginConfig = PhysicalDatasetUtils.toFormatPlugin(fileConfig, Collections.<String>emptyList());
      final FormatPlugin formatPlugin = getFsPlugin().getFormatCreator().newFormatPlugin(pluginConfig);
      return getDataset(datasetPath, oldConfig, formatPlugin, fs, fileConfig);
    } catch (NamespaceNotFoundException nfe){
      FormatPluginConfig formatPluginConfig = null;
      if(physicalDataset != null && physicalDataset.getFormatSettings() != null){
        formatPluginConfig = PhysicalDatasetUtils.toFormatPlugin(physicalDataset.getFormatSettings(), Collections.<String>emptyList());
      }
      return getDatasetWithFormat(datasetPath, oldConfig, formatPluginConfig, ignoreAuthErrors, null);
    }
  }

  @Override
  protected SourceTableDefinition getDatasetWithFormat(NamespaceKey datasetPath, DatasetConfig oldConfig, FormatPluginConfig formatPluginConfig,
    boolean ignoreAuthErrors, String user) throws Exception {
    try{
      final FileSystemWrapper fs = getFs();
      final DatasetConfig datasetConfig = getFsPlugin().getContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getDataset(datasetPath);

      if (!(datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE || datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER)) {
        throw new IllegalArgumentException(format("Table %s does not belong to home space", datasetPath.toString()));
      }

      final FormatPlugin formatPlugin = getFsPlugin().getFormatCreator().newFormatPlugin(formatPluginConfig);

      return getDataset(datasetPath, oldConfig, formatPlugin, fs, datasetConfig.getPhysicalDataset().getFormatSettings());
    } catch (NamespaceNotFoundException nfe){
      return super.getDatasetWithFormat(new NamespaceKey(relativePath(datasetPath.getPathComponents(), getFsPlugin()
        .getConfig().getPath())), oldConfig, formatPluginConfig, ignoreAuthErrors, null);
    }
  }

  private SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldConfig, FormatPlugin formatPlugin, FileSystemWrapper fs, FileConfig fileConfig) throws IOException {
    final FileSelection fileSelection = FileSelection.create(fs, "/", fileConfig.getLocation());
    final List<FileSystemCachedEntity> cachedEntities = Lists.newArrayList();
    final FileStatus rootStatus = fs.getFileStatus(new Path(fileConfig.getLocation()));

    if (fileSelection == null) {
      return null;
    }

    // first entity is always a root
    if (rootStatus.isDirectory()) {
      cachedEntities.add(fromFileStatus(rootStatus));
    }

    for (FileStatus dirStatus: fileSelection.getAllDirectories(fs)) {
      cachedEntities.add(fromFileStatus(dirStatus));
    }

    if(cachedEntities.isEmpty()){
      // this is a single file.
      cachedEntities.add(fromFileStatus(rootStatus));
    }
    final FileUpdateKey updateKey = new FileUpdateKey().setCachedEntitiesList(cachedEntities);
    final boolean hasDirectories = fileSelection.containsDirectories(fs);
    // Expand selection by copying it first used to check extensions of files in directory.
    final FileSelection fileSelectionWithoutDir =  hasDirectories? fileSelection.minusDirectories(fs): fileSelection;

    if(fileSelectionWithoutDir == null){
      // no files in the found directory, not a table.
      return null;
    }
    return formatPlugin.getDatasetAccessor(oldConfig, fs, fileSelectionWithoutDir, getFsPlugin(), datasetPath, datasetPath.getName(), updateKey);
  }

  private List<String> relativePath(List<String> tableSchemaPath, String rootPath) {
    List<String> rootPathComponents = PathUtils.toPathComponents(new Path(rootPath));
    List<String> tablePathComponents = PathUtils.toPathComponents(PathUtils.toFSPathSkipRoot(tableSchemaPath, null));
    return tablePathComponents.subList(rootPathComponents.size(), tablePathComponents.size());
  }
}

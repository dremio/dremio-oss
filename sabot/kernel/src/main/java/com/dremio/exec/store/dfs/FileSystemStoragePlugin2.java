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
package com.dremio.exec.store.dfs;

import static com.dremio.exec.store.dfs.easy.EasyDatasetXAttrSerDe.EASY_DATASET_SPLIT_XATTR_SERIALIZER;
import static com.dremio.exec.store.parquet.ParquetDatasetXAttrSerDe.PARQUET_DATASET_SPLIT_XATTR_SERIALIZER;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;
import org.apache.parquet.Preconditions;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.SplitsPointerImpl;
import com.dremio.exec.store.StoragePlugin2;
import com.dremio.exec.store.StoragePluginInstanceRulesFactory;
import com.dremio.exec.store.StoragePluginTypeRulesFactory;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.StoragePluginType;
import com.dremio.service.namespace.TableInstance;
import com.dremio.service.namespace.capabilities.BooleanCapabilityValue;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileSystemCachedEntity;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.FileUpdateKey;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;
import io.protostuff.ProtostuffIOUtil;

/**
 * Storage plugin for file system
 */
public class FileSystemStoragePlugin2 implements StoragePlugin2 {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemStoragePlugin2.class);

  private static final BooleanCapabilityValue REQUIRES_HARD_AFFINITY = new BooleanCapabilityValue(SourceCapabilities.REQUIRES_HARD_AFFINITY, true);

  private final FileSystemPlugin fsPlugin;
  private final String name;
  private final FileSystemConfig fileSystemConfig;
  private final FileSystemWrapper fs;
  private final FormatPluginOptionExtractor optionExtractor;

  public FileSystemStoragePlugin2(final String name, FileSystemConfig fileSystemConfig, FileSystemPlugin fsPlugin) {
    this.name = name;
    this.fileSystemConfig = fileSystemConfig;
    this.fsPlugin = fsPlugin;
    this.fs = fsPlugin.getFS(SYSTEM_USERNAME);
    this.optionExtractor = fsPlugin.getOptionExtractor();
  }

  @Override
  public Iterable<SourceTableDefinition> getDatasets(String user, boolean ignoreAuthErrors) throws Exception {
    return Collections.emptyList(); // file system does not know about physical datasets
  }

  @Override
  public StoragePluginId getId() {
    if(name == null){
      throw new IllegalStateException("Attempted to get the id for an ephemeral storage plugin.");
    }
    final StoragePluginType pluginType = new StoragePluginType("dfs", fsPlugin.getContext().getConfig().getClass("dremio.plugins.dfs.rulesfactory", StoragePluginTypeRulesFactory.class, FileSystemRulesFactory.class));
    return fs.isPdfs() ?
        new StoragePluginId(name, fileSystemConfig, new SourceCapabilities(REQUIRES_HARD_AFFINITY), pluginType) :
          new StoragePluginId(name, fileSystemConfig, SourceCapabilities.NONE, pluginType);
  }

  @Override
  public Class<? extends StoragePluginInstanceRulesFactory> getRulesFactoryClass() {
    return null;
  }

  @Override
  public SourceState getState() {
    return fsPlugin.getState();
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return fsPlugin.getView(tableSchemaPath, schemaConfig);
  }

  private List<String> getFullPath(List<String> tableSchemaPath) {
    String parentPath = fileSystemConfig.getPath();
    List<String> fullPath = new ArrayList<>();
    fullPath.addAll(PathUtils.toPathComponents(new Path(parentPath)));
    for (String pathComponent : tableSchemaPath.subList(1, tableSchemaPath.size())) {
      fullPath.add(PathUtils.removeQuotes(pathComponent));
    }
    return fullPath;
  }

  @Override
  public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldConfig, boolean ignoreAuthErrors) throws Exception {
    FormatPluginConfig formatPluginConfig = null;

    PhysicalDataset physicalDataset = oldConfig == null ? null : oldConfig.getPhysicalDataset();
    if(physicalDataset != null && physicalDataset.getFormatSettings() != null){
      formatPluginConfig = PhysicalDatasetUtils.toFormatPlugin(physicalDataset.getFormatSettings(), Collections.<String>emptyList());
    }

    return getDatasetWithFormat(datasetPath, oldConfig, formatPluginConfig, ignoreAuthErrors, null);
  }

  SourceTableDefinition getDatasetWithOptions(NamespaceKey datasetPath, TableInstance instance, boolean
    ignoreAuthErrors, String user) throws Exception{
    final FormatPluginConfig fconfig = optionExtractor.createConfigForTable(instance);
    return getDatasetWithFormat(datasetPath, null, fconfig, ignoreAuthErrors, user);
  }

  protected SourceTableDefinition getDatasetWithFormat(NamespaceKey datasetPath, DatasetConfig oldConfig, FormatPluginConfig formatPluginConfig,
                                                       boolean ignoreAuthErrors, String user) throws Exception {

    if(datasetPath.size() <= 1){
      return null;  // not a valid table schema path
    }
    final List<String> fullPath = getFullPath(datasetPath.getPathComponents());
    try {
      // TODO: why do we need distinguish between system user and process user?
      final String userName = fileSystemConfig.isImpersonationEnabled() ? SystemUser.SYSTEM_USERNAME : ImpersonationUtil.getProcessUserName();
      List<String> parentSchemaPath = new ArrayList<>(fullPath.subList(0, fullPath.size() - 1));
      FileSystemWrapper fs = fsPlugin.getFS((user != null) ? user : userName);
      FileSelection fileSelection = FileSelection.create(fs, fullPath);
      String tableName = datasetPath.getName();

      if (fileSelection == null) {
        fileSelection = FileSelection.createWithFullSchema(fs, PathUtils.toFSPathString(parentSchemaPath), tableName);
        if (fileSelection == null) {
          return null; // no table found
        } else {
          // table name is a full schema path (tableau use case), parse it and append it to schemapath.
          final List<String> tableNamePathComponents = PathUtils.parseFullPath(tableName);
          tableName = tableNamePathComponents.remove(tableNamePathComponents.size() - 1);
          parentSchemaPath.addAll(tableNamePathComponents);
        }
      }

      final boolean hasDirectories = fileSelection.containsDirectories(fs);
      final boolean isDir = fs.isDirectory(new Path(fileSelection.getSelectionRoot()));
      final String selectionRoot = fileSelection.getSelectionRoot();

      // Get subdirectories under file selection before pruning directories
      final List<FileSystemCachedEntity> cachedEntities = Lists.newArrayList();
      if (isDir) {
        // first entity is always a root
        final FileStatus rootStatus = fs.getFileStatus(new Path(selectionRoot));
        cachedEntities.add(fromFileStatus(rootStatus));
      }

      for (FileStatus dirStatus: fileSelection.getAllDirectories(fs)) {
        cachedEntities.add(fromFileStatus(dirStatus));
      }

      final FileUpdateKey updateKey = new FileUpdateKey().setCachedEntitiesList(cachedEntities);
      // Expand selection by copying it first used to check extensions of files in directory.
      final FileSelection fileSelectionWithoutDir =  hasDirectories? new FileSelection(fileSelection).minusDirectories(fs): fileSelection;
      if(fileSelectionWithoutDir == null){
        // no files in the found directory, not a table.
        return null;
      }

      SourceTableDefinition datasetAccessor = null;

      if (formatPluginConfig != null) {

        FormatPlugin formatPlugin = fsPlugin.getFormatPlugin(formatPluginConfig);
        if(formatPlugin == null){
          formatPlugin = fsPlugin.getFormatCreator().newFormatPlugin(formatPluginConfig);
        }
        datasetAccessor = formatPlugin.getDatasetAccessor(oldConfig, fs, fileSelectionWithoutDir, fsPlugin, datasetPath, tableName, updateKey);
      }

      if (datasetAccessor == null) {
        for (final FormatMatcher matcher : fsPlugin.getMatchers()) {
          try {
            if (matcher.matches(fs, fileSelection.getFirstPath(fs), fsPlugin.getCodecFactory())) {
              datasetAccessor = matcher.getFormatPlugin().getDatasetAccessor(oldConfig, fs, fileSelectionWithoutDir, fsPlugin, datasetPath, tableName, updateKey);
              if (datasetAccessor != null) {
                break;
              }
            }
          } catch (IOException e) {
            logger.debug("File read failed.", e);
          }
        }
      }

      if (datasetAccessor == null) {
        for (final FormatMatcher matcher : fsPlugin.getMatchers()) {
          try {
            if (matcher.matches(fs, fileSelectionWithoutDir.getFirstPath(fs), fsPlugin.getCodecFactory())) {
              datasetAccessor = matcher.getFormatPlugin().getDatasetAccessor(oldConfig, fs, fileSelectionWithoutDir, fsPlugin, datasetPath, tableName, updateKey);
              if (datasetAccessor != null) {
                break;
              }
            }
          } catch (IOException e) {
            logger.debug("File read failed.", e);
          }
        }
      }
      return datasetAccessor;
    } catch (AccessControlException e) {
      if (!ignoreAuthErrors) {
        logger.debug(e.getMessage());
        throw UserException.permissionError(e)
          .message("Not authorized to read table %s at path ", datasetPath)
          .build(logger);
      }
    } catch (IOException e) {
      logger.debug("Failed to create table {}", datasetPath, e);
    }
    return null;
  }

  protected FileSystemCachedEntity fromFileStatus(FileStatus status){
    return new FileSystemCachedEntity()
        .setPath(status.getPath().toString())
        .setLastModificationTime(status.getModificationTime());
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    if (fsPlugin.getConfig().isImpersonationEnabled()) {
      if (datasetConfig.getReadDefinition() != null) { // allow accessing partial datasets
        final FileSystemWrapper userFs = fsPlugin.getFS(user);
        return hasAccessPermissionReadSignature(datasetConfig, userFs) &&
          hasAccessPermissionSplits(datasetConfig, userFs, user);
      }
    }
    return true;
  }

  // Check if all sub directories can be listed/read
  private boolean hasAccessPermissionReadSignature(DatasetConfig datasetConfig, FileSystemWrapper userFs) {
    final FileUpdateKey fileUpdateKey = new FileUpdateKey();
    ProtostuffIOUtil.mergeFrom(datasetConfig.getReadDefinition().getReadSignature().toByteArray(), fileUpdateKey, fileUpdateKey.getSchema());
    if (fileUpdateKey.getCachedEntitiesList() == null || fileUpdateKey.getCachedEntitiesList().isEmpty()) {
      return true;
    }
    for (FileSystemCachedEntity cachedEntity : fileUpdateKey.getCachedEntitiesList()) {
      final Path cachedEntityPath = new Path(cachedEntity.getPath());
      try {
        //DX-7850 : remove once solution for maprfs is found
        if (userFs.isMapRfs()) {
          userFs.access(cachedEntityPath, FsAction.READ);
        } else {
          userFs.access(cachedEntityPath, FsAction.READ_EXECUTE);
        }
      } catch (AccessControlException ace) {
        return false;
      } catch (FileNotFoundException fnfe) {
        throw UserException.dataReadError(fnfe).message(String.format("Missing file %s under dataset", cachedEntityPath)).build(logger);
      } catch (IOException ioe) {
        throw UserException.dataReadError(ioe).message(String.format("Error reading file %s", cachedEntityPath)).build(logger);
      }
    }
    return true;
  }

  // Check if all splits are accessible
  private boolean hasAccessPermissionSplits(DatasetConfig datasetConfig, FileSystemWrapper userFs, String user) {
    final SplitsPointer splitsPointer = new SplitsPointerImpl(datasetConfig, fsPlugin.getContext().getNamespaceService(user));
    final boolean isParquet = datasetConfig.getPhysicalDataset().getFormatSettings().getType() == FileType.PARQUET;
    for (DatasetSplit split:  splitsPointer.getSplitIterable()) {
      Path filePath;
      if (isParquet) {
        filePath = new Path(PARQUET_DATASET_SPLIT_XATTR_SERIALIZER.revert(split.getExtendedProperty().toByteArray()).getPath());
      } else {
        filePath = new Path(EASY_DATASET_SPLIT_XATTR_SERIALIZER.revert(split.getExtendedProperty().toByteArray()).getPath());
      }
      try {
        userFs.access(filePath, FsAction.READ);
      } catch (AccessControlException ace) {
        return false;
      } catch (FileNotFoundException fnfe) {
        throw UserException.dataReadError(fnfe).message(String.format("Missing file %s under dataset", filePath)).build(logger);
      } catch (IOException ioe) {
        throw UserException.dataReadError(ioe).message(String.format("Error reading file %s", filePath)).build(logger);
      }
    }
    return true;
  }

  public FileSystemPlugin getFsPlugin() {
    return fsPlugin;
  }

  public FileSystemConfig getFileSystemConfig() {
    return fileSystemConfig;
  }

  public FileSystemWrapper getFs() {
    return fs;
  }

  public String getName() {
    return name;
  }

  public List<DatasetConfig> listDatasets() {
    return fsPlugin.listDatasets();
  }

  @Override
  public boolean containerExists(NamespaceKey key) {
    final List<String> keys = key.getPathComponents();
    try {
      return fsPlugin.folderExists(SYSTEM_USERNAME, keys);
    } catch (IOException e) {
      logger.debug("Failure reading path.", e);
      return false;
    }
  }

  @Override
  public boolean datasetExists(NamespaceKey key) {
    final List<String> keys = key.getPathComponents();
    try {
      return fsPlugin.exists(SYSTEM_USERNAME, keys);
    } catch (IOException e) {
      logger.debug("Failure reading path.", e);
      return false;
    }
  }

  @Override
  public CheckResult checkReadSignature(ByteString key, DatasetConfig oldConfig) throws Exception {
    FileUpdateKey fileUpdateKey = new FileUpdateKey();
    ProtostuffIOUtil.mergeFrom(key.toByteArray(), fileUpdateKey, fileUpdateKey.getSchema());

    if (fileUpdateKey.getCachedEntitiesList() == null || fileUpdateKey.getCachedEntitiesList().isEmpty()) {
      // single file dataset
      Preconditions.checkArgument(oldConfig.getType() == DatasetType.PHYSICAL_DATASET_SOURCE_FILE, "only file based datasets can have empty read signature");
      if (!fsPlugin.fileExists(SYSTEM_USERNAME, oldConfig.getFullPathList())) {
        return CheckResult.DELETED;
      } else {
        // assume file has changed
        final SourceTableDefinition newDatasetAccessor = getDataset(new NamespaceKey(oldConfig.getFullPathList()), oldConfig, false);
        return new CheckResult() {
          @Override
          public UpdateStatus getStatus() {
            return UpdateStatus.CHANGED;
          }

          @Override
          public SourceTableDefinition getDataset() {
            return newDatasetAccessor;
          }
        };
      }
    }

    boolean newUpdateKey = false;
    final List<FileSystemCachedEntity> cachedEntities = fileUpdateKey.getCachedEntitiesList();
    for (int i = 0; i < cachedEntities.size(); ++i) {
      final FileSystemCachedEntity cachedEntity = cachedEntities.get(i);
      final Path cachedEntityPath =  new Path(cachedEntity.getPath());
      try {
        if (fs.exists(cachedEntityPath)) {
          FileStatus fileStatus = fs.getFileStatus(cachedEntityPath);
          final long modificationTime = fileStatus.getModificationTime();
          Preconditions.checkArgument(fileStatus.isDirectory(), "fs based dataset update key must be composed of directories");
          if (cachedEntity.getLastModificationTime() < modificationTime || modificationTime == 0) {
            newUpdateKey = true;
          }
        } else {
          // if first entity (root) is missing then table is deleted
          if (i == 0) {
            return CheckResult.DELETED;
          }
          // missing directory force update for this dataset
          newUpdateKey = true;
        }
      } catch (IOException ioe) {
        // continue with other cached entities
        logger.error("Failed to get status for {}", cachedEntityPath, ioe);
      }
    }
    if (newUpdateKey) {
        final SourceTableDefinition newDatasetAccessor = getDataset(new NamespaceKey(oldConfig.getFullPathList()), oldConfig, false);
        return new CheckResult() {
          @Override
          public UpdateStatus getStatus() {
            return UpdateStatus.CHANGED;
          }

          @Override
          public SourceTableDefinition getDataset() {
            return newDatasetAccessor;
          }
        };
      }
    return CheckResult.UNCHANGED;
  }
}

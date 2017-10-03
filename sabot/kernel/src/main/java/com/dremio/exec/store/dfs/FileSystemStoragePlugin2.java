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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import com.dremio.exec.store.TimedRunnable;
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
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;
import io.protostuff.ProtostuffIOUtil;

/**
 * Storage plugin for file system
 */
public class FileSystemStoragePlugin2 implements StoragePlugin2 {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemStoragePlugin2.class);

  private static final BooleanCapabilityValue REQUIRES_HARD_AFFINITY = new BooleanCapabilityValue(SourceCapabilities.REQUIRES_HARD_AFFINITY, true);

  private static final int PERMISSION_CHECK_TASK_BATCH_SIZE = 10;

  public static final String FILESYSTEM_TYPE_NAME = "dfs";

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
    final StoragePluginType pluginType = new StoragePluginType(FILESYSTEM_TYPE_NAME, fsPlugin.getContext().getConfig().getClass("dremio.plugins.dfs.rulesfactory", StoragePluginTypeRulesFactory.class, FileSystemRulesFactory.class));
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

      final boolean hasDirectories = fileSelection.containsDirectories();
      final FileStatus rootStatus = fs.getFileStatus(new Path(fileSelection.getSelectionRoot()));

      // Get subdirectories under file selection before pruning directories
      final List<FileSystemCachedEntity> cachedEntities = Lists.newArrayList();
      if (rootStatus.isDirectory()) {
        // first entity is always a root
        cachedEntities.add(fromFileStatus(rootStatus));
      }

      for (FileStatus dirStatus: fileSelection.getAllDirectories()) {
        cachedEntities.add(fromFileStatus(dirStatus));
      }

      final FileUpdateKey updateKey = new FileUpdateKey().setCachedEntitiesList(cachedEntities);
      // Expand selection by copying it first used to check extensions of files in directory.
      final FileSelection fileSelectionWithoutDir =  hasDirectories? new FileSelection(fileSelection).minusDirectories(): fileSelection;
      if(fileSelectionWithoutDir == null || fileSelectionWithoutDir.isEmpty()){
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
            if (matcher.matches(fs, fileSelection, fsPlugin.getCodecFactory())) {
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
        final List<TimedRunnable<Boolean>> permissionCheckTasks = Lists.newArrayList();

        permissionCheckTasks.addAll(getUpdateKeyPermissionTasks(datasetConfig, userFs));
        permissionCheckTasks.addAll(getSplitPermissiomTasks(datasetConfig, userFs, user));

        try {
          Stopwatch stopwatch = Stopwatch.createStarted();
          final List<Boolean> accessPermissions = TimedRunnable.run("check access permission for " + key, logger, permissionCheckTasks, 16);
          stopwatch.stop();
          logger.debug("Checking access permission for {} took {} ms", key, stopwatch.elapsed(TimeUnit.MILLISECONDS));
          for (Boolean permission : accessPermissions) {
            if (!permission) {
              return false;
            }
          }
        } catch (IOException ioe) {
          throw UserException.dataReadError(ioe).build(logger);
        }
      }
    }
    return true;
  }

  // Check if all sub directories can be listed/read
  private Collection<FsPermissionTask> getUpdateKeyPermissionTasks(DatasetConfig datasetConfig, FileSystemWrapper userFs) {
    final FileUpdateKey fileUpdateKey = new FileUpdateKey();
    ProtostuffIOUtil.mergeFrom(datasetConfig.getReadDefinition().getReadSignature().toByteArray(), fileUpdateKey, fileUpdateKey.getSchema());
    if (fileUpdateKey.getCachedEntitiesList() == null || fileUpdateKey.getCachedEntitiesList().isEmpty()) {
      return Collections.emptyList();
    }
    final List<FsPermissionTask> fsPermissionTasks = Lists.newArrayList();
    final FsAction action;
    final List<Path> batch = Lists.newArrayList();

    //DX-7850 : remove once solution for maprfs is found
    if (userFs.isMapRfs()) {
      action = FsAction.READ;
    } else {
      action = FsAction.READ_EXECUTE;
    }

    for (FileSystemCachedEntity cachedEntity : fileUpdateKey.getCachedEntitiesList()) {
      batch.add(new Path(cachedEntity.getPath()));
      if (batch.size() == PERMISSION_CHECK_TASK_BATCH_SIZE) {
        // make a copy of batch
        fsPermissionTasks.add(new FsPermissionTask(userFs, Lists.newArrayList(batch), action));
        batch.clear();
      }
    }
    if (!batch.isEmpty()) {
      fsPermissionTasks.add(new FsPermissionTask(userFs, batch, action));
    }
    return fsPermissionTasks;
  }

  // Check if all splits are accessible
  private Collection<FsPermissionTask> getSplitPermissiomTasks(DatasetConfig datasetConfig, FileSystemWrapper userFs, String user) {
    final SplitsPointer splitsPointer = new SplitsPointerImpl(datasetConfig, fsPlugin.getContext().getNamespaceService(user));
    final boolean isParquet = datasetConfig.getPhysicalDataset().getFormatSettings().getType() == FileType.PARQUET;
    final List<FsPermissionTask> fsPermissionTasks = Lists.newArrayList();
    final List<Path> batch = Lists.newArrayList();

    for (DatasetSplit split:  splitsPointer.getSplitIterable()) {
      final Path filePath;
      if (isParquet) {
        filePath = new Path(PARQUET_DATASET_SPLIT_XATTR_SERIALIZER.revert(split.getExtendedProperty().toByteArray()).getPath());
      } else {
        filePath = new Path(EASY_DATASET_SPLIT_XATTR_SERIALIZER.revert(split.getExtendedProperty().toByteArray()).getPath());
      }

      batch.add(filePath);
      if (batch.size() == PERMISSION_CHECK_TASK_BATCH_SIZE) {
        // make a copy of batch
        fsPermissionTasks.add(new FsPermissionTask(userFs, new ArrayList<>(batch), FsAction.READ));
        batch.clear();
      }
    }

    if (!batch.isEmpty()) {
      fsPermissionTasks.add(new FsPermissionTask(userFs, batch, FsAction.READ));
    }

    return fsPermissionTasks;
  }

  private class FsPermissionTask extends TimedRunnable<Boolean> {
    private final FileSystemWrapper userFs;
    private final List<Path> cachedEntityPaths;
    private final FsAction permission;

    FsPermissionTask(FileSystemWrapper userFs, List<Path> cachedEntityPaths, FsAction permission) {
      this.userFs = userFs;
      this.cachedEntityPaths = cachedEntityPaths;
      this.permission = permission;
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      if (e instanceof IOException) {
        return (IOException) e;
      }
      return new IOException(e);
    }

    @Override
    protected Boolean runInner() throws Exception {
      for (Path cachedEntityPath:  cachedEntityPaths) {
        try {
          userFs.access(cachedEntityPath, permission);
        } catch (AccessControlException ace) {
          return false;
        }
      }
      return true;
    }
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
    ProtostuffIOUtil.mergeFrom(key.toByteArray(), fileUpdateKey, FileUpdateKey.getSchema());

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

    final UpdateStatus status = checkMultifileStatus(fileUpdateKey);
    switch(status) {
    case DELETED:
      return CheckResult.DELETED;
    case UNCHANGED:
      return CheckResult.UNCHANGED;
    case CHANGED:
      // continue below.
      break;
    default:
      throw new UnsupportedOperationException(status.name());
    }


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

  /**
   * Given a file update key, determine whether the source system has changed since we last read the status.
   * @param fileUpdateKey
   * @return The type of status change.
   */
  private UpdateStatus checkMultifileStatus(FileUpdateKey fileUpdateKey) {
    final List<FileSystemCachedEntity> cachedEntities = fileUpdateKey.getCachedEntitiesList();
    for (int i = 0; i < cachedEntities.size(); ++i) {
      final FileSystemCachedEntity cachedEntity = cachedEntities.get(i);
      final Path cachedEntityPath =  new Path(cachedEntity.getPath());
      try {

        final Optional<FileStatus> optionalStatus = fs.getFileStatusSafe(cachedEntityPath);
        if(!optionalStatus.isPresent()) {
          // if first entity (root) is missing then table is deleted
          if (i == 0) {
            return UpdateStatus.DELETED;
          }
          // missing directory force update for this dataset
          return UpdateStatus.CHANGED;
        }

        if(cachedEntity.getLastModificationTime() == 0) {
          // this system doesn't support modification times, no need to further probe (S3)
          return UpdateStatus.CHANGED;
        }

        final FileStatus updatedFileStatus = optionalStatus.get();
        final long updatedModificationTime = updatedFileStatus.getModificationTime();
        Preconditions.checkArgument(updatedFileStatus.isDirectory(), "fs based dataset update key must be composed of directories");
        if (cachedEntity.getLastModificationTime() < updatedModificationTime) {
          // the file/folder has been changed since our last check.
          return UpdateStatus.CHANGED;
        }

      } catch (IOException ioe) {
        // continue with other cached entities
        logger.error("Failed to get status for {}", cachedEntityPath, ioe);
        return UpdateStatus.CHANGED;
      }
    }

    return UpdateStatus.UNCHANGED;
  }
}

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
package com.dremio.exec.store.dfs;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.calcite.schema.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.Preconditions;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.SqlUtils;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetNotFoundException;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.connector.metadata.extensions.ValidateMetadataOption;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CurrentSchemaOption;
import com.dremio.exec.catalog.DatasetSplitsPointer;
import com.dremio.exec.catalog.FileConfigOption;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.SortColumnsOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.dotfile.DotFile;
import com.dremio.exec.dotfile.DotFileType;
import com.dremio.exec.dotfile.DotFileUtil;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.ClassPathFileSystem;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.LocalSyncableFileSystem;
import com.dremio.exec.store.PartitionNotFoundException;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SchemaEntity;
import com.dremio.exec.store.SchemaEntity.SchemaEntityType;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.TimedRunnable;
import com.dremio.exec.store.dfs.SchemaMutability.MutationType;
import com.dremio.exec.store.file.proto.FileProtobuf.FileSystemCachedEntity;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf.EasyDatasetSplitXAttr;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitXAttr;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.TableInstance;
import com.dremio.service.namespace.capabilities.BooleanCapabilityValue;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DatasetSplit;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Storage plugin for file system
 */
public class FileSystemPlugin<C extends FileSystemConf<C, ?>> implements StoragePlugin, MutablePlugin, SupportsReadSignature {
  /**
   * Default {@link Configuration} instance. Use this instance through {@link #getNewFsConf()} to create new copies
   * of {@link Configuration} objects.
   */
  private static final Configuration DEFAULT_CONFIGURATION = new Configuration();

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemPlugin.class);

  private static final BooleanCapabilityValue REQUIRES_HARD_AFFINITY = new BooleanCapabilityValue(SourceCapabilities.REQUIRES_HARD_AFFINITY, true);

  private static final int PERMISSION_CHECK_TASK_BATCH_SIZE = 10;

  private final String name;
  private final LogicalPlanPersistence lpPersistance;
  private final C config;
  private final SabotContext context;
  private final Path basePath;

  private final Provider<StoragePluginId> idProvider;
  private FileSystemWrapper systemUserFS;
  private Configuration fsConf;
  private FormatPluginOptionExtractor optionExtractor;
  protected FormatCreator formatCreator;
  private ArrayList<FormatMatcher> matchers;
  private List<FormatMatcher> dropFileMatchers;
  private CompressionCodecFactory codecFactory;

  public FileSystemPlugin(final C config, final SabotContext context, final String name, Provider<StoragePluginId> idProvider) {
    this.name = name;
    this.config = config;
    this.idProvider = idProvider;
    this.context = context;
    this.fsConf = getNewFsConf();
    this.lpPersistance = context.getLpPersistence();
    this.basePath = config.getPath();
  }

  public C getConfig(){
    return config;
  }

  public boolean supportsColocatedReads() {
    return true;
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return systemUserFS.isPdfs() ? new SourceCapabilities(REQUIRES_HARD_AFFINITY) : SourceCapabilities.NONE;
  }

  public static final Configuration getNewFsConf() {
    return new Configuration(DEFAULT_CONFIGURATION);
  }

  public Configuration getFsConf() {
    return fsConf;
  }

  @Override
  public StoragePluginId getId() {
    return idProvider.get();
  }

  /**
   * Create a new {@link FileSystemWrapper} for a given user.
   *
   * @param userName
   * @return
   */
  public FileSystemWrapper createFS(String userName) {
    return createFs(userName, null);
  }

  public FileSystemWrapper createFs(String userName, OperatorContext context) {
    return ImpersonationUtil.createFileSystem(getUGIForUser(userName), getFsConf(), context != null ?context.getStats() : null,
        getConnectionUniqueProperties(),
      isAsyncEnabledForQuery(context) && getConfig().isAsyncEnabled());
  }

  public FileSystemWrapper getFileSystem(Configuration config, OperatorContext context) throws IOException {
    return FileSystemWrapper.get(config, context.getStats(),
      isAsyncEnabledForQuery(context) && getConfig().isAsyncEnabled());
  }

  public UserGroupInformation getUGIForUser(String userName) {
    if (!config.isImpersonationEnabled()) {
      return ImpersonationUtil.getProcessUserUGI();
    }

    return ImpersonationUtil.createProxyUgi(userName);
  }

  public Iterable<String> getSubPartitions(List<String> table,
                                           List<String> partitionColumns,
                                           List<String> partitionValues,
                                           SchemaConfig schemaConfig
  ) throws PartitionNotFoundException {
    List<FileStatus> fileStatuses;
    try {
      Path fullPath = PathUtils.toFSPath(resolveTableNameToValidPath(table));
      fileStatuses = createFS(schemaConfig.getUserName()).list(fullPath, false);
    } catch (IOException e) {
      throw new PartitionNotFoundException("Error finding partitions for table " + table, e);
    }
    return new SubDirectoryList(fileStatuses);
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return context.getConfig().getClass("dremio.plugins.dfs.rulesfactory", StoragePluginRulesFactory.class, FileSystemRulesFactory.class);
  }

  @Override
  public SourceState getState() {
    if (!systemUserFS.isPdfs()) {
      try {
        systemUserFS.listStatus(config.getPath());
        return SourceState.GOOD;
      } catch (Exception e) {
        return SourceState.badState(e);
      }
    } else {
      return SourceState.GOOD;
    }
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    List<DotFile> files = Collections.emptyList();
    try {
      try {
        files = DotFileUtil.getDotFiles(createFS(schemaConfig.getUserName()), config.getPath(), tableSchemaPath.get(tableSchemaPath.size() - 1), DotFileType.VIEW);
      } catch (AccessControlException e) {
        if (!schemaConfig.getIgnoreAuthErrors()) {
          logger.debug(e.getMessage());
          throw UserException.permissionError(e)
                  .message("Not authorized to list or query tables in schema %s", tableSchemaPath)
                  .build(logger);
        }
      } catch (IOException e) {
        logger.warn("Failure while trying to list view tables in workspace [{}]", tableSchemaPath, e);
      }

      for (DotFile f : files) {
        switch (f.getType()) {
        case VIEW:
          try {
            return new ViewTable(new NamespaceKey(tableSchemaPath), f.getView(lpPersistance), f.getOwner(), null);
          } catch (AccessControlException e) {
            if (!schemaConfig.getIgnoreAuthErrors()) {
              logger.debug(e.getMessage());
              throw UserException.permissionError(e)
                      .message("Not authorized to read view [%s] in schema %s", tableSchemaPath.get(tableSchemaPath.size() - 1), tableSchemaPath.subList(0, tableSchemaPath.size() - 1))
                      .build(logger);
            }
          } catch (IOException e) {
            logger.warn("Failure while trying to load {}.view.meta file in workspace [{}]", tableSchemaPath.get(tableSchemaPath.size() - 1), tableSchemaPath.subList(0, tableSchemaPath.size() - 1), e);
          }
        }
      }
    } catch (UnsupportedOperationException e) {
      logger.debug("The filesystem for this workspace does not support this operation.", e);
    }
    return null;
  }

  /**
   * Helper method which resolves the table name to actual file/folder on the filesystem.
   * If the resolved path refers to an entity not under the base of the source then a permission error is thrown.
   *
   * Ex. For given source named "dfs" which has base path of "/base" tableSchemaPath
   *    [dfs, tmp, a] -> "/base/tmp/a"
   *    [dfs, "/tmp/b"] -> "/base/tmp/b"
   *    [dfs, "value", tbl] -> "/base/value/tbl"
   * @param tableSchemaPath
   * @return
   */
  public List<String> resolveTableNameToValidPath(List<String> tableSchemaPath) {
    List<String> fullPath = new ArrayList<>();
    fullPath.addAll(PathUtils.toPathComponents(basePath));
    for (String pathComponent : tableSchemaPath.subList(1 /* need to skip the source name */, tableSchemaPath.size())) {
      fullPath.add(PathUtils.removeQuotes(pathComponent));
    }
    PathUtils.verifyNoAccessOutsideBase(basePath, PathUtils.toFSPath(fullPath));
    return fullPath;
  }

  /**
   * Resolve given table path relative to source resolve it to a valid path in filesystem.
   * If the resolved path refers to an entity not under the base of the source then a permission error is thrown.
   * @param tablePath
   * @return
   */
  public Path resolveTablePathToValidPath(String tablePath) {
    String relativePathClean = PathUtils.removeLeadingSlash(tablePath);
    Path combined = new Path(basePath, relativePathClean);
    PathUtils.verifyNoAccessOutsideBase(basePath, combined);
    return combined;
  }

  FileDatasetHandle getDatasetWithOptions(
      NamespaceKey datasetPath,
      TableInstance instance,
      boolean ignoreAuthErrors,
      String user,
      int maxLeafColumns
  ) throws Exception {
    final FormatPluginConfig fconfig = optionExtractor.createConfigForTable(instance);
    final DatasetRetrievalOptions options = DatasetRetrievalOptions.DEFAULT.toBuilder()
        .setIgnoreAuthzErrors(ignoreAuthErrors)
        .setMaxMetadataLeafColumns(maxLeafColumns)
        .build();

    return getDatasetWithFormat(datasetPath, new PreviousDatasetInfo(null, null, null), fconfig, options, user);
  }

  protected FileDatasetHandle getDatasetWithFormat(NamespaceKey datasetPath, PreviousDatasetInfo oldConfig, FormatPluginConfig formatPluginConfig,
                                                       DatasetRetrievalOptions retrievalOptions, String user) throws Exception {

    if(datasetPath.size() <= 1){
      return null;  // not a valid table schema path
    }
    final List<String> fullPath = resolveTableNameToValidPath(datasetPath.getPathComponents());
    try {
      // TODO: why do we need distinguish between system user and process user?
      final String userName = config.isImpersonationEnabled() ? SystemUser.SYSTEM_USERNAME : ImpersonationUtil.getProcessUserName();
      List<String> parentSchemaPath = new ArrayList<>(fullPath.subList(0, fullPath.size() - 1));
      FileSystemWrapper fs = createFS((user != null) ? user : userName);
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
        }
      }

      final boolean hasDirectories = fileSelection.containsDirectories();
      final FileStatus rootStatus = fs.getFileStatus(new Path(fileSelection.getSelectionRoot()));

      // Get subdirectories under file selection before pruning directories
      final FileUpdateKey.Builder updateKeyBuilder = FileUpdateKey.newBuilder();
      if (rootStatus.isDirectory()) {
        // first entity is always a root
        updateKeyBuilder.addCachedEntities(fromFileStatus(rootStatus));
      }

      for (FileStatus dirStatus: fileSelection.getAllDirectories()) {
        updateKeyBuilder.addCachedEntities(fromFileStatus(dirStatus));
      }
      final FileUpdateKey updateKey = updateKeyBuilder.build();

      // Expand selection by copying it first used to check extensions of files in directory.
      final FileSelection fileSelectionWithoutDir =  hasDirectories? new FileSelection(fileSelection).minusDirectories(): fileSelection;
      if(fileSelectionWithoutDir == null || fileSelectionWithoutDir.isEmpty()){
        // no files in the found directory, not a table.
        return null;
      }

      FileDatasetHandle datasetAccessor = null;


      if (formatPluginConfig != null) {

        FormatPlugin formatPlugin = formatCreator.getFormatPluginByConfig(formatPluginConfig);
        if(formatPlugin == null){
          formatPlugin = formatCreator.newFormatPlugin(formatPluginConfig);
        }
        DatasetType type = fs.isDirectory(new Path(fileSelectionWithoutDir.getSelectionRoot())) ? DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER : DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
        datasetAccessor = formatPlugin.getDatasetAccessor(type, oldConfig, fs, fileSelectionWithoutDir, this, datasetPath,
            updateKey, retrievalOptions.maxMetadataLeafColumns());
      }

      if (datasetAccessor == null &&
          retrievalOptions.autoPromote()) {
        for (final FormatMatcher matcher : matchers) {
          try {
            if (matcher.matches(fs, fileSelection, codecFactory)) {
              DatasetType type = fs.isDirectory(new Path(fileSelectionWithoutDir.getSelectionRoot())) ? DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER : DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
              datasetAccessor = matcher.getFormatPlugin()
                  .getDatasetAccessor(type, oldConfig, fs, fileSelectionWithoutDir, this, datasetPath,
                      updateKey, retrievalOptions.maxMetadataLeafColumns());
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
      if (!retrievalOptions.ignoreAuthzErrors()) {
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

  @Override
  public void start() throws IOException {
    List<Property> properties = getProperties();
    if (properties != null) {
      for (Property prop : properties) {
        fsConf.set(prop.name, prop.value);
      }
    }

    if (!Strings.isNullOrEmpty(config.getConnection())) {
      FileSystem.setDefaultUri(fsConf, config.getConnection());
    }

    Map<String,String> map =  ImmutableMap.of(
            "fs.classpath.impl", ClassPathFileSystem.class.getName(),
            "fs.dremio-local.impl", LocalSyncableFileSystem.class.getName()
    );
    for(Entry<String, String> prop : map.entrySet()) {
      fsConf.set(prop.getKey(), prop.getValue());
    }

    this.optionExtractor = new FormatPluginOptionExtractor(context.getClasspathScan());
    this.matchers = Lists.newArrayList();
    this.formatCreator = new FormatCreator(context, config, context.getClasspathScan(), this);
    this.codecFactory = new CompressionCodecFactory(fsConf);

    for (FormatMatcher m : formatCreator.getFormatMatchers()) {
      matchers.add(m);
    }

//    boolean footerNoSeek = contetMutext.getOptionManager().getOption(ExecConstants.PARQUET_FOOTER_NOSEEK);
    // NOTE: Add fallback format matcher if given in the configuration. Make sure fileMatchers is an order-preserving list.
    this.systemUserFS = createFS(SYSTEM_USERNAME);
    dropFileMatchers = matchers.subList(0, matchers.size());

    createIfNecessary();
  }

  protected List<Property> getProperties() {
    return config.getProperties();
  }

  /**
   * Create the supporting directory for this plugin if it doesn't yet exist.
   * @throws IOException
   */
  private void createIfNecessary() throws IOException {
    if(!config.createIfMissing()) {
      return;
    }

    try {
      // no need to exists here as FileSystemWrapper does an exists check and this is a noop if already existing.
      systemUserFS.mkdirs(config.getPath());
    } catch (IOException ex) {
      try {
        if(systemUserFS.exists(config.getPath())) {
          // race creation, ignore.
          return;
        }

      } catch (IOException existsFailure) {
        // we're doing the check above to detect a race condition. if we fail, ignore the failure and just fall through to throwing the originally caught exception.
        ex.addSuppressed(existsFailure);
      }

      throw new IOException(String.format("Failure to create directory %s.", config.getPath().toString()), ex);
    }

  }

  public FormatPlugin getFormatPlugin(String name) {
    return formatCreator.getFormatPluginByName(name);
  }

  public FormatPlugin getFormatPlugin(FormatPluginConfig config) {
    FormatPlugin plugin = formatCreator.getFormatPluginByConfig(config);
    if (plugin == null) {
      plugin = formatCreator.newFormatPlugin(config);
    }
    return plugin;
  }

  protected FileSystemCachedEntity fromFileStatus(FileStatus status){
    return FileSystemCachedEntity.newBuilder()
        .setPath(status.getPath().toString())
        .setLastModificationTime(status.getModificationTime())
        .build();
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    if (config.isImpersonationEnabled()) {
      if (datasetConfig.getReadDefinition() != null) { // allow accessing partial datasets
        final FileSystemWrapper userFs = createFS(user);
        final List<TimedRunnable<Boolean>> permissionCheckTasks = Lists.newArrayList();

        permissionCheckTasks.addAll(getUpdateKeyPermissionTasks(datasetConfig, userFs));
        permissionCheckTasks.addAll(getSplitPermissionTasks(datasetConfig, userFs, user));

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
          throw new RuntimeException("Failed to check access permission", ioe);
        }
      }
    }
    return true;
  }

  // Check if all sub directories can be listed/read
  private Collection<FsPermissionTask> getUpdateKeyPermissionTasks(DatasetConfig datasetConfig, FileSystemWrapper userFs) {
    FileUpdateKey fileUpdateKey;
    try {
      fileUpdateKey = LegacyProtobufSerializer.parseFrom(FileUpdateKey.PARSER, datasetConfig.getReadDefinition().getReadSignature().toByteArray());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Cannot read file update key", e);
    }
    if (fileUpdateKey.getCachedEntitiesList().isEmpty()) {
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
  private Collection<FsPermissionTask> getSplitPermissionTasks(DatasetConfig datasetConfig, FileSystemWrapper userFs, String user) {
    final SplitsPointer splitsPointer = DatasetSplitsPointer.of(context.getNamespaceService(user), datasetConfig);
    final boolean isParquet = datasetConfig.getPhysicalDataset().getFormatSettings().getType() == FileType.PARQUET;
    final List<FsPermissionTask> fsPermissionTasks = Lists.newArrayList();
    final List<Path> batch = Lists.newArrayList();

    for (PartitionChunkMetadata partitionChunkMetadata: splitsPointer.getPartitionChunks()) {
      for (DatasetSplit split : partitionChunkMetadata.getDatasetSplits()) {
        final Path filePath;
        try {
          if (isParquet) {
            filePath = new Path(LegacyProtobufSerializer.parseFrom(ParquetDatasetSplitXAttr.PARSER, split.getSplitExtendedProperty().toByteArray()).getPath());
          } else {
            filePath = new Path(LegacyProtobufSerializer.parseFrom(EasyDatasetSplitXAttr.PARSER, split.getSplitExtendedProperty().toByteArray()).getPath());
          }
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Could not deserialize split info", e);
        }

        batch.add(filePath);
        if (batch.size() == PERMISSION_CHECK_TASK_BATCH_SIZE) {
          // make a copy of batch
          fsPermissionTasks.add(new FsPermissionTask(userFs, new ArrayList<>(batch), FsAction.READ));
          batch.clear();
        }
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

  /**
   * Returns a shared {@link FileSystemWrapper} for the system user.
   *
   * @return
   */
  public FileSystemWrapper getSystemUserFS() {
    return systemUserFS;
  }

  public String getName() {
    return name;
  }

  protected boolean fileExists(String username, List<String> filePath) throws IOException {
    return createFS(username).isFile(PathUtils.toFSPath(resolveTableNameToValidPath(filePath)));
  }

  protected boolean isAsyncEnabledForQuery(OperatorContext context) {
    return true;
  }

  @Override
  public MetadataValidity validateMetadata(BytesOutput signature, DatasetHandle datasetHandle, DatasetMetadata metadata,
      ValidateMetadataOption... options) throws DatasetNotFoundException {
    final FileUpdateKey fileUpdateKey;
    try {
      fileUpdateKey = LegacyProtobufSerializer.parseFrom(FileUpdateKey.PARSER, MetadataProtoUtils.toProtobuf(signature));
    } catch (InvalidProtocolBufferException e) {
      // Wrap protobuf exception for consistency
      throw new RuntimeException(e);
    }

    if (fileUpdateKey.getCachedEntitiesList() == null || fileUpdateKey.getCachedEntitiesList().isEmpty()) {
      // TODO: evaluate the need for this.
//       Preconditions.checkArgument(oldConfig.getType() == DatasetType.PHYSICAL_DATASET_SOURCE_FILE,
//           "only file based datasets can have empty read signature");
      // single file dataset
      return MetadataValidity.INVALID;
    }

    final UpdateStatus status = checkMultifileStatus(fileUpdateKey);
    switch(status) {
    case DELETED:
      throw new DatasetNotFoundException(datasetHandle.getDatasetPath());
    case UNCHANGED:
      return MetadataValidity.VALID;
    case CHANGED:
      return MetadataValidity.INVALID;
    default:
      throw new UnsupportedOperationException(status.name());
    }

  }

  private enum UpdateStatus {
    /**
     * Metadata hasn't changed.
     */
    UNCHANGED,


    /**
     * Metadata has changed.
     */
    CHANGED,

    /**
     * Dataset has been deleted.
     */
    DELETED
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

        final com.google.common.base.Optional<FileStatus> optionalStatus = systemUserFS.getFileStatusSafe(cachedEntityPath);
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

  public SabotContext getContext() {
    return context;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean createOrUpdateView(NamespaceKey key, View view, SchemaConfig schemaConfig) throws IOException {
    if(!getMutability().hasMutationCapability(MutationType.VIEW, schemaConfig.isSystemUser())) {
      throw UserException.parseError()
        .message("Unable to create view. Schema [%s] is immutable for this user.", key.getParent())
        .build(logger);
    }

    Path viewPath = getViewPath(key.getPathComponents());
    FileSystemWrapper fs = createFS(schemaConfig.getUserName());
    boolean replaced = fs.exists(viewPath);
    final FsPermission viewPerms =
            new FsPermission(schemaConfig.getOption(ExecConstants.NEW_VIEW_DEFAULT_PERMS_KEY).getStringVal());
    try (OutputStream stream = FileSystemWrapper.create(fs, viewPath, viewPerms)) {
      lpPersistance.getMapper().writeValue(stream, view);
    }
    return replaced;
  }

  @Override
  public void dropView(SchemaConfig schemaConfig, List<String> tableSchemaPath) throws IOException {
    if(!getMutability().hasMutationCapability(MutationType.VIEW, schemaConfig.isSystemUser())) {
      throw UserException.parseError()
        .message("Unable to drop view. Schema [%s] is immutable for this user.", this.name)
        .build(logger);
    }

    createFS(schemaConfig.getUserName()).delete(getViewPath(tableSchemaPath), false);
  }

  private Path getViewPath(List<String> tableSchemaPath) {
    List<String> fullPath = resolveTableNameToValidPath(tableSchemaPath);
    String parentPath = PathUtils.toFSPathString(fullPath.subList(0, fullPath.size() - 1));
    return DotFileType.VIEW.getPath(parentPath, tableSchemaPath.get(tableSchemaPath.size() - 1));
  }

  public FormatPluginOptionExtractor getOptionExtractor() {
    return optionExtractor;
  }

  public List<Function> getFunctions(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return optionExtractor.getFunctions(tableSchemaPath, this, schemaConfig);
  }


  private FormatMatcher findMatcher(FileSystemWrapper fs, FileSelection file) {
    FormatMatcher matcher = null;
    try {
      for (FormatMatcher m : dropFileMatchers) {

        if (m.matches(fs, file, codecFactory)) {
          return m;
        }
      }
    } catch (IOException e) {
      logger.debug("Failed to find format matcher for file: %s", file, e);
    }
    return matcher;
  }

  private boolean isHomogeneous(FileSystemWrapper fs, FileSelection fileSelection) throws IOException {
    FormatMatcher matcher = null;
    FileSelection noDir = fileSelection.minusDirectories();

    if (noDir == null || noDir.getStatuses() == null) {
      return true;
    }

    for(FileStatus s : noDir.getStatuses()) {
      FileSelection subSelection = FileSelection.create(s);
      if (matcher == null) {
        matcher = findMatcher(fs, subSelection);
        if(matcher == null) {
          return false;
        }
      }

      if(!matcher.matches(fs, subSelection, codecFactory)) {
        return false;
      }
    }
    return true;
  }

  /**
   * We check if the table contains homogeneous file formats that Dremio can read. Once the checks are performed
   * we rename the file to start with an "_". After the rename we issue a recursive delete of the directory.
   */
  @Override
  public void dropTable(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    if(!getMutability().hasMutationCapability(MutationType.TABLE, schemaConfig.isSystemUser())) {
      throw UserException.parseError()
        .message("Unable to drop table. Schema [%s] is immutable for this user.", this.name)
        .build(logger);
    }

    FileSystemWrapper fs = createFS(schemaConfig.getUserName());
    List<String> fullPath = resolveTableNameToValidPath(tableSchemaPath);
    FileSelection fileSelection;
    try {
      fileSelection = FileSelection.create(fs, fullPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (fileSelection == null) {
      throw UserException
          .validationError()
          .message(String.format("Table [%s] not found", SqlUtils.quotedCompound(tableSchemaPath)))
          .build(logger);
    }

    try {
      if (!isHomogeneous(fs, fileSelection)) {
        throw UserException
                .validationError()
                .message("Table contains different file formats. \n" +
                        "Drop Table is only supported for directories that contain homogeneous file formats consumable by Dremio")
                .build(logger);
      }

      final Path fsPath = PathUtils.toFSPath(fullPath);

      if (!fs.delete(fsPath, true)) {
        throw UserException.ioExceptionError()
            .message("Failed to drop table: %s", PathUtils.constructFullPath(tableSchemaPath))
            .build(logger);
      }
    } catch (AccessControlException e) {
      throw UserException
              .permissionError(e)
              .message("Unauthorized to drop table")
              .build(logger);
    } catch (IOException e) {
      throw UserException
              .dataWriteError(e)
              .message("Failed to drop table: " + e.getMessage())
              .build(logger);
    }
  }

  /**
   * Fetches a single item from the filesystem plugin
   */
  public SchemaEntity get(List<String> path, String userName) {
    try {
      final FileStatus status = createFS(userName).getFileStatus(PathUtils.toFSPath(resolveTableNameToValidPath(path)));

      final Set<List<String>> tableNames = Sets.newHashSet();
      final NamespaceService ns = context.getNamespaceService(userName);
      final NamespaceKey folderNSKey = new NamespaceKey(path);

      if (ns.exists(folderNSKey)) {
        for(NameSpaceContainer entity : ns.list(folderNSKey)) {
          if (entity.getType() == Type.DATASET) {
            tableNames.add(resolveTableNameToValidPath(entity.getDataset().getFullPathList()));
          }
        }
      }

      List<String> p = PathUtils.toPathComponents(status.getPath());
      return getSchemaEntity(status, tableNames, p);
    } catch (IOException | NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  public List<SchemaEntity> list(List<String> folderPath, String userName) {
    try {
      final List<FileStatus> files = Lists.newArrayList(createFS(userName).listStatus(PathUtils.toFSPath(resolveTableNameToValidPath(folderPath))));
      final Set<List<String>> tableNames = Sets.newHashSet();
      final NamespaceService ns = context.getNamespaceService(userName);
      final NamespaceKey folderNSKey = new NamespaceKey(folderPath);
      if (ns.exists(folderNSKey, Type.DATASET)) {
        // if the folder is a dataset, then there is nothing to list
        return ImmutableList.of();
      }
      if (ns.exists(folderNSKey)) {
        for(NameSpaceContainer entity : ns.list(folderNSKey)) {
          if (entity.getType() == Type.DATASET) {
            tableNames.add(resolveTableNameToValidPath(entity.getDataset().getFullPathList()));
          }
        }
      }

      Iterable<SchemaEntity> itr = Iterables.transform(files, new com.google.common.base.Function<FileStatus, SchemaEntity>() {
        @Nullable
        @Override
        public SchemaEntity apply(@Nullable FileStatus input) {
          List<String> p = PathUtils.toPathComponents(input.getPath());
          return getSchemaEntity(input, tableNames, p);
        }
      });
      return ImmutableList.<SchemaEntity>builder().addAll(itr).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  private SchemaEntity getSchemaEntity(FileStatus status, Set<List<String>> tableNames, List<String> p) {
    SchemaEntityType entityType;

    if (status.isDirectory()) {
      if (tableNames.contains(p)) {
        entityType = SchemaEntityType.FOLDER_TABLE;
      } else {
        entityType = SchemaEntityType.FOLDER;
      }
    } else {
      if (tableNames.contains(p)) {
        entityType = SchemaEntityType.FILE_TABLE;
      } else {
        entityType = SchemaEntityType.FILE;
      }
    }

    return new SchemaEntity(PathUtils.getQuotedFileName(status.getPath()), entityType, status.getOwner());
  }

  public SchemaMutability getMutability() {
    return config.getSchemaMutability();
  }

  public FormatPluginConfig createConfigForTable(String tableName, Map<String, Object> storageOptions) {
    return optionExtractor.createConfigForTable(tableName, storageOptions);
  }

  public List<String> getConnectionUniqueProperties() {
    return config.getConnectionUniqueProperties();
  }

  protected static String getTableName(final NamespaceKey key) {
    final String tableName;
    if(key.size() == 2) {
      tableName = key.getLeaf();
    } else {
      List<String> subString = key.getPathComponents().subList(1, key.size());
      tableName = Joiner.on('/').join(subString);
    }
    return tableName;
  }

  @Override
  public CreateTableEntry createNewTable(SchemaConfig config, NamespaceKey key, WriterOptions writerOptions, Map<String, Object> storageOptions) {
    if(!getMutability().hasMutationCapability(MutationType.TABLE, config.isSystemUser())) {
      throw UserException.parseError()
        .message("Unable to create table. Schema [%s] is immutable for this user.", key.getParent())
        .build(logger);
    }

    final String tableName = getTableName(key);

    final FormatPlugin formatPlugin;
    if (storageOptions == null || storageOptions.isEmpty()) {
      final String storage = config.getOptions().getOption(ExecConstants.OUTPUT_FORMAT_VALIDATOR);
      formatPlugin = getFormatPlugin(storage);
      if (formatPlugin == null) {
        throw new UnsupportedOperationException(String.format("Unsupported format '%s' in '%s'", storage, key));
      }
    } else {
      final FormatPluginConfig formatConfig = createConfigForTable(tableName, storageOptions);
      formatPlugin = getFormatPlugin(formatConfig);
    }

    final String userName = this.config.isImpersonationEnabled() ? config.getUserName() : ImpersonationUtil.getProcessUserName();

    // check that there is no directory at the described path.
    Path path = resolveTablePathToValidPath(tableName);
    try {
      if(systemUserFS.exists(path)) {
        throw UserException.validationError().message("Folder already exists at path: %s.", key).build(logger);
      }
    } catch (IOException e) {
      throw UserException.validationError(e).message("Failure to check if table already exists at path %s.", key).build(logger);
    }

    return new FileSystemCreateTableEntry(
      userName,
      this,
      formatPlugin,
      path.toString(),
      writerOptions);
  }

  @Override
  public Writer getWriter(PhysicalOperator child, String location, WriterOptions options, OpProps props) throws IOException {
    throw new IllegalStateException("The ctas entry for a file system plugin should invoke get writer on the format plugin directly.");
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options)
      throws ConnectorException {
    BatchSchema currentSchema = CurrentSchemaOption.getSchema(options);
    FileConfig fileConfig = FileConfigOption.getFileConfig(options);
    List<String> sortColumns = SortColumnsOption.getSortColumns(options);

    FormatPluginConfig formatPluginConfig = null;
    if (fileConfig != null) {
      formatPluginConfig = PhysicalDatasetUtils.toFormatPlugin(fileConfig, Collections.<String>emptyList());
    }

    final PreviousDatasetInfo pdi = new PreviousDatasetInfo(fileConfig, currentSchema, sortColumns);
    try {
      return Optional.ofNullable(getDatasetWithFormat(MetadataObjectsUtils.toNamespaceKey(datasetPath), pdi,
          formatPluginConfig, DatasetRetrievalOptions.of(options), null));
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, ConnectorException.class);
      throw new ConnectorException(e);
    }
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle,
      PartitionChunkListing chunkListing,
      GetMetadataOption... options
  ) throws ConnectorException {
    return datasetHandle.unwrap(FileDatasetHandle.class)
        .getDatasetMetadata(options);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options)
      throws ConnectorException {
    return datasetHandle.unwrap(FileDatasetHandle.class)
        .listPartitionChunks(options);
  }

  @Override
  public BytesOutput provideSignature(DatasetHandle datasetHandle, DatasetMetadata metadata) throws ConnectorException {
    return datasetHandle.unwrap(FileDatasetHandle.class)
        .provideSignature(metadata);
  }

  @Override
  public boolean containerExists(EntityPath containerPath) {
    final List<String> folderPath = containerPath.getComponents();
    try {
      return systemUserFS.isDirectory(PathUtils.toFSPath(resolveTableNameToValidPath(folderPath)));
    } catch (IOException e) {
      logger.debug("Failure reading path.", e);
      return false;
    }
  }
}

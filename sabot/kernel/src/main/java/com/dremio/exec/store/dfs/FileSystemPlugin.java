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
package com.dremio.exec.store.dfs;

import static com.dremio.io.file.PathFilters.NO_HIDDEN_FILES;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.attribute.PosixFilePermission;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.schema.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.Preconditions;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.SqlUtils;
import com.dremio.config.DremioConfig;
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
import com.dremio.exec.hadoop.HadoopCompressionCodecFactory;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
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
import com.dremio.exec.store.iceberg.IcebergOpCommitter;
import com.dremio.exec.store.iceberg.IcebergOperation;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FileSystemUtils;
import com.dremio.io.file.MorePosixFilePermissions;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf.EasyDatasetSplitXAttr;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitXAttr;
import com.dremio.service.namespace.DatasetHelper;
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
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

  private final LoadingCache<String, org.apache.hadoop.fs.FileSystem> hadoopFS = CacheBuilder.newBuilder()
      .softValues()
      .removalListener(new RemovalListener<String, org.apache.hadoop.fs.FileSystem>() {
        @Override
        public void onRemoval(RemovalNotification<String, org.apache.hadoop.fs.FileSystem> notification) {
          try {
            notification.getValue().close();
          } catch (IOException e) {
            // Ignore
            logger.debug("Could not close filesystem", e);
          }
        }
      })
      .build(new CacheLoader<String, org.apache.hadoop.fs.FileSystem>() {
        @Override
        public org.apache.hadoop.fs.FileSystem load(String user) throws Exception {
          // If the request proxy user is same as process user name or same as system user, return the process UGI.
          final UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
          final UserGroupInformation ugi;
          if (user.equals(loginUser.getUserName()) || SYSTEM_USERNAME.equals(user)) {
            ugi = loginUser;
          } else {
            ugi = UserGroupInformation.createProxyUser(user, loginUser);
          }

          final PrivilegedExceptionAction<org.apache.hadoop.fs.FileSystem> fsFactory = () -> {
            // Do not use FileSystem#newInstance(Configuration) as it adds filesystem into the Hadoop cache :(
            // Mimic instead Hadoop FileSystem#createFileSystem() method
            final URI uri = org.apache.hadoop.fs.FileSystem.getDefaultUri(fsConf);
            final Class<? extends org.apache.hadoop.fs.FileSystem> fsClass = org.apache.hadoop.fs.FileSystem.getFileSystemClass(uri.getScheme(), fsConf);
            final org.apache.hadoop.fs.FileSystem fs = ReflectionUtils.newInstance(fsClass, fsConf);
            fs.initialize(uri, fsConf);
            return fs;
          };

          try {
            return ugi.doAs(fsFactory);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        };
      });

  private final String name;
  private final LogicalPlanPersistence lpPersistance;
  private final C config;
  private final SabotContext context;
  private final Path basePath;

  private final Provider<StoragePluginId> idProvider;
  private FileSystem systemUserFS;
  private Configuration fsConf;
  private FormatPluginOptionExtractor optionExtractor;
  protected FormatCreator formatCreator;
  private ArrayList<FormatMatcher> matchers;
  private ArrayList<FormatMatcher> layeredMatchers;
  private List<FormatMatcher> dropFileMatchers;
  private CompressionCodecFactory codecFactory;
  private boolean supportsIcebergTables;

  public FileSystemPlugin(final C config, final SabotContext context, final String name, Provider<StoragePluginId> idProvider) {
    this.name = name;
    this.config = config;
    this.idProvider = idProvider;
    this.context = context;
    this.fsConf = getNewFsConf();
    this.lpPersistance = context.getLpPersistence();
    this.basePath = config.getPath();
    this.supportsIcebergTables = getIcebergSupportFlag();
  }

  public C getConfig(){
    return config;
  }

  private boolean getIcebergSupportFlag() {
    final String nasConnection = "file";
    final String hdfsConnection = "hdfs";
    final String mparfsConnection = "maprfs";
    return this.getConfig().getConnection().toLowerCase().startsWith(nasConnection) ||
      this.getConfig().getConnection().toLowerCase().startsWith(hdfsConnection) ||
      this.getConfig().getConnection().toLowerCase().startsWith(mparfsConnection);
  }

  public boolean supportsIcebergTables() {
    return supportsIcebergTables;
  }

  public boolean supportsColocatedReads() {
    return true;
  }

  @Override
  public BatchSchema mergeSchemas(DatasetConfig oldConfig, BatchSchema newSchema) {
    boolean mixedTypesDisabled = context.getOptionManager().getOption(ExecConstants.MIXED_TYPES_DISABLED);
    return CalciteArrowHelper.fromDataset(oldConfig).merge(newSchema, mixedTypesDisabled);
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return systemUserFS.isPdfs() ? new SourceCapabilities(REQUIRES_HARD_AFFINITY) : SourceCapabilities.NONE;
  }

  public static final Configuration getNewFsConf() {
    return new Configuration(DEFAULT_CONFIGURATION);
  }

  protected Configuration getFsConf() {
    return fsConf;
  }

  public Configuration getFsConfCopy() {
    return new Configuration(fsConf);
  }

  public CompressionCodecFactory getCompressionCodecFactory() {
    return codecFactory;
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
  public FileSystem createFS(String userName) throws IOException {
    return createFS(userName, null);
  }

  public FileSystem createFS(String userName, OperatorContext operatorContext) throws IOException {
    return createFS(userName, operatorContext, false);
  }

  public FileSystem createFS(String userName, OperatorContext operatorContext, boolean metadata) throws IOException {
    return context.getFileSystemWrapper().wrap(newFileSystem(userName, operatorContext), name, config, operatorContext,
        isAsyncEnabledForQuery(operatorContext) && getConfig().isAsyncEnabled(), metadata);
  }

  protected FileSystem newFileSystem(String userName, OperatorContext operatorContext) throws IOException {
    // Create underlying filesystem
    if (Strings.isNullOrEmpty(userName)) {
      throw new IllegalArgumentException("Invalid value for user name");
    }

    final org.apache.hadoop.fs.FileSystem fs;
    try {
      fs = hadoopFS.get(getFSUser(userName));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.propagateIfPossible(cause, IOException.class);
      throw new RuntimeException(cause != null ? cause : e);
    }

    return HadoopFileSystem.get(fs, (operatorContext == null) ? null : operatorContext.getStats(),
        isAsyncEnabledForQuery(operatorContext) && getConfig().isAsyncEnabled());
  }

  public String getFSUser(String userName) {
    if (!config.isImpersonationEnabled()) {
      return SystemUser.SYSTEM_USERNAME;
    }

    return userName;
  }

  public Iterable<String> getSubPartitions(List<String> table,
                                           List<String> partitionColumns,
                                           List<String> partitionValues,
                                           SchemaConfig schemaConfig
  ) throws PartitionNotFoundException {
    List<FileAttributes> fileAttributesList;
    try {
      Path fullPath = PathUtils.toFSPath(resolveTableNameToValidPath(table));
      try(DirectoryStream<FileAttributes> stream = createFS(schemaConfig.getUserName()).list(fullPath, NO_HIDDEN_FILES)) {
        // Need to copy the content of the stream before closing
        fileAttributesList = Lists.newArrayList(stream);
      }
    } catch (IOException e) {
      throw new PartitionNotFoundException("Error finding partitions for table " + table, e);
    }
    return Iterables.transform(fileAttributesList, f -> f.getPath().toURI().toString());
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return context.getConfig().getClass("dremio.plugins.dfs.rulesfactory", StoragePluginRulesFactory.class, FileSystemRulesFactory.class);
  }

  @Override
  public SourceState getState() {
    if (systemUserFS == null) {
      return SourceState.NOT_AVAILABLE;
    }
    if (systemUserFS.isPdfs() || ClassPathFileSystem.SCHEME.equals(systemUserFS.getUri().getScheme())) {
      return SourceState.GOOD;
    }
    try {
      systemUserFS.access(config.getPath(), ImmutableSet.of(AccessMode.READ));
    } catch (AccessControlException ace) {
      logger.debug("Falling back to listing of source to check health", ace);
      try {
        systemUserFS.list(config.getPath());
      } catch (Exception e) {
        return SourceState.badState("", e);
      }
    } catch (Exception e) {
      return SourceState.badState("", e);
    }
    return SourceState.GOOD;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    if (!Boolean.getBoolean(DremioConfig.LEGACY_STORE_VIEWS_ENABLED)) {
      return null;
    }

    try {
      List<DotFile> files = Collections.emptyList();
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
    Path combined = basePath.resolve(relativePathClean);
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
      List<String> parentSchemaPath = new ArrayList<>(fullPath.subList(0, fullPath.size() - 1));
      FileSystem fs = createFS(user);
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
      final FileAttributes rootAttributes = fs.getFileAttributes(Path.of(fileSelection.getSelectionRoot()));

      // Get subdirectories under file selection before pruning directories
      final FileUpdateKey.Builder updateKeyBuilder = FileUpdateKey.newBuilder();
      if (rootAttributes.isDirectory()) {
        // first entity is always a root
        updateKeyBuilder.addCachedEntities(fromFileAttributes(rootAttributes));
      }

      for (FileAttributes dirAttributes: fileSelection.getAllDirectories()) {
        updateKeyBuilder.addCachedEntities(fromFileAttributes(dirAttributes));
      }
      final FileUpdateKey updateKey = updateKeyBuilder.build();

      // Expand selection by copying it first used to check extensions of files in directory.
      final FileSelection fileSelectionWithoutDir =  hasDirectories? new FileSelection(fileSelection).minusDirectories(): fileSelection;
      if(fileSelectionWithoutDir == null || fileSelectionWithoutDir.isEmpty()){
        // no files in the found directory, not a table.
        return null;
      }

      FileDatasetHandle.checkMaxFiles(datasetPath.getName(), fileSelectionWithoutDir.getFileAttributesList().size(), getContext(),
        getConfig().isInternal());
      FileDatasetHandle datasetAccessor = null;
      if (formatPluginConfig != null) {

        FormatPlugin formatPlugin = formatCreator.getFormatPluginByConfig(formatPluginConfig);
        if(formatPlugin == null){
          formatPlugin = formatCreator.newFormatPlugin(formatPluginConfig);
        }
        DatasetType type = fs.isDirectory(Path.of(fileSelectionWithoutDir.getSelectionRoot())) ? DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER : DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
        datasetAccessor = formatPlugin.getDatasetAccessor(type, oldConfig, fs, fileSelectionWithoutDir, this, datasetPath,
            updateKey, retrievalOptions.maxMetadataLeafColumns());
      }

      if (datasetAccessor == null &&
          retrievalOptions.autoPromote()) {
        for (final FormatMatcher matcher : matchers) {
          try {
            if (matcher.matches(fs, fileSelection, codecFactory)) {
              DatasetType type = fs.isDirectory(Path.of(fileSelectionWithoutDir.getSelectionRoot())) ? DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER : DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
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

  // Try to match against a layered format. If these is no match, returns null.
  public FileFormat findLayeredFormatMatch(FileSystem fs, FileSelection fileSelection) throws IOException {
    for (final FormatMatcher matcher : matchers) {
      if (matcher.matches(fs, fileSelection, codecFactory)) {
        return PhysicalDatasetUtils.toFileFormat(matcher.getFormatPlugin());
      }
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
      org.apache.hadoop.fs.FileSystem.setDefaultUri(fsConf, config.getConnection());
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
    this.layeredMatchers = Lists.newArrayList();
    this.formatCreator = new FormatCreator(context, config, context.getClasspathScan(), this);
    // Use default Hadoop implementation
    this.codecFactory = HadoopCompressionCodecFactory.DEFAULT;

    matchers.addAll(formatCreator.getFormatMatchers());
    layeredMatchers.addAll(formatCreator.getLayeredFormatMatchers());

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

  protected FileSystemCachedEntity fromFileAttributes(FileAttributes attributes){
    return FileSystemCachedEntity.newBuilder()
        .setPath(attributes.getPath().toString())
        .setLastModificationTime(attributes.lastModifiedTime().toMillis())
        .build();
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    if (config.isImpersonationEnabled()) {
      if (datasetConfig.getReadDefinition() != null) { // allow accessing partial datasets
        FileSystem userFs;
        try {
          userFs = createFS(user);
        } catch (IOException ioe) {
          throw new RuntimeException("Failed to check access permission", ioe);
        }
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
        } catch (FileNotFoundException fnfe) {
          throw UserException.invalidMetadataError(fnfe)
            .addContext(fnfe.getMessage())
            .setAdditionalExceptionContext(new InvalidMetadataErrorContext(ImmutableList.of(key.getPathComponents())))
            .buildSilently();
        } catch (IOException ioe) {
          throw new RuntimeException("Failed to check access permission", ioe);
        }
      }
    }
    return true;
  }

  // Check if all sub directories can be listed/read
  private Collection<FsPermissionTask> getUpdateKeyPermissionTasks(DatasetConfig datasetConfig, FileSystem userFs) {
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
    final Set<AccessMode> access;
    final List<Path> batch = Lists.newArrayList();

    //DX-7850 : remove once solution for maprfs is found
    if (userFs.isMapRfs()) {
      access = EnumSet.of(AccessMode.READ);
    } else {
      access = EnumSet.of(AccessMode.READ, AccessMode.EXECUTE);
    }

    for (FileSystemCachedEntity cachedEntity : fileUpdateKey.getCachedEntitiesList()) {
      batch.add(Path.of(cachedEntity.getPath()));
      if (batch.size() == PERMISSION_CHECK_TASK_BATCH_SIZE) {
        // make a copy of batch
        fsPermissionTasks.add(new FsPermissionTask(userFs, Lists.newArrayList(batch), access));
        batch.clear();
      }
    }
    if (!batch.isEmpty()) {
      fsPermissionTasks.add(new FsPermissionTask(userFs, batch, access));
    }
    return fsPermissionTasks;
  }

  // Check if all splits are accessible
  private Collection<FsPermissionTask> getSplitPermissionTasks(DatasetConfig datasetConfig, FileSystem userFs, String user) {
    final SplitsPointer splitsPointer = DatasetSplitsPointer.of(context.getNamespaceService(user), datasetConfig);
    final boolean isParquet = DatasetHelper.hasParquetDataFiles(datasetConfig.getPhysicalDataset().getFormatSettings());
    final List<FsPermissionTask> fsPermissionTasks = Lists.newArrayList();
    final List<Path> batch = Lists.newArrayList();

    for (PartitionChunkMetadata partitionChunkMetadata: splitsPointer.getPartitionChunks()) {
      for (DatasetSplit split : partitionChunkMetadata.getDatasetSplits()) {
        final Path filePath;
        try {
          if (isParquet) {
            filePath = Path.of(LegacyProtobufSerializer.parseFrom(ParquetDatasetSplitXAttr.PARSER, split.getSplitExtendedProperty().toByteArray()).getPath());
          } else {
            filePath = Path.of(LegacyProtobufSerializer.parseFrom(EasyDatasetSplitXAttr.PARSER, split.getSplitExtendedProperty().toByteArray()).getPath());
          }
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Could not deserialize split info", e);
        }

        batch.add(filePath);
        if (batch.size() == PERMISSION_CHECK_TASK_BATCH_SIZE) {
          // make a copy of batch
          fsPermissionTasks.add(new FsPermissionTask(userFs, new ArrayList<>(batch), EnumSet.of(AccessMode.READ)));
          batch.clear();
        }
      }
    }

    if (!batch.isEmpty()) {
      fsPermissionTasks.add(new FsPermissionTask(userFs, batch, EnumSet.of(AccessMode.READ)));
    }

    return fsPermissionTasks;
  }

  private class FsPermissionTask extends TimedRunnable<Boolean> {
    private final FileSystem userFs;
    private final List<Path> cachedEntityPaths;
    private final Set<AccessMode> permission;

    FsPermissionTask(FileSystem userFs, List<Path> cachedEntityPaths, Set<AccessMode> permission) {
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
  public FileSystem getSystemUserFS() {
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
      final Path cachedEntityPath =  Path.of(cachedEntity.getPath());
      try {

        try {
          final FileAttributes updatedFileAttributes = systemUserFS.getFileAttributes(cachedEntityPath);
          final long updatedModificationTime = updatedFileAttributes.lastModifiedTime().toMillis();
          Preconditions.checkArgument(updatedFileAttributes.isDirectory(), "fs based dataset update key must be composed of directories");
          if (cachedEntity.getLastModificationTime() < updatedModificationTime) {
            // the file/folder has been changed since our last check.
            return UpdateStatus.CHANGED;
          }
        } catch (FileNotFoundException e) {
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
    // Empty cache
    hadoopFS.invalidateAll();
    hadoopFS.cleanUp();
  }

  @Override
  public boolean createOrUpdateView(NamespaceKey key, View view, SchemaConfig schemaConfig) throws IOException {
    if (!Boolean.getBoolean(DremioConfig.LEGACY_STORE_VIEWS_ENABLED)) {
      throw UserException.parseError()
        .message("Unable to drop view. Filesystem views are unsupported.")
        .build(logger);
    } else if (!getMutability().hasMutationCapability(MutationType.VIEW, schemaConfig.isSystemUser())) {
      throw UserException.parseError()
        .message("Unable to create view. Schema [%s] is immutable for this user.", key.getParent())
        .build(logger);
    }

    Path viewPath = getViewPath(key.getPathComponents());
    FileSystem fs = createFS(schemaConfig.getUserName());
    boolean replaced = fs.exists(viewPath);
    final Set<PosixFilePermission> viewPerms =
            MorePosixFilePermissions.fromOctalMode(schemaConfig.getOption(ExecConstants.NEW_VIEW_DEFAULT_PERMS_KEY).getStringVal());
    try (OutputStream stream = FileSystemUtils.create(fs, viewPath, viewPerms)) {
      lpPersistance.getMapper().writeValue(stream, view);
    }
    return replaced;
  }

  @Override
  public void dropView(SchemaConfig schemaConfig, List<String> tableSchemaPath) throws IOException {
    if (!Boolean.getBoolean(DremioConfig.LEGACY_STORE_VIEWS_ENABLED)) {
      throw UserException.parseError()
        .message("Unable to drop view. Filesystem views are unsupported.")
        .build(logger);
    } else if (!getMutability().hasMutationCapability(MutationType.VIEW, schemaConfig.isSystemUser())) {
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

  @Override
  public List<Function> getFunctions(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return optionExtractor.getFunctions(tableSchemaPath, this, schemaConfig);
  }


  private FormatMatcher findMatcher(FileSystem fs, FileSelection file) {
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

  private boolean isHomogeneous(FileSystem fs, FileSelection fileSelection) throws IOException {
    FormatMatcher matcher = null;
    FileSelection noDir = fileSelection.minusDirectories();

    if (noDir == null || noDir.getFileAttributesList() == null) {
      return true;
    }

    for(FileAttributes attributes : noDir.getFileAttributesList()) {
      FileSelection subSelection = FileSelection.create(attributes);
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
  public void dropTable(List<String> tableSchemaPath, boolean isLayered, SchemaConfig schemaConfig) {
    if(!getMutability().hasMutationCapability(MutationType.TABLE, schemaConfig.isSystemUser())) {
      throw UserException.parseError()
        .message("Unable to drop table. Schema [%s] is immutable for this user.", this.name)
        .build(logger);
    }

    FileSystem fs;
    try {
      fs = createFS(schemaConfig.getUserName());
    } catch (IOException e) {
      throw UserException
        .ioExceptionError(e)
        .message("Failed to access filesystem: " + e.getMessage())
        .build(logger);
    }

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
      if (!isLayered && !isHomogeneous(fs, fileSelection)) {
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

  @Override
  public void truncateTable(NamespaceKey key, SchemaConfig schemaConfig) {
    IcebergOperation.truncateTable(getTableName(key), validateAndGetPath(key, schemaConfig), fsConf);
  }

  @Override
  public void addColumns(NamespaceKey key, List<Field> columnsToAdd, SchemaConfig schemaConfig) {
    SchemaConverter.NextIDImpl id = new SchemaConverter.NextIDImpl();
    IcebergOperation.addColumns(getTableName(key),
      validateAndGetPath(key, schemaConfig), columnsToAdd.stream().map(f -> SchemaConverter.toIcebergColumn(f, id)).collect(Collectors.toList()), fsConf);
  }

  @Override
  public void dropColumn(NamespaceKey table, String columnToDrop, SchemaConfig schemaConfig) {
    IcebergOperation.dropColumn(getTableName(table), validateAndGetPath(table, schemaConfig), columnToDrop, fsConf);
  }

  @Override
  public void changeColumn(NamespaceKey table, String columnToChange, Field fieldFromSql, SchemaConfig schemaConfig) {
    IcebergOperation.changeColumn(getTableName(table), validateAndGetPath(table, schemaConfig),
      columnToChange, fieldFromSql, fsConf);
  }

  private Path validateAndGetPath(NamespaceKey table, SchemaConfig schemaConfig) {
    if (!getMutability().hasMutationCapability(MutationType.TABLE, schemaConfig.isSystemUser())) {
      throw UserException.parseError()
          .message("Unable to modify table. Schema [%s] is immutable for this user.", this.name)
          .buildSilently();
    }

    FileSystem fs;
    try {
      fs = createFS(schemaConfig.getUserName());
    } catch (IOException e) {
      throw UserException
          .ioExceptionError(e)
          .message("Failed to access filesystem: " + e.getMessage())
          .buildSilently();
    }

    final String tableName = getTableName(table);

    Path path = resolveTablePathToValidPath(tableName);

    try {
      if (!fs.exists(path)) {
        throw UserException
            .validationError()
            .message("Unable to modify table, path [%s] doesn't exist.", path)
            .buildSilently();
      }
    } catch (IOException e) {
      throw UserException
          .ioExceptionError(e)
          .message("Failed to check if directory [%s] exists: " + e.getMessage(), path)
          .buildSilently();
    }
    return path;
  }


  /**
   * Fetches a single item from the filesystem plugin
   */
  public SchemaEntity get(List<String> path, String userName) {
    try {
      final FileAttributes fileAttributes = createFS(userName).getFileAttributes(PathUtils.toFSPath(resolveTableNameToValidPath(path)));

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

      List<String> p = PathUtils.toPathComponents(fileAttributes.getPath());
      return getSchemaEntity(fileAttributes, tableNames, p);
    } catch (IOException | NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  public List<SchemaEntity> list(List<String> folderPath, String userName) {
    try {
      final List<FileAttributes> files = Lists.newArrayList(createFS(userName).list(PathUtils.toFSPath(resolveTableNameToValidPath(folderPath))));
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

      Iterable<SchemaEntity> itr = Iterables.transform(files, new com.google.common.base.Function<FileAttributes, SchemaEntity>() {
        @Nullable
        @Override
        public SchemaEntity apply(@Nullable FileAttributes input) {
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

  private SchemaEntity getSchemaEntity(FileAttributes fileAttributes, Set<List<String>> tableNames, List<String> p) {
    SchemaEntityType entityType;

    if (fileAttributes.isDirectory()) {
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

    return new SchemaEntity(PathUtils.getQuotedFileName(fileAttributes.getPath()), entityType, fileAttributes.owner().getName());
  }

  public SchemaMutability getMutability() {
    return config.getSchemaMutability();
  }

  public FormatPluginConfig createConfigForTable(String tableName, Map<String, Object> storageOptions) {
    return optionExtractor.createConfigForTable(tableName, storageOptions);
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
  public void createEmptyTable(final SchemaConfig config, NamespaceKey key, BatchSchema batchSchema,
                               final WriterOptions writerOptions) {
    if(!getMutability().hasMutationCapability(MutationType.TABLE, config.isSystemUser())) {
      throw UserException.parseError()
        .message("Unable to create table. Schema [%s] is immutable for this user.", key.getParent())
        .buildSilently();
    }

    final String tableName = getTableName(key);
    // check that there is no directory at the described path.
    Path path = resolveTablePathToValidPath(tableName);

    try {
      if(systemUserFS.exists(path)) {
        throw UserException.validationError().message("Folder already exists at path: %s.", key).buildSilently();
      }
    } catch (IOException e) {
      throw UserException.validationError(e).message("Failure to check if table already exists at path %s.", key).buildSilently();
    }

    IcebergOpCommitter icebergOpCommitter = IcebergOperation.getCreateTableCommitter(tableName, path, batchSchema,
      writerOptions.getPartitionColumns(), fsConf);
    icebergOpCommitter.consumeData(Collections.emptyList()); // adds snapshot
    icebergOpCommitter.commit();
  }

  @Override
  public CreateTableEntry createNewTable(SchemaConfig config, NamespaceKey key, IcebergTableProps icebergTableProps,
                                         WriterOptions writerOptions, Map<String, Object> storageOptions) {
    if(!getMutability().hasMutationCapability(MutationType.TABLE, config.isSystemUser())) {
      throw UserException.parseError()
        .message("Unable to create table. Schema [%s] is immutable for this user.", key.getParent())
        .build(logger);
    }

    final String tableName = getTableName(key);

    final FormatPlugin formatPlugin;
    if (storageOptions == null || storageOptions.isEmpty() || !storageOptions.containsKey("type")) {
      final String storage = config.getOptions().getOption(ExecConstants.OUTPUT_FORMAT_VALIDATOR);
      formatPlugin = getFormatPlugin(storage);
      if (formatPlugin == null) {
        throw new UnsupportedOperationException(String.format("Unsupported format '%s' in '%s'", storage, key));
      }
    } else {
      final FormatPluginConfig formatConfig = createConfigForTable(tableName, storageOptions);
      formatPlugin = getFormatPlugin(formatConfig);
    }

    final String userName = this.config.isImpersonationEnabled() ? config.getUserName() : SystemUser.SYSTEM_USERNAME;

    // check that there is no directory at the described path.
    Path path = resolveTablePathToValidPath(tableName);
    try {
      if (icebergTableProps == null || icebergTableProps.getIcebergOpType() == IcebergOperation.Type.CREATE) {
        if (systemUserFS.exists(path)) {
          throw UserException.validationError().message("Folder already exists at path: %s.", key).build(logger);
        }
      } else if (icebergTableProps.getIcebergOpType() == IcebergOperation.Type.INSERT) {
        if (!systemUserFS.exists(path)) {
          throw UserException.validationError().message("Table folder does not exists at path: %s.", key).build(logger);
        }
      }
    } catch (IOException e) {
      throw UserException.validationError(e).message("Failure to check if table already exists at path %s.", key).build(logger);
    }

    if (icebergTableProps != null) {
      icebergTableProps = new IcebergTableProps(icebergTableProps);
      icebergTableProps.setTableLocation(path.toString());
      icebergTableProps.setTableName(tableName);
      Preconditions.checkState(icebergTableProps.getUuid() != null &&
        !icebergTableProps.getUuid().isEmpty(), "Unexpected state. UUID must be set");
      path = path.resolve(icebergTableProps.getUuid());
    }

    return new FileSystemCreateTableEntry(
      userName,
      this,
      formatPlugin,
      path.toString(),
      icebergTableProps,
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
          formatPluginConfig, DatasetRetrievalOptions.of(options), SystemUser.SYSTEM_USERNAME));
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

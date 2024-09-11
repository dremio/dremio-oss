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
package com.dremio.exec.store.hive;

import static com.dremio.exec.store.hive.metadata.HivePartitionChunkListing.SplitType.DELTA_COMMIT_LOGS;
import static com.dremio.exec.store.hive.metadata.HivePartitionChunkListing.SplitType.DIR_LIST_INPUT_SPLIT;
import static com.dremio.exec.store.hive.metadata.HivePartitionChunkListing.SplitType.ICEBERG_MANIFEST_SPLIT;
import static com.dremio.exec.store.hive.metadata.HivePartitionChunkListing.SplitType.INPUT_SPLIT;
import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;
import static com.dremio.exec.store.metadatarefresh.MetadataRefreshUtils.metadataSourceAvailable;
import static java.lang.Math.toIntExact;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.concurrent.ContextAwareCompletableFuture;
import com.dremio.common.concurrent.bulk.BulkRequest;
import com.dremio.common.concurrent.bulk.BulkResponse;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.common.utils.PathUtils;
import com.dremio.config.DremioConfig;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.impersonation.extensions.SupportsImpersonation;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetMetadataVerifyResult;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.EntityPathWithOptions;
import com.dremio.connector.metadata.ExtendedPropertyOption;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsAlteringDatasetMetadata;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.connector.metadata.extensions.SupportsMetadataVerify;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.connector.metadata.extensions.ValidateMetadataOption;
import com.dremio.connector.metadata.options.AlterMetadataOption;
import com.dremio.connector.metadata.options.InternalMetadataTableOption;
import com.dremio.connector.metadata.options.MetadataVerifyRequest;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.AlterTableOption;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.DatasetSplitsPointer;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.RollbackOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder;
import com.dremio.exec.planner.sql.handlers.refresh.UnlimitedSplitsMetadataProvider;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.BlockBasedSplitGenerator;
import com.dremio.exec.store.BulkSourceMetadata;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.SupportsPF4JStoragePlugin;
import com.dremio.exec.store.TimedRunnable;
import com.dremio.exec.store.deltalake.DeltaLakeTable;
import com.dremio.exec.store.dfs.AddColumn;
import com.dremio.exec.store.dfs.AddPrimaryKey;
import com.dremio.exec.store.dfs.AsyncStreamConf;
import com.dremio.exec.store.dfs.ChangeColumn;
import com.dremio.exec.store.dfs.CreateParquetTableEntry;
import com.dremio.exec.store.dfs.DropColumn;
import com.dremio.exec.store.dfs.DropPrimaryKey;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.dfs.MetadataIOPool;
import com.dremio.exec.store.hive.deltalake.DeltaHiveInputFormat;
import com.dremio.exec.store.hive.exec.AsyncReaderUtils;
import com.dremio.exec.store.hive.exec.HadoopFsCacheWrapperPluginClassLoader;
import com.dremio.exec.store.hive.exec.HadoopFsSupplierProviderPluginClassLoader;
import com.dremio.exec.store.hive.exec.HiveDatasetOptions;
import com.dremio.exec.store.hive.exec.HiveDirListingRecordReader;
import com.dremio.exec.store.hive.exec.HiveProxyingSubScan;
import com.dremio.exec.store.hive.exec.HiveReaderProtoUtil;
import com.dremio.exec.store.hive.exec.HiveScanBatchCreator;
import com.dremio.exec.store.hive.exec.HiveScanTableFunction;
import com.dremio.exec.store.hive.exec.HiveSplitCreator;
import com.dremio.exec.store.hive.exec.HiveSubScan;
import com.dremio.exec.store.hive.exec.apache.HadoopFileSystemWrapper;
import com.dremio.exec.store.hive.exec.dfs.DremioHadoopFileSystemWrapper;
import com.dremio.exec.store.hive.exec.dfs.HadoopFsWrapperWithCachePluginClassLoader;
import com.dremio.exec.store.hive.exec.metadatarefresh.HiveFullRefreshReadSignatureProvider;
import com.dremio.exec.store.hive.exec.metadatarefresh.HiveIncrementalRefreshReadSignatureProvider;
import com.dremio.exec.store.hive.exec.metadatarefresh.HivePartialRefreshReadSignatureProvider;
import com.dremio.exec.store.hive.exec.planner.sql.handlers.refresh.HiveFullRefreshDatasetPlanBuilder;
import com.dremio.exec.store.hive.exec.planner.sql.handlers.refresh.HiveIncrementalRefreshDatasetPlanBuilder;
import com.dremio.exec.store.hive.iceberg.HiveTableOperations;
import com.dremio.exec.store.hive.iceberg.NoOpHiveTableOperations;
import com.dremio.exec.store.hive.metadata.HiveDatasetHandle;
import com.dremio.exec.store.hive.metadata.HiveDatasetHandleListing;
import com.dremio.exec.store.hive.metadata.HiveDatasetMetadata;
import com.dremio.exec.store.hive.metadata.HiveMetadataUtils;
import com.dremio.exec.store.hive.metadata.HivePartitionChunkListing;
import com.dremio.exec.store.hive.metadata.HiveStorageCapabilities;
import com.dremio.exec.store.hive.metadata.MetadataAccumulator;
import com.dremio.exec.store.hive.metadata.PartitionIterator;
import com.dremio.exec.store.hive.metadata.StatsEstimationParameters;
import com.dremio.exec.store.hive.metadata.TableMetadata;
import com.dremio.exec.store.hive.proxy.HiveProxiedOrcScanFilter;
import com.dremio.exec.store.hive.proxy.HiveProxiedScanBatchCreator;
import com.dremio.exec.store.hive.proxy.HiveProxiedSubScan;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.iceberg.hive.IcebergHiveModel;
import com.dremio.exec.store.iceberg.hive.IcebergHiveTableIdentifier;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.metadatarefresh.committer.ReadSignatureProvider;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingRecordReader;
import com.dremio.exec.store.metadatarefresh.footerread.FooterReadTableFunction;
import com.dremio.exec.store.parquet.ParquetScanTableFunction;
import com.dremio.exec.store.parquet.ParquetSplitCreator;
import com.dremio.exec.store.parquet.ScanTableFunction;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.hive.proto.HiveReaderProto.FileSystemCachedEntity;
import com.dremio.hive.proto.HiveReaderProto.FileSystemPartitionUpdateKey;
import com.dremio.hive.proto.HiveReaderProto.HiveReadSignature;
import com.dremio.hive.proto.HiveReaderProto.HiveReadSignatureType;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.hive.proto.HiveReaderProto.PropertyCollectionType;
import com.dremio.hive.thrift.TException;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.MessageLevel;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.capabilities.BooleanCapabilityValue;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.dremio.service.users.SystemUser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.math.LongMath;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.InvalidProtocolBufferException;
import io.protostuff.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.inject.Provider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.orc.OrcConf;
import org.pf4j.PluginManager;

public class Hive3StoragePlugin extends BaseHiveStoragePlugin
    implements StoragePluginCreator.PF4JStoragePlugin,
        MutablePlugin,
        SupportsReadSignature,
        SupportsListingDatasets,
        SupportsAlteringDatasetMetadata,
        SupportsPF4JStoragePlugin,
        SupportsInternalIcebergTable,
        SupportsImpersonation,
        SupportsIcebergMutablePlugin,
        HadoopFsSupplierProviderPluginClassLoader,
        SupportsMetadataVerify,
        BulkSourceMetadata {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(Hive3StoragePlugin.class);

  private static final AbandonedConfig CLIENT_POOL_ABANDONED_CONFIG;

  static {
    CLIENT_POOL_ABANDONED_CONFIG = new AbandonedConfig();
    CLIENT_POOL_ABANDONED_CONFIG.setLogAbandoned(true);
    CLIENT_POOL_ABANDONED_CONFIG.setRemoveAbandonedOnMaintenance(true);
    CLIENT_POOL_ABANDONED_CONFIG.setRemoveAbandonedTimeout(60 * 60 * 4); // 4 hrs
  }

  private LoadingCache<String, HiveClient> clientsByUser;
  private final PluginManager pf4jManager;
  private final HiveConf hiveConf;
  private final SabotConfig sabotConfig;
  private final DremioConfig dremioConfig;

  // used in createConnectedClientWithAuthz even if pool is enabled
  private volatile HiveClient processUserMetastoreClient;
  private GenericObjectPool<HiveClient> processUserMSCPool;
  private final boolean storageImpersonationEnabled;
  private final boolean metastoreImpersonationEnabled;
  private final boolean isCoordinator;
  private final HiveSettings hiveSettings;
  private final OptionManager optionManager;
  private final Provider<StoragePluginId> pluginIdProvider;
  private final SabotContext context;
  private final String confUniqueIdentifier;
  private final Set<String> allowedDbsList;

  private final AtomicBoolean isOpen = new AtomicBoolean(false);
  private final AtomicBoolean isFirstPluginCreate = new AtomicBoolean(false);

  private int signatureValidationParallelism = 16;
  private long signatureValidationTimeoutMS = 2_000L;
  private final HadoopFsSupplierProviderPluginClassLoader
      hadoopFsSupplierProviderPluginClassLoader = new HadoopFsCacheWrapperPluginClassLoader();

  @VisibleForTesting
  public Hive3StoragePlugin(HiveConf hiveConf, SabotContext context, String name) {
    this(hiveConf, null, context, name, null);
  }

  public Hive3StoragePlugin(
      HiveConf hiveConf,
      PluginManager pf4jManager,
      SabotContext context,
      String name,
      Provider<StoragePluginId> pluginIdProvider) {
    super(context, name);
    this.isFirstPluginCreate.set(true);
    this.isCoordinator = context.isCoordinator();
    this.hiveConf = hiveConf;
    this.pf4jManager = pf4jManager;
    this.sabotConfig = context.getConfig();
    this.hiveSettings =
        new HiveSettings(context.getOptionManager(), HiveConfFactory.isHive2SourceType(hiveConf));
    this.optionManager = context.getOptionManager();
    this.dremioConfig = context.getDremioConfig();
    this.pluginIdProvider = pluginIdProvider;
    this.context = context;
    this.confUniqueIdentifier = name + UUID.randomUUID();
    HiveConfFactory.setConf(
        hiveConf, HiveFsUtils.UNIQUE_CONF_IDENTIFIER_PROPERTY_NAME, confUniqueIdentifier);

    storageImpersonationEnabled = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS);
    this.allowedDbsList = initAllowedDbs(hiveConf);

    // Hive Metastore impersonation is enabled if:
    // - "hive.security.authorization.enabled" is set to true,
    // - "hive.metastore.execute.setugi" is set to true (in SASL disabled scenarios) or
    // - "hive.metastore.sasl.enabled" is set to true in which case all metastore calls are
    // impersonated as
    //     the authenticated user.
    this.metastoreImpersonationEnabled =
        hiveConf.getBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED)
            || hiveConf.getBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI)
            || hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL);

    if (optionManager != null) {
      this.signatureValidationParallelism =
          Long.valueOf(optionManager.getOption(ExecConstants.HIVE_SIGNATURE_VALIDATION_PARALLELISM))
              .intValue();
      this.signatureValidationTimeoutMS =
          optionManager.getOption(ExecConstants.HIVE_SIGNATURE_VALIDATION_TIMEOUT_MS);
    }

    // Load Sasl providers from plugin classloader, to be used later by HBase client for RPC.
    // This is required for the HBase client that Hive 3 plugin uses irrespective of HBase server
    // side.
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      SaslClientAuthenticationProviders.getInstance(hiveConf);
    }

    if (HiveConfFactory.isHive2SourceType(hiveConf)) {
      // Hive 2 works with HBase 1 where region size calculation feature did not exist yet.
      hiveConf.setBoolean("hbase.regionsizecalculator.enable", false);
    }
  }

  private Set<String> initAllowedDbs(HiveConf hiveConf) {
    if (hiveConf == null) {
      return null;
    }

    String allowedDbsList = hiveConf.get(HiveConfFactory.HIVE_ALLOWED_DATABASES_LIST, "");
    if (allowedDbsList == null || allowedDbsList.isEmpty()) {
      return null;
    }

    hiveConf.unset(HiveConfFactory.HIVE_ALLOWED_DATABASES_LIST);
    String[] allowedDatabases =
        allowedDbsList.split(HiveCommonUtilities.HIVE_DATABASES_LIST_SEPARATOR);
    if (allowedDatabases.length == 0) {
      return null;
    }

    Set<String> allowedDatabasesSet = new HashSet<>(allowedDatabases.length);
    for (String allowedDb : allowedDatabases) {
      String db = allowedDb.trim().toLowerCase(Locale.ROOT);
      logger.info("Source {}: found allowed database {}", getName(), db);
      allowedDatabasesSet.add(db);
    }

    return allowedDatabasesSet;
  }

  private FileSystem createFileSystem(
      String filePath,
      String userName,
      OperatorContext operatorContext,
      boolean injectAsyncOptions,
      boolean disableHDFSCache)
      throws IOException {
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      Path path = new Path(filePath);
      final JobConf jobConf = new JobConf(hiveConf);
      AsyncStreamConf cacheAndAsyncConf =
          HiveAsyncStreamConf.from(path.toUri().getScheme(), jobConf, optionManager);
      URI uri =
          injectAsyncOptions && cacheAndAsyncConf.isAsyncEnabled()
              ? AsyncReaderUtils.injectDremioConfigForAsyncRead(path.toUri(), jobConf)
              : path.toUri();
      if (disableHDFSCache) {
        jobConf.setBoolean("fs.hdfs.impl.disable.cache", true);
      }
      return createFS(
          new DremioHadoopFileSystemWrapper(
              new Path(uri),
              jobConf,
              operatorContext != null ? operatorContext.getStats() : null,
              cacheAndAsyncConf.isAsyncEnabled(),
              this.getHadoopFsSupplierPluginClassLoader(uri.toString(), jobConf, userName).get()),
          operatorContext,
          cacheAndAsyncConf);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Supplier<org.apache.hadoop.fs.FileSystem> getHadoopFsSupplierPluginClassLoader(
      String path, Iterable<Map.Entry<String, String>> conf, String userName) {
    return hadoopFsSupplierProviderPluginClassLoader.getHadoopFsSupplierPluginClassLoader(
        path, conf, isImpersonationEnabled() ? userName : SystemUser.SYSTEM_USERNAME);
  }

  @Override
  public Supplier<org.apache.hadoop.fs.FileSystem> getHadoopFsSupplierPluginClassLoader(
      String path,
      Iterable<Map.Entry<String, String>> conf,
      String userName,
      boolean hdfsC3CacheEnabled) {
    return hadoopFsSupplierProviderPluginClassLoader.getHadoopFsSupplierPluginClassLoader(
        path,
        conf,
        isImpersonationEnabled() ? userName : SystemUser.SYSTEM_USERNAME,
        hdfsC3CacheEnabled);
  }

  @Override
  public FileSystem createFS(String filePath, String userName, OperatorContext operatorContext)
      throws IOException {
    return createFileSystem(filePath, userName, operatorContext, false, false);
  }

  @Override
  public FileSystem createFSWithAsyncOptions(
      String filePath, String userName, OperatorContext operatorContext) throws IOException {
    return createFileSystem(filePath, userName, operatorContext, true, false);
  }

  @Override
  public FileSystem createFSWithoutHDFSCache(
      String filePath, String userName, OperatorContext operatorContext) throws IOException {
    return createFileSystem(filePath, userName, operatorContext, false, true);
  }

  @Override
  public Iterable<Map.Entry<String, String>> getConfigProperties() {
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      return new JobConf(hiveConf);
    }
  }

  @Override
  public String getConfEntry(String key, String defaultValue) {
    return hiveConf.get(key, defaultValue);
  }

  @Override
  public boolean supportReadSignature(DatasetMetadata metadata, boolean isFileDataset) {
    final HiveDatasetMetadata hiveDatasetMetadata = metadata.unwrap(HiveDatasetMetadata.class);
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      HiveStorageCapabilities storageCapabilities =
          HiveMetadataUtils.getHiveStorageCapabilities(
              hiveDatasetMetadata.getMetadataAccumulator().getTableLocation());
      final JobConf job = new JobConf(hiveConf);
      job.setInputFormat(hiveDatasetMetadata.getMetadataAccumulator().getCurrentInputFormat());
      return HiveMetadataUtils.shouldGenerateFileSystemUpdateKeys(
          storageCapabilities, job.getInputFormat());
    }
  }

  @Override
  public boolean allowUnlimitedSplits(
      DatasetHandle handle, DatasetConfig datasetConfig, String user) {

    if (!metadataSourceAvailable(getSabotContext().getCatalogService())) {
      logger.debug(
          "Not using unlimited splits for {} since metadata source plugin is not available",
          handle.getDatasetPath().toString());
      return false;
    }

    try {
      if (datasetConfig.getReadDefinition() != null
          && datasetConfig.getReadDefinition().getExtendedProperty() != null) {
        HiveTableXattr tableXattr =
            HiveTableXattr.parseFrom(
                datasetConfig.getReadDefinition().getExtendedProperty().asReadOnlyByteBuffer());
        boolean varcharTruncationEnabled =
            HiveDatasetOptions.enforceVarcharWidth(
                HiveReaderProtoUtil.convertValuesToNonProtoAttributeValues(
                    tableXattr.getDatasetOptionMap()));
        if (varcharTruncationEnabled) {
          logger.debug(
              "Not using unlimited splits for {} as varchar truncation is enabled",
              handle.getDatasetPath().toString());
          return false;
        }
      }

      List<String> tablePathComponents = handle.getDatasetPath().getComponents();
      try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
        try (ManagedHiveClient wrapper = getClient(SystemUser.SYSTEM_USERNAME)) {
          final HiveMetadataUtils.SchemaComponents schemaComponents =
              HiveMetadataUtils.resolveSchemaComponents(tablePathComponents);
          final Table table;
          if (handle instanceof HiveDatasetHandle
              && ((HiveDatasetHandle) handle).getTable() != null) {
            table = ((HiveDatasetHandle) handle).getTable();
          } else {
            table =
                wrapper
                    .client()
                    .getTable(schemaComponents.getDbName(), schemaComponents.getTableName(), true);
            if (table == null) {
              throw new ConnectorException(
                  String.format("Dataset path '%s', table not found.", tablePathComponents));
            }
          }
          boolean isSupportedFormat =
              HiveMetadataUtils.isValidInputFormatForIcebergExecution(table, hiveConf);
          if (!isSupportedFormat) {
            logger.debug(
                "Not using unlimited splits for {} since table format is not supported",
                handle.getDatasetPath().toString());
          }
          return isSupportedFormat;
        }
      }
    } catch (InvalidProtocolBufferException | TException | ConnectorException e) {
      return false;
    }
  }

  @Override
  public void runRefreshQuery(String refreshQuery, String user) throws Exception {
    runQuery(refreshQuery, user, QUERY_TYPE_METADATA_REFRESH);
  }

  @Override
  public boolean canGetDatasetMetadataInCoordinator() {
    return true;
  }

  @Override
  public List<String> resolveTableNameToValidPath(List<String> tableSchemaPath) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(tableSchemaPath);
    return Arrays.asList(schemaComponents.getDbName(), schemaComponents.getTableName());
  }

  @Override
  public BlockBasedSplitGenerator.SplitCreator createSplitCreator(
      OperatorContext context, byte[] extendedBytes, boolean isInternalIcebergTable) {
    if (isInternalIcebergTable) {
      return new HiveSplitCreator(context, extendedBytes);
    } else {
      return new ParquetSplitCreator(context, false);
    }
  }

  @Override
  public ScanTableFunction createScanTableFunction(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig) {
    if (functionConfig.getFunctionContext().getInternalTablePluginId() != null) {
      return new HiveScanTableFunction(fec, context, props, functionConfig);
    } else {
      return new ParquetScanTableFunction(fec, context, props, functionConfig);
    }
  }

  @Override
  public AbstractRefreshPlanBuilder createRefreshDatasetPlanBuilder(
      SqlHandlerConfig config,
      SqlRefreshDataset sqlRefreshDataset,
      UnlimitedSplitsMetadataProvider metadataProvider,
      boolean isFullRefresh) {
    if (isFullRefresh) {
      return new HiveFullRefreshDatasetPlanBuilder(config, sqlRefreshDataset, metadataProvider);
    } else {
      return new HiveIncrementalRefreshDatasetPlanBuilder(
          config, sqlRefreshDataset, metadataProvider);
    }
  }

  @Override
  public ReadSignatureProvider createReadSignatureProvider(
      com.google.protobuf.ByteString existingReadSignature,
      final String dataTableRoot,
      final long queryStartTime,
      List<String> partitionPaths,
      Predicate<String> partitionExists,
      boolean isFullRefresh,
      boolean isPartialRefresh) {
    if (isFullRefresh) {
      return new HiveFullRefreshReadSignatureProvider(
          dataTableRoot, queryStartTime, partitionPaths, partitionExists);
    } else if (isPartialRefresh) {
      return new HivePartialRefreshReadSignatureProvider(
          existingReadSignature, dataTableRoot, queryStartTime, partitionPaths, partitionExists);
    } else {
      return new HiveIncrementalRefreshReadSignatureProvider(
          existingReadSignature, dataTableRoot, queryStartTime, partitionPaths, partitionExists);
    }
  }

  @Override
  public DirListingRecordReader createDirListRecordReader(
      OperatorContext context,
      FileSystem fs,
      DirListInputSplitProto.DirListInputSplit dirListInputSplit,
      boolean isRecursive,
      BatchSchema tableSchema,
      List<PartitionProtobuf.PartitionValue> partitionValues) {
    return new HiveDirListingRecordReader(
        context,
        fs,
        dirListInputSplit,
        isRecursive,
        tableSchema,
        partitionValues,
        false,
        getConfigProperties());
  }

  @Override
  public TableOperations createIcebergTableOperations(
      FileIO fileIO, String queryUserName, IcebergTableIdentifier tableIdentifier) {
    IcebergHiveTableIdentifier hiveTableIdentifier = (IcebergHiveTableIdentifier) tableIdentifier;
    if (hiveConf.getBoolean(HiveConfFactory.ENABLE_DML_TESTS_WITHOUT_LOCKING, false)) {
      try (ManagedHiveClient wrapper = getClient(SystemUser.SYSTEM_USERNAME)) {
        return new NoOpHiveTableOperations(
            hiveConf,
            wrapper.client(),
            fileIO,
            IcebergHiveModel.HIVE,
            hiveTableIdentifier.getNamespace(),
            hiveTableIdentifier.getTableName());
      }
    }
    try (ManagedHiveClient wrapper = getClient(queryUserName)) {
      return new HiveTableOperations(
          hiveConf,
          wrapper.client(),
          fileIO,
          IcebergHiveModel.HIVE,
          hiveTableIdentifier.getNamespace(),
          hiveTableIdentifier.getTableName());
    }
  }

  @Override
  public FileIO createIcebergFileIO(
      FileSystem fs,
      OperatorContext context,
      List<String> dataset,
      String datasourcePluginUID,
      Long fileLength) {
    return new DremioFileIO(
        fs,
        context,
        dataset,
        datasourcePluginUID,
        fileLength,
        new HiveFileSystemConfigurationAdapter(hiveConf));
  }

  @Override
  public boolean isIcebergMetadataValid(DatasetConfig config, NamespaceKey key) {
    if (config.getPhysicalDataset().getIcebergMetadata() == null
        || config.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation() == null
        || config.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation().isEmpty()) {
      return false;
    }
    String existingRootPointer =
        config.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation();
    try {
      final HiveMetadataUtils.SchemaComponents schemaComponents =
          HiveMetadataUtils.resolveSchemaComponents(key.getPathComponents());

      Table table;
      try (ManagedHiveClient wrapper = getClient(SystemUser.SYSTEM_USERNAME)) {
        table =
            wrapper
                .client()
                .getTable(schemaComponents.getDbName(), schemaComponents.getTableName(), true);
      }
      if (table == null) {
        throw new ConnectorException(
            String.format("Dataset path '%s', table not found.", schemaComponents));
      }
      Preconditions.checkState(
          HiveMetadataUtils.isIcebergTable(table),
          String.format("Table %s is not an Iceberg table", schemaComponents));
      String latestRootPointer = null;
      if (table.getParameters() != null) {
        latestRootPointer = table.getParameters().get(HiveMetadataUtils.METADATA_LOCATION);
      }

      if (!existingRootPointer.equals(latestRootPointer)) {
        logger.debug(
            "Iceberg Dataset {} metadata is not valid. Existing root pointer in catalog: {}. Latest Iceberg table root pointer: {}.",
            key,
            existingRootPointer,
            latestRootPointer);
        return false;
      }
    } catch (TException | ConnectorException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  @Override
  public boolean containerExists(EntityPath key, GetMetadataOption... options) {
    if (key.size() != 2) {
      return false;
    }
    try (ManagedHiveClient wrapper = getClient(SystemUser.SYSTEM_USERNAME)) {
      return wrapper.client().databaseExists(key.getComponents().get(1));
    }
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  @Override
  public CreateTableEntry createNewTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      IcebergTableProps icebergTableProps,
      WriterOptions writerOptions,
      Map<String, Object> storageOptions,
      boolean isResultsTable) {
    Preconditions.checkArgument(icebergTableProps != null, "Iceberg properties are not provided");
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(tableSchemaPath.getPathComponents());
    com.dremio.io.file.Path tableFolderPath = null;
    String tableFolderLocation = null;

    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      HiveClient client = wrapper.client();
      switch (icebergTableProps.getIcebergOpType()) {
        case CREATE:
          // do an early check for create privileges since the createTable call will only happen
          // after all
          // data files are written
          client.checkCreateTablePrivileges(
              schemaComponents.getDbName(), schemaComponents.getTableName());
          String tableLocation =
              resolveTableLocation(schemaComponents, schemaConfig, writerOptions);
          tableFolderLocation =
              HiveMetadataUtils.resolveCreateTableLocation(
                  hiveConf, schemaComponents, tableLocation);
          validateDatabaseExists(client, schemaComponents.getDbName());
          break;

        case DELETE:
        case INSERT:
        case MERGE:
        case UPDATE:
        case OPTIMIZE:
        case VACUUM:
          client.checkDmlPrivileges(
              schemaComponents.getDbName(),
              schemaComponents.getTableName(),
              getPrivilegeActionTypesForIcebergDml(icebergTableProps.getIcebergOpType()));
          try (ManagedHiveClient systemWrapper = getClient(SystemUser.SYSTEM_USERNAME)) {
            tableFolderLocation =
                HiveMetadataUtils.getIcebergTableLocation(systemWrapper.client(), schemaComponents);
          }
          break;

        default:
          break;
      }
    } catch (TException e) {
      throw UserException.validationError(e)
          .message("Failure to check if table already exists at path %s.", tableSchemaPath)
          .build(logger);
    }

    Preconditions.checkArgument(
        tableFolderLocation != null, "Table folder location can not be null");
    tableFolderPath = com.dremio.io.file.Path.of(tableFolderLocation);
    icebergTableProps = new IcebergTableProps(icebergTableProps);
    icebergTableProps.setTableLocation(tableFolderPath.toString());
    icebergTableProps.setTableName(schemaComponents.getTableName());
    icebergTableProps.setDatabaseName(schemaComponents.getDbName());
    Preconditions.checkState(
        icebergTableProps.getUuid() != null && !icebergTableProps.getUuid().isEmpty(),
        String.format(
            "Unexpected state. UUID must be set for DatabaseName: %s TableName %s",
            schemaComponents.getDbName(), schemaComponents.getTableName()));
    tableFolderPath = tableFolderPath.resolve(icebergTableProps.getUuid());

    return new CreateParquetTableEntry(
        schemaConfig.getUserName(),
        this,
        tableFolderPath.toString(),
        icebergTableProps,
        writerOptions,
        tableSchemaPath);
  }

  String resolveTableLocation(
      HiveMetadataUtils.SchemaComponents schemaComponents,
      SchemaConfig schemaConfig,
      WriterOptions writerOptions) {
    if (StringUtils.isNotEmpty(writerOptions.getTableLocation())) {
      return writerOptions.getTableLocation();
    }

    if (!optionManager.getOption(ExecConstants.ENABLE_HIVE_DATABASE_LOCATION)) {
      return null;
    }

    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      String dbLocationUri = wrapper.client().getDatabaseLocationUri(schemaComponents.getDbName());
      if (StringUtils.isNotEmpty(dbLocationUri)) {
        return PathUtils.removeTrailingSlash(dbLocationUri) + '/' + schemaComponents.getTableName();
      }
    }

    return null;
  }

  @Override
  public void createEmptyTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      BatchSchema batchSchema,
      WriterOptions writerOptions) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(tableSchemaPath.getPathComponents());
    validateDatabaseExists(
        getClient(schemaConfig.getUserName()).client(), schemaComponents.getDbName());

    String tableLocation = resolveTableLocation(schemaComponents, schemaConfig, writerOptions);
    tableLocation =
        HiveMetadataUtils.resolveCreateTableLocation(hiveConf, schemaComponents, tableLocation);
    IcebergModel icebergModel =
        getIcebergModel(tableLocation, schemaComponents, schemaConfig.getUserName());

    PartitionSpec partitionSpec =
        Optional.ofNullable(
                writerOptions
                    .getTableFormatOptions()
                    .getIcebergSpecificOptions()
                    .getIcebergTableProps())
            .map(props -> props.getDeserializedPartitionSpec())
            .orElse(null);
    Map<String, String> tableProperties =
        Optional.of(
                writerOptions
                    .getTableFormatOptions()
                    .getIcebergSpecificOptions()
                    .getIcebergTableProps())
            .map(props -> props.getTableProperties())
            .orElse(Collections.emptyMap());
    IcebergOpCommitter icebergOpCommitter =
        icebergModel.getCreateTableCommitter(
            schemaComponents.getTableName(),
            icebergModel.getTableIdentifier(tableLocation),
            batchSchema,
            writerOptions.getPartitionColumns(),
            null,
            partitionSpec,
            writerOptions.getDeserializedSortOrder(),
            tableProperties);
    icebergOpCommitter.commit();
  }

  @Override
  public StoragePluginId getId() {
    return context.getCatalogService().getManagedSource(getName()).getId();
  }

  @Override
  public void dropTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions) {

    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(tableSchemaPath.getPathComponents());

    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      wrapper
          .client()
          .dropTable(schemaComponents.getDbName(), schemaComponents.getTableName(), false);
    } catch (NoSuchObjectException | UnknownTableException e) {
      String message =
          String.format(
              "Table not found to drop for Source '%s', database '%s', tablename '%s'",
              this.getName(), schemaComponents.getDbName(), schemaComponents.getTableName());
      logger.error(message, e);
      throw UserException.validationError(e).message(message).buildSilently();
    } catch (TException e) {
      String message =
          String.format(
              "Problem occured while dropping table for Source '%s', database '%s', tablename '%s'. Please check the log for details",
              this.getName(), schemaComponents.getDbName(), schemaComponents.getTableName());
      logger.error(message, e);
      throw new RuntimeException(message, e);
    }
  }

  @Override
  public void alterTable(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      AlterTableOption alterTableOption,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(tableSchemaPath.getPathComponents());
    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      wrapper
          .client()
          .checkAlterTablePrivileges(schemaComponents.getDbName(), schemaComponents.getTableName());
    }

    SplitsPointer splits =
        DatasetSplitsPointer.of(
            context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation =
        IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    IcebergModel icebergModel =
        getIcebergModel(metadataLocation, schemaComponents, schemaConfig.getUserName());
    icebergModel.alterTable(icebergModel.getTableIdentifier(metadataLocation), alterTableOption);
  }

  @Override
  public void truncateTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(tableSchemaPath.getPathComponents());
    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      wrapper
          .client()
          .checkTruncateTablePrivileges(
              schemaComponents.getDbName(), schemaComponents.getTableName());
    }

    DatasetConfig datasetConfig = null;
    try {
      datasetConfig =
          context.getNamespaceService(schemaConfig.getUserName()).getDataset(tableSchemaPath);
    } catch (NamespaceException e) {
      logger.error("Unable to get datasetConfig for the table to truncate", e);
      throw UserException.unsupportedError(e)
          .message(
              "Failed to truncate the table.Unable to get the table info from Dremio.Please check the logs")
          .buildSilently();
    }
    SplitsPointer splits =
        DatasetSplitsPointer.of(
            context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation =
        IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    IcebergModel icebergModel =
        getIcebergModel(metadataLocation, schemaComponents, schemaConfig.getUserName());
    icebergModel.truncateTable(icebergModel.getTableIdentifier(metadataLocation));
  }

  @Override
  public void rollbackTable(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      RollbackOption rollbackOption,
      TableMutationOptions tableMutationOptions) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(tableSchemaPath.getPathComponents());
    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      wrapper
          .client()
          .checkDmlPrivileges(
              schemaComponents.getDbName(),
              schemaComponents.getTableName(),
              getPrivilegeActionTypesForIcebergDml(IcebergCommandType.ROLLBACK));
    }

    SplitsPointer splits =
        DatasetSplitsPointer.of(
            context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation =
        IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    IcebergModel icebergModel =
        getIcebergModel(metadataLocation, schemaComponents, schemaConfig.getUserName());
    icebergModel.rollbackTable(icebergModel.getTableIdentifier(metadataLocation), rollbackOption);
  }

  @Override
  public void addColumns(
      NamespaceKey key,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      List<Field> columnsToAdd,
      TableMutationOptions tableMutationOptions) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(key.getPathComponents());
    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      wrapper
          .client()
          .checkAlterTablePrivileges(schemaComponents.getDbName(), schemaComponents.getTableName());
    }

    SplitsPointer splits =
        DatasetSplitsPointer.of(
            context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation =
        IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    AddColumn columnOperations =
        new AddColumn(
            key,
            context,
            datasetConfig,
            schemaConfig,
            getIcebergModel(metadataLocation, schemaComponents, schemaConfig.getUserName()),
            com.dremio.io.file.Path.of(metadataLocation),
            this);
    columnOperations.performOperation(columnsToAdd);
  }

  @Override
  public void dropColumn(
      NamespaceKey key,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      String columnToDrop,
      TableMutationOptions tableMutationOptions) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(key.getPathComponents());
    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      wrapper
          .client()
          .checkAlterTablePrivileges(schemaComponents.getDbName(), schemaComponents.getTableName());
    }

    SplitsPointer splits =
        DatasetSplitsPointer.of(
            context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation =
        IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    DropColumn columnOperations =
        new DropColumn(
            key,
            context,
            datasetConfig,
            schemaConfig,
            getIcebergModel(metadataLocation, schemaComponents, schemaConfig.getUserName()),
            com.dremio.io.file.Path.of(metadataLocation),
            this);
    columnOperations.performOperation(columnToDrop);
  }

  @Override
  public void changeColumn(
      NamespaceKey key,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      String columnToChange,
      Field fieldFromSql,
      TableMutationOptions tableMutationOptions) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(key.getPathComponents());
    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      wrapper
          .client()
          .checkAlterTablePrivileges(schemaComponents.getDbName(), schemaComponents.getTableName());
    }

    SplitsPointer splits =
        DatasetSplitsPointer.of(
            context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation =
        IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    ChangeColumn columnOperations =
        new ChangeColumn(
            key,
            context,
            datasetConfig,
            schemaConfig,
            getIcebergModel(metadataLocation, schemaComponents, schemaConfig.getUserName()),
            com.dremio.io.file.Path.of(metadataLocation),
            this);
    columnOperations.performOperation(columnToChange, fieldFromSql);
  }

  @Override
  public void addPrimaryKey(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      List<Field> columns,
      ResolvedVersionContext versionContext) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(table.getPathComponents());
    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      wrapper
          .client()
          .checkAlterTablePrivileges(schemaComponents.getDbName(), schemaComponents.getTableName());
    }
    SplitsPointer splits =
        DatasetSplitsPointer.of(
            context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation =
        IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    AddPrimaryKey op =
        new AddPrimaryKey(
            table,
            context,
            datasetConfig,
            schemaConfig,
            getIcebergModel(metadataLocation, schemaComponents, schemaConfig.getUserName()),
            com.dremio.io.file.Path.of(metadataLocation),
            this);
    op.performOperation(columns);
  }

  @Override
  public void dropPrimaryKey(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      ResolvedVersionContext versionContext) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(table.getPathComponents());
    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      wrapper
          .client()
          .checkAlterTablePrivileges(schemaComponents.getDbName(), schemaComponents.getTableName());
    }

    SplitsPointer splits =
        DatasetSplitsPointer.of(
            context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation =
        IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    DropPrimaryKey op =
        new DropPrimaryKey(
            table,
            context,
            datasetConfig,
            schemaConfig,
            getIcebergModel(metadataLocation, schemaComponents, schemaConfig.getUserName()),
            com.dremio.io.file.Path.of(metadataLocation),
            this);
    op.performOperation();
  }

  @Override
  public List<String> getPrimaryKey(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      ResolvedVersionContext versionContext,
      boolean saveInKvStore) {
    if (datasetConfig.getPhysicalDataset() == null
        || // PK only supported for physical datasets
        datasetConfig.getPhysicalDataset().getIcebergMetadata()
            == null) { // Physical dataset not Iceberg format
      return null;
    }

    return IcebergUtils.validateAndGeneratePrimaryKey(
        this, context, table, datasetConfig, schemaConfig, versionContext, saveInKvStore);
  }

  @Override
  public List<String> getPrimaryKeyFromMetadata(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      ResolvedVersionContext versionContext,
      boolean saveInKvStore) {
    final String userName = schemaConfig.getUserName();
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(table.getPathComponents());
    try (ManagedHiveClient wrapper = getClient(userName)) {
      wrapper
          .client()
          .checkAlterTablePrivileges(schemaComponents.getDbName(), schemaComponents.getTableName());
    }

    final IcebergModel icebergModel;
    final String path;
    if (DatasetHelper.isInternalIcebergTable(datasetConfig)) {
      final FileSystemPlugin<?> metaStoragePlugin =
          context.getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME);
      icebergModel = metaStoragePlugin.getIcebergModel();
      String metadataTableName =
          datasetConfig.getPhysicalDataset().getIcebergMetadata().getTableUuid();
      path = metaStoragePlugin.resolveTablePathToValidPath(metadataTableName).toString();
    } else if (DatasetHelper.isIcebergDataset(datasetConfig)) {
      SplitsPointer splits =
          DatasetSplitsPointer.of(context.getNamespaceService(userName), datasetConfig);
      path =
          IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
      icebergModel = getIcebergModel(path, schemaComponents, userName);
    } else {
      return null;
    }

    return IcebergUtils.getPrimaryKey(
        icebergModel, path, table, datasetConfig, userName, this, context, saveInKvStore);
  }

  @Override
  public Writer getWriter(
      PhysicalOperator child, String location, WriterOptions options, OpProps props)
      throws IOException {
    throw new UnsupportedOperationException(
        getRealSourceTypeName() + " plugin doesn't support table creation via CTAS.");
  }

  @Override
  public boolean toggleSchemaLearning(
      NamespaceKey table, SchemaConfig schemaConfig, boolean enableSchemaLearning) {
    throw new UnsupportedOperationException(
        getRealSourceTypeName() + " plugin doesn't support schema update.");
  }

  @Override
  public void alterSortOrder(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      BatchSchema batchSchema,
      SchemaConfig schemaConfig,
      List<String> sortOrderColumns,
      TableMutationOptions tableMutationOptions) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(table.getPathComponents());

    SplitsPointer splits =
        DatasetSplitsPointer.of(
            context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation =
        IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    IcebergModel icebergModel =
        getIcebergModel(metadataLocation, schemaComponents, schemaConfig.getUserName());
    icebergModel.replaceSortOrder(
        icebergModel.getTableIdentifier(metadataLocation), sortOrderColumns);
  }

  @Override
  public void updateTableProperties(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      BatchSchema schema,
      SchemaConfig schemaConfig,
      Map<String, String> tableProperties,
      TableMutationOptions tableMutationOptions,
      boolean isRemove) {
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(table.getPathComponents());
    try (ManagedHiveClient wrapper = getClient(schemaConfig.getUserName())) {
      wrapper
          .client()
          .checkAlterTablePrivileges(schemaComponents.getDbName(), schemaComponents.getTableName());
    }

    SplitsPointer splits =
        DatasetSplitsPointer.of(
            context.getNamespaceService(schemaConfig.getUserName()), datasetConfig);
    String metadataLocation =
        IcebergUtils.getMetadataLocation(datasetConfig, splits.getPartitionChunks().iterator());
    IcebergModel icebergModel =
        getIcebergModel(metadataLocation, schemaComponents, schemaConfig.getUserName());
    if (isRemove) {
      List<String> propertyNameList = new ArrayList<>(tableProperties.keySet());
      icebergModel.removeTableProperties(
          icebergModel.getTableIdentifier(metadataLocation), propertyNameList);
    } else {
      icebergModel.updateTableProperties(
          icebergModel.getTableIdentifier(metadataLocation), tableProperties);
    }
  }

  @Override
  public boolean createOrUpdateView(
      NamespaceKey tableSchemaPath, SchemaConfig schemaConfig, View view, ViewOptions viewOptions)
      throws IOException {
    throw new UnsupportedOperationException(
        getRealSourceTypeName() + " plugin doesn't support view creation via CREATE VIEW.");
  }

  @Override
  public void dropView(
      NamespaceKey tableSchemaPath, ViewOptions viewOptions, SchemaConfig schemaConfig)
      throws IOException {
    throw new UnsupportedOperationException(
        getRealSourceTypeName() + " plugin doesn't support view drop via DROP VIEW.");
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    if (!isOpen.get()) {
      throw buildAlreadyClosedException();
    }

    if (!metastoreImpersonationEnabled) {
      return true;
    }

    try {
      final HiveMetadataUtils.SchemaComponents schemaComponents =
          HiveMetadataUtils.resolveSchemaComponents(key.getPathComponents());
      final Table table =
          clientsByUser
              .get(user)
              .getTable(schemaComponents.getDbName(), schemaComponents.getTableName(), true);
      if (table == null) {
        return false;
      }
      if (storageImpersonationEnabled) {
        if (datasetConfig.getReadDefinition() != null
            && datasetConfig.getReadDefinition().getReadSignature() != null) {
          final HiveReadSignature readSignature =
              HiveReadSignature.parseFrom(
                  datasetConfig.getReadDefinition().getReadSignature().toByteArray());
          // for now we only support fs based read signatures
          if (readSignature.getType() == HiveReadSignatureType.FILESYSTEM) {
            // get list of partition properties from read definition
            HiveTableXattr tableXattr =
                HiveTableXattr.parseFrom(
                    datasetConfig.getReadDefinition().getExtendedProperty().asReadOnlyByteBuffer());
            return hasFSPermission(
                getUsername(user), key, readSignature.getFsPartitionUpdateKeysList(), tableXattr);
          }
        }
      }
      return true;
    } catch (TException e) {
      throw UserException.connectionError(e)
          .message("Unable to connect to Hive metastore: %s", e.getMessage())
          .build(logger);
    } catch (ExecutionException | InvalidProtocolBufferException e) {
      throw new RuntimeException("Unable to connect to Hive metastore.", e);
    } catch (UncheckedExecutionException e) {
      Throwable rootCause = ExceptionUtils.getRootCause(e);
      if (rootCause instanceof TException) {
        throw UserException.connectionError(e)
            .message("Unable to connect to Hive metastore: %s", rootCause.getMessage())
            .build(logger);
      }

      Throwable cause = e.getCause();
      if (cause instanceof AuthorizerServiceException || cause instanceof RuntimeException) {
        throw e;
      }
      logger.error(
          "User: {} is trying to access {} dataset with path: {}.",
          this.getName(),
          getRealSourceTypeName(),
          key,
          e);
    }

    return false;
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return new SourceCapabilities(
        new BooleanCapabilityValue(SourceCapabilities.VARCHARS_WITH_WIDTH, true));
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    // Do not use SabotConfig#getClass() here, since we need to search within the plugin's
    // classloader.
    String rulesFactoryPath = "dremio.plugins.hive3.rulesfactory";
    if (sabotConfig.hasPath(rulesFactoryPath)) {
      final String rulesFactoryClassName = sabotConfig.getString(rulesFactoryPath);

      try {
        Class<?> clazz = Class.forName(rulesFactoryClassName);
        Preconditions.checkArgument(StoragePluginRulesFactory.class.isAssignableFrom(clazz));
        return (Class<? extends StoragePluginRulesFactory>) clazz;
      } catch (ClassNotFoundException e) {
        throw UserException.unsupportedError(e)
            .message(
                "Failure while attempting to find implementation class %s for interface  %s. The sabot config key is %s ",
                rulesFactoryClassName,
                StoragePluginRulesFactory.class.getName(),
                rulesFactoryClassName)
            .build(logger);
      }
    }

    return HiveRulesFactory.class;
  }

  private boolean hasFSPermission(
      String user,
      NamespaceKey key,
      List<FileSystemPartitionUpdateKey> updateKeys,
      HiveTableXattr tableXattr) {
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      List<TimedRunnable<Boolean>> permissionCheckers = new ArrayList<>();
      long totalChecks = 0, maxChecksInPartition = 0;
      for (FileSystemPartitionUpdateKey updateKey : updateKeys) {
        permissionCheckers.add(new FsTask(user, updateKey, TaskType.FS_PERMISSION));
        totalChecks += updateKey.getCachedEntitiesCount();
        maxChecksInPartition = Math.max(updateKey.getCachedEntitiesCount(), maxChecksInPartition);
      }

      if (permissionCheckers.isEmpty()) {
        return true;
      }

      final int effectiveParallelism =
          Math.min(signatureValidationParallelism, permissionCheckers.size());
      final long minimumTimeout =
          quietCheckedMultiply(signatureValidationTimeoutMS, maxChecksInPartition);
      final long computedTimeout =
          quietCheckedMultiply(
              (long) Math.ceil(totalChecks / effectiveParallelism), signatureValidationTimeoutMS);
      final long timeout = Math.max(computedTimeout, minimumTimeout);

      Stopwatch stopwatch = Stopwatch.createStarted();
      final List<Boolean> accessPermissions =
          TimedRunnable.run(
              "check access permission for " + key,
              logger,
              permissionCheckers,
              effectiveParallelism,
              timeout);
      stopwatch.stop();
      logger.debug(
          "Checking access permission for {} took {} ms",
          key,
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
      for (Boolean permission : accessPermissions) {
        if (!permission) {
          return false;
        }
      }
    } catch (IOException ioe) {
      if (ioe instanceof FileNotFoundException) {
        throw UserException.invalidMetadataError(ioe)
            .addContext(ioe.getMessage())
            .setAdditionalExceptionContext(
                new InvalidMetadataErrorContext(ImmutableList.of(key.getPathComponents())))
            .build(logger);
      }
      throw UserException.dataReadError(ioe).build(logger);
    }
    return true;
  }

  @Override
  public boolean isImpersonationEnabled() {
    return storageImpersonationEnabled;
  }

  public IcebergModel getIcebergModel(
      String location, HiveMetadataUtils.SchemaComponents schemaComponents, String userName) {
    FileIO fileIO = null;
    try {
      FileSystem fs = createFS(location, SystemUser.SYSTEM_USERNAME, null);
      fileIO = createIcebergFileIO(fs, null, null, null, null);
    } catch (IOException e) {
      throw UserException.validationError(e)
          .message("Failure creating File System instance for path %s", location)
          .buildSilently();
    }
    return new IcebergHiveModel(
        schemaComponents.getDbName(),
        schemaComponents.getTableName(),
        fileIO,
        userName,
        null,
        this);
  }

  @Override
  public IcebergModel getIcebergModel(
      IcebergTableProps tableProps, String userName, OperatorContext context, FileIO fileIO) {
    if (fileIO == null) {
      try {
        FileSystem fs = createFS(tableProps.getTableLocation(), SystemUser.SYSTEM_USERNAME, null);
        fileIO = createIcebergFileIO(fs, null, null, null, null);
      } catch (IOException e) {
        throw UserException.validationError(e)
            .message(
                "Failure creating File System instance for path %s", tableProps.getTableLocation())
            .buildSilently();
      }
    }
    return new IcebergHiveModel(
        tableProps.getDatabaseName(), tableProps.getTableName(), fileIO, userName, null, this);
  }

  @Nonnull
  @Override
  public Optional<DatasetMetadataVerifyResult> verifyMetadata(
      DatasetHandle datasetHandle, MetadataVerifyRequest metadataVerifyRequest) {

    HiveDatasetHandle hiveDatasetHandle = datasetHandle.unwrap(HiveDatasetHandle.class);
    try (ManagedHiveClient wrapper = getClient(SystemUser.SYSTEM_USERNAME)) {
      return HiveMetadataUtils.verifyMetadata(
          wrapper.client(),
          hiveDatasetHandle.getDatasetPath(),
          hiveDatasetHandle.getInternalMetadataTableOption(),
          this,
          context,
          metadataVerifyRequest);
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }
  }

  private enum TaskType {
    FS_PERMISSION,
    FS_VALIDATION
  }

  private class FsTask extends TimedRunnable<Boolean> {
    private final String user;
    private final FileSystemPartitionUpdateKey updateKey;
    private final TaskType taskType;

    FsTask(String user, FileSystemPartitionUpdateKey updateKey, TaskType taskType) {
      this.user = user;
      this.updateKey = updateKey;
      this.taskType = taskType;
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
      try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
        if (updateKey != null) {
          switch (taskType) {
            case FS_PERMISSION:
              return checkAccessPermission();
            case FS_VALIDATION:
              return hasChanged();
            default:
              throw new IllegalArgumentException("Invalid task type " + taskType);
          }
        }
        return true;
      }
    }

    private boolean checkAccessPermission() throws IOException {
      final JobConf jobConf = new JobConf(hiveConf);
      Preconditions.checkArgument(
          updateKey.getCachedEntitiesCount() > 0,
          "hive partition update key should contain at least one path");

      for (FileSystemCachedEntity cachedEntity : updateKey.getCachedEntitiesList()) {
        final Path cachedEntityPath;
        if (cachedEntity.getPath() == null || cachedEntity.getPath().isEmpty()) {
          cachedEntityPath = new Path(updateKey.getPartitionRootDir());
        } else {
          cachedEntityPath = new Path(updateKey.getPartitionRootDir(), cachedEntity.getPath());
        }
        // Create filesystem for the given user and given path
        // TODO: DX-16001 - make async configurable for Hive.
        final HadoopFileSystemWrapper userFS =
            HiveImpersonationUtil.createFileSystem(user, jobConf, cachedEntityPath);
        try {
          if (cachedEntity.getIsDir()) {
            // DX-7850 : remove once solution for maprfs is found
            if (userFS.isMapRfs()) {
              userFS.access(cachedEntityPath, FsAction.READ);
            } else {
              userFS.access(cachedEntityPath, FsAction.READ_EXECUTE);
            }
          } else {
            userFS.access(cachedEntityPath, FsAction.READ);
          }
        } catch (AccessControlException ace) {
          return false;
        }
      }
      return true;
    }

    private boolean hasChanged() throws IOException {
      final JobConf jobConf = new JobConf(hiveConf);
      Preconditions.checkArgument(
          updateKey.getCachedEntitiesCount() > 0,
          "hive partition update key should contain at least one path");

      // create filesystem based on the first path which is root of the partition directory.
      final HadoopFileSystemWrapper fs =
          new HadoopFileSystemWrapper(new Path(updateKey.getPartitionRootDir()), jobConf);
      for (FileSystemCachedEntity cachedEntity : updateKey.getCachedEntitiesList()) {
        final Path cachedEntityPath;
        if (cachedEntity.getPath() == null || cachedEntity.getPath().isEmpty()) {
          cachedEntityPath = new Path(updateKey.getPartitionRootDir());
        } else {
          cachedEntityPath = new Path(updateKey.getPartitionRootDir(), cachedEntity.getPath());
        }
        if (fs.exists(cachedEntityPath)) {
          final FileStatus fileStatus = fs.getFileStatus(cachedEntityPath);
          if (cachedEntity.getLastModificationTime() < fileStatus.getModificationTime()) {
            logger.debug(
                "Read signature validation - partition has been modified: {}: "
                    + " cached last modification time = {}, actual modified time = {}",
                cachedEntityPath,
                cachedEntity.getLastModificationTime(),
                fileStatus.getModificationTime());
            return true;
          } else if (optionManager.getOption(ExecConstants.HIVE_SIGNATURE_CHANGE_RECURSIVE_LISTING)
              && (cachedEntity.getPath() == null || cachedEntity.getPath().isEmpty())) {
            final RemoteIterator<LocatedFileStatus> statuses = fs.listFiles(cachedEntityPath, true);
            while (statuses.hasNext()) {
              LocatedFileStatus attributes = statuses.next();
              if (cachedEntity.getLastModificationTime() < attributes.getModificationTime()) {
                logger.debug(
                    "Read signature validation - partition has been modified: {}: "
                        + " cached last modification time = {}, actual modified time = {}",
                    attributes.getPath(),
                    cachedEntity.getLastModificationTime(),
                    attributes.getModificationTime());
                return true;
              }
            }
          }
        } else {
          logger.debug(
              "Read signature validation - partition has been modified: {}: directory does not exist",
              cachedEntityPath);
          return true;
        }
      }
      return false;
    }
  }

  @VisibleForTesting
  MetadataValidity checkHiveMetadata(
      HiveTableXattr tableXattr,
      EntityPath datasetPath,
      BatchSchema tableSchema,
      final HiveReadSignature readSignature)
      throws TException {

    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(datasetPath.getComponents());

    try (ManagedHiveClient wrapper = getClient(SystemUser.SYSTEM_USERNAME)) {
      HiveClient client = wrapper.client();
      Table table =
          client.getTable(schemaComponents.getDbName(), schemaComponents.getTableName(), true);

      if (table == null) { // missing table?
        logger.debug("{}: metadata INVALID - table not found", datasetPath);
        return MetadataValidity.INVALID;
      }

      if (readSignature.getType() == HiveReadSignatureType.VERSION_BASED) {
        if (readSignature.getRootPointer() == null) {
          logger.debug("{}: metadata INVALID - read signature root pointer is null", datasetPath);
          return MetadataValidity.INVALID;
        }
        if (!readSignature
            .getRootPointer()
            .getPath()
            .equals(table.getParameters().get(HiveMetadataUtils.METADATA_LOCATION))) {
          logger.debug(
              "{}: metadata INVALID - read signature root pointer has changed, cached: {}, actual: {}",
              datasetPath,
              readSignature.getRootPointer().getPath(),
              table.getParameters().get(HiveMetadataUtils.METADATA_LOCATION));
          return MetadataValidity.INVALID;
        }
        // if the root pointer hasn't changed, no need to check for anything else and return Valid.
        return MetadataValidity.VALID;
      }

      int tableHash =
          HiveMetadataUtils.getHash(
              table,
              HiveDatasetOptions.enforceVarcharWidth(
                  HiveReaderProtoUtil.convertValuesToNonProtoAttributeValues(
                      tableXattr.getDatasetOptionMap())),
              hiveConf);
      if (tableHash != tableXattr.getTableHash()) {
        logger.debug(
            "{}: metadata INVALID - table hash has changed, cached: {}, actual: {}",
            datasetPath,
            tableXattr.getTableHash(),
            tableHash);
        return MetadataValidity.INVALID;
      }

      // cached schema may have $_dremio_update_$ column added, this should not be considered during
      // schema comparisons
      BatchSchema tableSchemaWithoutInternalCols =
          tableSchema.dropField(IncrementalUpdateUtils.UPDATE_COLUMN);
      BatchSchema hiveSchema =
          HiveMetadataUtils.getBatchSchema(
              table, hiveConf, new HiveSchemaTypeOptions(optionManager));
      if (!hiveSchema.equalsTypesWithoutPositions(tableSchemaWithoutInternalCols)) {
        // refresh metadata if converted schema is not same as schema in kvstore
        logger.debug(
            "{}: metadata INVALID - schema has changed, cached: {}, actual: {}",
            datasetPath,
            tableSchemaWithoutInternalCols,
            hiveSchema);
        return MetadataValidity.INVALID;
      }

      List<Integer> partitionHashes = new ArrayList<>();
      PartitionIterator partitionIterator =
          PartitionIterator.newBuilder()
              .client(client)
              .dbName(schemaComponents.getDbName())
              .tableName(schemaComponents.getTableName())
              .partitionBatchSize((int) hiveSettings.getPartitionBatchSize())
              .build();

      while (partitionIterator.hasNext()) {
        partitionHashes.add(HiveMetadataUtils.getHash(partitionIterator.next()));
      }

      if (!tableXattr.hasPartitionHash() || tableXattr.getPartitionHash() == 0) {
        if (partitionHashes.isEmpty()) {
          return MetadataValidity.VALID;
        } else {
          // found new partitions
          logger.debug("{}: metadata INVALID - found new partitions", datasetPath);
          return MetadataValidity.INVALID;
        }
      }

      Collections.sort(partitionHashes);
      // There were partitions in last read signature.
      if (tableXattr.getPartitionHash() != Objects.hash(partitionHashes)) {
        logger.debug("{}: metadata INVALID - found new or updated partitions", datasetPath);
        return MetadataValidity.INVALID;
      }

      return MetadataValidity.VALID;
    }
  }

  @Override
  public MetadataValidity validateMetadata(
      BytesOutput signature,
      DatasetHandle datasetHandle,
      DatasetMetadata metadata,
      ValidateMetadataOption... options)
      throws ConnectorException {

    if (null == metadata
        || null == metadata.getExtraInfo()
        || BytesOutput.NONE == metadata.getExtraInfo()
        || null == signature
        || BytesOutput.NONE == signature) {
      logger.debug(
          "{}: metadata INVALID - metadata and/or signature is null/missing",
          datasetHandle.getDatasetPath().toString());
      return MetadataValidity.INVALID;
    } else {
      try {
        final HiveTableXattr tableXattr =
            HiveTableXattr.parseFrom(bytesOutputToByteArray(metadata.getExtraInfo()));
        BatchSchema tableSchema = new BatchSchema(metadata.getRecordSchema().getFields());
        final HiveReadSignature readSignature =
            HiveReadSignature.parseFrom(bytesOutputToByteArray(signature));

        // check for hive table and partition definition changes
        MetadataValidity hiveTableStatus =
            checkHiveMetadata(
                tableXattr, datasetHandle.getDatasetPath(), tableSchema, readSignature);

        switch (hiveTableStatus) {
          case VALID:
            {
              if (readSignature.getType() == HiveReadSignatureType.FILESYSTEM) {
                try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
                  // get list of partition properties from read definition
                  List<TimedRunnable<Boolean>> signatureValidators = new ArrayList<>();
                  int totalChecks = 0, maxChecksInPartition = 0;
                  for (FileSystemPartitionUpdateKey updateKey :
                      readSignature.getFsPartitionUpdateKeysList()) {
                    signatureValidators.add(
                        new FsTask(SystemUser.SYSTEM_USERNAME, updateKey, TaskType.FS_VALIDATION));
                    totalChecks += updateKey.getCachedEntitiesCount();
                    maxChecksInPartition =
                        Math.max(maxChecksInPartition, updateKey.getCachedEntitiesCount());
                  }

                  if (signatureValidators.isEmpty()) {
                    return MetadataValidity.VALID;
                  }

                  final int effectiveParallelism =
                      Math.min(signatureValidationParallelism, signatureValidators.size());
                  final long minimumTimeout =
                      quietCheckedMultiply(signatureValidationTimeoutMS, maxChecksInPartition);
                  final long computedTimeout =
                      quietCheckedMultiply(
                          (long) Math.ceil(totalChecks / effectiveParallelism),
                          signatureValidationTimeoutMS);
                  final long timeout = Math.max(computedTimeout, minimumTimeout);

                  Stopwatch stopwatch = Stopwatch.createStarted();
                  final List<Boolean> validations =
                      runValidations(
                          datasetHandle, signatureValidators, effectiveParallelism, timeout);
                  stopwatch.stop();
                  logger.debug(
                      "Checking read signature for {} took {} ms",
                      PathUtils.constructFullPath(datasetHandle.getDatasetPath().getComponents()),
                      stopwatch.elapsed(TimeUnit.MILLISECONDS));

                  for (Boolean hasChanged : validations) {
                    if (hasChanged) {
                      logger.debug(
                          "{}: metadata INVALID - read signature has changed",
                          datasetHandle.getDatasetPath().toString());
                      return MetadataValidity.INVALID;
                    }
                  }
                }
                // fallback
              }
            }
            break;

          case INVALID:
            {
              return MetadataValidity.INVALID;
            }

          default:
            throw UserException.unsupportedError(
                    new IllegalArgumentException("Invalid hive table status " + hiveTableStatus))
                .build(logger);
        }
      } catch (IOException | TException ioe) {
        throw new ConnectorException(ioe);
      }
    }
    return MetadataValidity.VALID;
  }

  @VisibleForTesting
  void validateDatabaseExists(HiveClient client, String dbName) {
    if (!client.databaseExists(dbName)) {
      throw UserException.validationError()
          .message("Database does not exist: [%s]", dbName)
          .buildSilently();
    }
  }

  @VisibleForTesting
  List<Boolean> runValidations(
      DatasetHandle datasetHandle,
      List<TimedRunnable<Boolean>> signatureValidators,
      Integer effectiveParallelism,
      Long timeout)
      throws IOException {
    return TimedRunnable.run(
        "check read signature for "
            + PathUtils.constructFullPath(datasetHandle.getDatasetPath().getComponents()),
        logger,
        signatureValidators,
        effectiveParallelism,
        timeout);
  }

  private static long quietCheckedMultiply(long a, long b) {
    try {
      return LongMath.checkedMultiply(a, b);
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  static byte[] bytesOutputToByteArray(BytesOutput signature) throws ConnectorException {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      signature.writeTo(bos);
    } catch (IOException e) {
      throw new ConnectorException(e);
    }
    return bos.toByteArray();
  }

  protected Boolean isStorageImpersonationEnabled() {
    return storageImpersonationEnabled;
  }

  private StatsEstimationParameters getStatsParams() {
    return new StatsEstimationParameters(
        hiveSettings.useStatsInMetastore(),
        toIntExact(optionManager.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE)),
        toIntExact(optionManager.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE)),
        hiveSettings);
  }

  @Override
  public BulkResponse<EntityPathWithOptions, Optional<DatasetHandle>> bulkGetDatasetHandles(
      BulkRequest<EntityPathWithOptions> requestedDatasets) {
    MetadataIOPool metadataIOPool = context.getMetadataIOPool();
    return requestedDatasets.handleRequests(
        dataset ->
            ContextAwareCompletableFuture.createFrom(
                metadataIOPool.execute(
                    new MetadataIOPool.MetadataTask<>(
                        "bulk_get_dataset_handles_async",
                        dataset.entityPath(),
                        () -> {
                          try {
                            return internalGetDatasetHandle(
                                dataset.entityPath(), dataset.options());
                          } catch (ConnectorException ex) {
                            throw new RuntimeException(ex);
                          }
                        }))));
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(
      EntityPath datasetPath, GetDatasetOption... options) throws ConnectorException {
    return internalGetDatasetHandle(datasetPath, options);
  }

  private Optional<DatasetHandle> internalGetDatasetHandle(
      EntityPath datasetPath, GetDatasetOption... options) throws ConnectorException {
    if (!HiveMetadataUtils.isValidPathSchema(datasetPath.getComponents())) {
      return Optional.empty();
    }

    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(datasetPath.getComponents());

    if (allowedDbsList != null && !allowedDbsList.isEmpty()) {
      boolean isDbAllowed =
          allowedDbsList.stream().anyMatch(s -> s.equalsIgnoreCase(schemaComponents.getDbName()));
      if (!isDbAllowed) {
        logger.warn(
            "Plugin '{}', database '{}', table '{}', DatasetHandle empty, database not allowed.",
            this.getName(),
            schemaComponents.getDbName(),
            schemaComponents.getTableName());
        return Optional.empty();
      }
    }
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      final Table table;
      try (ManagedHiveClient wrapper = getClient(SystemUser.SYSTEM_USERNAME)) {
        table =
            wrapper
                .client()
                .getTable(schemaComponents.getDbName(), schemaComponents.getTableName(), false);
      } catch (TException e) {
        String message =
            String.format(
                "Plugin '%s', database '%s', table '%s', problem checking if table exists.",
                this.getName(), schemaComponents.getDbName(), schemaComponents.getTableName());
        logger.error(message, e);
        throw new ConnectorException(message, e);
      }

      // table exists
      if (table != null) {
        logger.debug(
            "Plugin '{}', database '{}', table '{}', DatasetHandle returned.",
            this.getName(),
            schemaComponents.getDbName(),
            schemaComponents.getTableName());
        return Optional.of(
            HiveDatasetHandle.newBuilder()
                .datasetpath(
                    new EntityPath(
                        ImmutableList.of(
                            datasetPath.getComponents().get(0),
                            schemaComponents.getDbName(),
                            schemaComponents.getTableName())))
                .internalMetadataTableOption(
                    InternalMetadataTableOption.getInternalMetadataTableOption(options))
                .table(table)
                .build());
      } else {
        logger.warn(
            "Plugin '{}', database '{}', table '{}', DatasetHandle empty, table not found.",
            this.getName(),
            schemaComponents.getDbName(),
            schemaComponents.getTableName());
        return Optional.empty();
      }
    }
  }

  protected ManagedHiveClient getClient(String user) {
    if (!isOpen.get()) {
      throw buildAlreadyClosedException();
    }

    if (!metastoreImpersonationEnabled || SystemUser.SYSTEM_USERNAME.equals(user)) {
      if (processUserMSCPool != null) {
        return ManagedHiveClient.wrapClientFromPool(processUserMSCPool);
      } else {
        return ManagedHiveClient.wrapClient(processUserMetastoreClient);
      }
    } else {
      try {
        return ManagedHiveClient.wrapClient(clientsByUser.get(user));
      } catch (ExecutionException e) {
        Throwable ex = e.getCause();
        throw Throwables.propagate(ex);
      }
    }
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
      throws ConnectorException {

    try (ManagedHiveClient wrapper = getClient(SystemUser.SYSTEM_USERNAME)) {
      return new HiveDatasetHandleListing(
          wrapper.client(),
          this.getName(),
          allowedDbsList,
          HiveMetadataUtils.isIgnoreAuthzErrors(options));
    } catch (TException e) {
      throw new ConnectorException(
          String.format("Error listing dataset handles for source %s", this.getName()), e);
    }
  }

  @Override
  public PartitionChunkListing listPartitionChunks(
      DatasetHandle datasetHandle, ListPartitionChunkOption... options) throws ConnectorException {
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      boolean enforceVarcharWidth = false;
      Optional<BytesOutput> extendedProperty =
          ExtendedPropertyOption.getExtendedPropertyFromListPartitionChunkOption(options);
      if (extendedProperty.isPresent()) {
        HiveTableXattr hiveTableXattrFromKVStore;
        try {
          hiveTableXattrFromKVStore =
              HiveTableXattr.parseFrom(bytesOutputToByteArray(extendedProperty.get()));
        } catch (InvalidProtocolBufferException e) {
          throw UserException.parseError(e).buildSilently();
        }
        enforceVarcharWidth =
            HiveDatasetOptions.enforceVarcharWidth(
                HiveReaderProtoUtil.convertValuesToNonProtoAttributeValues(
                    hiveTableXattrFromKVStore.getDatasetOptionMap()));
      }

      final HivePartitionChunkListing.Builder builder =
          HivePartitionChunkListing.newBuilder()
              .hiveConf(hiveConf)
              .storageImpersonationEnabled(storageImpersonationEnabled)
              .statsParams(getStatsParams())
              .enforceVarcharWidth(enforceVarcharWidth)
              .maxInputSplitsPerPartition(toIntExact(hiveSettings.getMaxInputSplitsPerPartition()))
              .optionManager(optionManager);

      try (ManagedHiveClient wrapper = getClient(SystemUser.SYSTEM_USERNAME)) {
        HiveClient client = wrapper.client();

        final Table table;
        if (!(datasetHandle instanceof HiveDatasetHandle)
            || ((HiveDatasetHandle) datasetHandle).getTable() == null) {
          try {
            EntityPath datasetPath = datasetHandle.getDatasetPath();
            final HiveMetadataUtils.SchemaComponents schemaComponents =
                HiveMetadataUtils.resolveSchemaComponents(datasetPath.getComponents());

            // if the dataset path is not canonized we need to get it from the source
            table =
                client.getTable(
                    schemaComponents.getDbName(),
                    schemaComponents.getTableName(),
                    HiveMetadataUtils.isIgnoreAuthzErrors(options));
            if (table == null) {
              // invalid. Guarded against at both entry points.
              throw new ConnectorException(
                  String.format("Dataset path '%s', table not found.", datasetPath));
            }
          } catch (Exception e) {
            throw new ConnectorException(e);
          }
        } else {
          table = ((HiveDatasetHandle) datasetHandle).getTable();
        }

        final TableMetadata tableMetadata =
            HiveMetadataUtils.getTableMetadata(
                table,
                datasetHandle.getDatasetPath(),
                InternalMetadataTableOption.getInternalMetadataTableOption(options),
                HiveMetadataUtils.getMaxLeafFieldCount(options),
                HiveMetadataUtils.getMaxNestedFieldLevels(options),
                TimeTravelOption.getTimeTravelOption(options),
                new HiveSchemaTypeOptions(optionManager),
                hiveConf,
                this,
                context);

        if (!tableMetadata.getPartitionColumns().isEmpty()) {
          try {
            builder.partitions(
                PartitionIterator.newBuilder()
                    .client(client)
                    .dbName(tableMetadata.getTable().getDbName())
                    .tableName(tableMetadata.getTable().getTableName())
                    .filteredPartitionNames(
                        HiveMetadataUtils.getFilteredPartitionNames(
                            // tableMetadata.getPartitionColumns() source of truth for partition
                            // cols ordering
                            tableMetadata.getPartitionColumns(),
                            tableMetadata.getTable().getPartitionKeys(),
                            options))
                    .partitionBatchSize(toIntExact(hiveSettings.getPartitionBatchSize()))
                    .build());
          } catch (TException | RuntimeException e) {
            throw new ConnectorException(e);
          }
        }

        return buildSplits(builder, tableMetadata, options).tableMetadata(tableMetadata).build();
      }
    }
  }

  private HivePartitionChunkListing.Builder buildSplits(
      HivePartitionChunkListing.Builder builder,
      TableMetadata tableMetadata,
      ListPartitionChunkOption[] options) {
    HivePartitionChunkListing.SplitType splitType = getSplitType(tableMetadata, options);
    builder.splitType(splitType);
    if (splitType == DELTA_COMMIT_LOGS) {
      builder.deltaSplits(
          getDeltaSplits(tableMetadata, TimeTravelOption.getTimeTravelOption(options)));
    }
    return builder;
  }

  private List<DatasetSplit> getDeltaSplits(
      TableMetadata tableMetadata, TimeTravelOption timeTravelOption) {
    try {
      String tableLocation = DeltaHiveInputFormat.getLocation(tableMetadata.getTable());
      FileSystem fs = createFS(tableLocation, SystemUser.SYSTEM_USERNAME, null);
      TimeTravelOption.TimeTravelRequest timeTravelRequest =
          timeTravelOption != null ? timeTravelOption.getTimeTravelRequest() : null;
      DeltaLakeTable deltaLakeTable =
          new DeltaLakeTable(getSabotContext(), fs, tableLocation, timeTravelRequest);
      return deltaLakeTable.getAllSplits();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private HivePartitionChunkListing.SplitType getSplitType(
      TableMetadata tableMetadata, ListPartitionChunkOption[] options) {
    if (HiveMetadataUtils.isIcebergTable(tableMetadata.getTable())
        || InternalMetadataTableOption.getInternalMetadataTableOption(options) != null) {
      return ICEBERG_MANIFEST_SPLIT;
    }

    if (DeltaHiveInputFormat.isDeltaTable(tableMetadata.getTable())) {
      return DELTA_COMMIT_LOGS;
    }

    if (HiveMetadataUtils.isDirListInputSplitType(options)) {
      return DIR_LIST_INPUT_SPLIT;
    }

    return INPUT_SPLIT;
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle, PartitionChunkListing chunkListing, GetMetadataOption... options)
      throws ConnectorException {

    final HivePartitionChunkListing hivePartitionChunkListing =
        chunkListing.unwrap(HivePartitionChunkListing.class);
    final TableMetadata tableMetadata = hivePartitionChunkListing.getTableMetadata();

    final MetadataAccumulator metadataAccumulator =
        hivePartitionChunkListing.getMetadataAccumulator();
    final Table table = tableMetadata.getTable();
    final Properties tableProperties = tableMetadata.getTableProperties();

    final HiveTableXattr.Builder tableExtended = HiveTableXattr.newBuilder();

    accumulateTableMetadata(tableExtended, metadataAccumulator, table, tableProperties);

    boolean enforceVarcharWidth = false;
    Optional<BytesOutput> extendedProperty =
        ExtendedPropertyOption.getExtendedPropertyFromMetadataOption(options);
    if (extendedProperty.isPresent()) {
      HiveTableXattr hiveTableXattrFromKVStore;
      try {
        hiveTableXattrFromKVStore =
            HiveTableXattr.parseFrom(bytesOutputToByteArray(extendedProperty.get()));
      } catch (InvalidProtocolBufferException e) {
        throw UserException.parseError(e).buildSilently();
      }
      enforceVarcharWidth =
          HiveDatasetOptions.enforceVarcharWidth(
              HiveReaderProtoUtil.convertValuesToNonProtoAttributeValues(
                  hiveTableXattrFromKVStore.getDatasetOptionMap()));
    }

    tableExtended.setTableHash(HiveMetadataUtils.getHash(table, enforceVarcharWidth, hiveConf));
    tableExtended.setPartitionHash(metadataAccumulator.getPartitionHash());
    tableExtended.setReaderType(metadataAccumulator.getReaderType());
    tableExtended.addAllColumnInfo(tableMetadata.getColumnInfos());
    tableExtended.putDatasetOption(
        HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH,
        HiveReaderProtoUtil.toProtobuf(AttributeValue.of(enforceVarcharWidth)));

    tableExtended.addAllInputFormatDictionary(metadataAccumulator.buildInputFormatDictionary());
    tableExtended.addAllSerializationLibDictionary(
        metadataAccumulator.buildSerializationLibDictionary());
    tableExtended.addAllStorageHandlerDictionary(
        metadataAccumulator.buildStorageHandlerDictionary());
    tableExtended.addAllPropertyDictionary(metadataAccumulator.buildPropertyDictionary());

    // this is used as an indicator on read to determine if dictionaries are used or if the metadata
    // is from a prior version of Dremio.
    tableExtended.setPropertyCollectionType(PropertyCollectionType.DICTIONARY);

    return HiveDatasetMetadata.newBuilder()
        .schema(tableMetadata.getBatchSchema())
        .partitionColumns(tableMetadata.getPartitionColumns())
        .sortColumns(
            FluentIterable.from(table.getSd().getSortCols())
                .transform(order -> order.getCol())
                .toList())
        .metadataAccumulator(metadataAccumulator)
        .extraInfo(os -> os.write((tableExtended.build().toByteArray())))
        .icebergMetadata(tableMetadata.getIcebergMetadata())
        .manifestStats(tableMetadata.getManifestStats())
        .build();
  }

  private void accumulateTableMetadata(
      HiveTableXattr.Builder tableExtended,
      MetadataAccumulator metadataAccumulator,
      Table table,
      Properties tableProperties) {
    for (Prop prop : HiveMetadataUtils.fromProperties(tableProperties)) {
      tableExtended.addTablePropertySubscript(metadataAccumulator.getTablePropSubscript(prop));
    }

    if (table.getSd().getInputFormat() != null) {
      tableExtended.setTableInputFormatSubscript(
          metadataAccumulator.getTableInputFormatSubscript(table.getSd().getInputFormat()));
    }

    final String storageHandler = table.getParameters().get(META_TABLE_STORAGE);
    if (storageHandler != null) {
      tableExtended.setTableStorageHandlerSubscript(
          metadataAccumulator.getTableStorageHandlerSubscript(storageHandler));
    }

    tableExtended.setTableSerializationLibSubscript(
        metadataAccumulator.getTableSerializationLibSubscript(
            table.getSd().getSerdeInfo().getSerializationLib()));
  }

  @Override
  public BytesOutput provideSignature(DatasetHandle datasetHandle, DatasetMetadata metadata)
      throws ConnectorException {
    final HiveDatasetMetadata hiveDatasetMetadata = metadata.unwrap(HiveDatasetMetadata.class);

    final MetadataAccumulator metadataAccumulator = hiveDatasetMetadata.getMetadataAccumulator();

    if (metadataAccumulator.allFSBasedPartitions()
        && !metadataAccumulator.getFileSystemPartitionUpdateKeys().isEmpty()) {
      return os ->
          os.write(
              HiveReadSignature.newBuilder()
                  .setType(HiveReadSignatureType.FILESYSTEM)
                  .addAllFsPartitionUpdateKeys(
                      metadataAccumulator.getFileSystemPartitionUpdateKeys())
                  .build()
                  .toByteArray());
    } else if (!metadataAccumulator.allFSBasedPartitions()
        && metadataAccumulator.getRootPointer() != null) {
      return os ->
          os.write(
              HiveReadSignature.newBuilder()
                  .setType(HiveReadSignatureType.VERSION_BASED)
                  .setRootPointer(metadataAccumulator.getRootPointer())
                  .build()
                  .toByteArray());
    } else {
      return BytesOutput.NONE;
    }
  }

  @Override
  public SourceState getState() {
    // Executors maintain no state about Hive; they do not communicate with the Hive meta store, so
    // only tables can
    // have a bad state, and not the source.
    if (!isCoordinator) {
      return SourceState.GOOD;
    }

    if (!isOpen.get()) {
      logger.debug(
          "Tried to get the state of a {} plugin that is either not started or already closed: {}.",
          getRealSourceTypeName(),
          this.getName());
      return new SourceState(
          SourceStatus.bad,
          getBadStateErrorMessage(),
          ImmutableList.of(
              new SourceState.Message(
                  MessageLevel.ERROR,
                  String.format(
                      "Hive Metastore client on source %s was not started or already closed.",
                      this.getName()))));
    }

    try {
      checkClientState();
      return SourceState.GOOD;
    } catch (Exception ex) {
      logger.debug(
          "Caught exception while trying to get status of {} source {}, error: ",
          getRealSourceTypeName(),
          this.getName(),
          ex);
      // DX-24956
      return new SourceState(
          SourceStatus.bad,
          getBadStateErrorMessage(),
          Collections.singletonList(
              new SourceState.Message(
                  MessageLevel.ERROR, "Failure connecting to source: " + ex.getMessage())));
    }
  }

  @VisibleForTesting
  void checkClientState() throws Exception {

    HiveClient hiveClient =
        processUserMSCPool != null ? processUserMSCPool.borrowObject() : processUserMetastoreClient;

    try {
      hiveClient.checkState(false);

      if (processUserMSCPool != null) {
        // note: not in finally as we potentitally make client swaps in the catch clause
        processUserMSCPool.returnObject(hiveClient);
      }
    } catch (Exception originalEx) {
      if (optionManager.getOption(CatalogOptions.RETRY_CONNECTION_ON_FAILURE)) {

        if (processUserMSCPool != null) {
          // destroy all clients in the pool in an attempt to refresh all of them
          processUserMSCPool.clear();

          try (ManagedHiveClient wrapper =
              ManagedHiveClient.wrapClientFromPool(processUserMSCPool)) {
            wrapper.client().checkState(false);
          } catch (Exception ex) {
            throw originalEx;
          }
        }

        try (AutoCloseable oldClient = processUserMetastoreClient) {
          processUserMetastoreClient = createConnectedClient();
          processUserMetastoreClient.checkState(false);
          return;
        } catch (Exception ex) {
          throw originalEx;
        }

      } else {
        throw originalEx;
      }
    }
  }

  private String getBadStateErrorMessage() {
    return String.format(
        "Could not connect to %s source %s, check your credentials and network settings.",
        getRealSourceTypeName(), this.getName());
  }

  @Override
  public void close() {
    HivePf4jPlugin.unregisterMetricMBean();

    if (!isOpen.getAndSet(false)) {
      if (!isFirstPluginCreate.getAndSet(false)) {
        return;
      }
    }

    if (processUserMSCPool != null) {
      processUserMSCPool.close();
      processUserMSCPool = null;
    }
    if (processUserMetastoreClient != null) {
      processUserMetastoreClient.close();
      processUserMetastoreClient = null;
    }
    if (clientsByUser != null) {
      clientsByUser.invalidateAll();
      clientsByUser.cleanUp();
      clientsByUser = null;
    }

    try {
      hadoopFsSupplierProviderPluginClassLoader.close();
    } catch (Exception e) {
      logger.warn("Failed to close hadoopFsSupplierProviderPluginClassLoader", e);
    }

    if (HiveFsUtils.isFsPluginCacheEnabled(hiveConf)) {
      HadoopFsWrapperWithCachePluginClassLoader.cleanCache(confUniqueIdentifier);
    }

    if (pf4jManager != null) {
      pf4jManager.stopPlugins();
    }
  }

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  @Override
  public void start() {

    try {
      setupHadoopUserUsingKerberosKeytab();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    if (isCoordinator
        || optionManager.getOption(ExecConstants.CREATE_HIVECLIENT_ON_EXECUTOR_NODES)) {
      try {
        if (hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL)) {
          logger.info(
              "Hive Metastore SASL enabled. Kerberos principal: "
                  + hiveConf.getVar(ConfVars.METASTORE_KERBEROS_PRINCIPAL));
        }

        int clientPoolSize =
            (int) optionManager.getOption(Hive3PluginOptions.HIVE_CLIENT_POOL_SIZE);
        Preconditions.checkState(clientPoolSize >= 0, "expected positive client pool size");
        if (clientPoolSize != 0) {
          GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
          poolConfig.setMaxTotal(clientPoolSize);
          poolConfig.setJmxNamePrefix(getName());
          processUserMSCPool =
              new GenericObjectPool<>(
                  new PooledHiveClientFactory(), poolConfig, CLIENT_POOL_ABANDONED_CONFIG);

          try (ManagedHiveClient wrapper =
              ManagedHiveClient.wrapClientFromPool(processUserMSCPool)) {
            // no-op - just a client open
          }
        }

        processUserMetastoreClient = createConnectedClient();

      } catch (Exception e) {
        throw Throwables.propagate(e);
      }

      // Note: We are assuming any code after assigning processUserMSCPool cannot throw.
      isOpen.set(true);

      boolean useZeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(hiveConf);
      logger.info("ORC Zero-Copy {}.", useZeroCopy ? "enabled" : "disabled");

      clientsByUser =
          CacheBuilder.newBuilder()
              .expireAfterAccess(5, TimeUnit.MINUTES)
              .maximumSize(5) // Up to 5 clients for impersonation-enabled.
              .removalListener(
                  new RemovalListener<String, HiveClient>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, HiveClient> notification) {
                      HiveClient client = notification.getValue();
                      client.close();
                    }
                  })
              .build(
                  new CacheLoader<String, HiveClient>() {
                    @Override
                    public HiveClient load(String userName) throws Exception {
                      final UserGroupInformation ugiForRpc;

                      if (!storageImpersonationEnabled) {
                        // If the user impersonation is disabled in Hive storage plugin, use the
                        // process user UGI credentials.
                        ugiForRpc = HiveImpersonationUtil.getProcessUserUGI();
                      } else {
                        ugiForRpc = HiveImpersonationUtil.createProxyUgi(getUsername(userName));
                      }

                      return createConnectedClientWithAuthz(userName, ugiForRpc);
                    }
                  });
    } else {
      processUserMSCPool = null;
      clientsByUser = null;
    }
  }

  /**
   * Set up the current user in {@link UserGroupInformation} using the kerberos principal and keytab
   * file path if present in config. If not present, this method call is a no-op. When communicating
   * with the kerberos enabled Hadoop based filesystem credentials in {@link UserGroupInformation}
   * will be used..
   *
   * @throws IOException
   */
  private void setupHadoopUserUsingKerberosKeytab() throws IOException {
    final String kerberosPrincipal = dremioConfig.getString(DremioConfig.KERBEROS_PRINCIPAL);
    final String kerberosKeytab = dremioConfig.getString(DremioConfig.KERBEROS_KEYTAB_PATH);

    if (Strings.isNullOrEmpty(kerberosPrincipal) || Strings.isNullOrEmpty(kerberosKeytab)) {
      return;
    }

    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);

    logger.info(
        "Setup Hadoop user info using kerberos principal {} and keytab file {} successful.",
        kerberosPrincipal,
        kerberosKeytab);
  }

  public String getUsername(String name) {
    if (isStorageImpersonationEnabled()) {
      return name;
    }
    return SystemUser.SYSTEM_USERNAME;
  }

  /** Creates an instance of a class that was loaded by PF4J. */
  @Override
  public Class<? extends HiveProxiedSubScan> getSubScanClass() {
    return HiveSubScan.class;
  }

  @Override
  public HiveProxiedScanBatchCreator createScanBatchCreator(
      FragmentExecutionContext fragmentExecContext,
      OperatorContext context,
      HiveProxyingSubScan config)
      throws ExecutionSetupException {
    return new HiveScanBatchCreator(fragmentExecContext, context, config);
  }

  @Override
  public HiveProxiedScanBatchCreator createScanBatchCreator(
      FragmentExecutionContext fragmentExecContext,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig tableFunctionConfig)
      throws ExecutionSetupException {
    return new HiveScanBatchCreator(fragmentExecContext, context, props, tableFunctionConfig);
  }

  @Override
  public Class<? extends HiveProxiedOrcScanFilter> getOrcScanFilterClass() {
    return ORCScanFilter.class;
  }

  @Override
  public DatasetMetadata alterMetadata(
      DatasetHandle datasetHandle,
      DatasetMetadata oldDatasetMetadata,
      Map<String, AttributeValue> attributes,
      AlterMetadataOption... options)
      throws ConnectorException {

    final HiveTableXattr hiveTableXattr;
    try {
      hiveTableXattr =
          HiveTableXattr.parseFrom(bytesOutputToByteArray(oldDatasetMetadata.getExtraInfo()));
    } catch (InvalidProtocolBufferException e) {
      throw UserException.parseError(e).buildSilently();
    }

    boolean noneChanged = true; // option values are same as stored values
    HiveTableXattr.Builder newXattrsBuilder = hiveTableXattr.toBuilder();

    for (Map.Entry<String, AttributeValue> entry : attributes.entrySet()) {
      String key = entry.getKey().toLowerCase();
      AttributeValue newValue = entry.getValue();

      Optional<AttributeValue> defaultValue = HiveDatasetOptions.getDefaultValue(key);

      if (!defaultValue.isPresent()) {
        throw UserException.validationError().message("Invalid Option [%s]", key).buildSilently();
      }

      if (!(newValue.getClass().equals(defaultValue.get().getClass()))) {
        throw UserException.validationError()
            .message(
                "Option [%s] requires a value of type [%s]",
                key, HiveReaderProtoUtil.getTypeName(defaultValue.get()))
            .buildSilently();
      }

      AttributeValue currentValue =
          HiveReaderProtoUtil.convertValuesToNonProtoAttributeValues(
                  newXattrsBuilder.getDatasetOptionMap())
              .getOrDefault(key.toLowerCase(), defaultValue.get());

      boolean oldAndNewValueIsSame;
      if (currentValue instanceof AttributeValue.StringValue) {
        oldAndNewValueIsSame =
            ((AttributeValue.StringValue) currentValue)
                .equalsIgnoreCase((AttributeValue.StringValue) newValue);
      } else {
        oldAndNewValueIsSame = currentValue.equals(newValue);
      }

      if (!oldAndNewValueIsSame) { // value changed
        newXattrsBuilder.putDatasetOption(key, HiveReaderProtoUtil.toProtobuf(newValue));
      }
      noneChanged = noneChanged && oldAndNewValueIsSame;
    }

    if (noneChanged) {
      return oldDatasetMetadata;
    }

    return DatasetMetadata.of(
        oldDatasetMetadata.getDatasetStats(),
        oldDatasetMetadata.getRecordSchema(),
        oldDatasetMetadata.getPartitionColumns(),
        oldDatasetMetadata.getSortColumns(),
        os -> ByteString.writeTo(os, ByteString.copyFrom(newXattrsBuilder.build().toByteArray())));
  }

  @VisibleForTesting
  HiveClient createConnectedClient() throws MetaException {
    return HiveClientImpl.createConnectedClient(hiveConf);
  }

  @VisibleForTesting
  HiveClient createConnectedClientWithAuthz(
      final String userName, final UserGroupInformation ugiForRpc) throws MetaException {
    Preconditions.checkArgument(processUserMetastoreClient instanceof HiveClientImpl);
    return HiveClientImpl.createConnectedClientWithAuthz(
        processUserMetastoreClient, hiveConf, userName, ugiForRpc);
  }

  private UserException buildAlreadyClosedException() {
    return UserException.sourceInBadState()
        .message(
            "The %s source %s is either not started or already closed",
            getRealSourceTypeName(), this.getName())
        .addContext("name", this.getName())
        .buildSilently();
  }

  @Override
  public <T> T getPF4JStoragePlugin() {
    return (T) this;
  }

  @Override
  public FooterReadTableFunction getFooterReaderTableFunction(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig) {
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      return new HiveFooterReaderTableFunction(fec, context, props, functionConfig);
    }
  }

  @Override
  public boolean createFunction(
      CatalogEntityKey key, SchemaConfig schemaConfig, UserDefinedFunction userDefinedFunction) {
    throw new UnsupportedOperationException(
        "Hive3 plugin doesn't support function creation via CREATE FUNCTION.");
  }

  @Override
  public boolean updateFunction(
      CatalogEntityKey key, SchemaConfig schemaConfig, UserDefinedFunction userDefinedFunction) {
    throw new UnsupportedOperationException(
        "Hive3 plugin doesn't support function update via CREATE OR REPLACE FUNCTION.");
  }

  @Override
  public void dropFunction(CatalogEntityKey key, SchemaConfig schemaconfig) {
    throw new UnsupportedOperationException(
        "Hive3 plugin doesn't support function drop via DROP FUNCTION.");
  }

  /**
   * Returns the source type name served by this plugin. Since Hive2, Hive3 and AWS Glue all use
   * this Hive3 plugin, it can be any of them. Unfortunately linking the name property of the
   * respective source config classes is not possible due to the dependency indirection going the
   * other way.
   *
   * @return source type name
   */
  private String getRealSourceTypeName() {
    if (HiveConfFactory.isAWSGlueSourceType(hiveConf)) {
      return "AWS Glue";
    }
    return String.format("Hive %d.x", HiveConfFactory.isHive2SourceType(hiveConf) ? 2 : 3);
  }

  protected List<HivePrivObjectActionType> getPrivilegeActionTypesForIcebergDml(
      IcebergCommandType commandType) {
    switch (commandType) {
      case INSERT:
        return ImmutableList.of(HivePrivObjectActionType.INSERT);
      case DELETE:
        return ImmutableList.of(HivePrivObjectActionType.DELETE);
      case UPDATE:
      case OPTIMIZE:
      case ROLLBACK:
      case VACUUM:
        return ImmutableList.of(HivePrivObjectActionType.UPDATE);
      case MERGE:
        return ImmutableList.of(
            HivePrivObjectActionType.INSERT,
            HivePrivObjectActionType.DELETE,
            HivePrivObjectActionType.UPDATE);
      default:
        throw new IllegalArgumentException(
            String.format("Unexpected command type %s - expected DML command type", commandType));
    }
  }

  private final class PooledHiveClientFactory extends BasePooledObjectFactory<HiveClient> {

    @Override
    public HiveClient create() throws Exception {
      return createConnectedClient();
    }

    @Override
    public PooledObject<HiveClient> wrap(HiveClient hiveClient) {
      return new DefaultPooledObject<>(hiveClient);
    }

    @Override
    public void destroyObject(PooledObject<HiveClient> p) throws Exception {
      p.getObject().close();
      super.destroyObject(p);
    }
  }
}

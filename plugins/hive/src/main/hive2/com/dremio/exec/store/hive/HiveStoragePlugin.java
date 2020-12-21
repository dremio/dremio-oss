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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.orc.OrcConf;
import org.pf4j.PluginManager;
import org.slf4j.helpers.MessageFormatter;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.common.utils.PathUtils;
import com.dremio.config.DremioConfig;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.ExtendedPropertyOption;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsAlteringDatasetMetadata;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.connector.metadata.extensions.ValidateMetadataOption;
import com.dremio.connector.metadata.options.AlterMetadataOption;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.SupportsPF4JStoragePlugin;
import com.dremio.exec.store.TimedRunnable;
import com.dremio.exec.store.hive.exec.HiveDatasetOptions;
import com.dremio.exec.store.hive.exec.HiveReaderProtoUtil;
import com.dremio.exec.store.hive.exec.HiveScanBatchCreator;
import com.dremio.exec.store.hive.exec.HiveSubScan;
import com.dremio.exec.store.hive.exec.apache.HadoopFileSystemWrapper;
import com.dremio.exec.store.hive.metadata.HiveDatasetHandle;
import com.dremio.exec.store.hive.metadata.HiveDatasetHandleListing;
import com.dremio.exec.store.hive.metadata.HiveDatasetMetadata;
import com.dremio.exec.store.hive.metadata.HiveMetadataUtils;
import com.dremio.exec.store.hive.metadata.HivePartitionChunkListing;
import com.dremio.exec.store.hive.metadata.MetadataAccumulator;
import com.dremio.exec.store.hive.metadata.PartitionIterator;
import com.dremio.exec.store.hive.metadata.StatsEstimationParameters;
import com.dremio.exec.store.hive.metadata.TableMetadata;
import com.dremio.exec.store.hive.proxy.HiveProxiedOrcScanFilter;
import com.dremio.exec.store.hive.proxy.HiveProxiedScanBatchCreator;
import com.dremio.exec.store.hive.proxy.HiveProxiedSubScan;
import com.dremio.hive.proto.HiveReaderProto.FileSystemCachedEntity;
import com.dremio.hive.proto.HiveReaderProto.FileSystemPartitionUpdateKey;
import com.dremio.hive.proto.HiveReaderProto.HiveReadSignature;
import com.dremio.hive.proto.HiveReaderProto.HiveReadSignatureType;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.hive.proto.HiveReaderProto.PropertyCollectionType;
import com.dremio.hive.thrift.TException;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.MessageLevel;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.capabilities.BooleanCapabilityValue;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
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

public class HiveStoragePlugin extends BaseHiveStoragePlugin implements StoragePluginCreator.PF4JStoragePlugin, SupportsReadSignature,
    SupportsListingDatasets, SupportsAlteringDatasetMetadata, SupportsPF4JStoragePlugin {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveStoragePlugin.class);

  private LoadingCache<String, HiveClient> clientsByUser;
  private final PluginManager pf4jManager;
  private final HiveConf hiveConf;
  private final SabotConfig sabotConfig;
  private final DremioConfig dremioConfig;

  private HiveClient processUserMetastoreClient;
  private final boolean storageImpersonationEnabled;
  private final boolean metastoreImpersonationEnabled;
  private final boolean isCoordinator;
  private final HiveSettings hiveSettings;
  private final OptionManager optionManager;

  private final AtomicBoolean isOpen = new AtomicBoolean(false);

  private int signatureValidationParallelism = 16;
  private long signatureValidationTimeoutMS = 2_000L;

  @VisibleForTesting
  public HiveStoragePlugin(HiveConf hiveConf, SabotContext context, String name) {
    this(hiveConf, null, context, name);
  }

  public HiveStoragePlugin(HiveConf hiveConf, PluginManager pf4jManager, SabotContext context, String name) {
    super(context, name);
    this.isCoordinator = context.isCoordinator();
    this.hiveConf = hiveConf;
    this.pf4jManager = pf4jManager;
    this.sabotConfig = context.getConfig();
    this.hiveSettings = new HiveSettings(context.getOptionManager());
    this.optionManager = context.getOptionManager();
    this.dremioConfig = context.getDremioConfig();

    storageImpersonationEnabled = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS);

    // Hive Metastore impersonation is enabled if:
    // - "hive.security.authorization.enabled" is set to true,
    // - "hive.metastore.execute.setugi" is set to true (in SASL disabled scenarios) or
    // - "hive.metastore.sasl.enabled" is set to true in which case all metastore calls are impersonated as
    //     the authenticated user.
    this.metastoreImpersonationEnabled =
      hiveConf.getBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED) ||
        hiveConf.getBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI) ||
        hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL);

    if (optionManager != null) {
      this.signatureValidationParallelism = Long.valueOf(optionManager.getOption(ExecConstants.HIVE_SIGNATURE_VALIDATION_PARALLELISM)).intValue();
      this.signatureValidationTimeoutMS = optionManager.getOption(ExecConstants.HIVE_SIGNATURE_VALIDATION_TIMEOUT_MS);
    }
  }

  @Override
  public boolean containerExists(EntityPath key) {
    if(key.size() != 2){
      return false;
    }
    return getClient(SystemUser.SYSTEM_USERNAME).databaseExists(key.getComponents().get(1));
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  public HiveConf getHiveConf() {
    return hiveConf;
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
      final HiveMetadataUtils.SchemaComponents schemaComponents = HiveMetadataUtils.resolveSchemaComponents(key.getPathComponents(), true);
      final Table table = clientsByUser
        .get(user).getTable(schemaComponents.getDbName(), schemaComponents.getTableName(), true);
      if (table == null) {
        return false;
      }
      if (storageImpersonationEnabled) {
        if (datasetConfig.getReadDefinition() != null && datasetConfig.getReadDefinition().getReadSignature() != null) {
          final HiveReadSignature readSignature = HiveReadSignature.parseFrom(datasetConfig.getReadDefinition().getReadSignature().toByteArray());
          // for now we only support fs based read signatures
          if (readSignature.getType() == HiveReadSignatureType.FILESYSTEM) {
            // get list of partition properties from read definition
            HiveTableXattr tableXattr = HiveTableXattr.parseFrom(datasetConfig.getReadDefinition().getExtendedProperty().asReadOnlyByteBuffer());
            return hasFSPermission(getUsername(user), key, readSignature.getFsPartitionUpdateKeysList(), tableXattr);
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
      if(rootCause instanceof TException) {
        throw UserException.connectionError(e)
          .message("Unable to connect to Hive metastore: %s", rootCause.getMessage())
          .build(logger);
      }

      Throwable cause = e.getCause();
      if (cause instanceof AuthorizerServiceException || cause instanceof RuntimeException) {
        throw e;
      }
      logger.error("User: {} is trying to access Hive dataset with path: {}.", this.getName(), key, e);
    }

    return false;
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return new SourceCapabilities(new BooleanCapabilityValue(SourceCapabilities.VARCHARS_WITH_WIDTH, true));
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    // Do not use SabotConfig#getClass() here, since we need to search within the plugin's classloader.
    final String RULES_FACTORY_PATH = "dremio.plugins.hive.rulesfactory";
    if (sabotConfig.hasPath(RULES_FACTORY_PATH)) {
      final String rulesFactoryClassName = sabotConfig.getString("dremio.plugins.hive.rulesfactory");

      try {
        Class<?> clazz = Class.forName(rulesFactoryClassName);
        Preconditions.checkArgument(StoragePluginRulesFactory.class.isAssignableFrom(clazz));
        return (Class<? extends StoragePluginRulesFactory>) clazz;
      } catch (ClassNotFoundException e) {
        throw UserException.unsupportedError(e)
          .message("Failure while attempting to find implementation class %s for interface  %s. The sabot config key is %s ",
            rulesFactoryClassName, StoragePluginRulesFactory.class.getName(), rulesFactoryClassName).build(logger);
      }
    }

    return HiveRulesFactory.class;
  }

  private boolean hasFSPermission(String user, NamespaceKey key, List<FileSystemPartitionUpdateKey> updateKeys,
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

      final int effectiveParallelism = Math.min(signatureValidationParallelism,  permissionCheckers.size());
      final long minimumTimeout = quietCheckedMultiply(signatureValidationTimeoutMS, maxChecksInPartition);
      final long computedTimeout = quietCheckedMultiply((long) Math.ceil(totalChecks / effectiveParallelism), signatureValidationTimeoutMS);
      final long timeout = Math.max(computedTimeout, minimumTimeout);

      Stopwatch stopwatch = Stopwatch.createStarted();
      final List<Boolean> accessPermissions = TimedRunnable.run("check access permission for " + key, logger, permissionCheckers, effectiveParallelism, timeout);
      stopwatch.stop();
      logger.debug("Checking access permission for {} took {} ms", key, stopwatch.elapsed(TimeUnit.MILLISECONDS));
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
            new InvalidMetadataErrorContext(ImmutableList.of(key.getPathComponents()))
          ).build(logger);
      }
      throw UserException.dataReadError(ioe).build(logger);
    }
    return true;
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
      Preconditions.checkArgument(updateKey.getCachedEntitiesCount() > 0, "hive partition update key should contain at least one path");

      for (FileSystemCachedEntity cachedEntity : updateKey.getCachedEntitiesList()) {
        final Path cachedEntityPath;
        if (cachedEntity.getPath() == null || cachedEntity.getPath().isEmpty()) {
          cachedEntityPath = new Path(updateKey.getPartitionRootDir());
        } else {
          cachedEntityPath = new Path(updateKey.getPartitionRootDir(), cachedEntity.getPath());
        }
        // Create filesystem for the given user and given path
        // TODO: DX-16001 - make async configurable for Hive.
        final HadoopFileSystemWrapper userFS = HiveImpersonationUtil.createFileSystem(user, jobConf, cachedEntityPath);
        try {
          if (cachedEntity.getIsDir()) {
            //DX-7850 : remove once solution for maprfs is found
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
      Preconditions.checkArgument(updateKey.getCachedEntitiesCount() > 0, "hive partition update key should contain at least one path");

      // create filesystem based on the first path which is root of the partition directory.
      final HadoopFileSystemWrapper fs = new HadoopFileSystemWrapper(new Path(updateKey.getPartitionRootDir()), jobConf);
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
            return true;
          }
        } else {
          return true;
        }
      }
      return false;
    }
  }

  @VisibleForTesting
  MetadataValidity checkHiveMetadata(HiveTableXattr tableXattr, EntityPath datasetPath, BatchSchema tableSchema) throws TException {
    final HiveClient client = getClient(SystemUser.SYSTEM_USERNAME);

    final HiveMetadataUtils.SchemaComponents schemaComponents = HiveMetadataUtils.resolveSchemaComponents(datasetPath.getComponents(), true);

    Table table = client.getTable(schemaComponents.getDbName(), schemaComponents.getTableName(), true);

    if (table == null) { // missing table?
      return MetadataValidity.INVALID;
    }

    if (HiveMetadataUtils.getHash(table,
        HiveDatasetOptions.enforceVarcharWidth(HiveReaderProtoUtil.convertValuesToNonProtoAttributeValues(tableXattr.getDatasetOptionMap())), hiveConf)
        != tableXattr.getTableHash()) {
      return MetadataValidity.INVALID;
    }
    boolean includeComplexTypes = optionManager.getOption(ExecConstants.HIVE_COMPLEXTYPES_ENABLED);
    int hiveTableHash = HiveMetadataUtils.getBatchSchema(table, hiveConf, includeComplexTypes).hashCode();
    if (hiveTableHash != tableSchema.hashCode()) {
      // refresh metadata if converted schema is not same as schema in kvstore
      return MetadataValidity.INVALID;
    }

    List<Integer> partitionHashes = new ArrayList<>();
    PartitionIterator partitionIterator = PartitionIterator.newBuilder()
      .client(client)
      .dbName(schemaComponents.getDbName())
      .tableName(schemaComponents.getTableName())
      .partitionBatchSize(toIntExact(hiveSettings.getPartitionBatchSize()))
      .build();

    while (partitionIterator.hasNext()) {
      partitionHashes.add(HiveMetadataUtils.getHash(partitionIterator.next()));
    }

    if (!tableXattr.hasPartitionHash() || tableXattr.getPartitionHash() == 0) {
      if (partitionHashes.isEmpty()) {
        return MetadataValidity.VALID;
      } else {
        // found new partitions
        return MetadataValidity.INVALID;
      }
    }

    Collections.sort(partitionHashes);
    // There were partitions in last read signature.
    if (tableXattr.getPartitionHash() != Objects.hash(partitionHashes)) {
      return MetadataValidity.INVALID;
    }

    return MetadataValidity.VALID;
  }

  @Override
  public MetadataValidity validateMetadata(BytesOutput signature, DatasetHandle datasetHandle, DatasetMetadata metadata, ValidateMetadataOption... options) throws ConnectorException {

    if (null == metadata || null == metadata.getExtraInfo() || BytesOutput.NONE == metadata.getExtraInfo() ||
      null == signature || BytesOutput.NONE == signature) {
      return MetadataValidity.INVALID;
    } else {
      try {
        final HiveTableXattr tableXattr = HiveTableXattr.parseFrom(bytesOutputToByteArray(metadata.getExtraInfo()));
        BatchSchema tableSchema = new BatchSchema(metadata.getRecordSchema().getFields());

        // check for hive table and partition definition changes
        MetadataValidity hiveTableStatus = checkHiveMetadata(tableXattr, datasetHandle.getDatasetPath(), tableSchema);

        switch (hiveTableStatus) {
          case VALID: {
            final HiveReadSignature readSignature = HiveReadSignature.parseFrom(bytesOutputToByteArray(signature));
            // for now we only support fs based read signatures
            if (readSignature.getType() == HiveReadSignatureType.FILESYSTEM) {
              try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
                // get list of partition properties from read definition
                List<TimedRunnable<Boolean>> signatureValidators = new ArrayList<>();
                int totalChecks = 0, maxChecksInPartition = 0;
                for (FileSystemPartitionUpdateKey updateKey : readSignature.getFsPartitionUpdateKeysList()) {
                  signatureValidators.add(new FsTask(SystemUser.SYSTEM_USERNAME, updateKey, TaskType.FS_VALIDATION));
                  totalChecks += updateKey.getCachedEntitiesCount();
                  maxChecksInPartition = Math.max(maxChecksInPartition, updateKey.getCachedEntitiesCount());
                }

                if (signatureValidators.isEmpty()) {
                  return MetadataValidity.VALID;
                }

                final int effectiveParallelism = Math.min(signatureValidationParallelism,  signatureValidators.size());
                final long minimumTimeout = quietCheckedMultiply(signatureValidationTimeoutMS, maxChecksInPartition);
                final long computedTimeout = quietCheckedMultiply((long) Math.ceil(totalChecks / effectiveParallelism), signatureValidationTimeoutMS);
                final long timeout = Math.max(computedTimeout, minimumTimeout);

                Stopwatch stopwatch = Stopwatch.createStarted();
                final List<Boolean> validations = runValidations(datasetHandle, signatureValidators,
                  effectiveParallelism, timeout);
                stopwatch.stop();
                logger.debug("Checking read signature for {} took {} ms",
                  PathUtils.constructFullPath(datasetHandle.getDatasetPath().getComponents()),
                  stopwatch.elapsed(TimeUnit.MILLISECONDS));

                for (Boolean hasChanged : validations) {
                  if (hasChanged) {
                    return MetadataValidity.INVALID;
                  }
                }
              }
              // fallback
            }
          }
          break;

          case INVALID: {
            return MetadataValidity.INVALID;
          }

          default:
            throw UserException.unsupportedError(new IllegalArgumentException("Invalid hive table status " + hiveTableStatus)).build(logger);
        }
      } catch (IOException | TException ioe) {
        throw new ConnectorException(ioe);
      }
    }
    return MetadataValidity.VALID;
  }

  @VisibleForTesting
  List<Boolean> runValidations(DatasetHandle datasetHandle,
                               List<TimedRunnable<Boolean>> signatureValidators,
                               Integer effectiveParallelism, Long timeout) throws IOException {
    return TimedRunnable.run("check read signature for " +
        PathUtils.constructFullPath(datasetHandle.getDatasetPath().getComponents()),
      logger, signatureValidators, effectiveParallelism, timeout);
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
        toIntExact(optionManager.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE))
    );
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) throws ConnectorException {
    final HiveClient client = getClient(SystemUser.SYSTEM_USERNAME);
    final HiveMetadataUtils.SchemaComponents schemaComponents =
        HiveMetadataUtils.resolveSchemaComponents(datasetPath.getComponents(), false);
    if (schemaComponents == null) {
      return Optional.empty();
    }

    final boolean tableExists;
    try {
      tableExists = client.tableExists(schemaComponents.getDbName(), schemaComponents.getTableName());
    } catch (TException e) {
      String message = MessageFormatter.arrayFormat("Plugin '{}', database '{}', table '{}', problem checking if table exists.",
        new String[]{this.getName(), schemaComponents.getDbName(), schemaComponents.getTableName()}).getMessage();
      logger.error(message, e);
      throw new ConnectorException(message, e);
    }

    if (tableExists) {
      logger.debug("Plugin '{}', database '{}', table '{}', DatasetHandle returned.", this.getName(),
        schemaComponents.getDbName(), schemaComponents.getTableName());
      return Optional.of(
        HiveDatasetHandle
          .newBuilder()
          .datasetpath(new EntityPath(
            ImmutableList.of(datasetPath.getComponents().get(0), schemaComponents.getDbName(), schemaComponents.getTableName())))
          .build());
    } else {
      logger.warn("Plugin '{}', database '{}', table '{}', DatasetHandle empty, table not found.", this.getName(),
        schemaComponents.getDbName(), schemaComponents.getTableName());
      return Optional.empty();
    }
  }

  private HiveClient getClient(String user) {
    Preconditions.checkState(isCoordinator, "Hive client only available on coordinator nodes");
    if (!isOpen.get()) {
      throw buildAlreadyClosedException();
    }

    if(!metastoreImpersonationEnabled || SystemUser.SYSTEM_USERNAME.equals(user)){
      return processUserMetastoreClient;
    } else {
      try {
        return clientsByUser.get(user);
      } catch (ExecutionException e) {
        Throwable ex = e.getCause();
        throw Throwables.propagate(ex);
      }
    }
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) throws ConnectorException {
    final HiveClient client = getClient(SystemUser.SYSTEM_USERNAME);

    return new HiveDatasetHandleListing(client, this.getName(), HiveMetadataUtils.isIgnoreAuthzErrors(options));
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options) throws ConnectorException {
    try(Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      boolean enforceVarcharWidth = false;
      Optional<BytesOutput> extendedProperty = ExtendedPropertyOption.getExtendedPropertyFromListPartitionChunkOption(options);
      if (extendedProperty.isPresent()) {
        HiveTableXattr hiveTableXattrFromKVStore;
        try {
          hiveTableXattrFromKVStore = HiveTableXattr.parseFrom(bytesOutputToByteArray(extendedProperty.get()));
        } catch (InvalidProtocolBufferException e) {
          throw UserException.parseError(e).buildSilently();
        }
        enforceVarcharWidth = HiveDatasetOptions.enforceVarcharWidth(
            HiveReaderProtoUtil.convertValuesToNonProtoAttributeValues(hiveTableXattrFromKVStore.getDatasetOptionMap()));
      }

      final HivePartitionChunkListing.Builder builder = HivePartitionChunkListing
        .newBuilder()
        .hiveConf(hiveConf)
        .storageImpersonationEnabled(storageImpersonationEnabled)
        .statsParams(getStatsParams())
        .enforceVarcharWidth(enforceVarcharWidth)
        .maxInputSplitsPerPartition(toIntExact(hiveSettings.getMaxInputSplitsPerPartition()));
      boolean includeComplexTypes = optionManager.getOption(ExecConstants.HIVE_COMPLEXTYPES_ENABLED);

      final HiveClient client = getClient(SystemUser.SYSTEM_USERNAME);
      final TableMetadata tableMetadata = HiveMetadataUtils.getTableMetadata(
        client,
        datasetHandle.getDatasetPath(),
        HiveMetadataUtils.isIgnoreAuthzErrors(options),
        HiveMetadataUtils.getMaxLeafFieldCount(options),
        HiveMetadataUtils.getMaxNestedFieldLevels(options),
        includeComplexTypes,
        hiveConf);

      if (!tableMetadata.getPartitionColumns().isEmpty()) {
        try {
          builder.partitions(PartitionIterator.newBuilder()
            .client(client)
            .dbName(tableMetadata.getTable().getDbName())
            .tableName(tableMetadata.getTable().getTableName())
            .partitionBatchSize(toIntExact(hiveSettings.getPartitionBatchSize()))
            .build());
        } catch (TException | RuntimeException e) {
          throw new ConnectorException(e);
        }
      }

      return builder
        .tableMetadata(tableMetadata)
        .build();
    }
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle,
      PartitionChunkListing chunkListing,
      GetMetadataOption... options
  ) throws ConnectorException {

    final HivePartitionChunkListing hivePartitionChunkListing = chunkListing.unwrap(HivePartitionChunkListing.class);
    final TableMetadata tableMetadata = hivePartitionChunkListing.getTableMetadata();

    final MetadataAccumulator metadataAccumulator = hivePartitionChunkListing.getMetadataAccumulator();
    final Table table = tableMetadata.getTable();
    final Properties tableProperties = tableMetadata.getTableProperties();

    final HiveTableXattr.Builder tableExtended = HiveTableXattr.newBuilder();

    accumulateTableMetadata(tableExtended, metadataAccumulator, table, tableProperties);

    boolean enforceVarcharWidth = false;
    Optional<BytesOutput> extendedProperty = ExtendedPropertyOption.getExtendedPropertyFromMetadataOption(options);
    if (extendedProperty.isPresent()) {
      HiveTableXattr hiveTableXattrFromKVStore;
      try {
        hiveTableXattrFromKVStore = HiveTableXattr.parseFrom(bytesOutputToByteArray(extendedProperty.get()));
      } catch (InvalidProtocolBufferException e) {
        throw UserException.parseError(e).buildSilently();
      }
      enforceVarcharWidth = HiveDatasetOptions.enforceVarcharWidth(
          HiveReaderProtoUtil.convertValuesToNonProtoAttributeValues(hiveTableXattrFromKVStore.getDatasetOptionMap()));
    }

    tableExtended.setTableHash(HiveMetadataUtils.getHash(table, enforceVarcharWidth, hiveConf));
    tableExtended.setPartitionHash(metadataAccumulator.getPartitionHash());
    tableExtended.setReaderType(metadataAccumulator.getReaderType());
    tableExtended.addAllColumnInfo(tableMetadata.getColumnInfos());
    tableExtended.putDatasetOption(HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH,
        HiveReaderProtoUtil.toProtobuf(AttributeValue.of(enforceVarcharWidth)));

    tableExtended.addAllInputFormatDictionary(metadataAccumulator.buildInputFormatDictionary());
    tableExtended.addAllSerializationLibDictionary(metadataAccumulator.buildSerializationLibDictionary());
    tableExtended.addAllStorageHandlerDictionary(metadataAccumulator.buildStorageHandlerDictionary());
    tableExtended.addAllPropertyDictionary(metadataAccumulator.buildPropertyDictionary());

    // this is used as an indicator on read to determine if dictionaries are used or if the metadata is from a prior version of Dremio.
    tableExtended.setPropertyCollectionType(PropertyCollectionType.DICTIONARY);

    return HiveDatasetMetadata.newBuilder()
      .schema(tableMetadata.getBatchSchema())
      .partitionColumns(tableMetadata.getPartitionColumns())
      .sortColumns(FluentIterable.from(table.getSd().getSortCols())
        .transform(order -> order.getCol())
        .toList())
      .metadataAccumulator(metadataAccumulator)
      .extraInfo(os -> os.write((tableExtended.build().toByteArray())))
      .build();
  }

  private void accumulateTableMetadata(HiveTableXattr.Builder tableExtended, MetadataAccumulator metadataAccumulator, Table table, Properties tableProperties) {
    for(Prop prop: HiveMetadataUtils.fromProperties(tableProperties)) {
      tableExtended.addTablePropertySubscript(metadataAccumulator.getTablePropSubscript(prop));
    }

    if (table.getSd().getInputFormat() != null) {
      tableExtended.setTableInputFormatSubscript(metadataAccumulator.getTableInputFormatSubscript(table.getSd().getInputFormat()));
    }

    final String storageHandler = table.getParameters().get(META_TABLE_STORAGE);
    if (storageHandler != null) {
      tableExtended.setTableStorageHandlerSubscript(metadataAccumulator.getTableStorageHandlerSubscript(storageHandler));
    }

    tableExtended.setTableSerializationLibSubscript(
      metadataAccumulator.getTableSerializationLibSubscript(table.getSd().getSerdeInfo().getSerializationLib()));
  }

  @Override
  public BytesOutput provideSignature(DatasetHandle datasetHandle, DatasetMetadata metadata) throws ConnectorException {
    final HiveDatasetMetadata hiveDatasetMetadata = metadata.unwrap(HiveDatasetMetadata.class);

    final MetadataAccumulator metadataAccumulator = hiveDatasetMetadata.getMetadataAccumulator();

    if (metadataAccumulator.allFSBasedPartitions() && !metadataAccumulator.getFileSystemPartitionUpdateKeys().isEmpty()) {
      return os -> os.write(
        HiveReadSignature.newBuilder()
          .setType(HiveReadSignatureType.FILESYSTEM)
          .addAllFsPartitionUpdateKeys(metadataAccumulator.getFileSystemPartitionUpdateKeys())
          .build()
          .toByteArray());
    } else {
      return BytesOutput.NONE;
    }
  }

  @Override
  public SourceState getState() {
    // Executors maintain no state about Hive; they do not communicate with the Hive meta store, so only tables can
    // have a bad state, and not the source.
    if (!isCoordinator) {
      return SourceState.GOOD;
    }

    if (!isOpen.get()) {
      logger.debug("Tried to get the state of a Hive plugin that is either not started or already closed: {}.", this.getName());
      return new SourceState(SourceStatus.bad,
        String.format("Could not connect to Hive source %s, check your Hive credentials and network settings.", this.getName()),
        ImmutableList.of(new SourceState.Message(MessageLevel.ERROR,
          String.format("Hive Metastore client on source %s was not started or already closed.", this.getName()))));
    }

    try {
      processUserMetastoreClient.getDatabases(false);
      return SourceState.GOOD;
    } catch (Exception ex) {
      logger.debug("Caught exception while trying to get status of HIVE source {}, error: ", this.getName(), ex);
      return new SourceState(SourceStatus.bad,
          String.format("Could not connect to Hive source %s, check your Hive credentials and network settings.", this.getName()),
          Collections.singletonList(new SourceState.Message(MessageLevel.ERROR,
              "Failure connecting to source: " + ex.getMessage())));
    }
  }

  @Override
  public void close() {
    HivePf4jPlugin.unregisterMetricMBean();

    if (!isOpen.getAndSet(false)) {
      return;
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
    if (pf4jManager != null) {
      pf4jManager.stopPlugins();
    }
  }

  @Override
  public void start() {

    try {
      setupHadoopUserUsingKerberosKeytab();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    if (isCoordinator) {
      try {
        if (hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL)) {
          logger.info("Hive Metastore SASL enabled. Kerberos principal: " +
              hiveConf.getVar(ConfVars.METASTORE_KERBEROS_PRINCIPAL));
        }

        processUserMetastoreClient = createConnectedClient();
      } catch (MetaException e) {
        throw Throwables.propagate(e);
      }

      // Note: We are assuming any code after assigning processUserMetastoreClient cannot throw.
      isOpen.set(true);

      boolean useZeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(hiveConf);
      logger.info("ORC Zero-Copy {}.", useZeroCopy ? "enabled" : "disabled");

      clientsByUser = CacheBuilder
        .newBuilder()
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .maximumSize(5) // Up to 5 clients for impersonation-enabled.
        .removalListener(new RemovalListener<String, HiveClient>() {
          @Override
          public void onRemoval(RemovalNotification<String, HiveClient> notification) {
            HiveClient client = notification.getValue();
            client.close();
          }
        })
        .build(new CacheLoader<String, HiveClient>() {
          @Override
          public HiveClient load(String userName) throws Exception {
            final UserGroupInformation ugiForRpc;

            if (!storageImpersonationEnabled) {
              // If the user impersonation is disabled in Hive storage plugin, use the process user UGI credentials.
              ugiForRpc = HiveImpersonationUtil.getProcessUserUGI();
            } else {
              ugiForRpc = HiveImpersonationUtil.createProxyUgi(getUsername(userName));
            }

            return createConnectedClientWithAuthz(userName, ugiForRpc);
          }
        });
    } else {
      processUserMetastoreClient = null;
      clientsByUser = null;
    }
  }

  /**
   * Set up the current user in {@link UserGroupInformation} using the kerberos principal and keytab file path if
   * present in config. If not present, this method call is a no-op. When communicating with the kerberos enabled
   * Hadoop based filesystem credentials in {@link UserGroupInformation} will be used..
   * @throws IOException
   */
  private void setupHadoopUserUsingKerberosKeytab() throws IOException {
    final String kerberosPrincipal = dremioConfig.getString(DremioConfig.KERBEROS_PRINCIPAL);
    final String kerberosKeytab = dremioConfig.getString(DremioConfig.KERBEROS_KEYTAB_PATH);

    if (Strings.isNullOrEmpty(kerberosPrincipal) || Strings.isNullOrEmpty(kerberosKeytab)) {
      return;
    }

    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);

    logger.info("Setup Hadoop user info using kerberos principal {} and keytab file {} successful.",
      kerberosPrincipal, kerberosKeytab);
  }

  public String getUsername(String name) {
    if (isStorageImpersonationEnabled()) {
      return name;
    }
    return SystemUser.SYSTEM_USERNAME;
  }

  /**
   * Creates an instance of a class that was loaded by PF4J.
   */
  @Override
  public Class<? extends HiveProxiedSubScan> getSubScanClass() {
    return HiveSubScan.class;
  }

  @Override
  public HiveProxiedScanBatchCreator createScanBatchCreator() {
    return new HiveScanBatchCreator();
  }

  @Override
  public Class<? extends HiveProxiedOrcScanFilter> getOrcScanFilterClass() {
    return ORCScanFilter.class;
  }

  @Override
  public DatasetMetadata alterMetadata(final DatasetHandle datasetHandle, final DatasetMetadata oldDatasetMetadata,
                                       final Map<String, AttributeValue> attributes, final AlterMetadataOption... options) throws ConnectorException {

    final HiveTableXattr hiveTableXattr;
    try {
      hiveTableXattr = HiveTableXattr.parseFrom(bytesOutputToByteArray(oldDatasetMetadata.getExtraInfo()));
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
        throw UserException.validationError().message("Option [%s] requires a value of type [%s]", key,
            HiveReaderProtoUtil.getTypeName(defaultValue.get())).buildSilently();
      }

      AttributeValue currentValue = HiveReaderProtoUtil.convertValuesToNonProtoAttributeValues(newXattrsBuilder.getDatasetOptionMap())
          .getOrDefault(key, defaultValue.get());

      boolean oldAndNewValueIsSame;
      if (currentValue instanceof AttributeValue.StringValue) {
        oldAndNewValueIsSame = ((AttributeValue.StringValue) currentValue)
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

    return DatasetMetadata.of(oldDatasetMetadata.getDatasetStats(),
        oldDatasetMetadata.getRecordSchema(), oldDatasetMetadata.getPartitionColumns(),
        oldDatasetMetadata.getSortColumns(), os -> ByteString.writeTo(os, ByteString.copyFrom(newXattrsBuilder.build().toByteArray())));
  }

  @VisibleForTesting
  HiveClient createConnectedClient() throws MetaException {
    return HiveClientImpl.createConnectedClient(hiveConf);
  }

  @VisibleForTesting
  HiveClient createConnectedClientWithAuthz(final String userName, final UserGroupInformation ugiForRpc)
      throws MetaException {
    Preconditions.checkArgument(processUserMetastoreClient instanceof HiveClientImpl);
    return HiveClientImpl.createConnectedClientWithAuthz(processUserMetastoreClient, hiveConf, userName, ugiForRpc);
  }

  private UserException buildAlreadyClosedException() {
    return UserException.sourceInBadState()
      .message("The Hive source %s is either not started or already closed", this.getName())
      .addContext("name", this.getName())
      .buildSilently();
  }

  public <T> T getPF4JStoragePlugin() {
    return (T) this;
  }
}

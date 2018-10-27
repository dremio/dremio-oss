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
package com.dremio.exec.store.hive;

import static com.dremio.service.namespace.capabilities.SourceCapabilities.STORAGE_IMPERSONATION;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.AccessControlException;
import org.apache.orc.OrcConf;
import org.apache.thrift.TException;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.TimedRunnable;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.hive.DatasetBuilder.StatsEstimationParameters;
import com.dremio.exec.store.hive.exec.HiveReaderProtoUtil;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.hive.proto.HiveReaderProto.FileSystemCachedEntity;
import com.dremio.hive.proto.HiveReaderProto.FileSystemPartitionUpdateKey;
import com.dremio.hive.proto.HiveReaderProto.HiveReadSignature;
import com.dremio.hive.proto.HiveReaderProto.HiveReadSignatureType;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.MessageLevel;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.capabilities.BooleanCapabilityValue;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.InvalidProtocolBufferException;

import io.protostuff.ByteString;

public class HiveStoragePlugin implements StoragePlugin {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveStoragePlugin.class);

  private LoadingCache<String, HiveClient> clientsByUser;
  private final String name;
  private final SabotConfig sabotConfig;

  private HiveClient processUserMetastoreClient;
  private HiveConf hiveConf;
  private final boolean storageImpersonationEnabled;
  private final boolean metastoreImpersonationEnabled;
  private final boolean isCoordinator;
  private final OptionManager options;

  public HiveStoragePlugin(HiveConf hiveConf, SabotContext context, String name) {
    this.isCoordinator = context.isCoordinator();
    this.hiveConf = hiveConf;
    this.name = name;
    this.sabotConfig = context.getConfig();
    this.options = context.getOptionManager();
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
  }

  @Override
  public boolean containerExists(NamespaceKey key) {
    if(key.size() != 2){
      return false;
    }
    return getClient(SystemUser.SYSTEM_USERNAME).databaseExists(key.getPathComponents().get(1));
  }

  @Override
  public boolean datasetExists(NamespaceKey key) {
    if(key.size() != 3){
      return false;
    }
    return getClient(SystemUser.SYSTEM_USERNAME).tableExists(key.getPathComponents().get(1), key.getPathComponents().get(2));
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
    if (!metastoreImpersonationEnabled) {
      return true;
    }

    List<String> path = key.getPathComponents();
    try {
      Table table = null;
      if (path.size() == 3) {
        table = clientsByUser.get(user).getTable(path.get(1), path.get(2), true);
      } else if (path.size() == 2) {
        table = clientsByUser.get(user).getTable("default", path.get(1), true);
      }
      if (table == null) {
        return false;
      }
      if (storageImpersonationEnabled) {
        if (datasetConfig.getReadDefinition() != null && datasetConfig.getReadDefinition().getReadSignature() != null) {
          final HiveReadSignature readSignature = HiveReadSignature.parseFrom(datasetConfig.getReadDefinition().getReadSignature().toByteArray());
          // for now we only support fs based read signatures
          if (readSignature.getType() == HiveReadSignatureType.FILESYSTEM) {
            // get list of partition properties from read definition
            HiveTableXattr tableXattr = HiveTableXattr.parseFrom(datasetConfig.getReadDefinition().getExtendedProperty().toByteArray());
            return hasFSPermission(user, key, readSignature.getFsPartitionUpdateKeysList(), tableXattr);
          }
        }
      }
      return true;
    } catch (TException | ExecutionException | InvalidProtocolBufferException e) {
      throw UserException.dataReadError(e).message("Unable to connect to Hive metastore.").build(logger);
    } catch (UncheckedExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof AuthorizerServiceException) {
        throw UserException.dataReadError(e).message(cause.getMessage()).build(logger);
      }
    }

    return false;
  }


  @Override
  public SourceCapabilities getSourceCapabilities() {
    return new SourceCapabilities(new BooleanCapabilityValue(STORAGE_IMPERSONATION, storageImpersonationEnabled));
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return sabotConfig.getClass("dremio.plugins.hive.rulesfactory", StoragePluginRulesFactory.class, HiveRulesFactory.class);
  }

  private boolean hasFSPermission(String user, NamespaceKey key, List<FileSystemPartitionUpdateKey> updateKeys,
                                  HiveTableXattr tableXattr) {
    List<TimedRunnable<Boolean>> permissionCheckers = Lists.newArrayList();
    for (FileSystemPartitionUpdateKey updateKey : updateKeys) {
      permissionCheckers.add(new FsTask(user, updateKey, tableXattr, TaskType.FS_PERMISSION));
    }
    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      final List<Boolean> accessPermissions = TimedRunnable.run("check access permission for " + key, logger, permissionCheckers, 16);
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
    return true;
  }

  private enum TaskType {
    FS_PERMISSION,
    FS_VALIDATION
  }

  private class FsTask extends TimedRunnable<Boolean> {
    private final String user;
    private final FileSystemPartitionUpdateKey updateKey;
    private final HiveTableXattr tableXattr;
    private final TaskType taskType;

    FsTask(String user, FileSystemPartitionUpdateKey updateKey, HiveTableXattr tableXattr, TaskType taskType) {
      this.user = user;
      this.updateKey = updateKey;
      this.tableXattr = tableXattr;
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

    private boolean checkAccessPermission() throws IOException {
      final JobConf jobConf = new JobConf(hiveConf);
      for (Prop prop : HiveReaderProtoUtil.getPartitionProperties(tableXattr, updateKey.getPartitionId())) {
        jobConf.set(prop.getKey(), prop.getValue());
      }
      Preconditions.checkArgument(updateKey.getCachedEntitiesCount() > 0, "hive partition update key should contain at least one path");

      for (FileSystemCachedEntity cachedEntity : updateKey.getCachedEntitiesList()) {
        final Path cachedEntityPath;
        if (cachedEntity.getPath() == null || cachedEntity.getPath().isEmpty()) {
          cachedEntityPath = new Path(updateKey.getPartitionRootDir());
        } else {
          cachedEntityPath = new Path(updateKey.getPartitionRootDir(), cachedEntity.getPath());
        }
        // Create filesystem for the given user and given path
        final FileSystemWrapper userFS = ImpersonationUtil.createFileSystem(user, jobConf, cachedEntityPath);
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
      for (Prop prop : HiveReaderProtoUtil.getPartitionProperties(tableXattr, updateKey.getPartitionId())) {
        jobConf.set(prop.getKey(), prop.getValue());
      }
      Preconditions.checkArgument(updateKey.getCachedEntitiesCount() > 0, "hive partition update key should contain at least one path");

      // create filesystem based on the first path which is root of the partition directory.
      final FileSystemWrapper fs = FileSystemWrapper.get(new Path(updateKey.getPartitionRootDir()), jobConf);
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



  private UpdateStatus checkHiveMetadata(Integer tableHash, Integer partitionHash, DatasetConfig datasetConfig) throws TException {
    final HiveClient client = getClient(SystemUser.SYSTEM_USERNAME);
    Table table;
    final String dbName;
    final String tableName;
    final List<String> path = datasetConfig.getFullPathList();

    if (path.size() == 3) {
      dbName = path.get(1);
      tableName = path.get(2);
    } else if (path.size() == 2) {
      dbName = "default";
      tableName = path.get(1);
    } else {
      throw new RuntimeException("Invalid hive table path " + new NamespaceKey(datasetConfig.getFullPathList()));
    }

    table = client.getTable(dbName, tableName, true);

    if (table == null) { // missing table?
      return UpdateStatus.DELETED;
    }
    if (DatasetBuilder.getHash(table) != tableHash) {
      return UpdateStatus.CHANGED;
    }

    List<Integer> partitionHashes = Lists.newArrayList();
    for (Partition partition : client.getPartitions(dbName, tableName)) {
      partitionHashes.add(DatasetBuilder.getHash(partition));
    }

    if (partitionHash == null || partitionHash == 0) {
      if (partitionHashes.isEmpty()) {
        return UpdateStatus.UNCHANGED;
      } else {
        // found new partitions
        return UpdateStatus.CHANGED;
      }
    }

    Collections.sort(partitionHashes);
    // There were partitions in last read signature.
    if (partitionHash != Objects.hash(partitionHashes)) {
      return UpdateStatus.CHANGED;
    }

    return UpdateStatus.UNCHANGED;
  }

  @Override
  public CheckResult checkReadSignature(ByteString key, final DatasetConfig datasetConfig, DatasetRetrievalOptions retrievalOptions) throws Exception {
    boolean newUpdateKey = false;

    if (retrievalOptions.forceUpdate() ||
        datasetConfig.getReadDefinition() == null ||
        datasetConfig.getReadDefinition().getReadSignature() == null) {
      // for non fs tables always return true
      newUpdateKey = true;
    } else {

      final HiveTableXattr tableXattr = HiveTableXattr.parseFrom(datasetConfig.getReadDefinition().getExtendedProperty().toByteArray());

      // check for hive table and partition definition changes
      final UpdateStatus hiveTableStatus = checkHiveMetadata(tableXattr.getTableHash(), tableXattr.getPartitionHash(), datasetConfig);

      switch (hiveTableStatus) {
        case UNCHANGED: {
          final HiveReadSignature readSignature = HiveReadSignature.parseFrom(datasetConfig.getReadDefinition().getReadSignature().toByteArray());
          // for now we only support fs based read signatures
          if (readSignature.getType() == HiveReadSignatureType.FILESYSTEM) {
            // get list of partition properties from read definition
            List<TimedRunnable<Boolean>> signatureValidators = Lists.newArrayList();
            for (FileSystemPartitionUpdateKey updateKey : readSignature.getFsPartitionUpdateKeysList()) {
              signatureValidators.add(new FsTask(SystemUser.SYSTEM_USERNAME, updateKey, tableXattr, TaskType.FS_VALIDATION));
            }
            try {
              Stopwatch stopwatch = Stopwatch.createStarted();
              final List<Boolean> validations = TimedRunnable.run("check read signature for " + key, logger, signatureValidators, 16);
              stopwatch.stop();
              logger.debug("Checking read signature for {} took {} ms", key, stopwatch.elapsed(TimeUnit.MILLISECONDS));
              for (Boolean hasChanged : validations) {
                newUpdateKey |= hasChanged;
              }
            } catch (IOException ioe) {
              throw UserException.dataReadError(ioe).build(logger);
            }
            // fallback
          }
        }
        break;

        case CHANGED: {
          newUpdateKey = true;
          // fallback
        }
        break;

        case DELETED: {
          return CheckResult.DELETED;
        }

        default:
          throw UserException.unsupportedError(new IllegalArgumentException("Invalid hive table status " + hiveTableStatus)).build(logger);
      }
    }

    if (newUpdateKey) {
      return new CheckResult() {

        @Override
        public UpdateStatus getStatus() {
          return UpdateStatus.CHANGED;
        }

        @Override
        public SourceTableDefinition getDataset() {
          final HiveClient client = getClient(SystemUser.SYSTEM_USERNAME);
          try {
            // checkReadSignature() is only called with a datasetConfig coming from the namespace so we can safely
            // assume it has a canonized path
            // it may not be safe to always assume so and we should probably figure out a better way to ensure
            // this assumption in the future
            return DatasetBuilder.getDatasetBuilder(
              client,
              getStorageUser(SystemUser.SYSTEM_USERNAME),
              new NamespaceKey(datasetConfig.getFullPathList()),
              true,
              false,
              getStatsParams(),
              hiveConf,
              datasetConfig);
          } catch (TException e) {
            throw UserException.dataReadError(e).message("Failure while retrieving dataset definition.").build(logger);
          }
        }
      };
    }
    return CheckResult.UNCHANGED;
  }

  private String getStorageUser(String user){
    if(storageImpersonationEnabled){
      return user;
    } return SystemUser.SYSTEM_USERNAME;
  }

  private StatsEstimationParameters getStatsParams() {
    return new StatsEstimationParameters(
        options.getOption(HivePluginOptions.HIVE_USE_STATS_IN_METASTORE),
        (int) options.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE),
        (int) options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE)
    );
  }

  @Override
  public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldConfig, DatasetRetrievalOptions retrievalOptions) throws Exception {
    try {
      final HiveClient client = getClient(SystemUser.SYSTEM_USERNAME);
      return DatasetBuilder.getDatasetBuilder(
        client,
        getStorageUser(SystemUser.SYSTEM_USERNAME),
        datasetPath,
        false, // we can't assume the path is canonized, so we'll have to hit the source
        retrievalOptions.ignoreAuthzErrors(),
        getStatsParams(),
        hiveConf,
        oldConfig);
    } catch(RuntimeException e){
      throw e;
    } catch(Exception e){
      throw new RuntimeException(e);
    }
  }

  private HiveClient getClient(String user) {
    Preconditions.checkState(isCoordinator, "Hive client only available on coordinator nodes");
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
  public Iterable<SourceTableDefinition> getDatasets(String user, DatasetRetrievalOptions retrievalOptions) {
    final HiveClient client = getClient(user);
    final ImmutableList.Builder<SourceTableDefinition> accessors = ImmutableList.builder();
    try {
      for(String dbName : client.getDatabases(retrievalOptions.ignoreAuthzErrors())) {
        try {
          for(String table : client.getTableNames(dbName, retrievalOptions.ignoreAuthzErrors())) {
            DatasetBuilder builder = DatasetBuilder.getDatasetBuilder(
              client,
              getStorageUser(user),
              new NamespaceKey(ImmutableList.of(name, dbName, table)),
              true, // we got the path from HiveClient so it's safe to assume it's canonized
              retrievalOptions.ignoreAuthzErrors(),
              getStatsParams(),
              hiveConf,
              null);
            if(builder != null){
              accessors.add(builder);
            }
          }
        } catch(TException e){
          logger.warn("User {} is unable to retrieve table names for {}.{}.", user, name, dbName, e);
        }
      }
    } catch(TException e){
      logger.warn("User {} is unable to retrieve database names for Hive source named {}.", user, name, e);
    }
    return accessors.build();
  }

  @Override
  public SourceState getState() {
    // Executors maintain no state about Hive; they do not communicate with the Hive meta store, so only tables can
    // have a bad state, and not the source.
    if (!isCoordinator) {
      return SourceState.GOOD;
    }

    try {
      processUserMetastoreClient.getDatabases(false);
      return SourceState.GOOD;
    } catch (Exception ex) {
      logger.debug("Caught exception while trying to get status of HIVE source, error: ", ex);
      return new SourceState(SourceStatus.bad,
          Collections.singletonList(new SourceState.Message(MessageLevel.ERROR,
              "Failure connecting to source: " + ex.getMessage())));
    }
  }

  @Override
  public void close() {
  }

  @Override
  public void start() {
    if (isCoordinator) {
      try {
        if (hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL)) {
          logger.info("Hive Metastore SASL enabled. Kerberos principal: " +
              hiveConf.getVar(ConfVars.METASTORE_KERBEROS_PRINCIPAL));
        }

        processUserMetastoreClient = HiveClient.createClient(hiveConf);
      } catch (MetaException e) {
        throw Throwables.propagate(e);
      }

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
            return HiveClient.createClientWithAuthz(processUserMetastoreClient, hiveConf, userName);
          }
        });
    } else {
      processUserMetastoreClient = null;
      clientsByUser = null;
    }
  }


}

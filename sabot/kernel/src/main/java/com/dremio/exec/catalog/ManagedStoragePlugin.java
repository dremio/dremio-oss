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
package com.dremio.exec.catalog;

import static com.dremio.exec.ExecConstants.SOURCE_ASYNC_MODIFICATION_ENABLED;
import static com.dremio.service.namespace.source.proto.SourceChangeState.SOURCE_CHANGE_STATE_CREATING;
import static com.dremio.service.namespace.source.proto.SourceChangeState.SOURCE_CHANGE_STATE_NONE;
import static com.dremio.service.namespace.source.proto.SourceChangeState.SOURCE_CHANGE_STATE_UPDATING;

import com.dremio.common.AutoCloseables;
import com.dremio.common.VM;
import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.common.concurrent.bulk.BulkFunction;
import com.dremio.common.concurrent.bulk.BulkRequest;
import com.dremio.common.concurrent.bulk.BulkResponse;
import com.dremio.common.concurrent.bulk.ValueTransformer;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.common.util.Retryer;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.connector.metadata.extensions.SupportsAlteringDatasetMetadata;
import com.dremio.connector.metadata.options.AlterMetadataOption;
import com.dremio.datastore.DatastoreException;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.exec.catalog.CatalogInternalRPC.UpdateLastRefreshDateRequest;
import com.dremio.exec.catalog.CatalogServiceImpl.UpdateType;
import com.dremio.exec.catalog.DatasetCatalog.UpdateStatus;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SupportsGlobalKeys;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.MissingPluginConf;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.dfs.MetadataIOPool;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.options.OptionManager;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.InvalidNamespaceNameException;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.Message;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.SourceNamespaceService;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceChangeState;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import com.dremio.service.users.SystemUser;
import com.dremio.services.credentials.CredentialsServiceUtils;
import com.dremio.services.credentials.NoopSecretsCreator;
import com.dremio.services.credentials.SecretsCreator;
import com.dremio.telemetry.api.metrics.Metrics;
import com.dremio.telemetry.api.metrics.SimpleCounter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessControlException;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Provider;

/**
 * Manages the Dremio system state related to a StoragePlugin.
 *
 * <p>Also owns the SourceMetadataManager, the task driver responsible for maintaining metadata
 * freshness.
 *
 * <p>Locking model: exposes a readLock (using the inner plugin) and a writeLock (changing the inner
 * plugin). The locking model is exposed externally so that CatalogServiceImpl can get locks as
 * necessary during modifications.
 */
public class ManagedStoragePlugin implements AutoCloseable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ManagedStoragePlugin.class);

  private final String name;
  private final SabotContext context;
  private final ConnectionReader reader;
  private final NamespaceKey sourceKey;
  private final Executor executor;

  /**
   * A read lock for interacting with the plugin. Should be used for most external interactions
   * except where the methods were designed to be resilient to underlying changes to avoid
   * contention/locking needs.
   */
  private final ReentrantReadWriteLock.ReadLock readLock;

  /** A write lock that must be acquired before starting, stopping or replacing the plugin. */
  private final ReentrantReadWriteLock.WriteLock writeLock;

  private final PermissionCheckCache permissionsCache;
  private final SourceMetadataManager metadataManager;
  private final OptionManager options;
  private final NamespaceService systemUserNamespaceService;
  private final Orphanage orphanage;

  protected volatile SourceConfig sourceConfig;
  private volatile StoragePlugin plugin;
  private volatile MetadataPolicy metadataPolicy;
  private volatile StoragePluginId pluginId;
  private volatile ConnectionConf<?, ?> conf;
  private volatile Stopwatch startup = Stopwatch.createUnstarted();
  private volatile SourceState state = SourceState.badState("Source not yet started.");
  private final Thread fixFailedThread;
  private final Predicate<String> influxSourcePred;
  private volatile boolean closed = false;

  /** Included in instance variables because it is very useful during debugging. */
  private final ReentrantReadWriteLock rwlock;

  private final Lock refreshStateLock = new ReentrantLock();

  public ManagedStoragePlugin(
      SabotContext context,
      Executor executor,
      boolean isMaster,
      ModifiableSchedulerService modifiableScheduler,
      NamespaceService systemUserNamespaceService,
      Orphanage orphanage,
      LegacyKVStore<NamespaceKey, SourceInternalData> sourceDataStore,
      SourceConfig sourceConfig,
      OptionManager options,
      ConnectionReader reader,
      CatalogServiceMonitor monitor,
      Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider,
      Predicate<String> influxSourcePred) {
    this.rwlock = new ReentrantReadWriteLock(true);
    this.executor = executor;
    this.readLock = rwlock.readLock();
    this.writeLock = rwlock.writeLock();
    this.context = context;
    this.sourceConfig = sourceConfig;
    this.sourceKey = new NamespaceKey(sourceConfig.getName());
    this.name = sourceConfig.getName();
    this.systemUserNamespaceService = systemUserNamespaceService;
    this.orphanage = orphanage;
    this.options = options;
    this.reader = reader;
    this.conf =
        this.reader.getConnectionConf(sourceConfig); // must use decorated reader (this.reader)
    this.plugin =
        resolveConnectionConf(conf).newPlugin(context, sourceConfig.getName(), this::getId);
    this.metadataPolicy =
        sourceConfig.getMetadataPolicy() == null
            ? CatalogService.NEVER_REFRESH_POLICY
            : sourceConfig.getMetadataPolicy();
    this.permissionsCache =
        new PermissionCheckCache(
            this::getPlugin, getAuthTtlMsProvider(options, sourceConfig), 2500);

    this.influxSourcePred = influxSourcePred;

    fixFailedThread = new FixFailedToStart();
    // leaks this so do last.
    this.metadataManager =
        createSourceMetadataManager(
            sourceKey,
            modifiableScheduler,
            isMaster,
            sourceDataStore,
            new MetadataBridge(),
            options,
            monitor,
            broadcasterProvider,
            context.getClusterCoordinator());
  }

  private ConnectionConf<?, ?> resolveConnectionConf(ConnectionConf<?, ?> connectionConf) {
    // Handle only in-line encrypted secrets.
    return connectionConf.resolveSecrets(
        context.getCredentialsServiceProvider().get(),
        CredentialsServiceUtils::isEncryptedCredentials);
  }

  protected SourceMetadataManager createSourceMetadataManager(
      NamespaceKey sourceName,
      ModifiableSchedulerService modifiableScheduler,
      boolean isMaster,
      LegacyKVStore<NamespaceKey, SourceInternalData> sourceDataStore,
      final ManagedStoragePlugin.MetadataBridge bridge,
      final OptionManager options,
      final CatalogServiceMonitor monitor,
      final Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider,
      final ClusterCoordinator clusterCoordinator) {
    return new SourceMetadataManager(
        sourceName,
        modifiableScheduler,
        isMaster,
        sourceDataStore,
        bridge,
        options,
        monitor,
        broadcasterProvider,
        clusterCoordinator);
  }

  protected PermissionCheckCache getPermissionsCache() {
    return permissionsCache;
  }

  protected StoragePlugin getPlugin() {
    return plugin;
  }

  protected MetadataPolicy getMetadataPolicy() {
    return metadataPolicy;
  }

  protected AutoCloseableLock readLock() {
    return AutoCloseableLock.lockAndWrap(readLock, true);
  }

  @VisibleForTesting
  protected AutoCloseableLock writeLock() {
    return AutoCloseableLock.lockAndWrap(writeLock, true);
  }

  protected Provider<Long> getAuthTtlMsProvider(OptionManager options, SourceConfig sourceConfig) {
    return () -> getMetadataPolicy().getAuthTtlMs();
  }

  private long createWaitMillis() {
    if (VM.isDebugEnabled()) {
      return TimeUnit.DAYS.toMillis(365);
    }
    return context.getOptionManager().getOption(CatalogOptions.STORAGE_PLUGIN_CREATE_MAX);
  }

  /**
   * Synchronize plugin state to the provided target source config.
   *
   * <p>Note that this will fail if the target config is older than the existing config, in terms of
   * creation time or version.
   *
   * @param targetConfig target source config
   * @return managed storage plugin
   * @throws Exception if synchronization fails
   */
  @WithSpan("synchronize-source")
  void synchronizeSource(final SourceConfig targetConfig) throws Exception {

    if (matches(targetConfig)) {
      logger.trace("Source [{}] already up-to-date, not synchronizing", targetConfig.getName());
      return;
    }

    // do this in under the write lock.
    try (Closeable write = writeLock()) {

      // check again under write lock to make sure things didn't already synchronize.
      if (matches(targetConfig)) {
        logger.trace("Source [{}] already up-to-date, not synchronizing", targetConfig.getName());
        return;
      }

      logger.debug(
          "Sync source [{}] from {} to {}.",
          targetConfig.getName(),
          sourceConfig.getConfigOrdinal(),
          targetConfig.getConfigOrdinal());
      // else, bring the plugin up to date, if possible
      final long creationTime = sourceConfig.getCtime();
      final long targetCreationTime = targetConfig.getCtime();

      if (creationTime > targetCreationTime) {
        // in-memory source config is newer than the target config, in terms of creation time
        throw new ConcurrentModificationException(
            String.format(
                "Source [%s] was updated, and the given configuration has older ctime (current: %d, given: %d)",
                targetConfig.getName(), creationTime, targetCreationTime));

      } else if (creationTime == targetCreationTime) {
        final SourceConfig currentConfig = sourceConfig;

        compareConfigs(currentConfig, targetConfig);

        final int compareTo = SOURCE_CONFIG_COMPARATOR.compare(currentConfig, targetConfig);

        if (compareTo > 0) {
          // in-memory source config is newer than the target config, in terms of version
          throw new ConcurrentModificationException(
              String.format(
                  "Source [%s] was updated, and the given configuration has older version (current: %d, given: %d)",
                  currentConfig.getName(),
                  currentConfig.getConfigOrdinal(),
                  targetConfig.getConfigOrdinal()));
        }

        if (compareTo == 0) {
          // in-memory source config has the same config ordinal (version)

          if (sourceConfigDiffersTagAlone(targetConfig)) {
            // warn but do not throw as this error is benign - if all coordinators and executors
            // share the same effective source config value, they can all function to plan and
            // execute
            // the query without issue - the configs having a different tag value reflects a
            // programming
            // bug but one that can be gracefully recovered from
            logger.warn(
                "Current and given configurations for source [{}] have same version ({}) but different tags"
                    + " [current source: {}, given source: {}]",
                currentConfig.getName(),
                currentConfig.getConfigOrdinal(),
                reader.toStringWithoutSecrets(currentConfig),
                reader.toStringWithoutSecrets(targetConfig));
          } else {
            // if the ordinal is the same but some property other than the tag differs, we do not
            // know if
            // we have the correct or the outdated config, so we must fail synchronization
            throw new IllegalStateException(
                String.format(
                    "Current and given configurations for source [%s] have same version (%d) but different values"
                        + " [current source: %s, given source: %s]",
                    currentConfig.getName(),
                    currentConfig.getConfigOrdinal(),
                    reader.toStringWithoutSecrets(currentConfig),
                    reader.toStringWithoutSecrets(targetConfig)));
          }
        }
      }
      // else (creationTime < targetCreationTime), the source config is new but plugins has an entry
      // with the same
      // name, so replace the plugin regardless of the checks

      // in-memory storage plugin is older than the one persisted or differs in tag alone, update
      replacePlugin(targetConfig, createWaitMillis(), false);
    }
  }

  private void setSourceChangeState(SourceConfig config, boolean create) {
    if (create) {
      config.setSourceChangeState(SOURCE_CHANGE_STATE_CREATING);
    } else {
      config.setSourceChangeState(SOURCE_CHANGE_STATE_UPDATING);
    }
  }

  private void addDefaults(SourceConfig config) {
    if (config.getCtime() == null) {
      config.setCtime(System.currentTimeMillis());
    }

    if (config.getMetadataPolicy() == null) {
      config.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    }
  }

  private void updateConfig(
      NamespaceService userNamespace, SourceConfig config, NamespaceAttribute... attributes)
      throws ConcurrentModificationException, NamespaceException {
    if (logger.isTraceEnabled()) {
      logger.trace(
          "Adding or updating source [{}].",
          config.getName(),
          new RuntimeException("Nothing wrong, just show stack trace for debug."));
    } else {
      logger.debug("Adding or updating source [{}].", config.getName());
    }

    try {
      SourceConfig existingConfig = userNamespace.getSource(config.getKey());
      final ConnectionConf<?, ?> connectionConf = config.getConnectionConf(reader);
      final ConnectionConf<?, ?> existingConf = reader.getConnectionConf(existingConfig);

      // add back any secrets
      connectionConf.applySecretsFrom(existingConf);

      // handle data migration for update scenario.
      connectionConf.migrateLegacyFormat(existingConf);

      config.setConfig(connectionConf.toBytesString());
      userNamespace.canSourceConfigBeSaved(config, existingConfig, attributes);

      // if config can be saved we can set the ordinal
      config.setConfigOrdinal(existingConfig.getConfigOrdinal());
    } catch (NamespaceNotFoundException ex) {
      if (config.getTag() != null) {
        throw new ConcurrentModificationException("Source was already created.");
      }
    }
  }

  void createSource(SourceConfig config, String userName, NamespaceAttribute... attributes)
      throws TimeoutException, Exception {
    createOrUpdateSource(true, config, userName, attributes);
  }

  void updateSource(SourceConfig config, String userName, NamespaceAttribute... attributes)
      throws ConcurrentModificationException, NamespaceException {
    createOrUpdateSource(false, config, userName, attributes);
  }

  private void createOrUpdateSource(
      final boolean create, SourceConfig config, String userName, NamespaceAttribute... attributes)
      throws ConcurrentModificationException, NamespaceException {
    final NamespaceService userNamespace = context.getNamespaceService(userName);
    boolean createOrUpdateSucceeded = false;

    if (logger.isTraceEnabled()) {
      logger.trace(
          "{} source [{}].",
          create ? "Creating" : "Updating",
          config.getName(),
          new RuntimeException("Nothing wrong, just show stack trace for debug."));
    } else if (logger.isDebugEnabled()) {
      logger.debug("{} source [{}].", create ? "Creating" : "Updating", config.getName());
    }
    // VersionedPlugin  is a source that does not save datasets in Namespace. So they never need to
    // be refreshed
    if (this.plugin != null && this.plugin.isWrapperFor(VersionedPlugin.class)) {
      config.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    }

    final SourceConfig originalConfigCopy = ProtostuffUtil.copy(sourceConfig);

    // Preprocessing on the config before creation
    addDefaults(config);
    addGlobalKeys(config);
    encryptSecrets(config);
    validateAccelerationSettings(config);
    setSourceChangeState(config, create);

    if (!create) {
      updateConfig(userNamespace, config, attributes);
    } else {
      migrateLegacyFormat(config);
    }

    try {
      final Stopwatch stopwatchForPlugin = Stopwatch.createStarted();

      final boolean refreshDatasetNames;

      // **** Start Local Update **** //
      try (AutoCloseableLock writeLock = writeLock()) {

        final boolean oldMetadataIsBad;

        if (create) {
          // this isn't really a replace.
          replacePlugin(config, createWaitMillis(), true);
          oldMetadataIsBad = false;
          refreshDatasetNames = true;
        } else {
          boolean metadataStillGood = replacePlugin(config, createWaitMillis(), false);
          oldMetadataIsBad = !metadataStillGood;
          refreshDatasetNames = !metadataStillGood;
        }

        if (oldMetadataIsBad) {
          if (!keepStaleMetadata()) {
            logger.debug(
                "Old metadata data may be bad; deleting all descendants of source [{}]",
                config.getName());
            // TODO: expensive call on non-master coordinators (sends as many RPC requests as
            // entries under the source)
            SourceNamespaceService.DeleteCallback deleteCallback =
                (DatasetConfig datasetConfig) -> {
                  CatalogUtil.addIcebergMetadataOrphan(datasetConfig, orphanage);
                };
            systemUserNamespaceService.deleteSourceChildren(
                config.getKey(), config.getTag(), deleteCallback);
          } else {
            logger.info(
                "Old metadata data may be bad, but preserving descendants of source [{}] because '{}' is enabled",
                config.getName(),
                CatalogOptions.STORAGE_PLUGIN_KEEP_METADATA_ON_REPLACE.getOptionName());
          }
        }

        // Now let's create the plugin in the system namespace.
        // This increments the version in the config object as well.
        try {
          userNamespace.addOrUpdateSource(config.getKey(), config, attributes);

        } catch (AccessControlException | ConcurrentModificationException | DatastoreException e) {
          logger.trace(
              "Saving to namespace store failed, reverting the in-memory source config to the original version...");
          setLocals(originalConfigCopy, true);
          throw e;
        }
      }

      // ***** Complete Local Update **** //
      createOrUpdateSucceeded = true;

      // now that we're outside the plugin lock, we should refresh the dataset names. Note that this
      // could possibly get
      // run on a different config if two source updates are racing to this lock.
      if (options.getOption(SOURCE_ASYNC_MODIFICATION_ENABLED)) {
        CompletableFuture.runAsync(
                () -> refreshNames(config.getName(), refreshDatasetNames), executor)
            .whenComplete(
                (res, ex) -> {
                  try {
                    userNamespace.addOrUpdateSource(
                        config.getKey(),
                        config.setSourceChangeState(SOURCE_CHANGE_STATE_NONE),
                        attributes);
                  } catch (NamespaceException e) {
                    throw new RuntimeException(e);
                  }
                  logCompletion(config.getName(), stopwatchForPlugin);
                });
      } else {
        refreshNames(config.getName(), refreshDatasetNames);
        logCompletion(config.getName(), stopwatchForPlugin);
        userNamespace.addOrUpdateSource(
            config.getKey(), config.setSourceChangeState(SOURCE_CHANGE_STATE_NONE), attributes);
      }

    } catch (ConcurrentModificationException ex) {
      throw UserException.concurrentModificationError(ex)
          .message(
              "Source update failed due to a concurrent update. Please try again [%s].",
              config.getName())
          .build(logger);
    } catch (AccessControlException ex) {
      throw UserException.permissionError(ex)
          .message("Update privileges on source [%s] failed: %s", config.getName(), ex.getMessage())
          .build(logger);
    } catch (InvalidNamespaceNameException ex) {
      throw UserException.validationError(ex)
          .message(
              String.format(
                  "Failure creating/updating this source [%s]: %s",
                  config.getName(), ex.getMessage()))
          .build(logger);
    } catch (Exception ex) {
      String suggestedUserAction = getState().getSuggestedUserAction();
      if (suggestedUserAction == null || suggestedUserAction.isEmpty()) {
        // If no user action was suggested, fall back to a basic message.
        suggestedUserAction =
            String.format(
                "Failure creating/updating this source [%s]: %s.",
                config.getName(), ex.getMessage());
      }
      throw UserException.validationError(ex).message(suggestedUserAction).build(logger);
    } finally {
      if (create) {
        // Cleanup any secrets if the source creation fails
        if (!createOrUpdateSucceeded) {
          cleanupSecrets(config);
        }
      } else {
        // Cleanup stale secrets after source update.
        if (createOrUpdateSucceeded) {
          cleanupSecretsAfterUpdate(config, originalConfigCopy);
        } else {
          cleanupSecretsAfterUpdate(originalConfigCopy, config);
        }
      }
    }
  }

  private void updateCreatedSourceCount() {
    SimpleCounter createdSourcesCounter =
        SimpleCounter.of(
            Metrics.join("sources", "created"),
            "Total sources created",
            Tags.of(Tag.of("source_type", this.getConfig().getType())));
    createdSourcesCounter.increment();
  }

  private void refreshNames(String name, boolean refreshDatasetNames) {
    if (refreshDatasetNames) {
      if (!keepStaleMetadata()) {
        logger.debug("Refreshing names in source [{}]", name);
        try {
          refresh(UpdateType.NAMES, null);
        } catch (NamespaceException e) {
          throw UserException.validationError(e)
              .message(
                  String.format("Failure refreshing this source [%s]: %s", name, e.getMessage()))
              .buildSilently();
        }
      } else {
        logger.debug("Not refreshing names in source [{}]", name);
      }
    }
  }

  private void logCompletion(String name, Stopwatch stopwatch) {
    // once we have potentially done initial refresh, we can run subsequent refreshes.
    // This allows
    // us to avoid two separate threads refreshing simultaneously (possible if we put
    // this above
    // the inline plugin.refresh() call.)
    stopwatch.stop();
    updateCreatedSourceCount();
    logger.debug(
        "Source added [{}], took {} milliseconds", name, stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }

  void validateAccelerationSettings(SourceConfig config) {}

  OptionManager getOptionManager() {
    return context.getOptionManager();
  }

  private void addGlobalKeys(SourceConfig config) {
    final ConnectionConf<?, ?> connectionConf = config.getConnectionConf(reader);
    if (connectionConf instanceof SupportsGlobalKeys) {
      SupportsGlobalKeys sgc = (SupportsGlobalKeys) connectionConf;
      try {
        List<Property> globalKeys =
            context.getGlobalCredentialsServiceProvider().get().getGlobalKeys();
        sgc.setGlobalKeys(globalKeys);
        config.setConnectionConf(connectionConf);
      } catch (IllegalStateException e) {
        // If GlobalKeys is not provided by the GlobalKeysServiceProvider, then ignore.
      }
    }
  }

  /**
   * In-line encrypt SecretRefs within the connectionConf on the given SourceConfig. Has no effect
   * on non-SecretRef secrets.
   */
  private void encryptSecrets(SourceConfig config) {
    final SecretsCreator secretsCreator = context.getSecretsCreator().get();
    // Short-circuit early if encryption is disabled via binding
    if (secretsCreator instanceof NoopSecretsCreator) {
      return;
    }
    // Get Conf from Decorated Connection Reader
    final ConnectionConf<?, ?> connectionConf = config.getConnectionConf(reader);
    connectionConf.encryptSecrets(secretsCreator);
    config.setConnectionConf(connectionConf);
  }

  /** Delete encrypted secrets found in the old config but not in the new config. */
  private void cleanupSecrets(SourceConfig config) {
    final SecretsCreator secretsCreator = context.getSecretsCreator().get();
    // Short-circuit early if encryption is disabled via binding
    if (secretsCreator instanceof NoopSecretsCreator) {
      return;
    }

    // Get Conf from Decorated Connection Reader
    final ConnectionConf<?, ?> connectionConf = config.getConnectionConf(reader);
    connectionConf.deleteSecrets(secretsCreator);
  }

  /** Delete encrypted secrets found in the old config but not in the new config. */
  private void cleanupSecretsAfterUpdate(SourceConfig newConfig, SourceConfig oldConfig) {
    final SecretsCreator secretsCreator = context.getSecretsCreator().get();
    // Short-circuit early if encryption is disabled via binding
    if (secretsCreator instanceof NoopSecretsCreator) {
      return;
    }

    // Get Conf from Decorated Connection Reader
    final ConnectionConf<?, ?> newConf = newConfig.getConnectionConf(reader);
    final ConnectionConf<?, ?> oldConf = oldConfig.getConnectionConf(reader);
    oldConf.deleteSecretsExcept(secretsCreator, newConf);
  }

  /**
   * Handles source creation/upgrade scenarios. Calls ConnectionConf#migrateLegacyFormat to remove
   * old/deprecated fields and repacking them into new fields.
   */
  private void migrateLegacyFormat(SourceConfig config) {
    final ConnectionConf<?, ?> connectionConf = config.getConnectionConf(reader);

    connectionConf.migrateLegacyFormat();
    config.setConnectionConf(connectionConf);
  }

  private boolean keepStaleMetadata() {
    return context
        .getOptionManager()
        .getOption(CatalogOptions.STORAGE_PLUGIN_KEEP_METADATA_ON_REPLACE);
  }

  /** Given we know the source configs are the same, check if the tags are as well. */
  private void compareConfigs(SourceConfig existing, SourceConfig config) {
    if (Objects.equals(existing.getTag(), config.getTag())) {
      // in-memory source config has a different value but the same etag
      throw new IllegalStateException(
          String.format(
              "Current and given configurations for source [%s] have same etag (%s) but different values"
                  + " [current source: %s, given source: %s]",
              existing.getName(),
              existing.getTag(),
              reader.toStringWithoutSecrets(existing),
              reader.toStringWithoutSecrets(config)));
    }
  }

  DatasetSaver getSaver() {
    // note, this is a protected saver so no one will be able to save a dataset if the source is
    // currently going through editing changes (write lock held).
    return metadataManager.getSaver();
  }

  /**
   * Return clone of the sourceConfig
   *
   * @return
   */
  public SourceConfig getConfig() {
    return ProtostuffUtil.copy(sourceConfig);
  }

  public long getStartupTime() {
    return startup.elapsed(TimeUnit.MILLISECONDS);
  }

  public NamespaceKey getName() {
    return sourceKey;
  }

  public SourceState getState() {
    // not read under a read lock since it is updated via volatile. Allows us to avoid locking in
    // read paths.
    return state;
  }

  public SourceChangeState sourceChangeState() {
    return sourceConfig.getSourceChangeState();
  }

  public boolean matches(SourceConfig config) {
    try (AutoCloseableLock readLock = readLock()) {
      return MissingPluginConf.TYPE.equals(sourceConfig.getType()) || sourceConfig.equals(config);
    }
  }

  int getMaxMetadataColumns() {
    return Ints.saturatedCast(options.getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
  }

  int getMaxNestedLevel() {
    return Ints.saturatedCast(options.getOption(CatalogOptions.MAX_NESTED_LEVELS));
  }

  public ConnectionConf<?, ?> getConnectionConf() {
    // not read under a read lock since it is updated via volatile. Allows us to avoid locking in
    // read paths.
    return conf;
  }

  /**
   * Gets dataset retrieval options as defined on the source.
   *
   * @return dataset retrieval options defined on the source
   */
  DatasetRetrievalOptions getDefaultRetrievalOptions() {
    // not read under a read lock since it is updated via volatile. Allows us to avoid locking in
    // read paths.
    return DatasetRetrievalOptions.fromMetadataPolicy(metadataPolicy).toBuilder()
        .setMaxMetadataLeafColumns(getMaxMetadataColumns())
        .setMaxNestedLevel(getMaxNestedLevel())
        .build()
        .withFallback(DatasetRetrievalOptions.DEFAULT);
  }

  public StoragePluginRulesFactory getRulesFactory()
      throws InstantiationException,
          IllegalAccessException,
          InvocationTargetException,
          NoSuchMethodException {
    // not read under a read lock since it is updated via volatile. Allows us to avoid locking in
    // read paths. This is
    // especially important here since this method is used even if the query does not refer to this
    // source.

    // grab local to avoid changes under us later.
    final StoragePlugin plugin = this.plugin;
    if (plugin == null) {
      return null;
    }

    if (plugin.getRulesFactoryClass() != null) {
      return plugin.getRulesFactoryClass().getDeclaredConstructor().newInstance();
    }

    return null;
  }

  /**
   * Start this plugin asynchronously
   *
   * @return A future that returns the state of this plugin once started (or throws Exception if the
   *     startup failed).
   */
  CompletableFuture<SourceState> startAsync() {
    return startAsync(sourceConfig, true);
  }

  /**
   * Generate a supplier that produces source state
   *
   * @param config
   * @param closeMetaDataManager - During dremio startup, we don't close metadataManager when
   *     sources are in bad state, because we need state refresh for bad sources. When a user tries
   *     to add a source and it's in bad state, we close metadataManager to avoid wasting additional
   *     space.
   * @return
   */
  private Supplier<SourceState> newStartSupplier(
      SourceConfig config, final boolean closeMetaDataManager) {
    try {
      return nameSupplier(
          "start-" + sourceConfig.getName(),
          () -> {
            try {
              startup = Stopwatch.createStarted();
              logger.debug("Starting: {}", sourceConfig.getName());
              plugin.start();
              setLocals(config, false);
              startup.stop();
              if (state.getStatus() == SourceStatus.bad) {
                // Check the state here and throw exception so that we close the partially started
                // plugin properly in the
                // exception handling code
                throw new Exception(state.toString());
              }

              return state;
            } catch (Throwable e) {
              if (config.getType() != MissingPluginConf.TYPE) {
                logger.warn("Error starting new source: {}", sourceConfig.getName(), e);
              }
              // TODO: Throwables.gerRootCause(e)
              state = SourceState.badState(e.getMessage(), e.getMessage());

              try {
                // failed to startup, make sure to close.
                if (closeMetaDataManager) {
                  AutoCloseables.close(metadataManager, plugin);
                } else {
                  plugin.close();
                }
                plugin = null;
              } catch (Exception ex) {
                e.addSuppressed(
                    new RuntimeException("Cleanup exception after initial failure.", ex));
              }

              throw new CompletionException(e);
            }
          });
    } catch (Exception ex) {
      return () -> {
        throw new CompletionException(ex);
      };
    }
  }

  /**
   * Start this plugin asynchronously
   *
   * @param config The configuration to use for this startup.
   * @return A future that returns the state of this plugin once started (or throws Exception if the
   *     startup failed).
   */
  private CompletableFuture<SourceState> startAsync(
      final SourceConfig config, final boolean isDuringStartUp) {
    // we run this in a separate thread to allow early timeout. This doesn't use the scheduler since
    // that is
    // bound and we're frequently holding a lock when running this.
    return CompletableFuture.supplyAsync(newStartSupplier(config, !isDuringStartUp), executor);
  }

  /**
   * If starting a plugin on process restart failed, this method will spawn a background task that
   * will keep trying to re-start the plugin on a fixed schedule (minimum of the metadata name and
   * dataset refresh rates)
   */
  public void initiateFixFailedStartTask() {
    fixFailedThread.start();
  }

  /**
   * Ensures a supplier names the thread it is run on.
   *
   * @param name Name to use for thread.
   * @param delegate Delegate supplier.
   * @return
   */
  private <T> Supplier<T> nameSupplier(String name, Supplier<T> delegate) {
    return () -> {
      Thread current = Thread.currentThread();
      String oldName = current.getName();
      try {
        current.setName(name);
        return delegate.get();
      } finally {
        current.setName(oldName);
      }
    };
  }

  /**
   * Alters dataset options
   *
   * @param key
   * @param datasetConfig
   * @param attributes
   * @return if table options are modified
   */
  public boolean alterDataset(
      final NamespaceKey key,
      final DatasetConfig datasetConfig,
      final Map<String, AttributeValue> attributes) {
    return alterDatasetInternal(
        key,
        datasetConfig,
        (plugin, handle, oldDatasetMetadata, options) ->
            plugin.alterMetadata(handle, oldDatasetMetadata, attributes, options));
  }

  /**
   * Alters dataset column options.
   *
   * @param key
   * @param datasetConfig
   * @param columnToChange
   * @param attributeName
   * @param attributeValue
   * @return if table options are modified
   */
  public boolean alterDatasetSetColumnOption(
      final NamespaceKey key,
      final DatasetConfig datasetConfig,
      final String columnToChange,
      final String attributeName,
      final AttributeValue attributeValue) {
    return alterDatasetInternal(
        key,
        datasetConfig,
        (plugin, handle, oldDatasetMetadata, options) ->
            plugin.alterDatasetSetColumnOption(
                handle,
                oldDatasetMetadata,
                columnToChange,
                attributeName,
                attributeValue,
                options));
  }

  private boolean alterDatasetInternal(
      final NamespaceKey key,
      final DatasetConfig datasetConfig,
      AlterMetadataCallback pluginCallback) {
    if (!(plugin instanceof SupportsAlteringDatasetMetadata)) {
      throw UserException.unsupportedError()
          .message("Source [%s] doesn't support modifying options", this.name)
          .buildSilently();
    }

    final DatasetRetrievalOptions retrievalOptions = getDefaultRetrievalOptions();
    final Optional<DatasetHandle> handle;
    try {
      handle = getDatasetHandle(key, datasetConfig, retrievalOptions);
    } catch (ConnectorException e) {
      throw UserException.validationError(e)
          .message("Failure while retrieving dataset")
          .buildSilently();
    }

    if (!handle.isPresent()) {
      throw UserException.validationError()
          .message("Unable to find requested dataset.")
          .buildSilently();
    }

    if (Boolean.TRUE.equals(datasetConfig.getPhysicalDataset().getIcebergMetadataEnabled())) {
      throw UserException.unsupportedError()
          .message("ALTER unsupported on table '%s'", key.toString())
          .buildSilently();
    }

    boolean changed = false;
    final DatasetMetadata oldDatasetMetadata = new DatasetMetadataAdapter(datasetConfig);
    DatasetMetadata newDatasetMetadata;
    try (AutoCloseableLock l = readLock()) {
      newDatasetMetadata =
          pluginCallback.apply(
              (SupportsAlteringDatasetMetadata) plugin, handle.get(), oldDatasetMetadata);
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }

    if (oldDatasetMetadata == newDatasetMetadata) {
      changed = false;
    } else {
      Preconditions.checkState(
          newDatasetMetadata.getDatasetStats().getRecordCount() >= 0,
          "Record count should already be filled in when altering dataset metadata.");
      MetadataObjectsUtils.overrideExtended(
          datasetConfig,
          newDatasetMetadata,
          Optional.empty(),
          newDatasetMetadata.getDatasetStats().getRecordCount(),
          getMaxMetadataColumns());
      // Force a full refresh
      saveDatasetAndMetadataInNamespace(
          datasetConfig, handle.get(), retrievalOptions.toBuilder().setForceUpdate(true).build());
      changed = true;
    }
    return changed;
  }

  @FunctionalInterface
  private interface AlterMetadataCallback {
    DatasetMetadata apply(
        SupportsAlteringDatasetMetadata plugin,
        final DatasetHandle datasetHandle,
        final DatasetMetadata metadata,
        AlterMetadataOption... options)
        throws ConnectorException;
  }

  private class FixFailedToStart extends Thread {
    public FixFailedToStart() {
      super("fix-fail-to-start-" + sourceKey.getRoot());

      setDaemon(true);
    }

    @Override
    public void run() {
      final int baseMs =
          (int)
              Math.min(
                  Math.min(
                      metadataPolicy.getNamesRefreshMs(),
                      metadataPolicy.getDatasetDefinitionRefreshAfterMs()),
                  Integer.MAX_VALUE);
      final Retryer retryer =
          Retryer.newBuilder()
              .retryIfExceptionOfType(BadSourceStateException.class)
              .setWaitStrategy(
                  Retryer.WaitStrategy.EXPONENTIAL,
                  baseMs,
                  (int) CatalogService.DEFAULT_REFRESH_MILLIS)
              .setInfiniteRetries(true)
              .build();

      final String successMessage = String.format("Plugin %s started successfully!", name);
      final String errorMessage = String.format("Error while starting plugin %s", name);
      try {
        retryer.run(
            () -> {
              // something started the plugin successfully.
              if (state.getStatus() != SourceState.SourceStatus.bad) {
                logger.info(successMessage);
                return;
              }

              try {
                refreshState().get();
                if (state.getStatus() != SourceState.SourceStatus.bad) {
                  logger.info(successMessage);
                  return;
                }
              } catch (Exception e) {
                // Failure to refresh state means that we should just reschedule the next fix.
              }

              logger.error(errorMessage);
              throw new BadSourceStateException();
            });
      } catch (Retryer.OperationFailedAfterRetriesException e) {
        logger.error(errorMessage, e);
      }
    }

    private final class BadSourceStateException extends RuntimeException {}

    @Override
    public String toString() {
      return "fix-fail-to-start-" + sourceKey.getRoot();
    }
  }

  /** Before doing any operation associated with plugin, we should check the state of the plugin. */
  protected void checkState() {
    try (AutoCloseableLock l = readLock()) {
      SourceState state = this.state;
      if (state.getStatus() == SourceState.SourceStatus.bad) {
        final String msg =
            state.getMessages().stream().map(m -> m.getMessage()).collect(Collectors.joining(", "));

        StringBuilder badStateMessage = new StringBuilder();
        badStateMessage
            .append("The source [")
            .append(sourceKey)
            .append("] is currently unavailable. Metadata is not ");
        badStateMessage.append(
            "accessible; please check node health (or external storage) and permissions.");
        if (!Strings.isNullOrEmpty(msg)) {
          badStateMessage.append(" Info: [").append(msg).append("]");
        }
        String suggestedUserAction = this.state.getSuggestedUserAction();
        if (!Strings.isNullOrEmpty(suggestedUserAction)) {
          badStateMessage.append("\nAdditional actions: [").append(suggestedUserAction).append("]");
        }
        UserException.Builder builder =
            UserException.sourceInBadState().message(badStateMessage.toString());

        for (Message message : state.getMessages()) {
          builder.addContext(message.getLevel().name(), message.getMessage());
        }

        throw builder.buildSilently();
      }
    }
  }

  public StoragePluginId getId() {
    checkState();
    return pluginId;
  }

  @VisibleForTesting
  public long getLastFullRefreshDateMs() {
    return metadataManager.getLastFullRefreshDateMs();
  }

  @VisibleForTesting
  public long getLastNamesRefreshDateMs() {
    return metadataManager.getLastNamesRefreshDateMs();
  }

  void setMetadataSyncInfo(UpdateLastRefreshDateRequest request) {
    metadataManager.setMetadataSyncInfo(request);
  }

  public static boolean isComplete(DatasetConfig config) {
    return config != null
        && DatasetHelper.getSchemaBytes(config) != null
        && config.getReadDefinition() != null
        && config.getReadDefinition().getSplitVersion() != null;
  }

  public static enum MetadataAccessType {
    CACHED_METADATA,
    PARTIAL_METADATA,
    SOURCE_METADATA
  }

  @WithSpan("check-dataset-access")
  public void checkAccess(
      NamespaceKey key,
      DatasetConfig datasetConfig,
      String userName,
      final MetadataRequestOptions options) {
    if (SystemUser.isSystemUserName(userName)) {
      return;
    }

    try (AutoCloseableLock l = readLock()) {
      checkState();
      if (!getPermissionsCache()
          .hasAccess(userName, key, datasetConfig, options.getStatsCollector(), sourceConfig)) {
        throw UserException.permissionError()
            .message("Access denied reading dataset %s.", key)
            .build(logger);
      }
    }
  }

  /** Gets the current state of the metadata maintained by {@link SourceMetadataManager}. */
  public DatasetMetadataState getDatasetMetadataState(DatasetConfig datasetConfig) {
    try (AutoCloseableLock l = readLock()) {
      checkState();
      boolean isComplete = isComplete(datasetConfig);
      return isComplete
          ? DatasetMetadataState.builder()
              .from(metadataManager.getDatasetMetadataState(datasetConfig, plugin))
              .setIsComplete(true)
              .build()
          : DatasetMetadataState.builder().setIsComplete(false).setIsExpired(true).build();
    }
  }

  /**
   * Checks if the given metadata is complete and meets the given validity constraints.
   *
   * @param dataset dataset metadata
   * @param requestOptions request options
   * @return true iff the metadata is complete and meets validity constraints
   */
  public boolean checkValidity(
      NamespaceKeyWithConfig dataset, MetadataRequestOptions requestOptions) {

    // Check for validity can be overridden per source or per request.
    // Bypassing validity check means we consider metadata valid.
    MetadataRequestOptions overriddenOptions =
        ImmutableMetadataRequestOptions.copyOf(requestOptions)
            .withCheckValidity(
                requestOptions.checkValidity() && !getConfig().getDisableMetadataValidityCheck());

    BulkRequest<NamespaceKeyWithConfig> request =
        BulkRequest.<NamespaceKeyWithConfig>builder(1).add(dataset).build();
    Function<DatasetConfig, CompletionStage<Boolean>> validityCheck =
        datasetConfig ->
            CompletableFuture.completedFuture(
                metadataManager.isStillValid(
                    overriddenOptions, datasetConfig, this.unwrap(StoragePlugin.class)));

    return bulkCheckValidity(request, validityCheck)
        .get(dataset)
        .response()
        .toCompletableFuture()
        .join();
  }

  /**
   * Checks if the given metadata is complete and meets the given validity constraints. This
   * function will attempt to execute expensive validity checks on Iceberg tables asynchronously
   * when possible.
   *
   * @param datasets table metadata to validate
   * @param requestOptions request options
   * @return response saying if the metadata is complete and meets validity constraints
   */
  @WithSpan
  public BulkResponse<NamespaceKeyWithConfig, Boolean> bulkCheckValidity(
      BulkRequest<NamespaceKeyWithConfig> datasets, MetadataRequestOptions requestOptions) {

    // Check for validity can be overridden per source or per request.
    // Bypassing validity check means we consider metadata valid.
    MetadataRequestOptions overriddenOptions =
        ImmutableMetadataRequestOptions.copyOf(requestOptions)
            .withCheckValidity(
                requestOptions.checkValidity() && !getConfig().getDisableMetadataValidityCheck());

    return bulkCheckValidity(
        datasets, datasetConfig -> checkValidityAsync(datasetConfig, overriddenOptions));
  }

  private BulkResponse<NamespaceKeyWithConfig, Boolean> bulkCheckValidity(
      BulkRequest<NamespaceKeyWithConfig> datasets,
      Function<DatasetConfig, CompletionStage<Boolean>> asyncIcebergValidityCheck) {
    try (AutoCloseableLock l = readLock()) {
      checkState();

      // Handler for when DatasetConfig is incomplete
      BulkFunction<NamespaceKeyWithConfig, Boolean> incompleteHandler =
          inComplete ->
              inComplete.handleRequests(
                  dataset -> {
                    DatasetConfig datasetConfig = dataset.datasetConfig();
                    logger.debug(
                        "Dataset [{}] has incomplete metadata.",
                        datasetConfig == null || datasetConfig.getFullPathList() == null
                            ? "Unknown"
                            : new NamespaceKey(datasetConfig.getFullPathList()));
                    return CompletableFuture.completedFuture(Boolean.FALSE);
                  });
      // Handler for when DatasetConfig is complete: execute async validity check
      BulkFunction<NamespaceKeyWithConfig, Boolean> completeHandler =
          complete ->
              complete.handleRequests(
                  dataset -> {
                    DatasetConfig datasetConfig = Objects.requireNonNull(dataset.datasetConfig());
                    return asyncIcebergValidityCheck
                        .apply(datasetConfig)
                        .whenComplete(
                            (isValid, ex) -> {
                              if (!isValid) {
                                logger.debug(
                                    "Dataset [{}] has complete metadata but not valid any more.",
                                    new NamespaceKey(datasetConfig.getFullPathList()));
                              }
                            });
                  });

      return datasets.bulkPartitionAndHandleRequests(
          dataset -> isComplete(dataset.datasetConfig()),
          isComplete -> isComplete ? completeHandler : incompleteHandler,
          Function.identity(),
          ValueTransformer.identity());
    }
  }

  private CompletionStage<Boolean> checkValidityAsync(
      DatasetConfig datasetConfig, MetadataRequestOptions options) {
    // For Iceberg tables, check the validity async
    MetadataIOPool metadataIOPool = context.getMetadataIOPool();
    BiFunction<SupportsIcebergRootPointer, DatasetConfig, CompletionStage<Boolean>>
        asyncIcebergValidityCheck =
            (plugin, dataset) ->
                metadataIOPool.execute(
                    new MetadataIOPool.MetadataTask<>(
                        "iceberg_validity_check_async",
                        new EntityPath(dataset.getFullPathList()),
                        () ->
                            plugin.isIcebergMetadataValid(
                                dataset, new NamespaceKey(dataset.getFullPathList()))));
    return metadataManager.isStillValid(
        options, datasetConfig, this.unwrap(StoragePlugin.class), asyncIcebergValidityCheck);
  }

  public UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions) {
    checkState();
    try {
      return metadataManager.refreshDataset(key, retrievalOptions);
    } catch (StoragePluginChanging e) {
      throw UserException.validationError(e)
          .message("Storage plugin was changing during refresh attempt.")
          .build(logger);
    } catch (ConnectorException | NamespaceException e) {
      throw UserException.validationError(e)
          .message("Unable to refresh dataset. %s", e.getMessage())
          .build(logger);
    }
  }

  public void saveDatasetAndMetadataInNamespace(
      DatasetConfig datasetConfig,
      DatasetHandle datasetHandle,
      DatasetRetrievalOptions retrievalOptions) {
    checkState();
    try {
      metadataManager.saveDatasetAndMetadataInNamespace(
          datasetConfig, datasetHandle, retrievalOptions);
    } catch (StoragePluginChanging e) {
      throw UserException.validationError(e)
          .message("Storage plugin was changing during dataset update attempt.")
          .build(logger);
    } catch (ConnectorException e) {
      throw UserException.validationError(e).message("Unable to update dataset.").build(logger);
    }
  }

  public DatasetConfig getUpdatedDatasetConfig(DatasetConfig oldConfig, BatchSchema newSchema) {
    try (AutoCloseableLock l = readLock()) {
      checkState();
      return plugin.createDatasetConfigFromSchema(oldConfig, newSchema);
    }
  }

  public ViewTable getView(NamespaceKey key, final MetadataRequestOptions options) {
    try (AutoCloseableLock l = readLock()) {
      checkState();
      // TODO: move views to namespace and out of filesystem.
      return plugin.getView(key.getPathComponents(), options.getSchemaConfig());
    }
  }

  @WithSpan("get-dataset-handle")
  public Optional<DatasetHandle> getDatasetHandle(
      NamespaceKey key, DatasetConfig datasetConfig, DatasetRetrievalOptions retrievalOptions)
      throws ConnectorException {
    try (AutoCloseableLock ignored = readLock()) {
      checkState();
      final EntityPath entityPath;
      if (datasetConfig != null) {
        entityPath = new EntityPath(datasetConfig.getFullPathList());
      } else {
        entityPath = MetadataObjectsUtils.toEntityPath(key);
      }

      // include the full path of the dataset
      Span.current()
          .setAttribute(
              "dremio.dataset.path", PathUtils.constructFullPath(entityPath.getComponents()));
      return plugin.getDatasetHandle(
          entityPath, retrievalOptions.asGetDatasetOptions(datasetConfig));
    }
  }

  /**
   * Call after plugin start to register local variables.
   *
   * @param config new {@link SourceConfig} to store in this instance
   * @param runAsync whether to run parts of initPlugin asynchronously
   */
  private void setLocals(SourceConfig config, boolean runAsync) {
    if (getPlugin() == null) {
      return;
    }
    initPlugin(config, runAsync);
    this.pluginId = new StoragePluginId(sourceConfig, conf, getPlugin().getSourceCapabilities());
  }

  /**
   * Reset the plugin locals to the state before the plugin was started.
   *
   * @param config the original source configuration, must be not-null
   * @param pluginId the id of the plugin before startup
   */
  private void resetLocals(SourceConfig config, StoragePluginId pluginId) {
    if (getPlugin() == null) {
      return;
    }
    initPlugin(config, true);
    this.pluginId = pluginId;
  }

  /**
   * Helper function to set the plugin locals.
   *
   * @param config source config, must be not null
   * @param runAsync whether to run parts of the method asynchronously, if it's called from an async
   *     call already then this is expected to be false not to consume another thread from the pool
   *     or to create a race.
   */
  private void initPlugin(SourceConfig config, boolean runAsync) {
    this.sourceConfig = config;
    this.metadataPolicy =
        config.getMetadataPolicy() == null
            ? CatalogService.NEVER_REFRESH_POLICY
            : config.getMetadataPolicy();

    this.conf = config.getConnectionConf(reader);

    if (runAsync && options.getOption(CatalogOptions.ENABLE_ASYNC_GET_STATE)) {
      // StoragePlugin.getState may not resolve quickly due to long connection
      // timeouts, limit the call time.
      long timeoutSeconds = options.getOption(CatalogOptions.GET_STATE_TIMEOUT_SECONDS);
      CompletableFuture<SourceState> stateFuture =
          CompletableFuture.supplyAsync(() -> getPlugin().getState(), executor);
      try {
        this.state = stateFuture.get(timeoutSeconds, TimeUnit.SECONDS);
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        this.state =
            SourceState.badState(
                SourceState.NOT_AVAILABLE.getSuggestedUserAction(), e.getCause().getMessage());
        logger.error("Failed to obtain plugin's state in {}s: {}", timeoutSeconds, name, e);
        throw new RuntimeException(e);
      }
    } else {
      this.state = getPlugin().getState();
    }
  }

  /** Update the cached state of the plugin. */
  public CompletableFuture<SourceState> refreshState() throws Exception {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            Optional<AutoCloseableLock> refreshLock =
                AutoCloseableLock.of(this.refreshStateLock, true).tryOpen(0, TimeUnit.SECONDS);
            if (!refreshLock.isPresent()) {
              return state;
            }
            try (AutoCloseableLock rl = refreshLock.get()) {
              while (true) {
                if (plugin == null) {
                  Optional<AutoCloseableLock> writeLock =
                      AutoCloseableLock.of(this.writeLock, true).tryOpen(5, TimeUnit.SECONDS);
                  if (!writeLock.isPresent()) {
                    return state;
                  }

                  try (AutoCloseableLock l = writeLock.get()) {
                    if (plugin != null) {
                      // while waiting for write lock, someone else started things, start this loop
                      // over.
                      continue;
                    }
                    plugin =
                        resolveConnectionConf(conf)
                            .newPlugin(context, sourceConfig.getName(), this::getId);
                    return newStartSupplier(sourceConfig, false).get();
                  }
                }

                // the plugin is not null.
                Optional<AutoCloseableLock> readLock =
                    AutoCloseableLock.of(this.readLock, true).tryOpen(1, TimeUnit.SECONDS);
                if (!readLock.isPresent()) {
                  return state;
                }

                try (Closeable a = readLock.get()) {
                  final SourceState state = plugin.getState();
                  this.state = state;
                  return state;
                }
              }
            }
          } catch (Exception ex) {
            logger.debug("Failed to start plugin while trying to refresh state, error:", ex);
            if (ex.getCause() instanceof DeletedException) {
              logger.debug(String.format("[%s] is deleted", name), ex.getCause());
              SourceState state =
                  SourceState.badState(
                      SourceState.DELETED.getSuggestedUserAction(), ex.getCause().getMessage());
              this.state = state;
              return state;
            }
            this.state = SourceState.NOT_AVAILABLE;
            return SourceState.NOT_AVAILABLE;
          }
        },
        executor);
  }

  boolean replacePluginWithLock(
      SourceConfig config, final long waitMillis, boolean skipEqualityCheck) throws Exception {
    try (Closeable write = writeLock()) {
      return replacePlugin(config, waitMillis, skipEqualityCheck);
    }
  }

  private boolean sourceConfigDiffersTagAlone(SourceConfig theirs) {
    byte[] bytesOurs =
        ProtostuffIOUtil.toByteArray(
            this.sourceConfig, SourceConfig.getSchema(), LinkedBuffer.allocate());
    SourceConfig oursNoTag = new SourceConfig();
    ProtostuffIOUtil.mergeFrom(bytesOurs, oursNoTag, SourceConfig.getSchema());
    oursNoTag.setTag(null);

    byte[] bytesTheirs =
        ProtostuffIOUtil.toByteArray(theirs, SourceConfig.getSchema(), LinkedBuffer.allocate());
    SourceConfig theirsNoTag = new SourceConfig();
    ProtostuffIOUtil.mergeFrom(bytesTheirs, theirsNoTag, SourceConfig.getSchema());
    theirsNoTag.setTag(null);

    return !this.sourceConfig.equals(theirs) && oursNoTag.equals(theirsNoTag);
  }

  /**
   * Replace the plugin instance with one defined by the new SourceConfig. Do the minimal changes
   * necessary. Starts the new plugin.
   *
   * @param config
   * @param waitMillis
   * @return Whether metadata was maintained. Metdata will be maintained if the connection did not
   *     change or was changed with only non-metadata impacting changes.
   * @throws Exception
   */
  private boolean replacePlugin(
      SourceConfig config, final long waitMillis, boolean skipEqualityCheck) throws Exception {
    Preconditions.checkState(
        writeLock.isHeldByCurrentThread(),
        "You must hold the plugin write lock before replacing plugin.");

    final ConnectionConf<?, ?> existingConnectionConf = this.conf;
    final ConnectionConf<?, ?> newConnectionConf = reader.getConnectionConf(config);
    /* if the plugin startup had failed earlier (plugin is null) and
     * we are here to replace the plugin, we should not return here.
     */
    if (!skipEqualityCheck && existingConnectionConf.equals(newConnectionConf) && plugin != null) {
      // we just need to update external settings.
      setLocals(config, true);
      return true;
    }

    /*
     * we are here if
     * (1) current plugin is null OR
     * (2) current plugin is non-null but new and existing
     *     connection configurations don't match.
     */
    this.state = SourceState.NOT_AVAILABLE;

    // hold the old plugin until we successfully replace it.
    final SourceConfig oldConfig = sourceConfig;
    final StoragePlugin oldPlugin = plugin;
    final StoragePluginId oldPluginId = pluginId;
    this.plugin =
        resolveConnectionConf(newConnectionConf)
            .newPlugin(context, sourceKey.getRoot(), this::getId, influxSourcePred);
    try {
      logger.trace("Starting new plugin for [{}]", config.getName());
      startAsync(config, false).get(waitMillis, TimeUnit.MILLISECONDS);
      try {
        AutoCloseables.close(oldPlugin);
      } catch (Exception ex) {
        logger.warn("Failure while retiring old plugin [{}].", sourceKey, ex);
      }

      // if we replaced the plugin successfully, clear the permission cache
      getPermissionsCache().clear();

      return existingConnectionConf.equalsIgnoringNotMetadataImpacting(newConnectionConf);
    } catch (Exception ex) {
      // the update failed, go back to previous state.
      this.plugin = oldPlugin;
      try {
        resetLocals(oldConfig, oldPluginId);
      } catch (Exception e) {
        ex.addSuppressed(e);
      }
      throw ex;
    }
  }

  boolean isSourceConfigMetadataImpacting(SourceConfig config) {
    final ConnectionConf<?, ?> newConnectionConf = reader.getConnectionConf(config);
    try (AutoCloseableLock l = readLock()) {
      return !conf.equalsIgnoringNotMetadataImpacting(newConnectionConf);
    }
  }

  @Override
  public void close() throws Exception {
    close(null, c -> {});
  }

  /**
   * Close this storage plugin if it matches the provided configuration
   *
   * @param config
   * @return
   * @throws Exception
   */
  public boolean close(SourceConfig config, Consumer<ManagedStoragePlugin> runUnderLock)
      throws Exception {
    try (AutoCloseableLock l = writeLock()) {

      // it's possible that the delete is newer than the current version of the plugin. If versions
      // inconsistent,
      // synchronize before attempting to match.
      if (config != null) {
        if (!config.getTag().equals(sourceConfig.getTag())) {
          try {
            synchronizeSource(config);
          } catch (Exception ex) {
            logger.debug("Synchronization of source failed while attempting to delete.", ex);
          }
        }

        if (!matches(config)) {
          return false;
        }

        // Also delete any associated secrets
        try {
          cleanupSecrets(config);
        } catch (Exception ex) {
          logger.warn("Failed to cleanup secrets within source " + config.getName(), ex);
        }
      }

      try {
        closed = true;
        state = SourceState.badState("Source is being shutdown.");
        AutoCloseables.close(metadataManager, plugin);
      } finally {
        runUnderLock.accept(this);
      }
      return true;
    }
  }

  public void deleteServiceSet() throws Exception {
    metadataManager.deleteServiceSet();
  }

  boolean refresh(UpdateType updateType, MetadataPolicy policy) throws NamespaceException {
    checkState();
    return metadataManager.refresh(updateType, policy, true);
  }

  @SuppressWarnings("unchecked")
  public <T extends StoragePlugin> T unwrap(Class<T> clazz) {
    return unwrap(clazz, false);
  }

  @SuppressWarnings("unchecked")
  public <T extends StoragePlugin> T unwrap(Class<T> clazz, boolean skipStateCheck) {
    try (AutoCloseableLock l = readLock()) {
      if (!skipStateCheck) {
        checkState();
      }

      if (clazz.isAssignableFrom(plugin.getClass())) {
        return (T) plugin;
      }
    }
    return null;
  }

  private static final Comparator<SourceConfig> SOURCE_CONFIG_COMPARATOR =
      (s1, s2) -> {
        if (s2.getConfigOrdinal() == null) {
          if (s1.getConfigOrdinal() == null) {
            return 0;
          }
          return 1;
        } else if (s1.getConfigOrdinal() == null) {
          return -1;
        } else {
          return Long.compare(s1.getConfigOrdinal(), s2.getConfigOrdinal());
        }
      };

  interface SupplierWithEX<T, EX extends Throwable> {
    T get() throws EX;
  }

  interface RunnableWithEX<EX extends Throwable> {
    void run() throws EX;
  }

  private AutoCloseableLock tryReadLock() {

    // if the plugin was closed, we should always fail.
    if (closed) {
      throw new StoragePluginChanging(name + ": Plugin was closed.");
    }

    boolean locked = readLock.tryLock();
    if (!locked) {
      throw new StoragePluginChanging(name + ": Plugin is actively undergoing changes.");
    }

    return AutoCloseableLock.ofAlreadyOpen(readLock, true);
  }

  @SuppressWarnings("serial")
  class StoragePluginChanging extends RuntimeException {

    public StoragePluginChanging(String message) {
      super(message);
    }
  }

  /**
   * Ensures that all namespace operations are under a read lock to avoid issues where a plugin
   * starts changing and then we write to the namespace. If a safe run operation can't aquire the
   * read lock, we will throw a StoragePluginChanging exception. It will also throw if it was
   * created against an older version of the plugin.
   *
   * <p>This runner is snapshot based. When it is initialized, it will record the current tag of the
   * source configuration. If the tag changes, it will disallow future operations, even if the lock
   * becomes available. This ensures that if there are weird timing where an edit happens fast
   * enough such that a metadata refresh doesn't naturally hit the safe runner, it will still not be
   * able to do any metastore modifications (just in case the plugin change required a catalog
   * deletion).
   */
  class SafeRunner {

    private final String configTag = sourceConfig.getTag();

    private AutoCloseableLock tryReadLock() {
      // make sure we don't expose the read lock for a now out of date SafeRunner.
      if (!Objects.equals(sourceConfig.getTag(), configTag)) {
        throw new StoragePluginChanging(name + ": Plugin tag has changed since refresh started.");
      }
      return ManagedStoragePlugin.this.tryReadLock();
    }

    public <EX extends Throwable> void doSafe(RunnableWithEX<EX> runnable) throws EX {
      try (AutoCloseableLock read = tryReadLock()) {
        runnable.run();
      }
    }

    public <T, EX extends Throwable> T doSafe(SupplierWithEX<T, EX> supplier) throws EX {
      try (AutoCloseableLock read = tryReadLock()) {
        return supplier.get();
      }
    }

    public <I, T extends Iterable<I>, EX extends Throwable> Iterable<I> doSafeIterable(
        SupplierWithEX<T, EX> supplier) throws EX {
      try (AutoCloseableLock read = tryReadLock()) {
        final Iterable<I> innerIterable = supplier.get();
        return () -> wrapIterator(innerIterable.iterator());
      }
    }

    public <I, EX extends Throwable> Iterator<I> wrapIterator(final Iterator<I> innerIterator)
        throws EX {
      return new Iterator<I>() {
        @Override
        public boolean hasNext() {
          return SafeRunner.this.doSafe(
              () -> {
                return innerIterator.hasNext();
              });
        }

        @Override
        public I next() {
          return doSafe(() -> innerIterator.next());
        }
      };
    }
  }

  /**
   * A class that provides a lock protected bridge between the source metadata manager and the
   * ManagedStoragePlugin so the manager can't cause problems with plugin locking.
   */
  class MetadataBridge {
    SourceMetadata getMetadata() {
      try (AutoCloseableLock read = tryReadLock()) {
        if (plugin == null) {
          return null;
        }

        // note that we don't protect the methods inside this. This could possibly cause a weird
        // metadata exception if
        // the source is changing while a dataset is being refreshed. This was previously possible
        // with an inline
        // refresh but not a background refresh. With this pattern, it can also happen with a
        // background refresh.
        return plugin;
      }
    }

    DatasetRetrievalOptions getDefaultRetrievalOptions() {
      try (AutoCloseableLock read = tryReadLock()) {
        return ManagedStoragePlugin.this.getDefaultRetrievalOptions();
      }
    }

    public NamespaceService getNamespaceService() {
      try (AutoCloseableLock read = tryReadLock()) {
        return new SafeNamespaceService(systemUserNamespaceService, new SafeRunner());
      }
    }

    public Orphanage getOrphanage() {
      return orphanage;
    }

    public MetadataPolicy getMetadataPolicy() {
      try (AutoCloseableLock read = tryReadLock()) {
        return metadataPolicy;
      }
    }

    int getMaxMetadataColumns() {
      try (AutoCloseableLock read = tryReadLock()) {
        return Ints.saturatedCast(options.getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
      }
    }

    int getMaxNestedLevels() {
      try (AutoCloseableLock read = tryReadLock()) {
        return Ints.saturatedCast(options.getOption(CatalogOptions.MAX_NESTED_LEVELS));
      }
    }

    public void refreshState() throws Exception {
      ManagedStoragePlugin.this.refreshState().get(30, TimeUnit.SECONDS);
    }

    SourceState getState() {
      try (AutoCloseableLock read = tryReadLock()) {
        return state;
      }
    }
  }
}

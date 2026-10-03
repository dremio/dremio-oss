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

import static com.dremio.exec.catalog.CatalogOptions.SOURCE_ASYNC_MODIFICATION_ENABLED;
import static com.dremio.exec.catalog.CatalogOptions.SOURCE_ASYNC_MODIFICATION_TIMEOUT_MINUTES;
import static com.dremio.exec.catalog.CatalogOptions.SOURCE_ASYNC_MODIFICATION_WAIT_SECONDS;
import static com.dremio.exec.catalog.CatalogOptions.SOURCE_SEAMLESS_UPDATE_ALLOWED_DATABASES;
import static com.dremio.service.namespace.source.proto.SourceChangeState.SOURCE_CHANGE_STATE_CREATING;
import static com.dremio.service.namespace.source.proto.SourceChangeState.SOURCE_CHANGE_STATE_NONE;
import static com.dremio.service.namespace.source.proto.SourceChangeState.SOURCE_CHANGE_STATE_UPDATING;

import com.dremio.catalog.exception.SourceBadStateException;
import com.dremio.catalog.exception.SourceMalfunctionException;
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
import com.dremio.exec.catalog.DatasetCatalog.UpdateStatus;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SupportsGlobalKeys;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotQueryContext;
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
import com.dremio.service.namespace.dataset.proto.DatasetType;
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
import com.google.common.base.Defaults;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import java.lang.reflect.Field;
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
import javax.annotation.Nullable;
import javax.inject.Provider;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the Dremio system state related to a StoragePlugin.
 *
 * <p>Also owns the SourceMetadataManager, the task driver responsible for maintaining metadata
 * freshness for this source.
 */
public class ManagedStoragePlugin implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ManagedStoragePlugin.class);
  private final String name;
  private final CatalogSabotContext context;
  private final NamespaceService.Factory namespaceServiceFactory;
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

  /**
   * This is the same lock as readLock & writeLock above. It is included as an instance variable
   * because it is very useful during debugging.
   */
  @SuppressWarnings("FieldCanBeLocal")
  private final ReentrantReadWriteLock rwLock;

  /**
   * Parallel calls to refreshState could accumulate and get stuck on slow/synchronized getState
   * calls. Only let one refreshState happen at a time.
   */
  private final Lock refreshStateLock = new ReentrantLock();

  /**
   * "In flux" sources are those where this node has obtained the distributed lock for the source.
   */
  private final Predicate<String> isInFluxSource;

  private final PermissionCheckCache permissionsCache;
  private final SourceMetadataManager metadataManager;
  private final OptionManager options;
  private final NamespaceService systemUserNamespaceService;
  private final Orphanage orphanage;

  protected volatile SourceConfig sourceConfig;
  private volatile @Nullable StoragePlugin plugin;
  private volatile MetadataPolicy metadataPolicy;
  private volatile StoragePluginId pluginId;
  private volatile ConnectionConf<?, ?> conf;
  private volatile Stopwatch startup = Stopwatch.createUnstarted();
  private volatile SourceState state = SourceState.badState("Source not yet started.");
  private final Thread fixFailedThread;
  private volatile boolean closed = false;

  public ManagedStoragePlugin(
      CatalogSabotContext catalogSabotContext,
      SabotQueryContext sabotQueryContext,
      Executor executor,
      boolean isVirtualMaster,
      ModifiableSchedulerService modifiableScheduler,
      NamespaceService systemUserNamespaceService,
      Orphanage orphanage,
      LegacyKVStore<NamespaceKey, SourceInternalData> sourceDataStore,
      SourceConfig sourceConfig,
      OptionManager options,
      ConnectionReader reader,
      CatalogServiceMonitor monitor,
      Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider,
      Predicate<String> isInFluxSource,
      NamespaceService.Factory namespaceServiceFactory) {
    this.rwLock = new ReentrantReadWriteLock(true);
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();

    this.context = catalogSabotContext;
    this.executor = executor;
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
        resolveConnectionConf(conf)
            .newPlugin(catalogSabotContext, sourceConfig.getName(), this::getId);
    this.metadataPolicy =
        sourceConfig.getMetadataPolicy() == null
            ? CatalogService.NEVER_REFRESH_POLICY
            : sourceConfig.getMetadataPolicy();
    this.permissionsCache =
        new PermissionCheckCache(
            this::getPlugin, getAuthTtlMsProvider(options, sourceConfig), 2500);

    this.isInFluxSource = isInFluxSource;

    fixFailedThread = new FixFailedToStart();
    // leaks this so do last.
    this.metadataManager =
        createSourceMetadataManager(
            sourceKey,
            modifiableScheduler,
            isVirtualMaster,
            sourceDataStore,
            new MetadataBridge(),
            options,
            monitor,
            broadcasterProvider,
            catalogSabotContext.getClusterCoordinator(),
            sabotQueryContext);
    this.namespaceServiceFactory = namespaceServiceFactory;
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
      boolean isVirtualMaster,
      LegacyKVStore<NamespaceKey, SourceInternalData> sourceDataStore,
      final ManagedStoragePlugin.MetadataBridge bridge,
      final OptionManager options,
      final CatalogServiceMonitor monitor,
      final Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider,
      final ClusterCoordinator clusterCoordinator,
      final SabotQueryContext sabotQueryContext) {
    return new SourceMetadataManager(
        sourceName,
        modifiableScheduler,
        isVirtualMaster,
        sourceDataStore,
        bridge,
        options,
        monitor,
        broadcasterProvider,
        clusterCoordinator,
        sabotQueryContext);
  }

  protected PermissionCheckCache getPermissionsCache() {
    return permissionsCache;
  }

  protected Optional<StoragePlugin> getPlugin() {
    return Optional.ofNullable(plugin);
  }

  protected MetadataPolicy getMetadataPolicy() {
    return metadataPolicy;
  }

  protected AutoCloseableLock readLock() {
    return AutoCloseableLock.lockAndWrap(readLock, true);
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
    return options.getOption(CatalogOptions.STORAGE_PLUGIN_CREATE_MAX);
  }

  /**
   * Synchronize plugin state to the provided target source config.
   *
   * <p>Note that this will fail if the target config is older than the existing config, in terms of
   * creation time or version.
   *
   * @param targetConfig target source config
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
      replacePluginDeprecated(targetConfig, createWaitMillis(), false);
    }
  }

  private void addDefaults(SourceConfig config) {
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

  CompletionStage<Void> createSource(
      SourceConfig config, String userName, NamespaceAttribute... attributes)
      throws NamespaceException, SourceMalfunctionException {
    return createSource(
        config, userName, SourceRefreshOption.WAIT_FOR_DATASETS_CREATION, attributes);
  }

  CompletionStage<Void> createSource(
      SourceConfig config,
      String userName,
      SourceRefreshOption sourceRefreshOption,
      NamespaceAttribute... attributes)
      throws NamespaceException, SourceMalfunctionException {
    if (options.getOption(SOURCE_SEAMLESS_UPDATE_ALLOWED_DATABASES)) {
      return createOrUpdateSource(true, config, userName, sourceRefreshOption, attributes);
    } else {
      return createOrUpdateSourceDeprecated(
          true, config, userName, sourceRefreshOption, attributes);
    }
  }

  CompletionStage<Void> updateSource(
      SourceConfig config, String userName, NamespaceAttribute... attributes)
      throws ConcurrentModificationException, NamespaceException, SourceMalfunctionException {
    return updateSource(
        config, userName, SourceRefreshOption.WAIT_FOR_DATASETS_CREATION, attributes);
  }

  CompletionStage<Void> updateSource(
      SourceConfig config,
      String userName,
      SourceRefreshOption sourceRefreshOption,
      NamespaceAttribute... attributes)
      throws ConcurrentModificationException, NamespaceException, SourceMalfunctionException {
    if (options.getOption(SOURCE_SEAMLESS_UPDATE_ALLOWED_DATABASES)) {
      return createOrUpdateSource(false, config, userName, sourceRefreshOption, attributes);
    } else {
      return createOrUpdateSourceDeprecated(
          false, config, userName, sourceRefreshOption, attributes);
    }
  }

  @Deprecated
  CompletionStage<Void> createOrUpdateSourceDeprecated(
      final boolean create,
      SourceConfig config,
      String userName,
      SourceRefreshOption sourceRefreshOption,
      NamespaceAttribute... attributes)
      throws ConcurrentModificationException, NamespaceException, SourceMalfunctionException {
    final NamespaceService userNamespace = namespaceServiceFactory.get(userName);
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

    if (getPlugin().isPresent() && getPlugin().get().isWrapperFor(VersionedPlugin.class)) {
      config.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    }

    final SourceConfig originalConfigCopy = ProtostuffUtil.copy(sourceConfig);

    // Preprocessing on the config before creation
    addDefaults(config);
    addGlobalKeys(config);
    encryptSecrets(config);
    validateAccelerationSettings(config);
    config.setSourceChangeState(
        create ? SOURCE_CHANGE_STATE_CREATING : SOURCE_CHANGE_STATE_UPDATING);

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
          replacePluginDeprecated(config, createWaitMillis(), true);
          oldMetadataIsBad = false;
          refreshDatasetNames = true;
        } else {
          boolean metadataStillGood = replacePluginDeprecated(config, createWaitMillis(), false);
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
          setLocals(originalConfigCopy);
          throw e;
        }
      }

      // ***** Complete Local Update **** //
      createOrUpdateSucceeded = true;

      // now that we're outside the plugin lock, we should refresh the dataset names. Note that this
      // could possibly get
      // run on a different config if two source updates are racing to this lock.
      return startAsyncSourceCreateUpdate(
          config,
          originalConfigCopy,
          create,
          userNamespace,
          refreshDatasetNames ? List.of(SourceNameRefreshAction.newRefreshAllAction()) : null,
          sourceRefreshOption,
          stopwatchForPlugin,
          attributes);
    } catch (Exception e) {
      postSourceModify(config, originalConfigCopy, e, createOrUpdateSucceeded, create);
      return CompletableFuture.runAsync(() -> {});
    }
  }

  CompletionStage<Void> createOrUpdateSource(
      final boolean create,
      SourceConfig config,
      String userName,
      SourceRefreshOption sourceRefreshOption,
      NamespaceAttribute... attributes)
      throws ConcurrentModificationException, NamespaceException, SourceMalfunctionException {
    final NamespaceService userNamespace = namespaceServiceFactory.get(userName);
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
    if (getPlugin().isPresent() && getPlugin().get().isWrapperFor(VersionedPlugin.class)) {
      config.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    }

    final SourceConfig originalConfigCopy = ProtostuffUtil.copy(sourceConfig);

    // Preprocessing on the config before creation
    addDefaults(config);
    addGlobalKeys(config);
    encryptSecrets(config);
    validateAccelerationSettings(config);
    config.setSourceChangeState(
        create ? SOURCE_CHANGE_STATE_CREATING : SOURCE_CHANGE_STATE_UPDATING);

    if (!create) {
      updateConfig(userNamespace, config, attributes);
    } else {
      migrateLegacyFormat(config);
    }

    try {
      final Stopwatch stopwatchForPlugin = Stopwatch.createStarted();

      List<SourceNameRefreshAction> nameRefreshActions;

      // **** Start Local Update **** //
      try (AutoCloseableLock writeLock = writeLock()) {

        if (create) {
          // this isn't really a replace.
          replacePlugin(config, createWaitMillis(), true);
          nameRefreshActions = Lists.newArrayList(SourceNameRefreshAction.newRefreshAllAction());
        } else {
          nameRefreshActions = replacePlugin(config, createWaitMillis(), false);
        }

        // Now let's create the plugin in the system namespace.
        // This increments the version in the config object as well.
        try {
          userNamespace.addOrUpdateSource(config.getKey(), config, attributes);
        } catch (AccessControlException | ConcurrentModificationException | DatastoreException e) {
          logger.trace(
              "Saving to namespace store failed, reverting the in-memory source config to the original version...");
          setLocals(originalConfigCopy);
          throw e;
        }
      }

      // ***** Complete Local Update **** //
      createOrUpdateSucceeded = true;

      // now that we're outside the plugin lock, we should refresh the dataset names. Note that this
      // could possibly get
      // run on a different config if two source updates are racing to this lock.
      return startAsyncSourceCreateUpdate(
          config,
          originalConfigCopy,
          create,
          userNamespace,
          nameRefreshActions,
          sourceRefreshOption,
          stopwatchForPlugin,
          attributes);
    } catch (Exception e) {
      postSourceModify(config, originalConfigCopy, e, createOrUpdateSucceeded, create);
      return CompletableFuture.runAsync(() -> {});
    }
  }

  private CompletableFuture<Void> startAsyncSourceCreateUpdate(
      SourceConfig config,
      SourceConfig originalConfigCopy,
      boolean create,
      NamespaceService userNamespace,
      List<SourceNameRefreshAction> nameRefreshActions,
      SourceRefreshOption sourceRefreshOption,
      Stopwatch stopwatchForPlugin,
      NamespaceAttribute... attributes) {
    // When the flag is off, we are passing Runnable::run for running the code in the same thread,
    // which effectively makes the code run synchronously.
    CompletableFuture<Void> future =
        CompletableFuture.runAsync(
                () -> {
                  refreshNames(config.getName(), nameRefreshActions);
                },
                options.getOption(SOURCE_ASYNC_MODIFICATION_ENABLED) ? executor : Runnable::run)
            .whenComplete(
                (res, ex) -> {
                  try {
                    // Update source change state to none in namespace and in-memory.
                    try (AutoCloseableLock writeLock = writeLock()) {
                      userNamespace.addOrUpdateSource(
                          config.getKey(),
                          config.setSourceChangeState(SOURCE_CHANGE_STATE_NONE),
                          attributes);
                      replacePlugin(config, createWaitMillis(), false);
                    }
                  } catch (Exception e) {
                    logger.error("Error while updating source change state", e);
                    if (ex == null) {
                      ex = e;
                    } else {
                      ex.addSuppressed(e);
                    }
                  } finally {
                    postSourceModify(config, originalConfigCopy, ex, true, create);
                    logCompletion(config.getName(), stopwatchForPlugin);
                  }
                });

    // Most sources require very little time to get names of their children (tables/views), spent
    // SOURCE_ASYNC_MODIFICATION_WAIT_SECONDS to let them complete before this method returns. If
    // successful, the source is ready for use. Otherwise, the source completes initialization
    // asynchronously.
    waitAsyncSourceModification(
        future,
        options.getOption(SOURCE_ASYNC_MODIFICATION_WAIT_SECONDS),
        TimeUnit.SECONDS,
        create,
        true);

    // if it is synchronous path or if the feature flag is turned off, we wait.
    // or if nameRefreshActions are empty, do synchronous path
    if (sourceRefreshOption == SourceRefreshOption.WAIT_FOR_DATASETS_CREATION
        || !options.getOption(SOURCE_ASYNC_MODIFICATION_ENABLED)) {
      waitAsyncSourceModification(
          future,
          options.getOption(SOURCE_ASYNC_MODIFICATION_TIMEOUT_MINUTES),
          TimeUnit.MINUTES,
          create,
          false);
    }
    // Make sure the future cannot be completed by the caller.
    return future.copy();
  }

  private void waitAsyncSourceModification(
      CompletableFuture<Void> future,
      long timeout,
      TimeUnit timeUnit,
      boolean create,
      boolean ignoreTimeoutException) {
    try {
      future.get(timeout, timeUnit);
    } catch (InterruptedException e) {
      logAsyncWaitFailure(create, e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (TimeoutException te) {
      // Does not finish within given time.
      if (!ignoreTimeoutException) {
        logAsyncWaitFailure(create, te);
        throw new RuntimeException(te);
      }
    } catch (Exception e) {
      logAsyncWaitFailure(create, e);
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  private void logAsyncWaitFailure(boolean create, Exception e) {
    logger.error(
        String.format(
            "error while waiting async source %s. Error: %s", create ? "creation" : "update", e));
  }

  /** Delete encrypted secrets found in the old config but not in the new config. */
  private void cleanupSecretsIfNeeded(
      SourceConfig config,
      SourceConfig originalConfigCopy,
      boolean createOrUpdateSucceeded,
      boolean create) {
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

  private void postSourceModify(
      SourceConfig config,
      SourceConfig originalConfigCopy,
      Throwable t,
      boolean createOrUpdateSucceed,
      boolean create) {
    cleanupSecretsIfNeeded(config, originalConfigCopy, createOrUpdateSucceed, create);
    if (t instanceof ConcurrentModificationException) {
      throw UserException.concurrentModificationError(t)
          .message(
              "Source update failed due to a concurrent update. Please try again [%s].",
              config.getName())
          .build(logger);
    } else if (t instanceof AccessControlException) {
      throw UserException.permissionError(t)
          .message("Update privileges on source [%s] failed: %s", config.getName(), t.getMessage())
          .build(logger);
    } else if (t instanceof InvalidNamespaceNameException) {
      throw UserException.validationError(t)
          .message(
              String.format(
                  "Failure creating/updating this source [%s]: %s",
                  config.getName(), t.getMessage()))
          .build(logger);
    } else if (t instanceof Exception) { // Any exception other than the ones listed above.
      String suggestedUserAction = getState().getSuggestedUserAction();
      if (suggestedUserAction == null || suggestedUserAction.isEmpty()) {
        // If no user action was suggested, fall back to a basic message.
        suggestedUserAction =
            String.format(
                "Failure creating/updating this source [%s]: %s.",
                config.getName(), t.getMessage());
      }
      throw UserException.validationError(t).message(suggestedUserAction).build(logger);
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

  private void refreshNames(String name, List<SourceNameRefreshAction> actions) {
    if (actions == null || actions.isEmpty()) {
      return;
    }

    if (keepStaleMetadata()) {
      logger.debug("Not refreshing names in source [{}]", name);
      return;
    }

    logger.debug("Refreshing names in source [{}]", name);
    try {
      for (SourceNameRefreshAction action : actions) {
        switch (action.getAction()) {
          case DELETE_FOLDERS:
            logger.debug(
                "Delete entities in source [{}] with prefixes {}", name, action.getFolders());
            deleteSourceChildren(sourceConfig, action.getFolders());
            break;
          case DELETE_ALL:
            logger.debug("Delete all entities in source [{}]", name);
            deleteSourceChildren(sourceConfig, null);
            break;
          case REFRESH_FOLDERS:
            logger.debug(
                "Refresh entities in source [{}] with with prefixes {}", name, action.getFolders());
            refresh(SourceUpdateType.ofNamesInFolders(action.getFolders()), null);
            break;
          case REFRESH_ALL:
            logger.debug("Refresh all entities names in source [{}]", name);
            refresh(SourceUpdateType.NAMES, null);
            break;
          default:
            throw new IllegalStateException("Unexpected value: " + action.getAction());
        }
      }
    } catch (NamespaceException e) {
      throw UserException.validationError(e)
          .message(String.format("Failure refreshing source [%s]: %s", name, e.getMessage()))
          .buildSilently();
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

  void validateAccelerationSettings(SourceConfig config) throws SourceMalfunctionException {}

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
    return options.getOption(CatalogOptions.STORAGE_PLUGIN_KEEP_METADATA_ON_REPLACE);
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

  public Optional<StoragePluginRulesFactory> getRulesFactory()
      throws InstantiationException,
          IllegalAccessException,
          InvocationTargetException,
          NoSuchMethodException {
    // not read under a read lock since it is updated via volatile. Allows us to avoid locking in
    // read paths. This is
    // especially important here since this method is used even if the query does not refer to this
    // source.

    // grab local to avoid changes under us later.
    Optional<StoragePlugin> optionalStoragePlugin = getPlugin();
    if (optionalStoragePlugin.isEmpty()) {
      return Optional.empty();
    }

    if (optionalStoragePlugin.get().getRulesFactoryClass() != null) {
      return Optional.of(
          optionalStoragePlugin
              .get()
              .getRulesFactoryClass()
              .getDeclaredConstructor()
              .newInstance());
    }

    return Optional.empty();
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
              if (getPlugin().isPresent()) {
                getPlugin().get().start();
              } else {
                throw new SourceMalfunctionException(this.name);
              }
              setLocals(config);
              startup.stop();
              if (state.getStatus() == SourceStatus.bad) {
                // Check the state here and throw exception so that we close the partially started
                // plugin properly in the
                // exception handling code
                throw new SourceBadStateException(state.toString());
              }

              return state;
            } catch (Throwable e) {
              if (!MissingPluginConf.TYPE.equals(config.getType())) {
                logger.warn("Error starting new source: {}", sourceConfig.getName(), e);
              }
              // TODO: Throwables.gerRootCause(e)
              state = SourceState.badState(e.getMessage(), e.getMessage());

              try {
                // failed to startup, make sure to close.
                if (closeMetaDataManager) {
                  AutoCloseables.close(metadataManager, getPlugin().get());
                } else {
                  if (getPlugin().isPresent()) {
                    getPlugin().get().close();
                  }
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
   * Starts the plugin instance that replaces the current one.
   *
   * <p>When the start fails, the metadata manager is closed only for a source that is being created
   * ({@code skipEqualityCheck}), which is discarded after the failure. A failed update of an
   * existing source (including the synchronization of its configuration from the KV store) puts the
   * previous plugin back, so the metadata manager must keep running: once closed, its background
   * refresh never runs again until Dremio restarts.
   */
  private CompletableFuture<SourceState> startReplacementAsync(
      final SourceConfig config, final boolean skipEqualityCheck) {
    return CompletableFuture.supplyAsync(newStartSupplier(config, skipEqualityCheck), executor);
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
      final Map<String, AttributeValue> attributes)
      throws SourceMalfunctionException {
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
      final AttributeValue attributeValue)
      throws SourceMalfunctionException {
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
      AlterMetadataCallback pluginCallback)
      throws SourceMalfunctionException {
    if (getPlugin().isEmpty()) {
      throw new SourceMalfunctionException(this.name);
    }
    if (!(getPlugin().get() instanceof SupportsAlteringDatasetMetadata)) {
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

    if (handle.isEmpty()) {
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
              (SupportsAlteringDatasetMetadata) getPlugin().get(),
              handle.get(),
              oldDatasetMetadata);
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
                refreshState();
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
    return config != null && config.getType() == DatasetType.VIRTUAL_DATASET
        ? isCompleteVirtualDataset(config)
        : isCompletePhysicalDataset(config);
  }

  private static boolean isCompletePhysicalDataset(DatasetConfig config) {
    return config != null
        && DatasetHelper.getSchemaBytes(config) != null
        && config.getReadDefinition() != null
        && config.getReadDefinition().getSplitVersion() != null;
  }

  private static boolean isCompleteVirtualDataset(DatasetConfig config) {
    return !MetadataObjectsUtils.isShallowView(config);
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
      final MetadataRequestOptions options)
      throws SourceMalfunctionException {
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
  public DatasetMetadataState getDatasetMetadataState(DatasetConfig datasetConfig)
      throws SourceMalfunctionException {
    try (AutoCloseableLock l = readLock()) {
      checkState();
      boolean isComplete = isComplete(datasetConfig);
      if (isComplete) {
        if (getPlugin().isEmpty()) {
          throw new SourceMalfunctionException(this.name);
        }
        return DatasetMetadataState.builder()
            .from(metadataManager.getDatasetMetadataState(datasetConfig, getPlugin().get()))
            .setIsComplete(true)
            .build();
      }

      return DatasetMetadataState.builder().setIsComplete(false).setIsExpired(true).build();
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
    BiFunction<SupportsIcebergRootPointer, DatasetConfig, CompletionStage<Boolean>>
        asyncIcebergValidityCheck =
            (plugin, dataset) ->
                context
                    .getMetadataIOPool()
                    .execute(
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
    } catch (ConnectorException | NamespaceException | SourceMalfunctionException e) {
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
    } catch (ConnectorException | SourceMalfunctionException e) {
      throw UserException.validationError(e).message("Unable to update dataset.").build(logger);
    }
  }

  public DatasetConfig getUpdatedDatasetConfig(DatasetConfig oldConfig, BatchSchema newSchema)
      throws SourceMalfunctionException {
    try (AutoCloseableLock l = readLock()) {
      checkState();
      if (getPlugin().isEmpty()) {
        throw new SourceMalfunctionException(this.name);
      }
      return getPlugin().get().createDatasetConfigFromSchema(oldConfig, newSchema);
    }
  }

  public Optional<ViewTable> getView(NamespaceKey key, final MetadataRequestOptions options)
      throws SourceMalfunctionException {
    try (AutoCloseableLock l = readLock()) {
      checkState();
      if (getPlugin().isEmpty()) {
        throw new SourceMalfunctionException(this.name);
      }
      Optional<SupportsReadingViews> viewEnabledPlugin =
          PluginUtil.getStoragePluginIfWrapperFor(this, SupportsReadingViews.class);
      if (viewEnabledPlugin.isPresent()) {
        return viewEnabledPlugin.get().getView(key.getPathComponents(), options.getSchemaConfig());
      }
    }
    return Optional.empty();
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
      return getPlugin()
          .get()
          .getDatasetHandle(entityPath, retrievalOptions.asGetDatasetOptions(datasetConfig));
    }
  }

  /**
   * Call after plugin start to register local variables.
   *
   * @param config new {@link SourceConfig} to store in this instance
   */
  private void setLocals(SourceConfig config) throws SourceMalfunctionException {
    if (getPlugin().isEmpty()) {
      return;
    }
    initPlugin(config);
    this.pluginId =
        new StoragePluginId(sourceConfig, conf, getPlugin().get().getSourceCapabilities());
  }

  /**
   * Reset the plugin locals to the state before the plugin was started.
   *
   * @param config the original source configuration, must be not-null
   * @param pluginId the id of the plugin before startup
   */
  private void resetLocals(SourceConfig config, StoragePluginId pluginId)
      throws SourceMalfunctionException {
    if (getPlugin().isEmpty()) {
      return;
    }
    initPlugin(config);
    this.pluginId = pluginId;
  }

  /**
   * Helper function to set the plugin locals.
   *
   * @param config source config, must be not null
   */
  private void initPlugin(SourceConfig config) throws SourceMalfunctionException {
    this.sourceConfig = config;
    this.metadataPolicy =
        config.getMetadataPolicy() == null
            ? CatalogService.NEVER_REFRESH_POLICY
            : config.getMetadataPolicy();

    this.conf = config.getConnectionConf(reader);

    if (getPlugin().isEmpty()) {
      throw new SourceMalfunctionException(this.name);
    }

    if (options.getOption(CatalogOptions.ENABLE_ASYNC_GET_STATE)) {
      this.state = tryGetState();
    } else {
      // Legacy behavior. It is not recommended to disable this option. Enforcing
      // a timeout on calls to getState() helps to alleviate deadlocks around source locking.
      this.state = getPlugin().get().getState();
    }
  }

  /** Update the cached state of the plugin. */
  public SourceState refreshState() {
    long timeoutSeconds = options.getOption(CatalogOptions.REFRESH_STATE_TIMEOUT_SECONDS);
    try {
      return CompletableFuture.supplyAsync(this::tryRefreshState, executor)
          .get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      final SourceState newBadState =
          SourceState.badState(
              SourceState.NOT_AVAILABLE.getSuggestedUserAction(), e.getCause().getMessage());
      logger.warn("Failed to refresh plugin's state in {}s: {}", timeoutSeconds, name, e);
      this.state = newBadState;
      return newBadState;
    }
  }

  // We are using AutoCloseableLock in try-with-resources, we're just testing it in a guard clause
  // first
  @SuppressWarnings("resource")
  private SourceState tryRefreshState() {
    try {
      Optional<AutoCloseableLock> localRefreshStateLock =
          AutoCloseableLock.of(this.refreshStateLock, true).tryOpen(0, TimeUnit.SECONDS);
      if (localRefreshStateLock.isEmpty()) {
        logger.debug(
            "Source [{}] state is not refreshed. Refresh state lock status: {}",
            name,
            this.refreshStateLock);
        return state;
      }
      try (AutoCloseableLock refL = localRefreshStateLock.get()) {
        while (true) {
          if (getPlugin().isEmpty()) {
            Optional<AutoCloseableLock> localWriteLock =
                AutoCloseableLock.of(this.writeLock, true).tryOpen(5, TimeUnit.SECONDS);
            if (localWriteLock.isEmpty()) {
              logger.debug(
                  "Source [{}] state is not refreshed. Write lock status: {}",
                  name,
                  this.writeLock.toString());
              return state;
            }

            try (AutoCloseableLock wL = localWriteLock.get()) {
              if (getPlugin().isPresent()) {
                // while waiting for write lock, someone else started things, start this loop
                // over.
                continue;
              }
              this.plugin =
                  resolveConnectionConf(conf)
                      .newPlugin(context, sourceConfig.getName(), this::getId);
              return newStartSupplier(sourceConfig, false).get();
            }
          }

          // the plugin is not null.
          Optional<AutoCloseableLock> localReadLock =
              AutoCloseableLock.of(this.readLock, true).tryOpen(1, TimeUnit.SECONDS);
          if (localReadLock.isEmpty()) {
            logger.debug(
                "Source [{}] state is not refreshed. Read lock status: {}",
                name,
                this.readLock.toString());
            return state;
          }

          try (Closeable rL = localReadLock.get()) {
            // Always already in an async thread, stay in same async thread
            final SourceState newState = tryGetState();
            this.state = newState;
            return newState;
          }
        }
      }
    } catch (Exception ex) {
      logger.debug("Failed to start plugin while trying to refresh state, error:", ex);
      if (ex.getCause() instanceof DeletedException) {
        logger.debug("[{}] is deleted. {}", name, ex.getCause().toString());
        SourceState newBadState =
            SourceState.badState(
                SourceState.DELETED.getSuggestedUserAction(), ex.getCause().getMessage());
        this.state = newBadState;
        return newBadState;
      }
      this.state = SourceState.NOT_AVAILABLE;
      return SourceState.NOT_AVAILABLE;
    }
  }

  private SourceState tryGetState() {
    // StoragePlugin.getState may not resolve quickly due to long connection timeouts,
    // limit the call time.
    final long timeoutSeconds = options.getOption(CatalogOptions.GET_STATE_TIMEOUT_SECONDS);
    try {
      return CompletableFuture.supplyAsync(() -> getPlugin().get().getState(), executor)
          .get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      logger.warn("Failed to obtain plugin's state in {}s: {}", timeoutSeconds, name, e);
      return SourceState.badState(
          SourceState.NOT_AVAILABLE.getSuggestedUserAction(), e.getCause().getMessage());
    }
  }

  /**
   * @deprecated use {@link #replacePluginWithLock(SourceConfig, long, boolean)} instead
   */
  @Deprecated
  boolean replacePluginWithLockDeprecated(
      SourceConfig config, final long waitMillis, boolean skipEqualityCheck) throws Exception {
    try (Closeable write = writeLock()) {
      return replacePluginDeprecated(config, waitMillis, skipEqualityCheck);
    }
  }

  List<SourceNameRefreshAction> replacePluginWithLock(
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
   * @deprecated use {@link #replacePlugin(SourceConfig, long, boolean)} instead
   *     <p>Replace the plugin instance with one defined by the new SourceConfig. Do the minimal
   *     changes necessary. Starts the new plugin.
   * @param config
   * @param waitMillis
   * @return Whether metadata was maintained. Metdata will be maintained if the connection did not
   *     change or was changed with only non-metadata impacting changes.
   * @throws Exception
   */
  @Deprecated
  private boolean replacePluginDeprecated(
      SourceConfig config, final long waitMillis, boolean skipEqualityCheck) throws Exception {
    Preconditions.checkState(
        writeLock.isHeldByCurrentThread(),
        "You must hold the plugin write lock before replacing plugin.");

    final ConnectionConf<?, ?> existingConnectionConf = this.conf;
    final ConnectionConf<?, ?> newConnectionConf = reader.getConnectionConf(config);
    /* if the plugin startup had failed earlier (plugin is null) and
     * we are here to replace the plugin, we should not return here.
     */
    if (!skipEqualityCheck
        && existingConnectionConf.equals(newConnectionConf)
        && getPlugin().isPresent()) {
      // we just need to update external settings.
      setLocals(config);
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
    Optional<StoragePlugin> oldPlugin = getPlugin();
    final StoragePluginId oldPluginId = pluginId;
    this.plugin =
        resolveConnectionConf(newConnectionConf)
            .newPlugin(context, sourceKey.getRoot(), this::getId, isInFluxSource);
    try {
      logger.trace("Starting new plugin for [{}]", config.getName());
      startReplacementAsync(config, skipEqualityCheck).get(waitMillis, TimeUnit.MILLISECONDS);
      try {
        if (oldPlugin.isPresent()) {
          AutoCloseables.close(oldPlugin.get());
        }
      } catch (Exception ex) {
        logger.warn("Failure while retiring old plugin [{}].", sourceKey, ex);
      }

      // if we replaced the plugin successfully, clear the permission cache
      getPermissionsCache().clear();

      return existingConnectionConf.equalsIgnoringNotMetadataImpacting(newConnectionConf);
    } catch (Exception ex) {
      // the update failed, go back to previous state.
      this.plugin = oldPlugin.orElse(null);
      try {
        resetLocals(oldConfig, oldPluginId);
      } catch (Exception e) {
        ex.addSuppressed(e);
      }
      throw ex;
    }
  }

  /**
   * Replace the plugin instance with one defined by the new SourceConfig. Do the minimal changes
   * necessary. Starts the new plugin.
   *
   * @param config
   * @param waitMillis
   * @param skipEqualityCheck
   * @return A list of {@link SourceNameRefreshAction}s should be executed to refresh names in the
   *     source.
   * @throws Exception
   */
  private List<SourceNameRefreshAction> replacePlugin(
      SourceConfig config, final long waitMillis, boolean skipEqualityCheck) throws Exception {
    Preconditions.checkState(
        writeLock.isHeldByCurrentThread(),
        "You must hold the plugin write lock before replacing plugin.");

    final ConnectionConf<?, ?> existingConnectionConf = this.conf;
    final ConnectionConf<?, ?> newConnectionConf = reader.getConnectionConf(config);
    /* if the plugin startup had failed earlier (plugin is null) and
     * we are here to replace the plugin, we should not return here.
     */
    if (!skipEqualityCheck
        && existingConnectionConf.equals(newConnectionConf)
        && getPlugin().isPresent()) {
      // we just need to update external settings.
      setLocals(config);
      return Lists.newArrayList();
    }

    /*
     * we are here if
     * (1) current plugin is null OR
     * (2) current plugin is non-null but new and existing
     *     connection configurations don't match.
     */
    this.state = SourceState.NOT_AVAILABLE;
    boolean isMetadataImpactingChange = isSourceConfigMetadataImpacting(config);

    // hold the old plugin until we successfully replace it.
    final SourceConfig oldConfig = sourceConfig;
    Optional<StoragePlugin> oldPlugin = getPlugin();
    final StoragePluginId oldPluginId = pluginId;
    this.plugin =
        resolveConnectionConf(newConnectionConf)
            .newPlugin(context, sourceKey.getRoot(), this::getId, isInFluxSource);
    try {
      logger.trace("Starting new plugin for [{}]", config.getName());
      startReplacementAsync(config, skipEqualityCheck).get(waitMillis, TimeUnit.MILLISECONDS);
      try {
        if (oldPlugin.isPresent()) {
          AutoCloseables.close(oldPlugin.get());
        }
      } catch (Exception ex) {
        logger.warn("Failure while retiring old plugin [{}].", sourceKey, ex);
      }

      // if we replaced the plugin successfully, clear the permission cache
      getPermissionsCache().clear();

      // this is a metadata impacting change
      if (isMetadataImpactingChange) {
        return Lists.newArrayList(
            SourceNameRefreshAction.newDeleteAllAction(),
            SourceNameRefreshAction.newRefreshAllAction());
      }

      // Defer to plugin ConnectionConf to determine name refresh actions
      return existingConnectionConf.getNameRefreshActionsForNewConf(name, newConnectionConf);
    } catch (Exception ex) {
      // the update failed, go back to previous state.
      this.plugin = oldPlugin.orElse(null);
      try {
        resetLocals(oldConfig, oldPluginId);
      } catch (Exception e) {
        ex.addSuppressed(e);
      }
      throw ex;
    }
  }

  boolean isSourceConfigMetadataImpacting(SourceConfig config) {
    try (AutoCloseableLock l = readLock()) {
      ConnectionConf<?, ?> existingConnectionConf = conf.clone();
      ConnectionConf<?, ?> newConnectionConf = reader.getConnectionConf(config);

      return isSourceConfigMetadataImpacting(existingConnectionConf, newConnectionConf);
    }
  }

  private boolean isSourceConfigMetadataImpacting(
      ConnectionConf existingConfig, ConnectionConf newConfig) {
    ConnectionConf<?, ?> existingConnectionConfWithoutAllowedDbs;
    ConnectionConf<?, ?> newConnectionConfWithoutAllowedDbs;

    if (options.getOption(SOURCE_SEAMLESS_UPDATE_ALLOWED_DATABASES)) {
      existingConnectionConfWithoutAllowedDbs = removeAllowedDatabases(existingConfig);
      newConnectionConfWithoutAllowedDbs = removeAllowedDatabases(newConfig);
    } else {
      existingConnectionConfWithoutAllowedDbs = existingConfig;
      newConnectionConfWithoutAllowedDbs = newConfig;
    }

    return !existingConnectionConfWithoutAllowedDbs.equalsIgnoringNotMetadataImpacting(
        newConnectionConfWithoutAllowedDbs);
  }

  // This method is temporarily needed since it has to be behind a support option.
  // Once the support option is permanently enabled and ready for removal, we can
  // simply add "@NotMetadataImpacting" annotation to "allowedDatabases" field.
  private ConnectionConf<?, ?> removeAllowedDatabases(ConnectionConf<?, ?> connectionConf) {
    ConnectionConf<?, ?> confCopy = connectionConf.clone();

    try {
      for (Field field : FieldUtils.getAllFields(confCopy.getClass())) {
        if (field.getName().equals("allowedDatabases")) {
          field.set(confCopy, Defaults.defaultValue(field.getType()));
          break;
        }
      }
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }

    return confCopy;
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
        metadataManager.close();
        if (getPlugin().isPresent()) {
          getPlugin().get().close();
        }
      } finally {
        runUnderLock.accept(this);
      }
      return true;
    }
  }

  public void deleteServiceSet() throws Exception {
    metadataManager.deleteServiceSet();
  }

  private void deleteSourceChildren(SourceConfig config, List<List<String>> prefixes)
      throws NamespaceException {

    // TODO: expensive call on non-master coordinators (sends as many RPC requests as entries that
    // we delete)
    SourceNamespaceService.DeleteCallback deleteCallback =
        (DatasetConfig datasetConfig) -> {
          CatalogUtil.addIcebergMetadataOrphan(datasetConfig, orphanage);
        };

    if (prefixes == null) {
      if (!keepStaleMetadata()) {
        systemUserNamespaceService.deleteSourceChildren(
            config.getKey(), config.getTag(), deleteCallback);
      } else {
        logger.info(
            "Old metadata data may be bad, but preserving descendants of source [{}] because '{}' is enabled",
            config.getName(),
            CatalogOptions.STORAGE_PLUGIN_KEEP_METADATA_ON_REPLACE.getOptionName());
      }
    } else {
      for (List<String> prefix : prefixes) {
        systemUserNamespaceService.deleteSourceChildIfExists(
            new NamespaceKey(prefix), null, true, deleteCallback);
      }
    }
  }

  boolean refresh(SourceUpdateType sourceUpdateType, MetadataPolicy policy) {
    checkState();
    return getSourceMetadataManager().refresh(sourceUpdateType, policy, true);
  }

  SourceMetadataManager getSourceMetadataManager() {
    return metadataManager;
  }

  @SuppressWarnings("unchecked")
  public <T extends StoragePlugin> T unwrap(Class<T> clazz) {
    return unwrap(clazz, false);
  }

  @SuppressWarnings("unchecked")
  public <T extends StoragePlugin> T unwrap(Class<T> clazz, boolean skipStateCheck) {
    try (AutoCloseableLock l = readLock()) {
      if (getPlugin().isEmpty() || !skipStateCheck) {
        checkState();
      }

      if (clazz.isAssignableFrom(getPlugin().get().getClass())) {
        return (T) getPlugin().get();
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

  static class StoragePluginChanging extends RuntimeException {
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
          return SafeRunner.this.doSafe(innerIterator::hasNext);
        }

        @Override
        public I next() {
          return doSafe(innerIterator::next);
        }
      };
    }
  }

  /**
   * A class that provides a lock protected bridge between the source metadata manager and the
   * ManagedStoragePlugin so the manager can't cause problems with plugin locking.
   */
  class MetadataBridge {
    Optional<SourceMetadata> getMetadata() {
      try (AutoCloseableLock read = tryReadLock()) {
        if (getPlugin().isPresent()) {
          return Optional.of(getPlugin().get());
        }
        return Optional.empty();
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
      ManagedStoragePlugin.this.refreshState();
    }

    SourceState getState() {
      try (AutoCloseableLock read = tryReadLock()) {
        return state;
      }
    }
  }
}

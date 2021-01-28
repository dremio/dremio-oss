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

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.VM;
import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.connector.metadata.extensions.SupportsAlteringDatasetMetadata;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.exec.catalog.CatalogInternalRPC.UpdateLastRefreshDateRequest;
import com.dremio.exec.catalog.CatalogServiceImpl.UpdateType;
import com.dremio.exec.catalog.DatasetCatalog.UpdateStatus;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.MissingPluginConf;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.Message;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.primitives.Ints;

/**
 * Manages the Dremio system state related to a StoragePlugin.
 *
 * Also owns the SourceMetadataManager, the task driver responsible for maintaining metadata
 * freshness.
 *
 * Locking model: exposes a readLock (using the inner plugin) and a writeLock (changing the inner
 * plugin). The locking model is exposed externally so that CatalogServiceImpl can get locks as
 * necessary during modifications.
 *
 */
public class ManagedStoragePlugin implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ManagedStoragePlugin.class);

  private final String name;
  private final SabotContext context;
  private final ConnectionReader reader;
  private final NamespaceKey sourceKey;
  private final Executor executor;

  /**
   * A read lock for interacting with the plugin. Should be used for most external interactions except where the methods
   * were designed to be resilient to underlying changes to avoid contention/locking needs.
   */
  private final ReentrantReadWriteLock.ReadLock readLock;

  /**
   * A write lock that must be acquired before starting, stopping or replacing the plugin.
   */
  private final ReentrantReadWriteLock.WriteLock writeLock;
  private final PermissionCheckCache permissionsCache;
  private final SourceMetadataManager metadataManager;
  private final OptionManager options;
  private final CatalogServiceMonitor monitor;
  private final NamespaceService systemUserNamespaceService;

  protected volatile SourceConfig sourceConfig;
  private volatile StoragePlugin plugin;
  private volatile MetadataPolicy metadataPolicy;
  private volatile StoragePluginId pluginId;
  private volatile ConnectionConf<?,?> conf;
  private volatile Stopwatch startup = Stopwatch.createUnstarted();
  private volatile SourceState state = SourceState.badState("Source not yet started.");
  private final Thread fixFailedThread;
  private volatile boolean closed = false;

  /**
   * Included in instance variables because it is very useful during debugging.
   */
  private final ReentrantReadWriteLock rwlock;

  public ManagedStoragePlugin(
      SabotContext context,
      Executor executor,
      boolean isMaster,
      ModifiableSchedulerService modifiableScheduler,
      NamespaceService systemUserNamespaceService,
      LegacyKVStore<NamespaceKey, SourceInternalData> sourceDataStore,
      SourceConfig sourceConfig,
      OptionManager options,
      ConnectionReader reader,
      CatalogServiceMonitor monitor,
      Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider
  ) {
    this.rwlock = new ReentrantReadWriteLock(true);
    this.executor = executor;
    this.readLock = rwlock.readLock();
    this.writeLock = rwlock.writeLock();
    this.context = context;
    this.sourceConfig = sourceConfig;
    this.sourceKey = new NamespaceKey(sourceConfig.getName());
    this.name = sourceConfig.getName();
    this.systemUserNamespaceService = systemUserNamespaceService;
    this.conf = reader.getConnectionConf(sourceConfig);
    this.plugin = conf.newPlugin(context, sourceConfig.getName(), this::getId);
    this.metadataPolicy = sourceConfig.getMetadataPolicy() == null ? CatalogService.NEVER_REFRESH_POLICY : sourceConfig.getMetadataPolicy();
    this.permissionsCache = new PermissionCheckCache(this::getPlugin, () -> getMetadataPolicy().getAuthTtlMs(), 2500);
    this.options = options;
    this.reader = reader;
    this.monitor = monitor;

    fixFailedThread = new FixFailedToStart();
    // leaks this so do last.
    this.metadataManager = new SourceMetadataManager(
        sourceKey,
        modifiableScheduler,
        isMaster,
        sourceDataStore,
        new MetadataBridge(),
        options,
        monitor,
        broadcasterProvider);
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

  private long createWaitMillis() {
    if (VM.isDebugEnabled()) {
      return TimeUnit.DAYS.toMillis(365);
    }
    return context.getOptionManager().getOption(CatalogOptions.STORAGE_PLUGIN_CREATE_MAX);
  }


  /**
   * Synchronize plugin state to the provided target source config.
   *
   * Note that this will fail if the target config is older than the existing config, in terms of creation time or
   * version.
   *
   * @param targetConfig target source config
   * @return managed storage plugin
   * @throws Exception if synchronization fails
   */
  void synchronizeSource(final SourceConfig targetConfig) throws Exception {

    if(matches(targetConfig)) {
      logger.trace("Source [{}] already up-to-date, not synchronizing", targetConfig.getName());
      return;
    }

    // do this in under the write lock.
    try (Closeable write = writeLock()) {

      // check again under write lock to make sure things didn't already synchronize.
      if(matches(targetConfig)) {
        logger.trace("Source [{}] already up-to-date, not synchronizing", targetConfig.getName());
        return;
      }

      // else, bring the plugin up to date, if possible
      final long creationTime = sourceConfig.getCtime();
      final long targetCreationTime = targetConfig.getCtime();

      if (creationTime > targetCreationTime) {
        // in-memory source config is newer than the target config, in terms of creation time
        throw new ConcurrentModificationException(String.format(
            "Source [%s] was updated, and the given configuration has older ctime (current: %d, given: %d)",
            targetConfig.getName(), creationTime, targetCreationTime));

      } else if (creationTime == targetCreationTime) {
        final SourceConfig currentConfig = sourceConfig;

        compareConfigs(currentConfig, targetConfig);

        final int compareTo = SOURCE_CONFIG_COMPARATOR.compare(currentConfig, targetConfig);

        if (compareTo > 0) {
          // in-memory source config is newer than the target config, in terms of version
          throw new ConcurrentModificationException(String.format(
            "Source [%s] was updated, and the given configuration has older version (current: %d, given: %d)",
            currentConfig.getName(), currentConfig.getConfigOrdinal(), targetConfig.getConfigOrdinal()));
        }

        if (compareTo == 0) {
          // in-memory source config has a different version but the same value
          throw new IllegalStateException(String.format(
            "Current and given configurations for source [%s] have same version (%d) but different values" +
              " [current source: %s, given source: %s]",
            currentConfig.getName(), currentConfig.getConfigOrdinal(),
            reader.toStringWithoutSecrets(currentConfig),
            reader.toStringWithoutSecrets(targetConfig)));
        }
      }
      // else (creationTime < targetCreationTime), the source config is new but plugins has an entry with the same
      // name, so replace the plugin regardless of the checks

      // in-memory storage plugin is older than the one persisted, update
      replacePlugin(targetConfig, createWaitMillis(), false);
      return;
    }
  }

  private void addDefaults(SourceConfig config) {
    if(config.getCtime() == null) {
      config.setCtime(System.currentTimeMillis());
    }

    if(config.getMetadataPolicy() == null) {
      config.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    }
  }

  private void updateConfig(NamespaceService userNamespace, SourceConfig config, NamespaceAttribute...attributes) throws ConcurrentModificationException, NamespaceException {
    if (logger.isTraceEnabled()) {
      logger.trace("Adding or updating source [{}].", config.getName(), new RuntimeException("Nothing wrong, just show stack trace for debug."));
    } else {
      logger.debug("Adding or updating source [{}].", config.getName());
    }

    try {
      SourceConfig existingConfig = userNamespace.getSource(config.getKey());

      // add back any secrets
      final ConnectionConf<?, ?> connectionConf = config.getConnectionConf(reader);
      connectionConf.applySecretsFrom(reader.getConnectionConf(existingConfig));
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

  void createSource(SourceConfig config, String userName, NamespaceAttribute... attributes) throws TimeoutException, Exception {
    createOrUpdateSource(true, config, userName, attributes);
  }

  void updateSource(SourceConfig config, String userName, NamespaceAttribute... attributes) throws ConcurrentModificationException, NamespaceException {
    createOrUpdateSource(false, config, userName, attributes);
  }

  private void createOrUpdateSource(final boolean create, SourceConfig config, String userName, NamespaceAttribute... attributes) throws ConcurrentModificationException, NamespaceException {
    final NamespaceService userNamespace = context.getNamespaceService(userName);

    if (logger.isTraceEnabled()) {
      logger.trace("{} source [{}].", create ? "Creating" : "Updating", config.getName(), new RuntimeException("Nothing wrong, just show stack trace for debug."));
    } else if (logger.isDebugEnabled()) {
      logger.debug("{} source [{}].", create ? "Creating" : "Updating", config.getName());
    }

    addDefaults(config);

    if(!create) {
      updateConfig(userNamespace, config, attributes);
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
            logger.debug("Old metadata data may be bad; deleting all descendants of source [{}]", config.getName());
            // TODO: expensive call on non-master coordinators (sends as many RPC requests as entries under the source)
            systemUserNamespaceService.deleteSourceChildren(config.getKey(), config.getTag());
          } else {
            logger.info("Old metadata data may be bad, but preserving descendants of source [{}] because '{}' is enabled",
                config.getName(), CatalogOptions.STORAGE_PLUGIN_KEEP_METADATA_ON_REPLACE.getOptionName());
          }
        }

        // Now let's create the plugin in the system namespace.
        // This increments the version in the config object as well.
        userNamespace.addOrUpdateSource(config.getKey(), config, attributes);
      }

      // ***** Complete Local Update **** //

      // now that we're outside the plugin lock, we should refresh the dataset names. Note that this could possibly get
      // run on a different config if two source updates are racing to this lock.
      if (refreshDatasetNames) {
        if (!keepStaleMetadata()) {
          logger.debug("Refreshing names in source [{}]", config.getName());
          refresh(UpdateType.NAMES, null);
        } else {
          logger.debug("Not refreshing names in source [{}]", config.getName());
        }
      }

      // once we have potentially done initial refresh, we can run subsequent refreshes. This allows
      // us to avoid two separate threads refreshing simultaneously (possible if we put this above
      // the inline plugin.refresh() call.)
      stopwatchForPlugin.stop();
      logger.debug("Source added [{}], took {} milliseconds", config.getName(), stopwatchForPlugin.elapsed(TimeUnit.MILLISECONDS));

    } catch (ConcurrentModificationException ex) {
      throw UserException.concurrentModificationError(ex).message(
          "Source update failed due to a concurrent update. Please try again [%s].", config.getName()).build(logger);
    } catch (Exception ex) {
      String suggestedUserAction = getState().getSuggestedUserAction();
      if (suggestedUserAction == null || suggestedUserAction.isEmpty()) {
        // If no user action was suggested, fall back to a basic message.
        suggestedUserAction = String.format("Failure creating/updating this source [%s].", config.getName());
      }
      throw UserException.validationError(ex)
        .message(suggestedUserAction)
        .build(logger);
    }
  }

  private boolean keepStaleMetadata() {
    return context.getOptionManager().getOption(CatalogOptions.STORAGE_PLUGIN_KEEP_METADATA_ON_REPLACE);
  }

  private void compareConfigs(SourceConfig existing, SourceConfig config) {
    if (Objects.equals(existing.getTag(), config.getTag())) {
      // in-memory source config has a different value but the same etag
      throw new IllegalStateException(String.format(
        "Current and given configurations for source [%s] have same etag (%s) but different values" +
          " [current source: %s, given source: %s]",
        existing.getName(), existing.getTag(),
        reader.toStringWithoutSecrets(existing),
        reader.toStringWithoutSecrets(config)));
    }
  }

  DatasetSaver getSaver() {
    // note, this is a protected saver so no one will be able to save a dataset if the source is currently going through editing changes (write lock held).
    return metadataManager.getSaver();
  }

  /**
   * Return clone of the sourceConfig
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
    // not read under a read lock since it is updated via volatile. Allows us to avoid locking in read paths.
    return state;
  }

  public boolean matches(SourceConfig config) {
    try(AutoCloseableLock readLock = readLock()){
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
    // not read under a read lock since it is updated via volatile. Allows us to avoid locking in read paths.
    return conf;
  }

  /**
   * Gets dataset retrieval options as defined on the source.
   *
   * @return dataset retrieval options defined on the source
   */
  DatasetRetrievalOptions getDefaultRetrievalOptions() {
    // not read under a read lock since it is updated via volatile. Allows us to avoid locking in read paths.
    return DatasetRetrievalOptions.fromMetadataPolicy(metadataPolicy)
        .toBuilder()
        .setMaxMetadataLeafColumns(getMaxMetadataColumns())
        .setMaxNestedLevel(getMaxNestedLevel())
        .build()
        .withFallback(DatasetRetrievalOptions.DEFAULT);
  }

  public StoragePluginRulesFactory getRulesFactory() throws InstantiationException, IllegalAccessException {
    // not read under a read lock since it is updated via volatile. Allows us to avoid locking in read paths. This is
    // especially important here since this method is used even if the query does not refer to this source.

    // grab local to avoid changes under us later.
    final StoragePlugin plugin = this.plugin;
    if (plugin == null) {
      return null;
    }

    if(plugin.getRulesFactoryClass() != null) {
      return plugin.getRulesFactoryClass().newInstance();
    }

    return null;
  }

  /**
   * Start this plugin asynchronously

   * @return A future that returns the state of this plugin once started (or throws Exception if the startup failed).
   */
  CompletableFuture<SourceState> startAsync() {
    return startAsync(sourceConfig, true);
  }

  /**
   * Generate a supplier that produces source state
   * @param config
   * @param closeMetaDataManager - During dremio startup, we don't close metadataManager when sources are in bad state,
   *                             because we need state refresh for bad sources.
   *                             When a user tries to add a source and it's in bad state, we close metadataManager to avoid
   *                             wasting additional space.
   * @return
   */
  private Supplier<SourceState> newStartSupplier(SourceConfig config, final boolean closeMetaDataManager) {
    try {
      return nameSupplier("start-" + sourceConfig.getName(), () -> {
        try {
          startup = Stopwatch.createStarted();
          logger.debug("Starting: {}", sourceConfig.getName());
          plugin.start();
          setLocals(config);
          startup.stop();
          if (state.getStatus() == SourceStatus.bad) {
            // Check the state here and throw exception so that we close the partially started plugin properly in the
            // exception handling code
            throw new Exception(state.toString());
          }

          return state;
        } catch(Throwable e) {
          if (config.getType() != MissingPluginConf.TYPE) {
            logger.warn("Error starting new source: {}", sourceConfig.getName(), e);
          }
          state = SourceState.badState(e.getMessage());

          try {
            // failed to startup, make sure to close.
            if (closeMetaDataManager) {
              AutoCloseables.close(metadataManager, plugin);
            } else {
              plugin.close();
            }
            plugin = null;
          } catch (Exception ex) {
            e.addSuppressed(new RuntimeException("Cleanup exception after initial failure.", ex));
          }

          throw new CompletionException(e);
        }}
      );
    } catch (Exception ex) {
      return () -> {throw new CompletionException(ex);};
    }
  }

  /**
   * Start this plugin asynchronously
   *
   * @param config The configuration to use for this startup.
   * @return A future that returns the state of this plugin once started (or throws Exception if the startup failed).
   */
  private CompletableFuture<SourceState> startAsync(final SourceConfig config, final boolean isDuringStartUp) {
    // we run this in a separate thread to allow early timeout. This doesn't use the scheduler since that is
    // bound and we're frequently holding a lock when running this.
    return CompletableFuture.supplyAsync(newStartSupplier(config, !isDuringStartUp), executor);
  }

  /**
   * If starting a plugin on process restart failed, this method will spawn a background task that will keep trying
   * to re-start the plugin on a fixed schedule (minimum of the metadata name and dataset refresh rates)
   */
  public void initiateFixFailedStartTask() {
    fixFailedThread.start();
  }

  /**
   * Ensures a supplier names the thread it is run on.
   * @param name Name to use for thread.
   * @param delegate Delegate supplier.
   * @return
   */
  private <T> Supplier<T> nameSupplier(String name, Supplier<T> delegate){
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
  public boolean alterDataset(final NamespaceKey key, final DatasetConfig datasetConfig, final Map<String, AttributeValue> attributes) {
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

    boolean changed;
    try (AutoCloseableLock l = readLock()) {
      final DatasetMetadata oldDatasetMetadata = new DatasetMetadataAdapter(datasetConfig);
      final DatasetMetadata newDatasetMetadata = ((SupportsAlteringDatasetMetadata) plugin).alterMetadata(handle.get(),
          oldDatasetMetadata, attributes);

      if (oldDatasetMetadata == newDatasetMetadata) {
        changed = false;
      } else {
        Preconditions.checkState(newDatasetMetadata.getDatasetStats().getRecordCount() >= 0,
          "Record count should already be filled in when altering dataset metadata.");
        MetadataObjectsUtils.overrideExtended(datasetConfig, newDatasetMetadata, Optional.empty(),
                newDatasetMetadata.getDatasetStats().getRecordCount(), getMaxMetadataColumns());
        //systemUserNamespaceService.addOrUpdateDataset(key, datasetConfig);
        // Force a full refresh
        saveDatasetAndMetadataInNamespace(datasetConfig, handle.get(), retrievalOptions.toBuilder().setForceUpdate(true).build());
        changed = true;
      }
    } catch (ConnectorException e) {
      throw UserException.validationError(e)
                         .buildSilently();
    }
    return changed;
  }

  private class FixFailedToStart extends Thread {

    public FixFailedToStart() {
      super("fix-fail-to-start-" + sourceKey.getRoot());
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        while(true) {
          final long nextRefresh = Math.min(metadataPolicy.getNamesRefreshMs(), metadataPolicy.getDatasetDefinitionRefreshAfterMs());
          Thread.sleep(nextRefresh);

          // something started the plugin successfully.
          if (state.getStatus() != SourceState.SourceStatus.bad) {
            return;
          }

          try {
            refreshState().get();
            if (state.getStatus() != SourceState.SourceStatus.bad) {
              return;
            }
          } catch (Exception e) {
            // Failure to refresh state means that we should just reschedule the next fix
          }
        }
      } catch (InterruptedException e1) {
        logger.info("Discontinuing attempts to start failing plugin {}.", name, e1);
        return;
      }
    }

    @Override
    public String toString() {
      return "fix-fail-to-start-" + sourceKey.getRoot();
    }
  }

  /**
   * Before doing any operation associated with plugin, we should check the state of the plugin.
   */
  protected void checkState() {
    try(AutoCloseableLock l = readLock()) {
      SourceState state = this.state;
      if(state.getStatus() == SourceState.SourceStatus.bad) {
        final String msg = state.getMessages().stream()
          .map(m -> m.getMessage())
          .collect(Collectors.joining(", "));

        UserException.Builder builder = UserException.sourceInBadState()
          .message("The source [%s] is currently unavailable. Info: [%s]", sourceKey, msg);

        for(Message message : state.getMessages()) {
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

  private static boolean isComplete(DatasetConfig config) {
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

  public void checkAccess(NamespaceKey key, DatasetConfig datasetConfig, String userName, final MetadataRequestOptions options) {
    try(AutoCloseableLock l = readLock()) {
      checkState();
      if (!getPermissionsCache().hasAccess(userName, key, datasetConfig, options.getStatsCollector(), sourceConfig)) {
        throw UserException.permissionError()
          .message("Access denied reading dataset %s.", key)
          .build(logger);
      }
    }
  }

  /**
   * Checks if the given metadata is complete and meets the given validity constraints.
   *
   * @param datasetConfig dataset metadata
   * @param requestOptions request options
   * @return true iff the metadata is complete and meets validity constraints
   */
  public boolean isCompleteAndValid(DatasetConfig datasetConfig, MetadataRequestOptions requestOptions) {
    try(AutoCloseableLock l = readLock()) {
      checkState();
      return isComplete(datasetConfig) &&
          (!requestOptions.checkValidity() || metadataManager.isStillValid(requestOptions, datasetConfig));
    }
  }

  public UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions) {
    try(AutoCloseableLock l = readLock()) {
      checkState();
      return metadataManager.refreshDataset(key, retrievalOptions);
    } catch (StoragePluginChanging e) {
      throw UserException.validationError(e).message("Storage plugin was changing during refresh attempt.").build(logger);
    } catch (ConnectorException | NamespaceException e) {
      throw UserException.validationError(e).message("Unable to refresh dataset.").build(logger);
    }
  }

  public void saveDatasetAndMetadataInNamespace(DatasetConfig datasetConfig,
                                                DatasetHandle datasetHandle,
                                                DatasetRetrievalOptions retrievalOptions
  ) {
    try (AutoCloseableLock l = readLock()) {
      checkState();
      metadataManager.saveDatasetAndMetadataInNamespace(datasetConfig, datasetHandle, retrievalOptions);
    } catch (StoragePluginChanging e) {
      throw UserException.validationError(e)
        .message("Storage plugin was changing during dataset update attempt.")
        .build(logger);
    } catch (ConnectorException e) {
      throw UserException.validationError(e)
        .message("Unable to update dataset.")
        .build(logger);
    }
  }

  public DatasetConfig getUpdatedDatasetConfig(DatasetConfig oldConfig, BatchSchema newSchema) {
    try(AutoCloseableLock l = readLock()) {
      checkState();
      return plugin.createDatasetConfigFromSchema(oldConfig, newSchema);
    }
  }

  public ViewTable getView(NamespaceKey key, final MetadataRequestOptions options) {
    try(AutoCloseableLock l = readLock()) {
      checkState();
      // TODO: move views to namespace and out of filesystem.
      return plugin.getView(key.getPathComponents(), options.getSchemaConfig());
    }
  }

  public Optional<DatasetHandle> getDatasetHandle(
      NamespaceKey key,
      DatasetConfig datasetConfig,
      DatasetRetrievalOptions retrievalOptions
  ) throws ConnectorException {
    try (AutoCloseableLock ignored = readLock()) {
      checkState();
      final EntityPath entityPath;
      if(datasetConfig != null) {
        entityPath = new EntityPath(datasetConfig.getFullPathList());
      } else {
        entityPath = MetadataObjectsUtils.toEntityPath(key);
      }
      return plugin.getDatasetHandle(entityPath,
          retrievalOptions.asGetDatasetOptions(datasetConfig));
    }
  }

  /**
   * Call after plugin start to register local variables.
   * @param config
   */
  private void setLocals(SourceConfig config) {
    if (plugin == null) {
      return;
    }
    this.sourceConfig = config;
    this.metadataPolicy = config.getMetadataPolicy() == null? CatalogService.NEVER_REFRESH_POLICY : config.getMetadataPolicy();
    this.state = plugin.getState();
    this.conf = config.getConnectionConf(reader);
    this.pluginId = new StoragePluginId(sourceConfig, conf, plugin.getSourceCapabilities());
  }

  /**
   * Update the cached state of the plugin.
   *
   * Note that if this is
   */
  public CompletableFuture<SourceState> refreshState() throws Exception {
    return CompletableFuture
    .supplyAsync(() -> {
      try {
        while(true) {
          if(plugin == null) {
            Optional<AutoCloseableLock> writeLock = AutoCloseableLock.of(this.writeLock, true).tryOpen(5, TimeUnit.SECONDS);
            if(!writeLock.isPresent()) {
              // we failed to get the write lock, return current state;
              return state;
            }

            // we have the write lock.
            try(AutoCloseableLock l = writeLock.get()) {
              if(plugin != null) {
                // while waiting for write lock, someone else started things, start this loop over.
                continue;
              }
              plugin = conf.newPlugin(context, sourceConfig.getName(), this::getId);
              return newStartSupplier(sourceConfig, false).get();
            }
          }

          // the plugin is not null.
          Optional<AutoCloseableLock> readLock = AutoCloseableLock.of(this.readLock, true).tryOpen(1, TimeUnit.SECONDS);
          if(!readLock.isPresent()) {
            return state;
          }

          try (Closeable a = readLock.get()) {
            final SourceState state = plugin.getState();
            this.state = state;
            return state;
          }
        }
      } catch(Exception ex) {
        logger.debug("Failed to start plugin while trying to refresh state, error:", ex);
        this.state = SourceState.NOT_AVAILABLE;
        return SourceState.NOT_AVAILABLE;
      }
    }, executor);
  }

  boolean replacePluginWithLock(SourceConfig config, final long waitMillis, boolean skipEqualityCheck) throws Exception {
    try(Closeable write = writeLock()) {
      return replacePlugin(config, waitMillis, skipEqualityCheck);
    }
  }

  /**
   * Replace the plugin instance with one defined by the new SourceConfig. Do the minimal
   * changes necessary. Starts the new plugin.
   *
   * @param config
   * @param waitMillis
   * @return Whether metadata was maintained. Metdata will be maintained if the connection did not
   *         change or was changed with only non-metadata impacting changes.
   * @throws Exception
   */
  private boolean replacePlugin(SourceConfig config, final long waitMillis, boolean skipEqualityCheck) throws Exception {
    Preconditions.checkState(writeLock.isHeldByCurrentThread(), "You must hold the plugin write lock before replacing plugin.");

    final ConnectionConf<?, ?> existingConnectionConf = this.conf;
    final ConnectionConf<?, ?> newConnectionConf = reader.getConnectionConf(config);
    /* if the plugin startup had failed earlier (plugin is null) and
     * we are here to replace the plugin, we should not return here.
     */
    if(!skipEqualityCheck && existingConnectionConf.equals(newConnectionConf) && plugin != null) {
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
    final StoragePlugin oldPlugin = plugin;
    this.plugin = newConnectionConf.newPlugin(context, sourceKey.getRoot(), this::getId);
    try {
      logger.trace("Starting new plugin for [{}]", config.getName());
      startAsync(config, false).get(waitMillis, TimeUnit.MILLISECONDS);
      try {
        AutoCloseables.close(oldPlugin);
      }catch(Exception ex) {
        logger.warn("Failure while retiring old plugin [{}].", sourceKey, ex);
      }

      // if we replaced the plugin successfully, clear the permission cache
      getPermissionsCache().clear();

      return existingConnectionConf.equalsIgnoringNotMetadataImpacting(newConnectionConf);
    } catch(Exception ex) {
      // the update failed, go back to previous state.
      this.plugin = oldPlugin;
      try {
        setLocals(oldConfig);
      } catch (Exception e) {
        ex.addSuppressed(e);
      }
      throw ex;
    }
  }

  boolean isSourceConfigMetadataImpacting(SourceConfig config) {
    final ConnectionConf<?, ?> newConnectionConf = reader.getConnectionConf(config);
    try(AutoCloseableLock l = readLock()) {
      return !conf.equalsIgnoringNotMetadataImpacting(newConnectionConf);
    }
  }

  @Override
  public void close() throws Exception {
    close(null, c -> {});
  }

  /**
   * Close this storage plugin if it matches the provided configuration
   * @param config
   * @return
   * @throws Exception
   */
  public boolean close(SourceConfig config, Consumer<ManagedStoragePlugin> runUnderLock) throws Exception {
    try(AutoCloseableLock l = writeLock()) {

      // it's possible that the delete is newer than the current version of the plugin. If versions inconsistent,
      // synchronize before attempting to match.
      if(config != null) {
        if(!config.getTag().equals(sourceConfig.getTag())) {
          try {
            synchronizeSource(config);
          } catch (Exception ex) {
            logger.debug("Synchronization of source failed while attempting to delete.", ex);
          }
        }

        if(!matches(config)) {
          return false;
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

  boolean refresh(UpdateType updateType, MetadataPolicy policy) throws NamespaceException {
    checkState();
    return metadataManager.refresh(updateType, policy, true);
  }

  @SuppressWarnings("unchecked")
  public <T extends StoragePlugin> T unwrap(Class<T> clazz) {
    try(AutoCloseableLock l = readLock()) {
      checkState();
      if(clazz.isAssignableFrom(plugin.getClass())) {
        return (T) plugin;
      }
    }
    return null;
  }

  private static final Comparator<SourceConfig> SOURCE_CONFIG_COMPARATOR = (s1, s2) -> {
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
    if(closed) {
      throw new StoragePluginChanging(name + ": Plugin was closed.");
    }

    boolean locked = readLock.tryLock();
    if(!locked) {
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
   * Ensures that all namespace operations are under a read lock to avoid issues where a plugin starts changing and then
   * we write to the namespace. If a safe run operation can't aquire the read lock, we will throw a
   * StoragePluginChanging exception. It will also throw if it was created against an older version of the plugin.
   *
   * This runner is snapshot based. When it is initialized, it will record the current tag of the source configuration.
   * If the tag changes, it will disallow future operations, even if the lock becomes available. This ensures that if
   * there are weird timing where an edit happens fast enough such that a metadata refresh doesn't naturally hit the
   * safe runner, it will still not be able to do any metastore modifications (just in case the plugin change required a
   * catalog deletion).
   */
  class SafeRunner {

    private final String configTag = sourceConfig.getTag();

    private AutoCloseableLock tryReadLock() {
      // make sure we don't expose the read lock for a now out of date SafeRunner.
      if(!Objects.equals(sourceConfig.getTag(), configTag)) {
        throw new StoragePluginChanging(name + ": Plugin tag has changed since refresh started.");
      }
      return ManagedStoragePlugin.this.tryReadLock();

    }

    public <EX extends Throwable> void doSafe(RunnableWithEX<EX> runnable) throws EX {
      try(AutoCloseableLock read = tryReadLock()) {
        runnable.run();
      }
    }

    public <T, EX extends Throwable> T doSafe(SupplierWithEX<T, EX> supplier) throws EX {
      try(AutoCloseableLock read = tryReadLock()) {
        return supplier.get();
      }
    }

    public <I, T extends Iterable<I>, EX extends Throwable> Iterable<I> doSafeIterable(SupplierWithEX<T, EX> supplier) throws EX {
      try(AutoCloseableLock read = tryReadLock()) {
        final Iterable<I> innerIterable = supplier.get();
        return () -> wrapIterator(innerIterable.iterator());
      }
    }

    public <I, EX extends Throwable> Iterator<I> wrapIterator(final Iterator<I> innerIterator) throws EX {
      return new Iterator<I>() {
        @Override
        public boolean hasNext() {
          return SafeRunner.this.doSafe(() -> {return innerIterator.hasNext();});
        }

        @Override
        public I next() {
          return doSafe(() -> innerIterator.next());
        }};
    }
  }

  /**
   * A class that provides a lock protected bridge between the source metadata manager and the ManagedStoragePlugin so
   * the manager can't cause problems with plugin locking.
   */
  class MetadataBridge {

    // since refreshes coming from the metadata manager could back up if the refresh takes a long time, create a lock so
    // only one is actually pending at any point in time.
    private final Lock refreshStateLock = new ReentrantLock();

    SourceMetadata getMetadata() {
      try(AutoCloseableLock read = tryReadLock()) {
        if(plugin == null) {
          return null;
        }

        // note that we don't protect the methods inside this. This could possibly cause a weird metadata exception if
        // the source is changing while a dataset is being refreshed. This was previously possible with an inline
        // refresh but not a background refresh. With this pattern, it can also happen with a background refresh.
        return plugin;
      }
    }

    DatasetRetrievalOptions getDefaultRetrievalOptions() {
      try(AutoCloseableLock read = tryReadLock()) {
        return ManagedStoragePlugin.this.getDefaultRetrievalOptions();
      }
    }

    public NamespaceService getNamespaceService() {
      try(AutoCloseableLock read = tryReadLock()) {
        return new SafeNamespaceService(systemUserNamespaceService, new SafeRunner());
      }
    }

    public MetadataPolicy getMetadataPolicy() {
      try(AutoCloseableLock read = tryReadLock()) {
        return metadataPolicy;
      }
    }

    int getMaxMetadataColumns() {
      try(AutoCloseableLock read = tryReadLock()) {
        return Ints.saturatedCast(options.getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
      }
    }

    int getMaxNestedLevels() {
      try(AutoCloseableLock read = tryReadLock()) {
        return Ints.saturatedCast(options.getOption(CatalogOptions.MAX_NESTED_LEVELS));
      }
    }

    public void refreshState() throws Exception {
      Optional<AutoCloseableLock> refreshLock = AutoCloseableLock.of(refreshStateLock, true).tryOpen(0, TimeUnit.SECONDS);
      try {
        CompletableFuture<SourceState> refreshState;
        if (!refreshLock.isPresent()) {
          // don't refresh the state multiple times through MetadataBridge. All calls that are secondary should be skipped.
          refreshState = CompletableFuture.completedFuture(state);
        } else {
          try (AutoCloseableLock read = tryReadLock()) {
            refreshState = ManagedStoragePlugin.this.refreshState();
          }
        }

        refreshState.get(30, TimeUnit.SECONDS);
      } finally {
        if (refreshLock.isPresent()) {
          refreshLock.get().close();
        }
      }
    }

    SourceState getState() {
      try(AutoCloseableLock read = tryReadLock()) {
        return state;
      }
    }

  }

}

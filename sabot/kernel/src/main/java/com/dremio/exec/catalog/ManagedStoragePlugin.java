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
package com.dremio.exec.catalog;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.concurrent.Runnables;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.datastore.KVStore;
import com.dremio.exec.catalog.Catalog.UpdateStatus;
import com.dremio.exec.catalog.PluginsManager.IdProvider;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.CatalogService.UpdateType;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.Message;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.ScheduleUtils;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Stopwatch;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

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

  private final SabotContext context;
  private final ConnectionReader reader;
  private final SchedulerService scheduler;
  private final NamespaceKey sourceKey;
  private final Lock readLock;
  private final Lock writeLock;
  private final PermissionCheckCache permissionsCache;
  private final SourceMetadataManager metadataManager;
  private final OptionManager options;

  private volatile SourceConfig sourceConfig;
  private volatile StoragePlugin plugin;
  private volatile MetadataPolicy metadataPolicy;
  private volatile StoragePluginId pluginId;
  private volatile ConnectionConf<?,?> conf;
  private volatile Stopwatch startup = Stopwatch.createUnstarted();
  private volatile SourceState state = SourceState.badState("Source not yet started.");
  private Runnable fixFailedStartTask = null;
  private Cancellable fixFailedStartScheduled = null;
  private Object fixLock = new Object();

  public ManagedStoragePlugin(
      SabotContext context,
      boolean isMaster,
      SchedulerService scheduler,
      NamespaceService systemUserNamespaceService,
      KVStore<NamespaceKey, SourceInternalData> sourceDataStore,
      SourceConfig sourceConfig,
      ConnectionConf<?,?> conf,
      StoragePlugin plugin,
      OptionManager options,
      ConnectionReader reader) {
    ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
    readLock = rwlock.readLock();
    writeLock = rwlock.writeLock();

    this.context = context;
    this.scheduler = scheduler;
    this.sourceKey = new NamespaceKey(sourceConfig.getName());
    this.sourceConfig = sourceConfig;
    this.conf = conf;
    this.metadataPolicy = sourceConfig.getMetadataPolicy() == null ? CatalogService.NEVER_REFRESH_POLICY : sourceConfig.getMetadataPolicy();
    final Provider<Long> authTtlProvider = () -> ManagedStoragePlugin.this.metadataPolicy.getAuthTtlMs();
    this.permissionsCache = new PermissionCheckCache(new StoragePluginProvider(), authTtlProvider, 2500);
    this.plugin = plugin;
    this.options = options;
    this.reader = reader;

    // leaks this so do last.
    this.metadataManager = new SourceMetadataManager(scheduler, isMaster, systemUserNamespaceService, sourceDataStore, this, options);
  }

  private class StoragePluginProvider implements Provider<StoragePlugin> {

    @Override
    public StoragePlugin get() {
      return plugin;
    }

  }

  @SuppressWarnings("resource")
  AutoCloseableLock readLock() {
    return new AutoCloseableLock(readLock).open();
  }

  @SuppressWarnings("resource")
  AutoCloseableLock writeLock() {
    return new AutoCloseableLock(writeLock).open();
  }

  DatasetSaver getSaver() {
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

  public void initiateMetadataRefresh() {
    metadataManager.scheduleMetadataRefresh();
  }

  public MetadataPolicy getMetadataPolicy() {
    return metadataPolicy;
  }

  public NamespaceKey getName() {
    return sourceKey;
  }

  public SourceState getState() {
    return state;
  }

  public boolean matches(SourceConfig config) {
    try(AutoCloseableLock readLock = readLock()){
      return sourceConfig.equals(config);
    }
  }

  int getMaxMetadataColumns() {
    return Ints.saturatedCast(options.getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
  }

  public ConnectionConf<?, ?> getConnectionConf() {
    return conf;
  }

  /**
   * Gets dataset retrieval options as defined on the source.
   *
   * @return dataset retrieval options defined on the source
   */
  DatasetRetrievalOptions getDefaultRetrievalOptions() {
    return DatasetRetrievalOptions.fromMetadataPolicy(metadataPolicy)
        .toBuilder()
        .setMaxMetadataLeafColumns(getMaxMetadataColumns())
        .build()
        .withFallback(DatasetRetrievalOptions.DEFAULT);
  }

  public StoragePluginRulesFactory getRulesFactory() throws InstantiationException, IllegalAccessException {
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
  CheckedFuture<SourceState, Exception> startAsync() {
    return startAsync(sourceConfig);
  }

  /**
   * Start this plugin asynchronously
   *
   * @param config The configuration to use for this startup.
   * @return A future that returns the state of this plugin once started (or throws Exception if the startup failed).
   */
  private CheckedFuture<SourceState, Exception> startAsync(final SourceConfig config) {
    final SettableFuture<SourceState> future = SettableFuture.create();

    // we run this in a separate thread to allow early timeout. This doesn't scheduler since that is
    // bound and we're frequently holding a lock when running this.
    Runnables.executeInSeparateThread(Runnables.combo(new Runnable() {
      @Override
      public String toString() {
        return "Plugin Startup: " + sourceKey.getRoot();
      }

      @Override
      public void run() {
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

          future.set(state);
        } catch(Throwable e) {
          state = SourceState.badState(e.getMessage());

          try {
            // failed to startup, make sure to close.
            plugin.close();
            plugin = null;
          } catch (Exception ex) {
            e.addSuppressed(new RuntimeException("Cleanup exception after initial failure.", ex));
          }
          future.setException(e);
        }
      }}));

    return Futures.makeChecked(future, Functions.<Exception>identity());
  }

  /**
   * If starting a plugin on process restart failed, this method will spawn a background task that will keep trying
   * to re-start the plugin on a fixed schedule (minimum of the metadata name and dataset refresh rates)
   */
  public void initiateFixFailedStartTask() {
    // Schedule a new fix-at-failed-start task only for the first failure. The task will reschedule itself
    // on subsequent failures
    synchronized (fixLock) {
      if (fixFailedStartTask == null) {
        FixFailedToStart task = new FixFailedToStart();
        final long nextRefresh = task.getNextScheduledTimeMs();
        fixFailedStartTask = Runnables.combo(task);
        fixFailedStartScheduled = scheduler.schedule(ScheduleUtils.scheduleForRunningOnceAt(Instant.ofEpochMilli(nextRefresh)),
          fixFailedStartTask);
      }
    }
  }

  private class FixFailedToStart implements Runnable {
    /**
     * @return The next time we should try fixing a plugin that failed to start
     */
    long getNextScheduledTimeMs() {
      final long currentTimeMs = System.currentTimeMillis();
      final long minRefreshMs = Math.min(metadataPolicy.getNamesRefreshMs(), metadataPolicy.getDatasetDefinitionRefreshAfterMs());
      return currentTimeMs + Math.max(0L, minRefreshMs);
    }

    @Override
    public void run() {
      try {
        refreshState();
        initiateMetadataRefresh();
      } catch (Exception e) {
        // Failure to refresh state means that we should just reschedule the next fix
      }
      synchronized(fixLock) {
        if (state.getStatus() == SourceState.SourceStatus.bad) {
          // Failed to start, again. Schedule the next retry
          final long nextRefresh = getNextScheduledTimeMs();
          fixFailedStartScheduled = scheduler.schedule(ScheduleUtils.scheduleForRunningOnceAt(Instant.ofEpochMilli(nextRefresh)),
            fixFailedStartTask);
        } else {
          // Successful start
          fixFailedStartTask = null;
          fixFailedStartScheduled = null;
        }
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
  private void checkState() {
    if(!options.getOption(CatalogOptions.STORAGE_PLUGIN_CHECK_STATE)) {
      return;
    }

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

        throw builder.build(logger);
      }
    }
  }

  public StoragePluginId getId() {
    checkState();
    return pluginId;
  }

  @VisibleForTesting
  long getLastFullRefreshDateMs() {
    return metadataManager.getLastFullRefreshDateMs();
  }

  private boolean isComplete(DatasetConfig config) {
    return config != null
        && DatasetHelper.getSchemaBytes(config) != null
        && config.getReadDefinition() != null;
  }

  public static enum MetadataAccessType {
    CACHED_METADATA,
    PARTIAL_METADATA,
    SOURCE_METADATA
  }

  public void checkAccess(NamespaceKey key, DatasetConfig datasetConfig, final MetadataRequestOptions options) {
    try(AutoCloseableLock l = readLock()) {
      checkState();
      if (!permissionsCache.hasAccess(options.getSchemaConfig().getUserName(), key, datasetConfig, options.getStatsCollector())) {
        throw UserException.permissionError()
          .message("Access denied reading dataset %s.", key)
          .build(logger);
      }
    }
  }

  public boolean isValid(DatasetConfig datasetConfig, final MetadataRequestOptions options) {
    try(AutoCloseableLock l = readLock()) {
      checkState();
      return isComplete(datasetConfig) && metadataManager.isStillValid(options, datasetConfig);
    }
  }



  public UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions) {
    try(AutoCloseableLock l = readLock()) {
      checkState();
      return metadataManager.refreshDataset(key, retrievalOptions);
    } catch (ConnectorException | NamespaceException e) {
      throw UserException.validationError(e).message("Unable to refresh dataset.").build(logger);
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

      return plugin.getDatasetHandle(new EntityPath(key.getPathComponents()),
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
   */
  public void refreshState() throws Exception {
    boolean startedDuringRefresh = false;
    if (plugin == null) {
      /* serialize access with replacePlugin */
      try (AutoCloseable lock = writeLock()) {
        if (plugin == null) {
          final IdProvider provider = new IdProvider();
          plugin = conf.newPlugin(context, sourceConfig.getName(), provider);
          CheckedFuture<SourceState, Exception> pluginStartFuture = startAsync(sourceConfig);
          try {
            pluginStartFuture.checkedGet();
            startedDuringRefresh = true;
          } catch (Exception e) {
          /* no action needed since we will either do another attempt at
           * starting the plugin or just exit since startAsync() would have
           * already recorded the plugin state as BAD and this is what
           * will be seen by the caller of refreshState() -- ALTER REFRESH
           * DDL path
           */
            logger.debug("Failed to start plugin while trying to refresh state, error:", e);
          }
        }
      }
    }

    if (!startedDuringRefresh) {
      /* we should query the plugin to get latest state only if
       * we did not have to start the plugin above. if the startup
       * was done above, then we already have the latest state and
       * no need to query the plugin again. if we tried to start above
       * and the startup failed after multiple attempts then again we
       * have the state recorded as bad so the caller of refreshState()
       * will see BAD state for source.
       */
      // TODO: this could cause NPE as the startAsync sets the plugin to null if there is an exception in starting the
      // plugin.
      state = plugin.getState();
    }
  }

  /**
   * Replace the plugin instance with one defined by the new SourceConfig. Do the minimal
   * changes necessary.
   *
   * @param config
   * @param context
   * @param waitMillis
   * @return Whether metadata was maintained. Metdata will be maintained if the connection did not
   *         change or was changed with only non-metadata impacting changes.
   * @throws Exception
   */
  boolean replacePlugin(SourceConfig config, SabotContext context, final long waitMillis) throws Exception {
    try(AutoCloseableLock l = writeLock()) {

      final ConnectionConf<?, ?> existingConnectionConf = this.conf;
      final ConnectionConf<?, ?> newConnectionConf = reader.getConnectionConf(config);
      /* if the plugin startup had failed earlier (plugin is null) and
       * we are here to replace the plugin, we should not return here.
       */
      if(existingConnectionConf.equals(newConnectionConf) && plugin != null) {
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
      this.state = SourceState.badState("Plugin settings changed, restarting.");
      // closing the plugin also cancels the metadata refresh thread and waits for it to exit
      metadataManager.close();

      // hold the old plugin until we successfully replace it.
      final SourceConfig oldConfig = sourceConfig;
      final StoragePlugin oldPlugin = plugin;
      IdProvider provider = new IdProvider(this);
      this.plugin = newConnectionConf.newPlugin(context, sourceKey.getRoot(), provider);
      try {
        logger.trace("Starting new plugin for [{}]", config.getName());
        startAsync(config).checkedGet(waitMillis, TimeUnit.MILLISECONDS);
        try {
          if (oldPlugin != null) {
            oldPlugin.close();
          }
        }catch(Exception ex) {
          logger.warn("Failure while retiring old plugin [{}].", sourceKey, ex);
        }

        // if we replaced the plugin successfully, clear the permission cache
        permissionsCache.clear();

        return existingConnectionConf.equalsIgnoringNotMetadataImpacting(newConnectionConf);
      } catch(Exception ex) {
        // the update failed, go back to previous state.
        this.plugin = oldPlugin;
        setLocals(oldConfig);
        if (this.plugin != null) {
          initiateMetadataRefresh();
        }
        throw ex;
      }
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
    synchronized (fixLock) {
      Cancellable lastTask = this.fixFailedStartScheduled;
      if (lastTask != null && !lastTask.isCancelled()) {
        lastTask.cancel(false);
      }
    }
    try(AutoCloseableLock l = writeLock()) {
      state = SourceState.badState("Source is being shutdown.");
      AutoCloseables.close(metadataManager, plugin);
    }
  }

  void cancelMetadataRefreshTask() {
    metadataManager.cancelRefreshTask();
  }

  boolean refresh(UpdateType updateType, MetadataPolicy policy) throws NamespaceException {
    try(AutoCloseableLock l = readLock()) {
      checkState();
      return metadataManager.refresh(updateType, policy);
    }
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


}

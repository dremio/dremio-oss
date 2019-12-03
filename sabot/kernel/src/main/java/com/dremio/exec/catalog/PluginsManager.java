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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.dremio.common.AutoCloseables;
import com.dremio.concurrent.Runnables;
import com.dremio.datastore.KVStore;
import com.dremio.exec.rpc.CloseableThreadPool;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.util.DebugCheck;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SystemUser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Manages the creation, deletion and retrieval of storage plugins.
 *
 */
class PluginsManager implements AutoCloseable, Iterable<StoragePlugin> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PluginsManager.class);

  private final ConnectionReader reader;
  private final SabotContext context;
  private final SchedulerService scheduler;
  private final CloseableThreadPool executor = new CloseableThreadPool("source-management");
  private final DatasetListingService datasetListing;
  private final ConcurrentHashMap<String, ManagedStoragePlugin> plugins = new ConcurrentHashMap<>();
  private final Set<String> beingCreatedSources = Sets.newConcurrentHashSet();
  private final long startupWait;
  private final KVStore<NamespaceKey, SourceInternalData> sourceDataStore;
  private final CatalogServiceMonitor monitor;
  private Cancellable refresher;
  private final NamespaceService systemNamespace;

  public PluginsManager(
      SabotContext context,
      KVStore<NamespaceKey, SourceInternalData> sourceDataStore,
      SchedulerService scheduler,
      ConnectionReader reader,
      CatalogServiceMonitor monitor
      ) {
    this.reader = reader;
    this.sourceDataStore = sourceDataStore;
    this.context = context;
    this.systemNamespace = context.getNamespaceService(SystemUser.SYSTEM_USERNAME);
    this.scheduler = scheduler;
    this.datasetListing = context.getDatasetListing();
    this.startupWait = DebugCheck.IS_DEBUG ? TimeUnit.DAYS.toMillis(365) : context.getOptionManager().getOption(CatalogOptions.STARTUP_WAIT_MAX);
    this.monitor = monitor;
  }

  ConnectionReader getReader() {
    return reader;
  }

  public Set<String> getSourceNameSet(){
    return new HashSet<>(FluentIterable.from(plugins.values()).transform(new Function<ManagedStoragePlugin, String>(){
      @Override
      public String apply(ManagedStoragePlugin input) {
        return input.getName().getRoot();
      }}).toSet());
  }


  /**
   * Automatically synchronizing sources regularily
   */
  private class Refresher implements Runnable {

    @Override
    public void run() {
      try {
        synchronizeSources();
      } catch (Exception ex) {
        logger.warn("Failure while synchronizing sources.");
      }
    }

    @Override
    public String toString() {
      return "catalog-source-synchronization";
    }
  }

  Iterable<ManagedStoragePlugin> managed() {
    return plugins.values();
  }

  /**
   * Create a new managed storage plugin. Requires the PluginManager.writeLock() to be held.
   * @param config The configuration to create.
   * @return The newly created managed storage plugin. If a plugin with the provided name already exists, does nothing and returns null.
   * @throws Exception
   * @throws TimeoutException
   */
  public ManagedStoragePlugin create(SourceConfig config, String userName, NamespaceAttribute... attributes) throws TimeoutException, Exception {
    if(hasPlugin(config.getName())) {
      throw new SourceAlreadyExistsException();
    }

    if (!beingCreatedSources.add(config.getName())) {
      // Someone else is adding the same source
      throw new SourceAlreadyExistsException();
    }

    try {
      ManagedStoragePlugin plugin = newPlugin(config);
      plugin.createSource(config, userName, attributes);

      // use concurrency features of concurrent hash map to avoid locking.
      ManagedStoragePlugin existing = plugins.putIfAbsent(c(config.getName()), plugin);

      if (existing == null) {
        return plugin;
      }

      final SourceAlreadyExistsException e = new SourceAlreadyExistsException();
      try {
        // this happened in time with someone else.
        plugin.close();
      } catch (Exception ex) {
        e.addSuppressed(ex);
      }

      throw e;
    } finally {
      beingCreatedSources.remove(config.getName());
    }
  }

  /**
   *
   * @throws NamespaceException
   */
  public void start() throws NamespaceException {

    // Since this is run inside the system startup, no one should be able to interact with it until we've already
    // started everything. Thus no locking is necessary.

    ImmutableMap.Builder<String, CompletableFuture<SourceState>> futuresBuilder = ImmutableMap.builder();
    for (SourceConfig source : datasetListing.getSources(SystemUser.SYSTEM_USERNAME)) {
      ManagedStoragePlugin plugin = newPlugin(source);

      futuresBuilder.put(source.getName(), plugin.startAsync());
      plugins.put(c(source.getName()), plugin);
    }

    Map<String, CompletableFuture<SourceState>> futures = futuresBuilder.build();
    final CompletableFuture<Void> futureWait = CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[futures.size()]));
    try {
      // wait STARTUP_WAIT_MILLIS or until all plugins have started/failed to start.
      futureWait.get(startupWait, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      // ignore since we're going to evaluate individually below.
    }

    final StringBuilder sb = new StringBuilder();

    int count = 0;
    sb.append("Result of storage plugin startup: \n");
    for(final ManagedStoragePlugin p : plugins.values()) {
      count++;
      String name = p.getName().getRoot();
      final CompletableFuture<SourceState> future = futures.get(name);
      Preconditions.checkNotNull(future, "Unexpected failure to retrieve source %s from available futures %s.", name, futures.keySet());
      if(future.isDone()) {
        try {
          SourceState state = future.get();
          String result = state.getStatus() == SourceStatus.bad ? "started in bad state" : "success";
          sb.append(String.format("\t%s: %s (%dms). %s\n", name, result, p.getStartupTime(), state));
        }catch (Exception ex) {
          logger.error("Failure while starting plugin {} after {}ms.", p.getName(), p.getStartupTime(), ex);
          sb.append(String.format("\t%s: failed (%dms). %s\n", name, p.getStartupTime(), p.getState()));
          p.initiateFixFailedStartTask();
        }
      } else {
        // not finished, let's get a log entry later.
        future.thenRun(Runnables.combo(new LateSourceRunnable(future, p)));
        sb.append(String.format("\t%s: pending.\n", name));
      }

    }

    // for coordinator, ensure catalog synchronization. Don't start this until the plugins manager is started.
    if(context.getRoles().contains(Role.COORDINATOR)) {
      refresher = scheduler.schedule(Schedule.Builder.everyMillis(CatalogServiceImpl.CATALOG_SYNC).build(), Runnables.combo(new Refresher()));
    }

    if(count > 0) {
      logger.info(sb.toString());
    }

  }

  private ManagedStoragePlugin newPlugin(SourceConfig config) {
    final boolean isVirtualMaster = context.isMaster() ||
      (context.getDremioConfig().isMasterlessEnabled() && context.isCoordinator());

    final ManagedStoragePlugin msp = new ManagedStoragePlugin(
        context,
        executor,
        isVirtualMaster,
        scheduler,
        context.getNamespaceService(SystemUser.SYSTEM_USERNAME),
        sourceDataStore,
        config,
        context.getOptionManager(),
        reader,
        monitor.forPlugin(config.getName())
        );
    return msp;
  }

  /**
   * Runnable that is used to finish startup for sources that aren't completed starting up within
   * the initial startup time.
   */
  private final class LateSourceRunnable implements Runnable {

    private final CompletableFuture<SourceState> future;
    private final ManagedStoragePlugin plugin;

    public LateSourceRunnable(CompletableFuture<SourceState> future, ManagedStoragePlugin plugin) {
      this.future = future;
      this.plugin = plugin;
    }

    @Override
    public void run() {
      try {
        SourceState state = future.get();
        String result = state.getStatus() == SourceStatus.bad ? "started in bad state" : "started sucessfully";
        logger.info("Plugin {} {} after {}ms. Current status: {}", plugin.getName(), result, plugin.getStartupTime(), state);
      } catch (Exception ex) {
        logger.error("Failure while starting plugin {} after {}ms.", plugin.getName(), plugin.getStartupTime(), ex);
        plugin.initiateFixFailedStartTask();
      }
    }

    @Override
    public String toString() {
      return "late-load-" + plugin.getName().getRoot();
    }
  }


  /**
   * Canonicalize storage plugin name.
   * @param pluginName
   * @return a canonicalized version of the key.
   */
  private String c(String pluginName) {
    return pluginName.toLowerCase();
  }

  /**
   * Iterator only returns non-bad state plugins.
   */
  @Override
  public Iterator<StoragePlugin> iterator() {
    return FluentIterable.from(plugins.values())
        .transform(new Function<ManagedStoragePlugin, StoragePlugin>(){
          @Override
          public StoragePlugin apply(ManagedStoragePlugin input) {
            return input.unwrap(StoragePlugin.class);
          }})
        .filter(new Predicate<StoragePlugin>() {

        @Override
        public boolean apply(StoragePlugin input) {
          SourceState state = input.getState();
          return state.getStatus() != SourceStatus.bad;
        }}).iterator();
  }

  public boolean hasPlugin(String name) {
    return plugins.containsKey(c(name));
  }

  public ManagedStoragePlugin getSynchronized(SourceConfig pluginConfig) throws Exception {
    while(true) {
      ManagedStoragePlugin plugin = plugins.get(c(pluginConfig.getName()));

      if(plugin != null) {
        plugin.synchronizeSource(pluginConfig);
        return plugin;
      }

      if (beingCreatedSources.contains(pluginConfig.getName())) {
        throw new IllegalStateException(String.format("The source %s is still being created.", pluginConfig.getName()));
      }

      plugin = newPlugin(pluginConfig);
      plugin.replacePluginWithLock(pluginConfig, createWaitMillis(), true);

      // grab write lock before putting in map so we can finish starting.
      ManagedStoragePlugin existing = plugins.putIfAbsent(c(pluginConfig.getName()), plugin);

      if (existing == null) {
        return plugin;
      }
      try {
        // this happened in time with someone else.
        plugin.close();
      } catch (Exception ex) {
        logger.debug("Exception while closing concurrently created plugin.", ex);
      }
    }
  }

  private long createWaitMillis() {
    if(DebugCheck.IS_DEBUG) {
      return TimeUnit.DAYS.toMillis(365);
    }
    return context.getOptionManager().getOption(CatalogOptions.STORAGE_PLUGIN_CREATE_MAX);
  }

  /**
   * Remove a source by grabbing the write lock and then removing from the map
   * @param config The config of the source to dete.
   * @return True if the source matched and was removed.
   */
  public boolean closeAndRemoveSource(SourceConfig config) {
    final String name = config.getName();
    logger.debug("Deleting source [{}]", name);

    ManagedStoragePlugin plugin = plugins.get(c(name));

    if(plugin == null) {
      return true;
    }

    try {
      return plugin.close(config, s -> plugins.remove(c(name)));
    } catch(Exception ex) {
      logger.info("Exception while shutting down source.", ex);
      return true;
    }

  }

  public ManagedStoragePlugin get(String name) {
    return plugins.get(c(name));
  }


  /**
   * For each source, synchronize the sources definition to the namespace.
   */
  @VisibleForTesting
  void synchronizeSources() {
    logger.trace("Running scheduled source synchronization");

    // first collect up all the current source configs.
    final Map<String, SourceConfig> configs = FluentIterable.from(plugins.values()).transform(new Function<ManagedStoragePlugin, SourceConfig>(){

      @Override
      public SourceConfig apply(ManagedStoragePlugin input) {
        return input.getConfig();
      }}).uniqueIndex(new Function<SourceConfig, String>(){

        @Override
        public String apply(SourceConfig input) {
          return input.getName();
        }});

    // second, for each source, synchronize to latest state
    final Set<String> names = getSourceNameSet();
    for(SourceConfig config : systemNamespace.getSources()) {
      names.remove(config.getName());
      if (beingCreatedSources.contains(config.getName())) {
        // skip this source since it's still being created
        continue;
      }
      try {
        getSynchronized(config);
      } catch (Exception ex) {
        logger.warn("Failure updating source [{}] during scheduled updates.", config, ex);
      }
    }

    // third, delete everything that wasn't found, assuming it matches what we originally searched.
    for(String name : names) {
      SourceConfig originalConfig = configs.get(name);
      if(originalConfig != null) {
        try {
          this.closeAndRemoveSource(originalConfig);
        } catch (Exception e) {
          logger.warn("Failure while deleting source [{}] during source synchronization.", originalConfig.getName(), e);
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    if(refresher != null) {
      refresher.cancel(false);
    }

    AutoCloseables.close(Iterables.concat(Collections.singleton(executor), plugins.values()));
  }
}

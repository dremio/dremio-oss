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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.common.concurrent.WithAutoCloseableLock;
import com.dremio.concurrent.Runnables;
import com.dremio.datastore.KVStore;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.util.DebugCheck;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Manages the creation, deletion and retrieval of storage plugins.
 *
 * Also responsible for starting up any existing sources known upon startup. After initial startup,
 * all changes are driven externally.
 *
 * Locking: We have a global ReentrantReadWriteLock for addition and removal. Addition and removal
 * should be done quickly (for example, plugin start should not be done under the global write
 * lock). operations. Any in-place update of an existing source must have both the read lock here
 * plus a lock on the ManagedStoragePlugin.
 */
class PluginsManager implements AutoCloseable, Iterable<StoragePlugin> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PluginsManager.class);

  private final ConnectionReader reader;
  private final SabotContext context;
  private final SchedulerService scheduler;
  private final NamespaceService ns;
  private final ConcurrentHashMap<String, ManagedStoragePlugin> plugins = new ConcurrentHashMap<>();
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final long startupWait;
  private final KVStore<NamespaceKey, SourceInternalData> sourceDataStore;

  public PluginsManager(
      SabotContext context,
      KVStore<NamespaceKey, SourceInternalData> sourceDataStore,
      SchedulerService scheduler
      ) {
    this.reader = new ConnectionReader(context.getClasspathScan());
    this.sourceDataStore = sourceDataStore;
    this.context = context;
    this.scheduler = scheduler;
    this.ns = context.getNamespaceService(SystemUser.SYSTEM_USERNAME);
    ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
    readLock = rwlock.readLock();
    writeLock = rwlock.writeLock();
    this.startupWait = DebugCheck.IS_DEBUG ? TimeUnit.DAYS.toMillis(365) : context.getOptionManager().getOption(CatalogOptions.STARTUP_WAIT_MAX);
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

  @SuppressWarnings("resource")
  public AutoCloseableLock readLock() {
    return new AutoCloseableLock(readLock).open();
  }

  @SuppressWarnings("resource")
  public AutoCloseableLock writeLock() {
    return new AutoCloseableLock(writeLock).open();
  }

  /**
   * Create a new managed storage plugin. Requires the PluginManager.writeLock() to be held.
   * @param config The configuration to create.
   * @return The newly created managed storage plugin. If a plugin with the provided name already exists, does nothing and returns null.
   */
  public ManagedStoragePlugin create(SourceConfig config) {
    Preconditions.checkArgument(writeLock.isHeldByCurrentThread(), "You must have the write lock before creating a source.");
    if(hasPlugin(config.getName())) {
      return null;
    }

    ManagedStoragePlugin plugin = newPlugin(config);
    plugins.put(c(config.getName()), plugin);
    return plugin;
  }

  public Iterable<ManagedStoragePlugin> managed() {
    return plugins.values();
  }

  public void start() {

    // hold write lock for life of startup.
    try (AutoCloseableLock l = writeLock()) {
      ImmutableMap.Builder<String, CheckedFuture<SourceState, Exception>> futuresBuilder = ImmutableMap.builder();
      for(SourceConfig source : ns.getSources()) {
        ManagedStoragePlugin plugin = newPlugin(source);

        futuresBuilder.put(source.getName(), plugin.startAsync());
        plugins.put(c(source.getName()), plugin);
      }

      Map<String, CheckedFuture<SourceState, Exception>> futures = futuresBuilder.build();
      try {
        // wait STARTUP_WAIT_MILLIS or until all plugins have started/failed to start.
        Futures.successfulAsList(futures.values()).get(startupWait, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        // ignore.
      }

      final StringBuilder sb = new StringBuilder();

      int count = 0;
      sb.append("Result of storage plugin startup: \n");
      for(final ManagedStoragePlugin p : plugins.values()) {
        count++;
        String name = p.getName().getRoot();
        final CheckedFuture<SourceState, Exception> future = futures.get(name);
        Preconditions.checkNotNull(future, "Unexpected failure to retrieve source %s from available futures %s.", name, futures.keySet());
        if(future.isDone()) {
          try {
            SourceState state = future.checkedGet();
            p.initiateMetadataRefresh();
            String result = state.getStatus() == SourceStatus.bad ? "started in bad state" : "success";
            sb.append(String.format("\t%s: %s (%dms). %s\n", name, result, p.getStartupTime(), state));
          }catch (Exception ex) {
            logger.error("Failure while starting plugin {} after {}ms.", p.getName(), p.getStartupTime(), ex);
            sb.append(String.format("\t%s: failed (%dms). %s\n", name, p.getStartupTime(), p.getState()));
            p.initiateFixFailedStartTask();
          }
        } else {
          // not finished, let's get a log entry later.
          future.addListener(Runnables.combo(new LateSourceRunnable(future, p)), MoreExecutors.directExecutor());
          sb.append(String.format("\t%s: pending.\n", name));
        }

      }

      if(count > 0) {
        logger.info(sb.toString());
      }
    }

  }



  /**
   * Runnable that is used to finish startup for sources that aren't completed starting up within
   * the initial startup time.
   */
  private final class LateSourceRunnable implements Runnable {

    private final CheckedFuture<SourceState, Exception> future;
    private final ManagedStoragePlugin plugin;

    public LateSourceRunnable(CheckedFuture<SourceState, Exception> future, ManagedStoragePlugin plugin) {
      this.future = future;
      this.plugin = plugin;
    }

    @Override
    public void run() {
      try {
        SourceState state = future.checkedGet();
        plugin.initiateMetadataRefresh();
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

  /**
   * Delete a source. Need to grab the write lock for the plugin manager and then the write lock for the plugin (if exists) to ensure no changes are conflicting.
   *
   * @param config
   * @return true if the source was deleted. False if the source doesn't exist or the source's config doesn't match.
   */
  public boolean deleteSource(SourceConfig config) {
    logger.debug("Deleting source [{}]", config.getName());
    try(AutoCloseableLock l = writeLock()){
      ManagedStoragePlugin plugin = plugins.get(c(config.getName()));
      if(plugin == null) {
        return false;
      }

      try(AutoCloseableLock l2 = plugin.writeLock()) {
        if(!plugin.matches(config)) {
          return false;
        }
        plugins.remove(c(config.getName()));

        try {
          plugin.close();
        } catch(Exception ex) {
          logger.warn("Exception while shutting down source.", ex);
        }
        return true;
      }
    }
  }

  public ManagedStoragePlugin get(String name) {
    try(AutoCloseableLock l = readLock()) {
      return plugins.get(c(name));
    }
  }

  public WithAutoCloseableLock<ManagedStoragePlugin> getWithPluginWriteLock(String name) {
    try(AutoCloseableLock l = readLock()) {
      ManagedStoragePlugin p = plugins.get(c(name));
      if(p == null) {
        return null;
      }
      return new WithAutoCloseableLock<>(p.writeLock(), p);
    }
  }

  static class IdProvider implements Provider<StoragePluginId> {
    private ManagedStoragePlugin plugin;

    public IdProvider() {
      super();
    }

    public IdProvider(ManagedStoragePlugin plugin) {
      super();
      this.plugin = plugin;
    }

    @Override
    public StoragePluginId get() {
      if(plugin == null) {
        throw new IllegalStateException("Need to start plugin before attempting to retrieve plugin id.");
      }
      return plugin.getId();
    }
  }

  private ManagedStoragePlugin newPlugin(SourceConfig config) {
    ConnectionConf<?, ?> connectionConf = reader.getConnectionConf(config);
    IdProvider provider = new IdProvider();
    StoragePlugin plugin = connectionConf.newPlugin(context, config.getName(), provider);
    final ManagedStoragePlugin msp = new ManagedStoragePlugin(
        context,
        context.getRoles().contains(Role.MASTER),
        scheduler,
        context.getNamespaceService(SystemUser.SYSTEM_USERNAME),
        sourceDataStore,
        config,
        connectionConf,
        plugin,
        context.getOptionManager(),
        reader
        );
    provider.plugin = msp;
    return msp;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(plugins.values());
  }
}

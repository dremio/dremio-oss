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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import com.dremio.common.DeferredException;
import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.common.concurrent.WithAutoCloseableLock;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.concurrent.Runnables;
import com.dremio.datastore.KVStore;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.proto.CatalogRPC.RpcType;
import com.dremio.exec.proto.CatalogRPC.SourceWrapper;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.ischema.InfoSchemaConf;
import com.dremio.exec.util.DebugCheck;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.coordinator.DistributedSemaphore.DistributedLease;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SystemUser;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;

/**
 * CatalogServiceImpl, manages all sources and exposure of Catalog for table retrieval.
 *
 * Has several helper classes to get its job done:
 * - Catalog is the consumable interface for interacting with sources/storage plugins.
 * - ManagedStoragePlugin keeps tracks the system state for each plugin
 * - PluginsManager is a glorified map that includes all the StoragePugins.
 */
public class CatalogServiceImpl implements CatalogService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CatalogServiceImpl.class);
  private static final long CATALOG_SYNC = TimeUnit.MINUTES.toMillis(3);
  private static final long CHANGE_COMMUNICATION_WAIT = TimeUnit.SECONDS.toMillis(5);

  private static final Comparator<SourceConfig> SOURCE_CONFIG_COMPARATOR = new Comparator<SourceConfig>() {
    @Override
    public int compare(SourceConfig s1, SourceConfig s2) {
      if (s2.getVersion() == null) {
        if (s1.getVersion() == null) {
         return 0;
        }
        return 1;
      } else if (s1.getVersion() == null) {
        return -1;
      } else {
        return Long.compare(s1.getVersion(), s2.getVersion());
      }
    }
  };
  public static final String CATALOG_SOURCE_DATA_NAMESPACE = "catalog-source-data";

  private final Provider<SabotContext> context;
  private final Provider<SchedulerService> scheduler;
  private final Provider<? extends Provider<ConnectionConf<?, ?>>> sysTableConfProvider;
  private final Provider<FabricService> fabric;

  private KVStore<NamespaceKey, SourceInternalData> sourceDataStore;
  private NamespaceService systemNamespace;
  private PluginsManager plugins;
  private BufferAllocator allocator;
  private FabricRunnerFactory tunnelFactory;
  private Cancellable refresher;
  private CatalogProtocol protocol;

  public CatalogServiceImpl(
      Provider<SabotContext> context,
      Provider<SchedulerService> scheduler,
      Provider<? extends Provider<ConnectionConf<?, ?>>> sysTableConfProvider,
      Provider<FabricService> fabric
      ) {
    this.context = context;
    this.scheduler = scheduler;
    this.sysTableConfProvider = sysTableConfProvider;
    this.fabric = fabric;

  }

  @Override
  public void start() throws Exception {
    SabotContext context = this.context.get();
    this.allocator = context.getAllocator().newChildAllocator("catalog-protocol", 0, Long.MAX_VALUE);
    this.systemNamespace = context.getNamespaceService(SystemUser.SYSTEM_USERNAME);
    sourceDataStore = context.getKVStoreProvider().getStore(CatalogSourceDataCreator.class);
    this.plugins = new PluginsManager(context, sourceDataStore, this.scheduler.get());
    plugins.start();
    this.protocol =  new CatalogProtocol(allocator, new CatalogChangeListener(), context.getConfig());
    tunnelFactory = fabric.get().registerProtocol(protocol);

    if(context.getRoles().contains(Role.MASTER)) {
      if(createSourceIfMissing(new SourceConfig()
            .setConfig(new InfoSchemaConf().toBytesString())
            .setName("INFORMATION_SCHEMA")
            .setType("INFORMATION_SCHEMA")
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY))) {
        logger.debug("Refreshing 'INFORMATION_SCHEMA' source");
        refreshSource(new NamespaceKey("INFORMATION_SCHEMA"), CatalogService.NEVER_REFRESH_POLICY, UpdateType.FULL);
      };

      if(createSourceIfMissing(new SourceConfig()
          .setConnectionConf(sysTableConfProvider.get().get())
          .setName("sys")
          .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY))) {
        logger.debug("Refreshing 'sys' source");
        refreshSource(new NamespaceKey("sys"), CatalogService.NEVER_REFRESH_POLICY, UpdateType.FULL);
      };

    }


    // for coordinator, ensure catalog synchronization.
    if(context.getRoles().contains(Role.COORDINATOR)) {
      refresher = scheduler.get().schedule(Schedule.Builder.everyMillis(CATALOG_SYNC).build(), Runnables.combo(new Refresher()));
    }
  }

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

  private void communicateChange(SourceConfig config, RpcType rpcType) {

    final Set<NodeEndpoint> endpoints = new HashSet<>();
    endpoints.add(context.get().getEndpoint());

    List<RpcFuture<Ack>> futures = new ArrayList<>();
    SourceWrapper wrapper = SourceWrapper.newBuilder().setBytes(ByteString.copyFrom(ProtobufIOUtil.toByteArray(config, SourceConfig.getSchema(), LinkedBuffer.allocate()))).build();
    for(NodeEndpoint e : Iterables.concat(this.context.get().getCoordinators(), this.context.get().getExecutors())) {
      if(!endpoints.add(e)) {
        continue;
      }

      SendSource send = new SendSource(wrapper, rpcType);
      tunnelFactory.getCommandRunner(e.getAddress(), e.getFabricPort()).runCommand(send);;
      logger.trace("Sending [{}] to {}:{}", config.getName(), e.getAddress(), e.getUserPort());
      futures.add(send.getFuture());
    }

    try {
      Futures.successfulAsList(futures).get(CHANGE_COMMUNICATION_WAIT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e1) {
      logger.warn("Failure while communicating source change [{}].", config.getName(), e1);
    }
  }

  private static class SendSource extends FutureBitCommand<Ack, ProxyConnection> {
    private final SourceWrapper wrapper;
    private final RpcType rpcType;

    public SendSource(SourceWrapper wrapper, RpcType rpcType) {
      this.wrapper = wrapper;
      this.rpcType = rpcType;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, rpcType, wrapper, Ack.class);
    }
  }

  class CatalogChangeListener {
    void sourceUpdate(SourceConfig config) {
      try {
        logger.debug("Received source update for [{}]", config.getName());

        synchronize(config);
      } catch (Exception ex) {
        logger.warn("Failure while synchronizing source [{}].", config.getName(), ex);
      }
    }

    void sourceDelete(SourceConfig config) {
      try {
        logger.debug("Received delete source for [{}]", config.getName());

        plugins.deleteSource(config);
      } catch (Exception ex) {
        logger.warn("Failure while synchronizing source [{}].", config.getName(), ex);
      }
    }

  }

  /**
   * For each source, synchronize the sources definition to the namespace.
   */
  @VisibleForTesting
  public void synchronizeSources() {
    logger.trace("Running scheduled source synchronization");

    // first collect up all the current source configs.
    final Map<String, SourceConfig> configs = FluentIterable.from(plugins.managed()).transform(new Function<ManagedStoragePlugin, SourceConfig>(){

      @Override
      public SourceConfig apply(ManagedStoragePlugin input) {
        return input.getConfig();
      }}).uniqueIndex(new Function<SourceConfig, String>(){

        @Override
        public String apply(SourceConfig input) {
          return input.getName();
        }});

    // second, for each source, synchronize to latest state
    final Set<String> names = plugins.getSourceNameSet();
    for(SourceConfig config : systemNamespace.getSources()) {
      names.remove(config.getName());
      try {
        synchronize(config);
      } catch (Exception ex) {
        logger.warn("Failure updating source [{}] during scheduled updates.", config, ex);
      }
    }

    // third, delete everything that wasn't found, assuming it matches what we originally searched.
    for(String name : names) {
      SourceConfig originalConfig = configs.get(name);
      if(originalConfig != null) {
        try {
          plugins.deleteSource(originalConfig);
        } catch (Exception e) {
          logger.warn("Failure while deleting source [{}] during source synchronization.", originalConfig.getName(), e);
        }
      }
    }
  }

  /**
   * Delete all sources except those that are specifically referenced
   * @param rootsToSaveSet Root sources to save.
   * @throws NamespaceException
   */
  @VisibleForTesting
  public void deleteExcept(Set<String> rootsToSaveSet) throws NamespaceException {
    for(SourceConfig source : systemNamespace.getSources()) {
      if(rootsToSaveSet.contains(source.getName())) {
        continue;
      }

      deleteSource(source, SystemUser.SYSTEM_USERNAME);
    }
  }

  @Override
  public boolean refreshSource(NamespaceKey source, MetadataPolicy metadataPolicy, UpdateType updateType) throws NamespaceException {
    ManagedStoragePlugin plugin = plugins.get(source.getRoot());
    if(plugin == null){
      throw UserException.validationError().message("Unknown source %s", source.getRoot()).build(logger);
    }

    return plugin.refresh(updateType, metadataPolicy);
  }

  @Override
  public void close() throws Exception {
    DeferredException ex = new DeferredException();
    if(plugins != null) {
      try (AutoCloseableLock l = plugins.writeLock()) {
        ex.suppressingClose(plugins);
      }
    }
    if(refresher != null) {
      refresher.cancel(false);
    }
    ex.suppressingClose(protocol);
    ex.suppressingClose(allocator);
    ex.close();
  }

  // used only internally
  private boolean createSourceIfMissing(SourceConfig config) {
    Preconditions.checkArgument(config.getVersion() == null);
    try {
      return createSourceIfMissingWithThrow(config);
    } catch (ConcurrentModificationException ex) {
      // someone else got there first, ignore this failure.
      logger.error("Two source creations occurred simultaneously, ignoring the failed one.", ex);
      // fall through to false below.
    }
    return false;
  }

  public boolean createSourceIfMissingWithThrow(SourceConfig config) throws ConcurrentModificationException {
    Preconditions.checkArgument(config.getVersion() == null);
    if(!plugins.hasPlugin(config.getName())) {
        createOrUpdateSource(config, true, SystemUser.SYSTEM_USERNAME);
        return true;
    }
    return false;
  }

  private void createSource(SourceConfig config, String userName) {
    Preconditions.checkArgument(config.getVersion() == null);
    createOrUpdateSource(config, false, userName);
  }

  private void updateSource(SourceConfig config, String userName) {
    Preconditions.checkArgument(config.getVersion() != null);
    createOrUpdateSource(config, false, userName);
  }

  @VisibleForTesting
  public void deleteSource(String name) {
    ManagedStoragePlugin plugin = plugins.get(name);
    if(plugin == null) {
      return;
    }
    deleteSource(plugin.getId().getConfig(), SystemUser.SYSTEM_USERNAME);
  }

  /**
   * Synchronize the named source with the namespace.
   * @param name The name of the source to synchronize.
   * @param errorOnMissing Whether or not to throw an exception if the namespace does not contain the source.
   */
  private ManagedStoragePlugin synchronize(String name, boolean errorOnMissing) {
    try{
      logger.debug("Synchronizing source [{}] with namespace", name);
      SourceConfig config = systemNamespace.getSource(new NamespaceKey(name));
      return synchronize(config);
    } catch (NamespaceNotFoundException ex) {
      if(!errorOnMissing) {
        return null;
      }
      throw UserException.validationError().message("Tried to access non-existent source [%s].", name).build(logger);
    } catch (Exception ex) {
      throw UserException.validationError(ex).message("Failure while trying to read source [%s].", name).build(logger);
    }
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
  @SuppressWarnings("resource")
  private ManagedStoragePlugin synchronize(SourceConfig targetConfig) throws Exception {
    ManagedStoragePlugin plugin = plugins.get(targetConfig.getName());
    AutoCloseableLock pluginLock = null;

    try {
      // to avoid keeping locks for extended periods (across startup, etc) we loop until we either synchronize or mismatch.

      while (true) {
        if(plugin != null) {
          if(plugin.matches(targetConfig)) {
            logger.trace("Source [{}] already up-to-date, not synchronizing", targetConfig.getName());
            return plugin;
          }
        } else {
          logger.trace("Creating source [{}]", targetConfig.getName());
          // we need to create a new plugin. we'll do this under the plugins write lock.
          try(AutoCloseableLock l = plugins.writeLock()) {
            plugin = plugins.get(targetConfig.getName());

            if(plugin != null) {
              // start over, the plugin appeared while grabbing the lock.
              continue;
            }

            // create this plugin, it is missing.
            plugin = plugins.create(targetConfig);
            pluginLock = plugin.writeLock();
          }

          // now start the plugin outside the plugin map writelock.
          SourceState state = plugin.startAsync().checkedGet(createWaitMillis(), TimeUnit.MILLISECONDS);
          if(SourceStatus.bad == state.getStatus()) {
            logger.warn("Source [{}] in bad state after synchronization. Info: {}", targetConfig.getName(), state);
          }
          // since this is a synchronize, we don't do anything here since no one is listening.
          plugin.initiateMetadataRefresh();
          return plugin;
        }


        // we think the plugin exists. Now grab it under the plugin map read lock to make sure we only hold it while the plugin lives
        try(AutoCloseableLock l = plugins.readLock()) {
          plugin = plugins.get(targetConfig.getName());
          if(plugin == null) {
            // we lost the plugin, loop again.
            continue;
          }
          pluginLock = plugin.writeLock();
        }

        if(plugin.matches(targetConfig)) {
          // up to date.
          return plugin;
        }
        // else, bring the plugin up to date, if possible

        final long creationTime = plugin.getConfig().getCtime();
        final long targetCreationTime = targetConfig.getCtime();

        if (creationTime > targetCreationTime) {
          // in-memory source config is newer than the target config, in terms of creation time
          throw new ConcurrentModificationException(String.format(
              "Source [%s] was updated, and the given configuration has older ctime (current: %d, given: %d)",
              targetConfig.getName(), creationTime, targetCreationTime));

        } else if (creationTime == targetCreationTime) {
          final SourceConfig currentConfig = plugin.getConfig();
          final int compareTo = SOURCE_CONFIG_COMPARATOR.compare(currentConfig, targetConfig);

          if (compareTo > 0) {
            // in-memory source config is newer than the target config, in terms of version
            throw new ConcurrentModificationException(String.format(
                "Source [%s] was updated, and the given configuration has older version (current: %d, given: %d)",
                currentConfig.getName(), currentConfig.getVersion(), targetConfig.getVersion()));
          }

          if (compareTo == 0) {
            // in-memory source config has a different version but the same value
            throw new IllegalStateException(String.format(
                "Current and given configurations for source [%s] have same version (%d) but different values" +
                    " [current source: %s, given source: %s]",
                currentConfig.getName(), currentConfig.getVersion(),
                plugins.getReader().toStringWithoutSecrets(currentConfig),
                plugins.getReader().toStringWithoutSecrets(targetConfig)));
          }

          // otherwise (compareTo < 0), target config is newer
        }
        // else (creationTime < targetCreationTime), the source config is new but plugins has an entry with the same
        // name, so replace the plugin regardless of the checks

        // in-memory storage plugin is older than the one persisted, update
        plugin.replacePlugin(targetConfig, context.get(), createWaitMillis());
        plugin.initiateMetadataRefresh();
        return plugin;
      }

    } finally {
      if(pluginLock != null) {
        pluginLock.close();
      }
    }
  }

  private long createWaitMillis() {
    if(DebugCheck.IS_DEBUG) {
      return TimeUnit.DAYS.toMillis(365);
    }
    return context.get().getOptionManager().getOption(CatalogOptions.STORAGE_PLUGIN_CREATE_MAX);
  }

  private void addDefaults(SourceConfig config) {
    if(config.getCtime() == null) {
      config.setCtime(System.currentTimeMillis());
    }

    if(config.getMetadataPolicy() == null) {
      config.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    }
  }

  /**
   * Create or update an existing source. Uses a set of escalating locks.
   *
   * - Modification requires a distributed lock for the canonicalized source name.
   * - If this is a new plugin, we need the PluginManager writelock.
   * - Either way, once we have the ManagedStoragePlugin, we need its writelock
   *
   * @param config
   */
  private void createOrUpdateSource(SourceConfig config, boolean ignoreIfExists, String userName) {
    logger.debug("Adding or updating source [{}].", config.getName());

    NamespaceService namespaceService = context.get().getNamespaceService(userName);

    addDefaults(config);

    ManagedStoragePlugin plugin = null;

    // first, grab the source configuration lock for this source.
    try(
        final AutoCloseable sourceDistributedLock = getDistributedLock(config.getName());
        ) {
      // now let's pre check the commit.
      try {
        SourceConfig existingConfig = namespaceService.getSource(config.getKey());

        if(ignoreIfExists) {
          return;
        }

        if(!Objects.equals(config.getVersion(), existingConfig.getVersion())) {
          throw new ConcurrentModificationException("Source was already created.");
        }
      }catch (NamespaceNotFoundException ex) {
        if(config.getVersion() != null) {
          throw new ConcurrentModificationException("Source was already created.");
        }
      }

      final boolean keepStaleMetadata  = context.get().getOptionManager()
          .getOption(CatalogOptions.STORAGE_PLUGIN_KEEP_METADATA_ON_REPLACE);
      // now that we've confirmed that the version is correct, let's update the plugin.
      final WithAutoCloseableLock<ManagedStoragePlugin> pluginWithWriteLock = plugins.getWithPluginWriteLock(config.getName());

      boolean newlyCreated = false;
      Closeable pluginLock = null;
      boolean refreshNames = false;

      try {

        if(pluginWithWriteLock != null) {
          plugin = pluginWithWriteLock.getValue();
          pluginLock = pluginWithWriteLock;
        } else {
          try(final AutoCloseable pluginManagerWriteLock = plugins.writeLock()){
            // check again inside lock.
            plugin = plugins.get(config.getName());
            if(plugin == null) {
              plugin = plugins.create(config);
              newlyCreated = true;
              // acquire write lock before exiting plugin manager write lock. This ensures no one else can use this plugin until we are done starting it.
              pluginLock = plugin.writeLock();
            }
          }
        }

        boolean oldMetadataIsBad = false;

        // we now have the plugin lock.
        if(newlyCreated) {
          logger.debug("Starting the source [{}]", config.getName());
          try {
            SourceState state = plugin.startAsync().checkedGet(createWaitMillis(), TimeUnit.MILLISECONDS);
            refreshNames = true;

            // if the creation resulted in a bad state,
            if (SourceStatus.bad == state.getStatus() &&
              context.get().getOptionManager().getOption(CatalogOptions.STORAGE_PLUGIN_CHECK_STATE)) {
              plugins.deleteSource(config);
              throw UserException.connectionError().message("Failure while configuring source [%s]. Info: %s", config.getName(), state).build(logger);
            }
          } catch (Exception e) {
            plugins.deleteSource(config);
            throw UserException.connectionError(e).message("Failure while configuring source [%s]", config.getName())
              .build(logger);
          }

        } else {
          boolean metadataStillGood = plugin.replacePlugin(config, context.get(), createWaitMillis());
          oldMetadataIsBad = !metadataStillGood;
          refreshNames = !metadataStillGood;
        }

        if (oldMetadataIsBad) {
          if (!keepStaleMetadata) {
            logger.debug("Old metadata data may be bad; deleting all descendants of source [{}]", config.getName());
            // TODO: expensive call on non-master coordinators (sends as many RPC requests as entries under the source)
            namespaceService.deleteSourceChildren(config.getKey(), config.getVersion());
          } else {
            logger.info("Old metadata data may be bad, but preserving descendants of source [{}] because '{}' is enabled",
                config.getName(), CatalogOptions.STORAGE_PLUGIN_KEEP_METADATA_ON_REPLACE.getOptionName());
          }
        }

        // Now let's create the plugin in the system namespace.
        // This increments the version in the config object as well.
        namespaceService.addOrUpdateSource(config.getKey(), config);

      } finally {
        if(pluginLock != null) {
          pluginLock.close();
        }
      }

      // inform other nodes about the config change, only if successful
      communicateChange(config, RpcType.REQ_SOURCE_CONFIG);

      // now that we're outside the plugin lock, we should refresh the dataset names. Note that this could possibly get
      // run on a different config if two source updates are racing to this lock.
      if (refreshNames) {
        if (!keepStaleMetadata) {
          logger.debug("Refreshing names in source [{}]", config.getName());
          plugin.refresh(UpdateType.NAMES, null);
        } else {
          logger.debug("Not refreshing names in source [{}]", config.getName());
        }
      }

      // once we have potentially done initial refresh, we can run subsequent refreshes. This allows
      // us to avoid two separate threads refreshing simultaneously (possible if we put this above
      // the inline plugin.refresh() call.)
      plugin.initiateMetadataRefresh();

      logger.debug("Source added [{}].", config.getName());

    } catch (ConcurrentModificationException ex) {
      throw UserException.concurrentModificationError(ex).message("Failure creating/updating source [%s].", config.getName()).build(logger);
    } catch (Exception ex) {
      throw UserException.validationError(ex).message("Failure creating/updating source [%s].", config.getName()).build(logger);
    }
  }

  @SuppressWarnings("resource")
  private void deleteSource(SourceConfig config, String userName) {
    try {
      NamespaceService namespaceService = context.get().getNamespaceService(userName);

      while(true) { // delete loop

        ManagedStoragePlugin plugin = plugins.get(config.getName());
        if(plugin == null) {
          noExistDelete: {
            // delete the source under the source configuration lock and plugin map lock.
            try(AutoCloseable l = getDistributedLock(config.getName());
                AutoCloseable pluginMapLock = plugins.writeLock();
                ) {

              // double check that plugin wasn't created while we were waiting for distributed lock.
              plugin = plugins.get(config.getName());
              if(plugin != null) {
                break noExistDelete;
              }

              namespaceService.deleteSource(config.getKey(), config.getVersion());
              sourceDataStore.delete(config.getKey());
            }

            communicateChange(config, RpcType.REQ_DEL_SOURCE);
            return;
          }
        }

        // make sure we're at the latest
        SourceConfig existing = plugin.getConfig();
        if(!existing.equals(config)) {
          final int compareTo = SOURCE_CONFIG_COMPARATOR.compare(existing, config);
          if (compareTo == 0) {
            throw new IllegalStateException("Two different configurations shouldn't have the same version.");
          }

          if (compareTo > 0) {
            // storage plugin is newer than provided configuration.
            throw new ConcurrentModificationException("Source was updated or replaced and the provided configuration is out of date.");
          }

          // storage plugin is old, update.
          synchronize(config);
        }

        // delete the source under the source configuration lock.
        try(
            AutoCloseable l = getDistributedLock(config.getName());
            AutoCloseable pluginsLock = plugins.writeLock();
            ) {

          // reacquire plugin inside locks.
          plugin = plugins.get(config.getName());
          if(plugin == null) {
            // restart loop to include synchronize or simple delete.
            continue;
          }

          try (AutoCloseable pluginLock = plugin.writeLock()) {
            if(!plugin.matches(config)) {
              // restart loop to include synchronize or no exists delete.
              continue;
            }

            // we delete the source from the plugin once we confirm that it matches what was requested for deletion.
            // we do this first to ensure the SourceMetadataManager process is killed for this source so that subsequent deletes are correct.
            plugins.deleteSource(config);
            namespaceService.deleteSource(config.getKey(), config.getVersion());
            sourceDataStore.delete(config.getKey());
          }

        }

        communicateChange(config, RpcType.REQ_DEL_SOURCE);
        return;
      }

    } catch (Exception ex) {
      throw UserException.validationError(ex).message("Failure deleting source [%s].", config.getName()).build(logger);
    }
  }

  private AutoCloseable getDistributedLock(String sourceName) throws Exception {
    long millis = 15_000;
    DistributedLease lease = context.get().getClusterCoordinator().getSemaphore("-source-" + sourceName.toLowerCase(), 1).acquire(millis, TimeUnit.MILLISECONDS);
    if(lease == null) {
      throw UserException.resourceError().message("Unable to aquire source change lock for source [%s] within timeout.", sourceName).build(logger);
    }
    return lease;
  }

  /**
   * Get a plugin based on a particular StoragePluginId. If the plugin exists, return it. If it
   * doesn't exist, we will create the plugin. We specifically avoid any calls to the KVStore on
   * this path. If the configuration is older than the currently active plugin with this name, we
   * will fail the provision.
   *
   * @param id
   * @return Plugin with a matching id.
   */
  private ManagedStoragePlugin getPlugin(StoragePluginId id) {
    try(AutoCloseableLock l = plugins.readLock()) {
      ManagedStoragePlugin plugin = plugins.get(id.getName());

      // plugin.matches() here grabs the plugin read lock, guaranteeing that the plugin already exists and has been started
      if(plugin != null && plugin.matches(id.getConfig())) {
        return plugin;
      }
    }

    try {
      logger.debug("Source [{}] does not exist/match the one in-memory, synchronizing from id", id.getName());
      return synchronize(id.getConfig());
    } catch (Exception e) {
      throw UserException.validationError(e).message("Failure getting source [%s].", id.getName()).build(logger);
    }
  }

  private ManagedStoragePlugin getPlugin(String name, boolean errorOnMissing) {
    ManagedStoragePlugin plugin = plugins.get(name);
    if(plugin != null) {
      return plugin;
    }

    return synchronize(name, errorOnMissing);
  }

  @VisibleForTesting
  public ManagedStoragePlugin getManagedSource(String name) {
    return plugins.get(name);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends StoragePlugin> T getSource(StoragePluginId pluginId) {
    return (T) getPlugin(pluginId).unwrap(StoragePlugin.class);
  }

  @Override
  public SourceState getSourceState(String name) {
    // Preconditions.checkState(isCoordinator);
    ManagedStoragePlugin plugin = getPlugin(name, false);
    if(plugin == null) {
      return null;
    }
    return plugin.getState();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends StoragePlugin> T getSource(String name) {
    // Preconditions.checkState(isCoordinator); ??
    return (T) getPlugin(name, true).unwrap(StoragePlugin.class);
  }

  /**
   * Get a {@link CachingCatalog} contextualized to the provided SchemaConfig. Catalogs are used to interact with
   * datasets within the context a particular session.
   *
   * @param schemaConfig schema config
   * @return catalog
   */
  @Override
  public Catalog getCatalog(SchemaConfig schemaConfig) {
    return getCatalog(schemaConfig, Long.MAX_VALUE);
  }

  /**
   * Get a new {@link CachingCatalog} that only considers metadata valid if it is newer than the
   * provided maxRequestTime.
   *
   * @param schemaConfig schema config
   * @param maxRequestTime max request time
   * @return catalog with given constraints
   */
  @Override
  public Catalog getCatalog(SchemaConfig schemaConfig, long maxRequestTime) {
    final Catalog catalog = new CatalogImpl(context.get(),
        new MetadataRequestOptions(schemaConfig, maxRequestTime),
        new Retriever(), new SourceModifier(schemaConfig.getUserName()));
    return new CachingCatalog(catalog);
  }

  @Override
  public boolean isSourceConfigMetadataImpacting(SourceConfig config) {
    return plugins.get(config.getName()).isSourceConfigMetadataImpacting(config);
  }

  private class Retriever implements PluginRetriever {

    @Override
    public ManagedStoragePlugin getPlugin(String name, boolean synchronizeOnMissing) {
      // Preconditions.checkState(isCoordinator);

      if (!synchronizeOnMissing) {
        // get from in-memory
        return plugins.get(name);
      }
      // if plugin is missing in-memory, we will synchronize to kvstore
      return CatalogServiceImpl.this.getPlugin(name, true);
    }

  }

  @Override
  public RuleSet getStorageRules(OptimizerRulesContext context, PlannerPhase phase) {
    final ImmutableSet.Builder<RelOptRule> rules = ImmutableSet.builder();
    final Set<SourceType> types = new HashSet<>();

    try {
      for(ManagedStoragePlugin plugin : plugins.managed()){
        if(plugin.getState().getStatus() == SourceStatus.bad) {
          // we shouldn't consider rules for misbehaving plugins.
          continue;
        }
        final StoragePluginId pluginId = plugin.getId();

        StoragePluginRulesFactory factory = plugin.getRulesFactory();
        if(factory != null) {
          // add instance level rules.
          rules.addAll(factory.getRules(context, phase, pluginId));

          // add type level rules.
          if(types.add(pluginId.getType())) {
            rules.addAll(factory.getRules(context, phase, pluginId.getType()));
          }
        }
      }
    } catch (InstantiationException | IllegalAccessException e) {
      throw UserException.validationError(e).message("Failure getting plugin rules.").build(logger);
    }

    ImmutableSet<RelOptRule> rulesSet = rules.build();
    return RuleSets.ofList(rulesSet);
  }

  @VisibleForTesting
  public Catalog getSystemUserCatalog() {
    return getCatalog(SchemaConfig.newBuilder(SystemUser.SYSTEM_USERNAME).build());
  }

  /**
   * Proxy class for source modifications.  This allows us to keep all the logic in the service while restricting
   * access to the service methods to the Catalog instances this service generates.
   */
  public class SourceModifier {
    private final String userName;

    public SourceModifier(String userName) {
      this.userName = userName;
    }

    public <T extends StoragePlugin> T getSource(String name) {
      return CatalogServiceImpl.this.getSource(name);
    }

    public void createSource(SourceConfig sourceConfig) {
      CatalogServiceImpl.this.createSource(sourceConfig, userName);
    }

    public void updateSource(SourceConfig sourceConfig) {
      CatalogServiceImpl.this.updateSource(sourceConfig, userName);
    }

    public void deleteSource(SourceConfig sourceConfig) {
      CatalogServiceImpl.this.deleteSource(sourceConfig, userName);
    }
  }
}

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

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import com.dremio.common.DeferredException;
import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.CatalogRPC.RpcType;
import com.dremio.exec.proto.CatalogRPC.SourceWrapper;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.MissingPluginConf;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.ischema.InfoSchemaConf;
import com.dremio.options.OptionManager;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.coordinator.DistributedSemaphore.DistributedLease;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.ModifiableLocalSchedulerService;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import com.dremio.service.scheduler.ScheduleUtils;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SystemUser;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
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
  public static final long CATALOG_SYNC = TimeUnit.MINUTES.toMillis(3);
  private static final long CHANGE_COMMUNICATION_WAIT = TimeUnit.SECONDS.toMillis(5);


  private static final String LOCAL_TASK_LEADER_NAME = "catalogservice";

  public static final String CATALOG_SOURCE_DATA_NAMESPACE = "catalog-source-data";

  protected final Provider<SabotContext> context;
  protected final Provider<SchedulerService> scheduler;
  private final Provider<? extends Provider<ConnectionConf<?, ?>>> sysTableConfProvider;
  private final Provider<FabricService> fabric;
  protected final Provider<ConnectionReader> connectionReaderProvider;

  protected LegacyKVStore<NamespaceKey, SourceInternalData> sourceDataStore;
  protected NamespaceService systemNamespace;
  private PluginsManager plugins;
  private BufferAllocator allocator;
  private FabricRunnerFactory tunnelFactory;
  private CatalogProtocol protocol;
  private final Provider<BufferAllocator> bufferAllocator;
  private final Provider<LegacyKVStoreProvider> kvStoreProvider;
  protected final Provider<DatasetListingService> datasetListingService;
  protected final Provider<OptionManager> optionManager;
  protected final Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider;
  protected final DremioConfig config;
  protected final EnumSet<Role> roles;
  protected final CatalogServiceMonitor monitor;
  private final Set<String> influxSources; //will contain any sources influx(i.e actively being modified). Otherwise empty.
  protected final Predicate<String> isInfluxSource;
  protected ModifiableSchedulerService modifiableSchedulerService;

  public CatalogServiceImpl(
    Provider<SabotContext> context,
    Provider<SchedulerService> scheduler,
    Provider<? extends Provider<ConnectionConf<?, ?>>> sysTableConfProvider,
    Provider<FabricService> fabric,
    Provider<ConnectionReader> connectionReaderProvider,
    Provider<BufferAllocator> bufferAllocator,
    Provider<LegacyKVStoreProvider> kvStoreProvider,
    Provider<DatasetListingService> datasetListingService,
    Provider<OptionManager> optionManager,
    Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider,
    DremioConfig config,
    EnumSet<Role> roles
  ) {
    this(context, scheduler, sysTableConfProvider, fabric, connectionReaderProvider, bufferAllocator,
      kvStoreProvider, datasetListingService, optionManager, broadcasterProvider, config, roles, CatalogServiceMonitor.DEFAULT);
  }

  @VisibleForTesting
  CatalogServiceImpl(
    Provider<SabotContext> context,
    Provider<SchedulerService> scheduler,
    Provider<? extends Provider<ConnectionConf<?, ?>>> sysTableConfProvider,
    Provider<FabricService> fabric,
    Provider<ConnectionReader> connectionReaderProvider,
    Provider<BufferAllocator> bufferAllocator,
    Provider<LegacyKVStoreProvider> kvStoreProvider,
    Provider<DatasetListingService> datasetListingService,
    Provider<OptionManager> optionManager,
    Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider,
    DremioConfig config,
    EnumSet<Role> roles,
    final CatalogServiceMonitor monitor
  ) {
    this.context = context;
    this.scheduler = scheduler;
    this.sysTableConfProvider = sysTableConfProvider;
    this.fabric = fabric;
    this.connectionReaderProvider = connectionReaderProvider;
    this.bufferAllocator = bufferAllocator;
    this.kvStoreProvider = kvStoreProvider;
    this.datasetListingService = datasetListingService;
    this.optionManager = optionManager;
    this.broadcasterProvider = broadcasterProvider;
    this.config = config;
    this.roles = roles;
    this.monitor = monitor;
    this.influxSources = ConcurrentHashMap.newKeySet();
    this.isInfluxSource = this::isInfluxSource;
  }

  @Override
  public void start() throws Exception {
    SabotContext context = this.context.get();
    this.allocator = bufferAllocator.get().newChildAllocator("catalog-protocol", 0, Long.MAX_VALUE);
    this.systemNamespace = context.getNamespaceService(SystemUser.SYSTEM_USERNAME);
    this.sourceDataStore = kvStoreProvider.get().getStore(CatalogSourceDataCreator.class);
    this.modifiableSchedulerService = new ModifiableLocalSchedulerService(1, "modifiable-scheduler-",
        ExecConstants.MAX_CONCURRENT_METADATA_REFRESHES, optionManager.get());
    this.plugins = newPluginsManager();
    plugins.start();
    this.protocol =  new CatalogProtocol(allocator, new CatalogChangeListener(), config.getSabotConfig());
    tunnelFactory = fabric.get().registerProtocol(protocol);

    boolean isDistributedCoordinator = config.isMasterlessEnabled()
      && roles.contains(Role.COORDINATOR);
    if(roles.contains(Role.MASTER) || isDistributedCoordinator) {
      final CountDownLatch wasRun = new CountDownLatch(1);
      final Cancellable task = scheduler.get().schedule(ScheduleUtils.scheduleToRunOnceNow(
        LOCAL_TASK_LEADER_NAME), () -> {
          try {
            if (createSourceIfMissing(new SourceConfig()
              .setConfig(new InfoSchemaConf().toBytesString())
              .setName("INFORMATION_SCHEMA")
              .setType("INFORMATION_SCHEMA")
              .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY))) {
              logger.debug("Refreshing 'INFORMATION_SCHEMA' source");
              try {
                refreshSource(new NamespaceKey("INFORMATION_SCHEMA"), CatalogService.NEVER_REFRESH_POLICY, UpdateType.FULL);
              } catch (NamespaceException e) {
                throw new RuntimeException(e);
              }
            }

            createSourceIfMissing(new SourceConfig()
              .setConnectionConf(sysTableConfProvider.get().get())
              .setName("sys")
              .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY));

            logger.debug("Refreshing 'sys' source");
            try {
              refreshSource(new NamespaceKey("sys"), CatalogService.NEVER_REFRESH_POLICY, UpdateType.FULL);
            } catch (NamespaceException e) {
              throw new RuntimeException(e);
            }
          } finally {
            wasRun.countDown();
          }
        }
      );
      if (!task.isDone()) {
        // wait till task is done only if task leader
        wasRun.await();
      }
    }
  }

  protected PluginsManager newPluginsManager() {
    return new PluginsManager(context.get(), systemNamespace, datasetListingService.get(), optionManager.get(),
      config, roles, sourceDataStore, scheduler.get(), connectionReaderProvider.get(), monitor, broadcasterProvider,
      isInfluxSource, modifiableSchedulerService);
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
      tunnelFactory.getCommandRunner(e.getAddress(), e.getFabricPort()).runCommand(send);
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
        plugins.getSynchronized(config, isInfluxSource);
      } catch (Exception ex) {
        logger.warn("Failure while synchronizing source [{}].", config.getName(), ex);
      }
    }

    void sourceDelete(SourceConfig config) {
      try {
        logger.debug("Received delete source for [{}]", config.getName());

        plugins.closeAndRemoveSource(config);
      } catch (Exception ex) {
        logger.warn("Failure while synchronizing source [{}].", config.getName(), ex);
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

  @VisibleForTesting
  public boolean refreshSource(NamespaceKey source, MetadataPolicy metadataPolicy, UpdateType updateType) throws NamespaceException {
    ManagedStoragePlugin plugin = getPlugins().get(source.getRoot());
    if (plugin == null){
      throw UserException.validationError().message("Unknown source %s", source.getRoot()).build(logger);
    } else if (MissingPluginConf.TYPE.equals(plugin.getConfig().getType())) {
      return false;
    }

    return plugin.refresh(updateType, metadataPolicy);
  }

  @VisibleForTesting
  public void synchronizeSources() {
    getPlugins().synchronizeSources(isInfluxSource);
  }

  @Override
  public void close() throws Exception {
    DeferredException ex = new DeferredException();
    if(getPlugins() != null) {
      ex.suppressingClose(getPlugins());
    }
    ex.suppressingClose(protocol);
    ex.suppressingClose(allocator);
    ex.suppressingClose(modifiableSchedulerService);
    ex.close();
  }

  // used only internally
  private boolean createSourceIfMissing(SourceConfig config, NamespaceAttribute... attributes) {
    Preconditions.checkArgument(config.getTag() == null);
    try {
      return createSourceIfMissingWithThrow(config);
    } catch (ConcurrentModificationException ex) {
      // someone else got there first, ignore this failure.
      logger.error("Two source creations occurred simultaneously, ignoring the failed one.", ex);
      // fall through to false below.
    }
    return false;
  }

  public boolean createSourceIfMissingWithThrow(SourceConfig config) {
    Preconditions.checkArgument(config.getTag() == null);
    if(!getPlugins().hasPlugin(config.getName())) {
      createSource(config, SystemUser.SYSTEM_USERNAME);
      return true;
    }
    return false;
  }

  private void createSource(SourceConfig config, String userName, NamespaceAttribute... attributes) {

    boolean afterUnknownEx = false;

    try(final AutoCloseable sourceDistributedLock = getDistributedLock(config.getName())) {
      setInfluxSource(config.getName());
      getPlugins().create(config, userName, attributes);
      communicateChange(config, RpcType.REQ_SOURCE_CONFIG);
    } catch (SourceAlreadyExistsException e) {
      throw UserException.concurrentModificationError(e).message("Source already exists with name %s.", config.getName()).buildSilently();
    } catch (ConcurrentModificationException ex) {
      throw ex;
    } catch (Exception ex) {
      afterUnknownEx = true;
      throw UserException.validationError(ex).message("Failed to create source with name %s.", config.getName()).buildSilently();
    } finally {
      removeInfluxSource(config.getName(), afterUnknownEx);
    }
  }

  private void updateSource(SourceConfig config, String userName, NamespaceAttribute... attributes) {
    Preconditions.checkArgument(config.getTag() != null);
    boolean afterUnknownEx = false;

    try (final AutoCloseable sourceDistributedLock = getDistributedLock(config.getName());) {
      setInfluxSource(config.getName());
      ManagedStoragePlugin plugin = getPlugins().get(config.getName());
      if (plugin == null) {
        throw UserException.concurrentModificationError().message("Source not found.").buildSilently();
      }
      final String srcType = plugin.getConfig().getType();
      if (("HOME".equalsIgnoreCase(srcType)) || ("INTERNAL".equalsIgnoreCase(srcType)) || ("ACCELERATION".equalsIgnoreCase(srcType))) {
        config.setAllowCrossSourceSelection(true);
      }
      plugin.updateSource(config, userName, attributes);
      communicateChange(config, RpcType.REQ_SOURCE_CONFIG);
    } catch (ConcurrentModificationException ex) {
      throw ex;
    } catch (Exception ex) {
      afterUnknownEx = true;
      throw UserException.validationError(ex).message("Failed to update source with name %s.", config.getName()).buildSilently();
    } finally {
      removeInfluxSource(config.getName(), afterUnknownEx);
    }
  }

  @VisibleForTesting
  public void deleteSource(String name) {
    ManagedStoragePlugin plugin = getPlugins().get(name);
    if(plugin == null) {
      return;
    }
    deleteSource(plugin.getId().getConfig(), SystemUser.SYSTEM_USERNAME);
  }

  /**
   * To delete the source, we need to:
   *   Grab the distributed source lock.
   *   Check that we have a valid config.
   * @param config
   * @param userName
   */
  private void deleteSource(SourceConfig config, String userName) {
    NamespaceService namespaceService = context.get().getNamespaceService(userName);
    boolean afterUnknownEx = false;

    try (AutoCloseable l = getDistributedLock(config.getName()) ) {
      setInfluxSource(config.getName());
      if(!getPlugins().closeAndRemoveSource(config)) {
        throw UserException.invalidMetadataError().message("Unable to remove source as the provided definition is out of date.").buildSilently();
      }

      namespaceService.deleteSource(config.getKey(), config.getTag());
      sourceDataStore.delete(config.getKey());
    } catch (RuntimeException ex) {
      afterUnknownEx = true;
      throw ex;
    } catch (Exception ex) {
      afterUnknownEx = true;
      throw UserException.validationError(ex).message("Failure deleting source [%s].", config.getName()).build(logger);
    } finally {
      removeInfluxSource(config.getName(), afterUnknownEx);
    }

    communicateChange(config, RpcType.REQ_DEL_SOURCE);
  }

  private AutoCloseable getDistributedLock(String sourceName) throws Exception {
    long millis = 15_000;
    DistributedLease lease = context.get().getClusterCoordinator().getSemaphore("-source-" + sourceName.toLowerCase(), 1).acquire(millis, TimeUnit.MILLISECONDS);
    if(lease == null) {
      throw UserException.resourceError().message("Unable to acquire source change lock for source [%s] within timeout.", sourceName).build(logger);
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
    ManagedStoragePlugin plugin = getPlugins().get(id.getName());

    // plugin.matches() here grabs the plugin read lock, guaranteeing that the plugin already exists and has been started
    if(plugin != null && plugin.matches(id.getConfig())) {
      return plugin;
    }

    try {
      logger.debug("Source [{}] does not exist/match the one in-memory, synchronizing from id", id.getName());
      if (isInfluxSource(id.getName())) {
        throw UserException.validationError()
          .message("Source [%s] is being modified. Try again.", id.getName())
          .build(logger);
      }
      return getPlugins().getSynchronized(id.getConfig(), isInfluxSource);
    } catch (Exception e) {
      throw UserException.validationError(e).message("Failure getting source [%s].", id.getName()).build(logger);
    }
  }

  private ManagedStoragePlugin getPlugin(String name, boolean errorOnMissing) {
    ManagedStoragePlugin plugin = getPlugins().get(name);
    if(plugin != null) {
      return plugin;
    }

    try {
      logger.debug("Synchronizing source [{}] with namespace", name);
      if (isInfluxSource(name)) {
        if (!errorOnMissing) {
          return null;
        }
        throw UserException.validationError()
          .message("Source [%s] is being modified. Try again.", name)
          .build(logger);

      }
      SourceConfig config = systemNamespace.getSource(new NamespaceKey(name));
      return getPlugins().getSynchronized(config, isInfluxSource);
    } catch (NamespaceNotFoundException ex) {
      if (!errorOnMissing) {
        return null;
      }
      throw UserException.validationError().message("Tried to access non-existent source [%s].", name).build(logger);
    } catch (Exception ex) {
      throw UserException.validationError(ex).message("Failure while trying to read source [%s].", name).build(logger);
    }
  }

  @VisibleForTesting
  public ManagedStoragePlugin getManagedSource(String name) {
    return getPlugins().get(name);
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

  private boolean isInfluxSource(String source) {
    return influxSources.contains(source);
  }

  private void setInfluxSource(String sourceName) {
    // This is executing inside a Distributed Java semaphore on sourceNameso no one else could have added it
    Preconditions.checkArgument(influxSources.add(sourceName));
  }

  private void removeInfluxSource(String sourceName, boolean skipPreCheck) {
    if (!skipPreCheck) {
      //skip precheck if this is called after an exception
      Preconditions.checkArgument(!influxSources.isEmpty() && influxSources.contains(sourceName));
    }
    // This is executing inside a Distributed Java semaphore on sourceNameso no one else could have removed it
    if (!influxSources.remove(sourceName)) {
      logger.debug("Tried to remove source {} from influxSources. But it was missing. ", sourceName);
    }
  }

  @Override
  public Catalog getCatalog(MetadataRequestOptions requestOptions) {
    Preconditions.checkNotNull(requestOptions,  "request options are required");

    final Catalog decoratedCatalog = SourceAccessChecker.secureIfNeeded(requestOptions, createCatalog(requestOptions));
    return new CachingCatalog(decoratedCatalog);
  }

  protected Catalog createCatalog(MetadataRequestOptions requestOptions) {
    OptionManager optionManager = requestOptions.getSchemaConfig().getOptions();
    if (optionManager == null) {
      optionManager = context.get().getOptionManager();
    }

    return new CatalogImpl(
      requestOptions,
      new Retriever(),
      new SourceModifier(requestOptions.getSchemaConfig().getUserName()),
      optionManager,
      context.get().getNamespaceService(SystemUser.SYSTEM_USERNAME),
      context.get().getNamespaceServiceFactory(),
      context.get().getDatasetListing(),
      context.get().getViewCreatorFactoryProvider().get());
  }

  @Override
  public boolean isSourceConfigMetadataImpacting(SourceConfig config) {
    return getPlugins().get(config.getName()).isSourceConfigMetadataImpacting(config);
  }

  @Override
  public boolean isComplexTypeSupport() {
    return optionManager.get().getOption(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT);
  }

  private class Retriever implements PluginRetriever {

    @Override
    public ManagedStoragePlugin getPlugin(String name, boolean synchronizeOnMissing) {
      final String pluginName = (name.startsWith("@")) ? "__home" : name;
      // Preconditions.checkState(isCoordinator);

      if (!synchronizeOnMissing) {
        // get from in-memory
        return getPlugins().get(pluginName);
      }
      // if plugin is missing in-memory, we will synchronize to kvstore
      return CatalogServiceImpl.this.getPlugin(pluginName, true);
    }

  }

  @Override
  public RuleSet getStorageRules(OptimizerRulesContext context, PlannerPhase phase) {
    final ImmutableSet.Builder<RelOptRule> rules = ImmutableSet.builder();
    final Set<SourceType> types = new HashSet<>();

    try {
      for (ManagedStoragePlugin plugin : getPlugins().managed()) {
        // we want to check state without acquiring a read lock
        if (plugin.getState().getStatus() == SourceState.SourceStatus.bad) {
          // we shouldn't consider rules for misbehaving plugins.
          continue;
        }

        StoragePluginId pluginId;
        try {
          // getId has a check for plugin state
          pluginId = plugin.getId();
        } catch (UserException e) {
          if (e.getErrorType() == ErrorType.SOURCE_BAD_STATE) {
            // we shouldn't consider rules for misbehaving plugins.
            continue;
          }
          throw e;
        }

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
    return getCatalog(MetadataRequestOptions.of(SchemaConfig.newBuilder(SystemUser.SYSTEM_USERNAME).build()));
  }

  @VisibleForTesting
  PluginsManager getPlugins() {
    return plugins;
  }

  public enum UpdateType {
    NAMES, FULL, NONE
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

    public SourceModifier cloneWith(String userName) {
      return new SourceModifier(userName);
    }

    public <T extends StoragePlugin> T getSource(String name) {
      try {
        // Check userNamespace to resolve permissions since CatalogServiceImpl does not
        CatalogServiceImpl.this.context.get().getNamespaceService(userName).getSource(new NamespaceKey(name));
      } catch (NamespaceException ignored) {
        // Other errors should be handled by call to CatalogService
      }
      return CatalogServiceImpl.this.getSource(name);
    }

    public void createSource(SourceConfig sourceConfig, NamespaceAttribute... attributes) {
      CatalogServiceImpl.this.createSource(sourceConfig, userName, attributes);
    }

    public void updateSource(SourceConfig sourceConfig, NamespaceAttribute... attributes) {
      CatalogServiceImpl.this.updateSource(sourceConfig, userName, attributes);
    }

    public void deleteSource(SourceConfig sourceConfig) {
      CatalogServiceImpl.this.deleteSource(sourceConfig, userName);
    }
  }
}

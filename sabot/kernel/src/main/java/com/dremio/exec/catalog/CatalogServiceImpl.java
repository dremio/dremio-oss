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

import com.dremio.catalog.exception.SourceAlreadyExistsException;
import com.dremio.common.DeferredException;
import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogImpl.IdentityResolver;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.proto.CatalogRPC;
import com.dremio.exec.proto.CatalogRPC.RpcType;
import com.dremio.exec.proto.CatalogRPC.SourceWrapper;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos;
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
import com.dremio.service.namespace.NamespaceIdentity;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceUser;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvent;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventTopic;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvents;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusSubscriber;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.SourceNamespaceService;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import java.lang.reflect.InvocationTargetException;
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
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CatalogServiceImpl, manages all sources and exposure of Catalog for table retrieval.
 *
 * <p>Has several helper classes to get its job done: - Catalog is the consumable interface for
 * interacting with sources/storage plugins. - ManagedStoragePlugin keeps tracks the system state
 * for each plugin - PluginsManager is a glorified map that includes all the StoragePugins.
 */
public class CatalogServiceImpl implements CatalogService {
  private static final Logger logger = LoggerFactory.getLogger(CatalogServiceImpl.class);
  public static final long CATALOG_SYNC = TimeUnit.MINUTES.toMillis(3);
  private static final long CHANGE_COMMUNICATION_WAIT = TimeUnit.SECONDS.toMillis(10);

  public static final String CATALOG_SOURCE_DATA_NAMESPACE = "catalog-source-data";

  public static final String SYSTEM_TABLE_SOURCE_NAME = "sys";

  protected final Provider<SabotContext> sabotContext;
  protected final Provider<SchedulerService> scheduler;
  private final Provider<? extends Provider<ConnectionConf<?, ?>>> sysTableConfProvider;
  private final Provider<? extends Provider<ConnectionConf<?, ?>>> sysFlightTableConfProvider;
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
  private final Set<String>
      influxSources; // will contain any sources influx(i.e actively being modified). Otherwise
  // empty.
  protected final Predicate<String> isInfluxSource;
  protected final Provider<ModifiableSchedulerService> modifiableSchedulerServiceProvider;
  protected volatile ModifiableSchedulerService modifiableSchedulerService;
  private final Provider<VersionedDatasetAdapterFactory> versionedDatasetAdapterFactoryProvider;
  private final Provider<CatalogStatusEvents> catalogStatusEventsProvider;

  public CatalogServiceImpl(
      Provider<SabotContext> sabotContext,
      Provider<SchedulerService> scheduler,
      Provider<? extends Provider<ConnectionConf<?, ?>>> sysTableConfProvider,
      Provider<? extends Provider<ConnectionConf<?, ?>>> sysFlightTableConfProvider,
      Provider<FabricService> fabric,
      Provider<ConnectionReader> connectionReaderProvider,
      Provider<BufferAllocator> bufferAllocator,
      Provider<LegacyKVStoreProvider> kvStoreProvider,
      Provider<DatasetListingService> datasetListingService,
      Provider<OptionManager> optionManager,
      Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider,
      DremioConfig config,
      EnumSet<Role> roles,
      Provider<ModifiableSchedulerService> modifiableSchedulerService,
      Provider<VersionedDatasetAdapterFactory> versionedDatasetAdapterFactoryProvider,
      Provider<CatalogStatusEvents> catalogStatusEventsProvider) {
    this(
        sabotContext,
        scheduler,
        sysTableConfProvider,
        sysFlightTableConfProvider,
        fabric,
        connectionReaderProvider,
        bufferAllocator,
        kvStoreProvider,
        datasetListingService,
        optionManager,
        broadcasterProvider,
        config,
        roles,
        CatalogServiceMonitor.DEFAULT,
        modifiableSchedulerService,
        versionedDatasetAdapterFactoryProvider,
        catalogStatusEventsProvider);
  }

  @VisibleForTesting
  CatalogServiceImpl(
      Provider<SabotContext> sabotContext,
      Provider<SchedulerService> scheduler,
      Provider<? extends Provider<ConnectionConf<?, ?>>> sysTableConfProvider,
      Provider<? extends Provider<ConnectionConf<?, ?>>> sysFlightTableConfProvider,
      Provider<FabricService> fabric,
      Provider<ConnectionReader> connectionReaderProvider,
      Provider<BufferAllocator> bufferAllocator,
      Provider<LegacyKVStoreProvider> kvStoreProvider,
      Provider<DatasetListingService> datasetListingService,
      Provider<OptionManager> optionManager,
      Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider,
      DremioConfig config,
      EnumSet<Role> roles,
      final CatalogServiceMonitor monitor,
      Provider<ModifiableSchedulerService> modifiableSchedulerService,
      Provider<VersionedDatasetAdapterFactory> versionedDatasetAdapterFactoryProvider,
      Provider<CatalogStatusEvents> catalogStatusEventsProvider) {
    this.sabotContext = sabotContext;
    this.scheduler = scheduler;
    this.sysTableConfProvider = sysTableConfProvider;
    this.sysFlightTableConfProvider = sysFlightTableConfProvider;
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
    this.versionedDatasetAdapterFactoryProvider = versionedDatasetAdapterFactoryProvider;
    this.influxSources = ConcurrentHashMap.newKeySet();
    this.isInfluxSource = this::isInfluxSource;
    this.modifiableSchedulerServiceProvider = modifiableSchedulerService;
    this.catalogStatusEventsProvider = catalogStatusEventsProvider;
  }

  @Override
  public void start() throws Exception {
    SabotContext context = this.sabotContext.get();
    this.allocator = bufferAllocator.get().newChildAllocator("catalog-protocol", 0, Long.MAX_VALUE);
    this.systemNamespace = context.getNamespaceService(SystemUser.SYSTEM_USERNAME);
    this.sourceDataStore = kvStoreProvider.get().getStore(CatalogSourceDataCreator.class);
    this.modifiableSchedulerService = modifiableSchedulerServiceProvider.get();
    this.modifiableSchedulerService.start();
    this.plugins = newPluginsManager();
    plugins.start();
    this.protocol =
        new CatalogProtocol(allocator, new CatalogChangeListener(), config.getSabotConfig());
    tunnelFactory = fabric.get().registerProtocol(protocol);

    boolean isDistributedCoordinator =
        config.isMasterlessEnabled() && roles.contains(Role.COORDINATOR);
    if (roles.contains(Role.MASTER) || isDistributedCoordinator) {
      final CountDownLatch wasRun = new CountDownLatch(1);
      final String taskName =
          optionManager.get().getOption(ExecConstants.CATALOG_SERVICE_LOCAL_TASK_LEADER_NAME);
      final Cancellable task =
          scheduler
              .get()
              .schedule(
                  Schedule.SingleShotBuilder.now()
                      .asClusteredSingleton(taskName)
                      .inLockStep()
                      .build(),
                  () -> {
                    try {
                      if (createSourceIfMissing(
                          new SourceConfig()
                              .setConfig(new InfoSchemaConf().toBytesString())
                              .setName("INFORMATION_SCHEMA")
                              .setType("INFORMATION_SCHEMA")
                              .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY))) {
                        logger.debug("Refreshing 'INFORMATION_SCHEMA' source");
                        try {
                          refreshSource(
                              new NamespaceKey("INFORMATION_SCHEMA"),
                              CatalogService.NEVER_REFRESH_POLICY,
                              UpdateType.FULL);
                        } catch (NamespaceException e) {
                          throw new RuntimeException(e);
                        }
                      }

                      if (optionManager.get().getOption(ExecConstants.ENABLE_SYSFLIGHT_SOURCE)) {
                        if (sysFlightTableConfProvider != null) {
                          logger.info("Creating SysFlight source plugin.");
                          // check if the sys plugin config type is matching as expected with the
                          // flight config type {SYSFLIGHT},
                          // if not, delete and recreate with flight config.
                          if (getPlugins().get(SYSTEM_TABLE_SOURCE_NAME) != null
                              && !getPlugins()
                                  .get(SYSTEM_TABLE_SOURCE_NAME)
                                  .getConnectionConf()
                                  .getType()
                                  .equals(sysFlightTableConfProvider.get().get().getType())) {
                            deleteSource(SYSTEM_TABLE_SOURCE_NAME);
                          }
                          createSourceIfMissing(
                              new SourceConfig()
                                  .setConnectionConf(sysFlightTableConfProvider.get().get())
                                  .setName(SYSTEM_TABLE_SOURCE_NAME)
                                  .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY));
                        }
                      } else {
                        // check if the sys plugin config type is matching as expected with the old
                        // system table config type {SYS},
                        // if not, delete and recreate with old system table config type.
                        if (getPlugins().get(SYSTEM_TABLE_SOURCE_NAME) != null
                            && !getPlugins()
                                .get(SYSTEM_TABLE_SOURCE_NAME)
                                .getConnectionConf()
                                .getType()
                                .equals(sysTableConfProvider.get().get().getType())) {
                          deleteSource(SYSTEM_TABLE_SOURCE_NAME);
                        }
                        createSourceIfMissing(
                            new SourceConfig()
                                .setConnectionConf(sysTableConfProvider.get().get())
                                .setName(SYSTEM_TABLE_SOURCE_NAME)
                                .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY));
                      }

                      logger.debug("Refreshing {} source", SYSTEM_TABLE_SOURCE_NAME);
                      try {
                        refreshSource(
                            new NamespaceKey(SYSTEM_TABLE_SOURCE_NAME),
                            CatalogService.NEVER_REFRESH_POLICY,
                            UpdateType.FULL);
                      } catch (NamespaceException e) {
                        throw new RuntimeException(e);
                      }
                    } finally {
                      wasRun.countDown();
                    }
                  });
      if (task.isScheduled()) {
        // wait till task is done only if the task is scheduled locally
        logger.debug("Awaiting local lock step schedule completion for task {}", taskName);
        wasRun.await();
        logger.debug("Lock step schedule completed for task {}", taskName);
      } else {
        logger.debug("Lock step schedule for task {} completed remotely", taskName);
      }
    }
  }

  protected PluginsManager newPluginsManager() {
    return new PluginsManager(
        sabotContext.get(),
        systemNamespace,
        sabotContext.get().getOrphanageFactory().get(),
        datasetListingService.get(),
        optionManager.get(),
        config,
        sourceDataStore,
        scheduler.get(),
        connectionReaderProvider.get(),
        monitor,
        broadcasterProvider,
        isInfluxSource,
        modifiableSchedulerService,
        kvStoreProvider);
  }

  public void communicateChange(SourceConfig config, RpcType rpcType) {
    final Set<NodeEndpoint> endpoints = new HashSet<>();
    endpoints.add(sabotContext.get().getEndpoint());

    List<RpcFuture<Ack>> futures = new ArrayList<>();
    SourceWrapper wrapper =
        SourceWrapper.newBuilder()
            .setBytes(
                ByteString.copyFrom(
                    ProtobufIOUtil.toByteArray(
                        config, SourceConfig.getSchema(), LinkedBuffer.allocate())))
            .build();
    for (NodeEndpoint e :
        Iterables.concat(
            this.sabotContext.get().getCoordinators(), this.sabotContext.get().getExecutors())) {
      if (!endpoints.add(e)) {
        continue;
      }

      SendSource send = new SendSource(wrapper, rpcType);
      tunnelFactory.getCommandRunner(e.getAddress(), e.getFabricPort()).runCommand(send);
      logger.info("Sending [{}] to {}:{}", config.getName(), e.getAddress(), e.getUserPort());
      futures.add(send.getFuture());
    }

    try {
      Futures.successfulAsList(futures).get(CHANGE_COMMUNICATION_WAIT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e1) {
      // Error is ignored here as plugin propagation is best effort
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
        logger.info("Received source update for [{}]", config.getName());
        plugins.getSynchronized(config, isInfluxSource);
      } catch (Exception ex) {
        logger.warn("Failure while synchronizing source [{}].", config.getName(), ex);
      }
    }

    void sourceDelete(SourceConfig config) {
      try {
        logger.info("Received delete source for [{}]", config.getName());

        plugins.closeAndRemoveSource(config);
      } catch (Exception ex) {
        logger.warn("Failure while synchronizing source [{}].", config.getName(), ex);
      }
    }
  }

  /**
   * Delete all sources except those that are specifically referenced
   *
   * @param rootsToSaveSet Root sources to save.
   * @throws NamespaceException
   */
  @VisibleForTesting
  public void deleteExcept(Set<String> rootsToSaveSet) throws NamespaceException {
    for (SourceConfig source : systemNamespace.getSources()) {
      if (rootsToSaveSet.contains(source.getName())) {
        continue;
      }

      deleteSource(source, CatalogUser.from(SystemUser.SYSTEM_USERNAME), null);
    }
  }

  @VisibleForTesting
  public boolean refreshSource(
      NamespaceKey source, MetadataPolicy metadataPolicy, UpdateType updateType)
      throws NamespaceException {
    ManagedStoragePlugin plugin = getPlugins().get(source.getRoot());
    if (plugin == null) {
      throw UserException.validationError()
          .message("Unknown source %s", source.getRoot())
          .build(logger);
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
    if (getPlugins() != null) {
      ex.suppressingClose(getPlugins());
    }
    ex.suppressingClose(protocol);
    ex.suppressingClose(allocator);
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

  @Override
  public boolean createSourceIfMissingWithThrow(SourceConfig config) {
    Preconditions.checkArgument(config.getTag() == null);
    if (!getPlugins().hasPlugin(config.getName())) {
      createSource(config, CatalogUser.from(SystemUser.SYSTEM_USERNAME));
      return true;
    }
    return false;
  }

  private void createSource(
      SourceConfig config, CatalogIdentity subject, NamespaceAttribute... attributes) {
    boolean afterUnknownEx = false;

    try (final AutoCloseable sourceDistributedLock = getDistributedLock(config.getName())) {
      logger.debug("Obtained distributed lock for source {}", "-source-" + config.getName());
      setInfluxSource(config.getName());
      getPlugins().create(config, subject.getName(), attributes);
      communicateChange(config, RpcType.REQ_SOURCE_CONFIG);
    } catch (SourceAlreadyExistsException e) {
      throw UserException.concurrentModificationError(e)
          .message("Source already exists with name %s.", config.getName())
          .buildSilently();
    } catch (ConcurrentModificationException ex) {
      throw ex;
    } catch (UserException ue) {
      // If it's a UserException, message is probably helpful, so rethrow
      throw ue;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception ex) {
      afterUnknownEx = true;
      logger.error("Exception encountered: {}", ex.getMessage(), ex);
      throw UserException.validationError(ex)
          .message("Failed to create source with name %s.", config.getName())
          .buildSilently();
    } finally {
      logger.debug("Releasing distributed lock for source {}", "-source-" + config.getName());
      removeInfluxSource(config.getName(), afterUnknownEx);
    }
  }

  private void updateSource(
      SourceConfig config, CatalogIdentity subject, NamespaceAttribute... attributes) {
    Preconditions.checkArgument(config.getTag() != null);
    boolean afterUnknownEx = false;

    try (final AutoCloseable sourceDistributedLock = getDistributedLock(config.getName()); ) {
      logger.debug("Obtained distributed lock for source {}", "-source-" + config.getName());
      setInfluxSource(config.getName());
      ManagedStoragePlugin plugin = getPlugins().get(config.getName());
      if (plugin == null) {
        throw UserException.concurrentModificationError()
            .message("Source not found.")
            .buildSilently();
      }
      final String srcType = plugin.getConfig().getType();
      if (("HOME".equalsIgnoreCase(srcType))
          || ("INTERNAL".equalsIgnoreCase(srcType))
          || ("ACCELERATION".equalsIgnoreCase(srcType))) {
        config.setAllowCrossSourceSelection(true);
      }
      plugin.updateSource(config, subject.getName(), attributes);
      communicateChange(config, RpcType.REQ_SOURCE_CONFIG);
    } catch (ConcurrentModificationException ex) {
      throw ex;
    } catch (Exception ex) {
      afterUnknownEx = true;
      throw UserException.validationError(ex)
          .message("Failed to update source with name %s.", config.getName())
          .buildSilently();
    } finally {
      logger.debug("Releasing distributed lock for source {}", "-source-" + config.getName());
      removeInfluxSource(config.getName(), afterUnknownEx);
    }
  }

  @VisibleForTesting
  public void deleteSource(String name) {
    ManagedStoragePlugin plugin = getPlugins().get(name);
    if (plugin == null) {
      return;
    }
    deleteSource(plugin.getId().getConfig(), CatalogUser.from(SystemUser.SYSTEM_USERNAME), null);
  }

  /**
   * To delete the source, we need to: Grab the distributed source lock. Check that we have a valid
   * config.
   *
   * @param config
   * @param subject
   */
  @WithSpan
  private void deleteSource(
      SourceConfig config,
      CatalogIdentity subject,
      SourceNamespaceService.DeleteCallback callback) {
    NamespaceService namespaceService = sabotContext.get().getNamespaceService(subject.getName());
    boolean afterUnknownEx = false;

    try (AutoCloseable l = getDistributedLock(config.getName())) {
      logger.debug("Obtained distributed lock for source {}", "-source-" + config.getName());
      setInfluxSource(config.getName());
      if (!getPlugins().closeAndRemoveSource(config)) {
        throw UserException.invalidMetadataError()
            .message("Unable to remove source as the provided definition is out of date.")
            .buildSilently();
      }

      namespaceService.deleteSourceWithCallBack(config.getKey(), config.getTag(), callback);
      sourceDataStore.delete(config.getKey());
    } catch (RuntimeException ex) {
      afterUnknownEx = true;
      throw ex;
    } catch (Exception ex) {
      afterUnknownEx = true;
      throw UserException.validationError(ex)
          .message("Failure deleting source [%s].", config.getName())
          .build(logger);
    } finally {
      logger.debug("Releasing distributed lock for source {}", "-source-" + config.getName());
      removeInfluxSource(config.getName(), afterUnknownEx);
    }

    communicateChange(config, RpcType.REQ_DEL_SOURCE);
  }

  private AutoCloseable getDistributedLock(String sourceName) throws Exception {
    long millis = 15_000;
    DistributedLease lease =
        sabotContext
            .get()
            .getClusterCoordinator()
            .getSemaphore("-source-" + sourceName.toLowerCase(), 1)
            .acquire(millis, TimeUnit.MILLISECONDS);
    if (lease == null) {
      throw UserException.resourceError()
          .message(
              "Unable to acquire source change lock for source [%s] within timeout.", sourceName)
          .build(logger);
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

    // plugin.matches() here grabs the plugin read lock, guaranteeing that the plugin already exists
    // and has been started
    if (plugin != null && plugin.matches(id.getConfig())) {
      return plugin;
    }

    try {
      logger.debug(
          "Source [{}] does not exist/match the one in-memory, synchronizing from id",
          id.getName());
      if (isInfluxSource(id.getName())) {
        throw UserException.validationError()
            .message("Source [%s] is being modified. Try again.", id.getName())
            .build(logger);
      }
      return getPlugins().getSynchronized(id.getConfig(), isInfluxSource);
    } catch (Exception e) {
      logger.error("Failed to get source", e);
      throw UserException.validationError(e)
          .message("Failure getting source [%s].", id.getName())
          .build(logger);
    }
  }

  private ManagedStoragePlugin getPlugin(String name, boolean errorOnMissing) {
    ManagedStoragePlugin plugin = getPlugins().get(name);

    final boolean pluginFoundInPlugins = plugin != null;
    Span.current()
        .setAttribute(
            "Catalog.CatalogServiceImpl.getPlugin.pluginFoundInPlugins", pluginFoundInPlugins);

    if (pluginFoundInPlugins) {
      return plugin;
    }

    try {
      logger.debug("Synchronizing source [{}] with namespace", name);
      final boolean isSourceAnInfluxSource = isInfluxSource(name);
      Span.current()
          .setAttribute(
              "Catalog.CatalogServiceImpl.getPlugin.isInfluxSource", isSourceAnInfluxSource);

      if (isSourceAnInfluxSource) {
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
      throw UserException.validationError(ex)
          .message("Tried to access non-existent source [%s].", name)
          .build(logger);
    } catch (Exception ex) {
      throw UserException.validationError(ex)
          .message("Failure while trying to read source [%s].", name)
          .build(logger);
    }
  }

  @Override
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
  @WithSpan
  public SourceState getSourceState(String name) {
    try {
      ManagedStoragePlugin plugin = getPlugin(name, false);
      if (plugin == null) {
        return SourceState.badState(
            String.format("Source %s could not be found. Please verify the source name.", name),
            "Unable to find source.");
      }
      return plugin.getState();
    } catch (Exception e) {
      return SourceState.badState("", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends StoragePlugin> T getSource(String name) {
    return (T) getPlugin(name, true).unwrap(StoragePlugin.class);
  }

  private boolean isInfluxSource(String source) {
    return influxSources.contains(source);
  }

  private void setInfluxSource(String sourceName) {
    // This is executing inside a Distributed Java semaphore on sourceNameso no one else could have
    // added it
    Preconditions.checkArgument(influxSources.add(sourceName));
  }

  private void removeInfluxSource(String sourceName, boolean skipPreCheck) {
    if (!skipPreCheck) {
      // skip precheck if this is called after an exception
      Preconditions.checkArgument(!influxSources.isEmpty() && influxSources.contains(sourceName));
    }
    // This is executing inside a Distributed Java semaphore on sourceNameso no one else could have
    // removed it
    if (!influxSources.remove(sourceName)) {
      logger.debug(
          "Tried to remove source {} from influxSources. But it was missing. ", sourceName);
    }
  }

  @Override
  public Catalog getCatalog(MetadataRequestOptions requestOptions) {
    Preconditions.checkNotNull(requestOptions, "request options are required");

    final Catalog decoratedCatalog =
        SourceAccessChecker.secureIfNeeded(requestOptions, createCatalog(requestOptions));
    return new CachingCatalog(decoratedCatalog);
  }

  protected Catalog createCatalog(MetadataRequestOptions requestOptions) {
    return createCatalog(
        requestOptions,
        new CatalogIdentityResolver(),
        sabotContext.get().getNamespaceServiceFactory());
  }

  protected Catalog createCatalog(
      MetadataRequestOptions requestOptions,
      IdentityResolver identityProvider,
      NamespaceService.Factory namespaceServiceFactory) {
    OptionManager optionManager = requestOptions.getSchemaConfig().getOptions();
    if (optionManager == null) {
      optionManager = sabotContext.get().getOptionManager();
    }

    PluginRetriever retriever = new Retriever();

    return new CatalogImpl(
        requestOptions,
        retriever,
        new SourceModifier(requestOptions.getSchemaConfig().getAuthContext().getSubject()),
        optionManager,
        sabotContext.get().getNamespaceService(SystemUser.SYSTEM_USERNAME),
        namespaceServiceFactory,
        sabotContext.get().getOrphanageFactory().get(),
        sabotContext.get().getDatasetListing(),
        sabotContext.get().getViewCreatorFactoryProvider().get(),
        identityProvider,
        new VersionContextResolverImpl(retriever),
        catalogStatusEventsProvider.get(),
        versionedDatasetAdapterFactoryProvider.get());
  }

  @Override
  @WithSpan
  public boolean isSourceConfigMetadataImpacting(SourceConfig config) {
    return getPlugins().get(config.getName()).isSourceConfigMetadataImpacting(config);
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

    @Override
    public Stream<VersionedPlugin> getAllVersionedPlugins() {
      return getPlugins().getAllVersionedPlugins();
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
        if (factory != null) {
          // add instance level rules.
          rules.addAll(factory.getRules(context, phase, pluginId));

          // add type level rules.
          if (types.add(pluginId.getType())) {
            rules.addAll(factory.getRules(context, phase, pluginId.getType()));
          }
        }
      }
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw UserException.validationError(e).message("Failure getting plugin rules.").build(logger);
    }

    ImmutableSet<RelOptRule> rulesSet = rules.build();
    return RuleSets.ofList(rulesSet);
  }

  public Catalog getSystemUserCatalog() {
    return getCatalog(
        MetadataRequestOptions.of(
            SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build()));
  }

  PluginsManager getPlugins() {
    return plugins;
  }

  public enum UpdateType {
    NAMES,
    FULL,
    NONE
  }

  /**
   * Proxy class for source modifications. This allows us to keep all the logic in the service while
   * restricting access to the service methods to the Catalog instances this service generates.
   */
  public class SourceModifier {
    private final CatalogIdentity subject;

    public SourceModifier(CatalogIdentity subject) {
      this.subject = subject;
    }

    public SourceModifier cloneWith(CatalogIdentity subject) {
      return new SourceModifier(subject);
    }

    public <T extends StoragePlugin> T getSource(String name) {
      return CatalogServiceImpl.this.getSource(name);
    }

    public void createSource(SourceConfig sourceConfig, NamespaceAttribute... attributes) {
      CatalogServiceImpl.this.createSource(sourceConfig, subject, attributes);
    }

    public void updateSource(SourceConfig sourceConfig, NamespaceAttribute... attributes) {
      CatalogServiceImpl.this.updateSource(sourceConfig, subject, attributes);
    }

    public void deleteSource(
        SourceConfig sourceConfig, SourceNamespaceService.DeleteCallback callback) {
      CatalogServiceImpl.this.deleteSource(sourceConfig, subject, callback);
    }
  }

  private class CatalogIdentityResolver implements IdentityResolver {
    @Override
    public CatalogIdentity getOwner(List<String> path) throws NamespaceException {
      NameSpaceContainer nameSpaceContainer =
          systemNamespace.getEntityByPath(new NamespaceKey(path));
      if (null == nameSpaceContainer) {
        return null;
      }
      switch (nameSpaceContainer.getType()) {
        case DATASET:
          {
            final DatasetConfig dataset = nameSpaceContainer.getDataset();
            if (dataset.getType() == DatasetType.VIRTUAL_DATASET) {
              return null;
            } else {
              return new CatalogUser(dataset.getOwner());
            }
          }
        case FUNCTION:
          {
            return null;
          }
        default:
          throw new RuntimeException(
              "Unexpected type for getOwner " + nameSpaceContainer.getType());
      }
    }

    @Override
    public NamespaceIdentity toNamespaceIdentity(CatalogIdentity identity) {
      if (identity instanceof CatalogUser) {
        if (identity.getName().equals(SystemUser.SYSTEM_USERNAME)) {
          return new NamespaceUser(() -> SystemUser.SYSTEM_USER);
        }

        try {
          final User user = sabotContext.get().getUserService().getUser(identity.getName());
          return new NamespaceUser(() -> user);
        } catch (UserNotFoundException ignored) {
        }
      }

      return null;
    }
  }

  @Override
  public void communicateChangeToExecutors(
      List<CoordinationProtos.NodeEndpoint> nodeEndpointList,
      SourceConfig config,
      CatalogRPC.RpcType rpcType) {
    List<RpcFuture<GeneralRPCProtos.Ack>> futures = new ArrayList<>();
    CatalogRPC.SourceWrapper wrapper =
        CatalogRPC.SourceWrapper.newBuilder()
            .setBytes(
                ByteString.copyFrom(
                    ProtobufIOUtil.toByteArray(
                        config, SourceConfig.getSchema(), LinkedBuffer.allocate())))
            .build();

    for (CoordinationProtos.NodeEndpoint e : nodeEndpointList) {
      SendSource send = new SendSource(wrapper, rpcType);
      tunnelFactory.getCommandRunner(e.getAddress(), e.getFabricPort()).runCommand(send);
      logger.info("Sending [{}] to {}:{}", config.getName(), e.getAddress(), e.getUserPort());
      futures.add(send.getFuture());
    }

    try {
      Futures.successfulAsList(futures).get(CHANGE_COMMUNICATION_WAIT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e1) {
      // Error is ignored here as plugin propagation is best effort
      logger.warn("Failure while communicating source change [{}].", config.getName(), e1);
    }
  }

  @Override
  public Stream<VersionedPlugin> getAllVersionedPlugins() {
    return getPlugins().getAllVersionedPlugins();
  }

  @Override
  public void subscribe(CatalogStatusEventTopic topic, CatalogStatusSubscriber subscriber) {
    catalogStatusEventsProvider.get().subscribe(topic, subscriber);
  }

  @Override
  public void publish(CatalogStatusEvent event) {
    catalogStatusEventsProvider.get().publish(event);
  }
}

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
package com.dremio.exec.server;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.zookeeper.Environment;

import com.dremio.common.GuiceServiceModule;
import com.dremio.common.StackTrace;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.context.RequestContext;
import com.dremio.context.TenantContext;
import com.dremio.context.UserContext;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.InformationSchemaServiceImpl;
import com.dremio.exec.catalog.MetadataRefreshInfoBroadcaster;
import com.dremio.exec.exception.NodeStartupException;
import com.dremio.exec.maestro.MaestroForwarder;
import com.dremio.exec.maestro.MaestroService;
import com.dremio.exec.maestro.MaestroServiceImpl;
import com.dremio.exec.maestro.NoOpMaestroForwarder;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.OptionChangeBroadcaster;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.OptionNotificationService;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.service.executor.ExecutorServiceProductClientFactory;
import com.dremio.exec.service.jobresults.JobResultsSoftwareClientFactory;
import com.dremio.exec.service.jobtelemetry.JobTelemetrySoftwareClientFactory;
import com.dremio.exec.service.maestro.MaestroGrpcServerFacade;
import com.dremio.exec.service.maestro.MaestroSoftwareClientFactory;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.util.GuavaPatcher;
import com.dremio.exec.work.WorkStats;
import com.dremio.exec.work.protector.ForemenTool;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.exec.work.rpc.CoordTunnelCreator;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.resource.ClusterResourceInformation;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.QueryCancelTool;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.basic.BasicResourceAllocator;
import com.dremio.sabot.exec.ExecToCoordTunnelCreator;
import com.dremio.sabot.exec.FragmentWorkManager;
import com.dremio.sabot.exec.TaskPoolInitializer;
import com.dremio.sabot.exec.WorkloadTicketDepot;
import com.dremio.sabot.exec.WorkloadTicketDepotService;
import com.dremio.sabot.exec.context.ContextInformationFactory;
import com.dremio.sabot.op.common.spill.SpillServiceOptionsImpl;
import com.dremio.sabot.rpc.CoordExecService;
import com.dremio.sabot.rpc.ExecToCoordResultsHandler;
import com.dremio.sabot.rpc.ExecToCoordStatusHandler;
import com.dremio.sabot.rpc.user.UserServer;
import com.dremio.sabot.task.TaskPool;
import com.dremio.security.CredentialsService;
import com.dremio.service.BindingCreator;
import com.dremio.service.BindingProvider;
import com.dremio.service.SingletonRegistry;
import com.dremio.service.catalog.InformationSchemaServiceGrpc;
import com.dremio.service.catalog.InformationSchemaServiceGrpc.InformationSchemaServiceBlockingStub;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.commandpool.CommandPoolFactory;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.conduit.client.ConduitProviderImpl;
import com.dremio.service.conduit.server.ConduitServer;
import com.dremio.service.conduit.server.ConduitServiceRegistry;
import com.dremio.service.conduit.server.ConduitServiceRegistryImpl;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ExecutorSetService;
import com.dremio.service.coordinator.LocalExecutorSetService;
import com.dremio.service.execselector.ExecutorSelectionService;
import com.dremio.service.execselector.ExecutorSelectionServiceImpl;
import com.dremio.service.execselector.ExecutorSelectorFactory;
import com.dremio.service.execselector.ExecutorSelectorFactoryImpl;
import com.dremio.service.execselector.ExecutorSelectorProvider;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.dremio.service.grpc.GrpcChannelBuilderFactory;
import com.dremio.service.grpc.GrpcServerBuilderFactory;
import com.dremio.service.grpc.MultiTenantGrpcServerBuilderFactory;
import com.dremio.service.grpc.SingleTenantGrpcChannelBuilderFactory;
import com.dremio.service.jobresults.client.JobResultsClientFactory;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.service.jobtelemetry.client.JobTelemetryExecutorClientFactory;
import com.dremio.service.jobtelemetry.server.LocalJobTelemetryServer;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.listing.DatasetListingServiceImpl;
import com.dremio.service.maestroservice.MaestroClientFactory;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.spill.SpillService;
import com.dremio.service.spill.SpillServiceImpl;
import com.dremio.service.users.UserService;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.dremio.telemetry.utils.TracerFacade;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import com.google.inject.util.Providers;

import io.opentracing.Tracer;

/**
 * Test class to start execution framework without ui.
 */
public class SabotNode implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SabotNode.class);

  static {
    /*
     * HBase client uses older version of Guava's Stopwatch API,
     * while Dremio ships with 18.x which has changes the scope of
     * these API to 'package', this code make them accessible.
     */
    GuavaPatcher.patch();
    Environment.logEnv("SabotNode environment: ", logger);
  }

  private boolean isClosed = false;

  private final SingletonRegistry registry = new SingletonRegistry();

  private Injector injector;

  private ShutdownThread shutdownHook;
  private GuiceServiceModule guiceSingletonHandler;
  private List<AbstractModule> overrideModules;

  public SabotNode(
          final SabotConfig config,
          final ClusterCoordinator clusterCoordinator) throws Exception {
    this(config, clusterCoordinator, ClassPathScanner.fromPrescan(config), true);
  }

  @VisibleForTesting
  public SabotNode(
          final SabotConfig config,
          final ClusterCoordinator clusterCoordinator,
          final ScanResult classpathScan,
          boolean allRoles) throws Exception {
    init(registry, config, Preconditions.checkNotNull(clusterCoordinator), classpathScan, allRoles, null);
  }

  @VisibleForTesting
  public SabotNode(
          final SabotConfig config,
          final ClusterCoordinator clusterCoordinator,
          final ScanResult classpathScan,
          boolean allRoles,
          List<AbstractModule> overrideModules) throws Exception {
    init(registry, config, Preconditions.checkNotNull(clusterCoordinator), classpathScan, allRoles, overrideModules);
  }

  protected void init(
          SingletonRegistry registry,
          SabotConfig config,
          ClusterCoordinator clusterCoordinator,
          ScanResult classpathScan,
          boolean allRoles,
          List<AbstractModule> overrideModules) throws Exception {
    this.overrideModules = overrideModules;
    DremioConfig dremioConfig = DremioConfig.create(null, config);
    dremioConfig = dremioConfig.withValue(DremioConfig.ENABLE_COORDINATOR_BOOL, allRoles);

    final BootStrapContext bootstrap = new BootStrapContext(dremioConfig, classpathScan, registry);
    guiceSingletonHandler = new GuiceServiceModule();
    injector = createInjector(dremioConfig, classpathScan, registry, clusterCoordinator, allRoles, bootstrap);
    registry.registerGuiceInjector(injector);
  }

  protected Injector createInjector(DremioConfig config, ScanResult classpathScan, SingletonRegistry registry,
                                    ClusterCoordinator clusterCoordinator, boolean allRoles,
                                    BootStrapContext bootStrapContext) {
    final SabotModule sabotModule = new SabotModule(config, classpathScan, registry, clusterCoordinator, allRoles, bootStrapContext);

    return createInjector(sabotModule);
  }

  protected Injector createInjector(Module mainModule) {
    if (overrideModules != null && !overrideModules.isEmpty()) {
      return Guice.createInjector(Modules.override(guiceSingletonHandler, mainModule).with(overrideModules));
    }

    return Guice.createInjector(guiceSingletonHandler, mainModule);
  }

  protected List<AbstractModule> getOverrideModules() {
    return overrideModules;
  }

  public LocalQueryExecutor getLocalQueryExecutor(){
    return injector.getInstance(LocalQueryExecutor.class);
  }

  public void run() throws Exception {
    final Stopwatch w = Stopwatch.createStarted();
    logger.debug("Startup begun.");

    registry.start();

    shutdownHook = new ShutdownThread(this, new StackTrace());
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    logger.info("Startup completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
  }

  @Override
  public synchronized void close() {
    if (shutdownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (IllegalStateException e) {
        logger.info("Cannot cancel shutdown hook, VM already exiting", e);
      }
    }
    doClose();
  }

  private void doClose() {
    // avoid complaints about double closing
    if (isClosed) {
      return;
    }
    final Stopwatch w = Stopwatch.createStarted();
    logger.debug("Shutdown begun.");

    // wait for anything that is running to complete

    try {
      registry.close();
      guiceSingletonHandler.close(getInjector());
    } catch(Exception e) {
      logger.warn("Failure on close()", e);
    }

    logger.info("Shutdown completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
    isClosed = true;
  }


  /**
   * Shutdown hook for SabotNode. Closes the node, and reports on errors that
   * occur during closure, as well as the location the node was started from.
   */
  private static class ShutdownThread extends Thread {
    private final static AtomicInteger idCounter = new AtomicInteger(0);
    private final SabotNode node;
    private final StackTrace stackTrace;

    /**
     * Constructor.
     *
     * @param node the node to close down
     * @param stackTrace the stack trace from where the SabotNode was started;
     *   use new StackTrace() to generate this
     */
    public ShutdownThread(final SabotNode node, final StackTrace stackTrace) {
      this.node = node;
      this.stackTrace = stackTrace;
      /*
       * TODO should we try to determine a test class name?
       * See https://blogs.oracle.com/tor/entry/how_to_determine_the_junit
       */

      setName("SabotNode-ShutdownHook#" + idCounter.getAndIncrement());
    }

    @Override
    public void run() {
      logger.info("Received shutdown request.");
      try {
        /*
         * We can avoid metrics deregistration concurrency issues by only closing
         * one node at a time. To enforce that, we synchronize on a convenient
         * singleton object.
         */
        synchronized(idCounter) {
          node.doClose();
        }
      } catch(final Exception e) {
        throw new RuntimeException("Caught exception closing SabotNode started from\n" + stackTrace, e);
      }
    }
  }

  @VisibleForTesting
  public SabotContext getContext() {
    return injector.getInstance(SabotContext.class);
  }

  @VisibleForTesting
  public BindingCreator getBindingCreator(){
    return registry.getBindingCreator();
  }

  @VisibleForTesting
  public BindingProvider getBindingProvider(){
    return registry.getBindingProvider();
  }

  @VisibleForTesting
  public Injector getInjector() {
    return injector;
  }

  public static void main(final String[] cli) throws NodeStartupException {
    final StartupOptions options = StartupOptions.parse(cli);
    start(options);
  }

  public static SabotNode start(final StartupOptions options) throws NodeStartupException {
    return start(SabotConfig.create(options.getConfigLocation()), null);
  }

  public static SabotNode start(final SabotConfig config) throws NodeStartupException {
    return start(config, null);
  }

  public static SabotNode start(final SabotConfig config, final ClusterCoordinator clusterCoordinator)
          throws NodeStartupException {
    return start(config, clusterCoordinator, ClassPathScanner.fromPrescan(config));
  }

  public static SabotNode start(final SabotConfig config, final ClusterCoordinator clusterCoordinator, ScanResult classpathScan)
          throws NodeStartupException {
    logger.debug("Starting new SabotNode.");
    SabotNode bit;
    try {
      bit = new SabotNode(config, clusterCoordinator, classpathScan, true);
    } catch (final Exception ex) {
      throw new NodeStartupException("Failure while initializing values in SabotNode.", ex);
    }

    try {
      bit.run();
    } catch (final Exception e) {
      bit.close();
      throw new NodeStartupException("Failure during initial startup of SabotNode.", e);
    }
    logger.debug("Started new SabotNode.");
    return bit;
  }

  /**
   * Guice Module for SabotNode dependancy injection
   */
  protected class SabotModule extends AbstractModule {
    private final DremioConfig config;
    private final ScanResult classpathScan;
    private final SingletonRegistry registry;
    private final ClusterCoordinator clusterCoordinator;
    private final boolean allRoles;
    private final BootStrapContext bootstrap;
    private final RequestContext defaultRequestContext;

    public SabotModule(DremioConfig config, ScanResult classpathScan, SingletonRegistry registry,
                       ClusterCoordinator clusterCoordinator, boolean allRoles, BootStrapContext bootstrap) {
      this.config = config;
      this.classpathScan = classpathScan;
      this.registry = registry;
      this.clusterCoordinator = clusterCoordinator;
      this.allRoles = allRoles;
      this.defaultRequestContext = RequestContext.empty()
          .with(TenantContext.CTX_KEY, TenantContext.DEFAULT_SERVICE_CONTEXT)
          .with(UserContext.CTX_KEY, new UserContext(SYSTEM_USERNAME));
      this.bootstrap = bootstrap;
    }

    @Override
    protected void configure() {
      try {
        bind(DremioConfig.class).toInstance(config);
        bind(Tracer.class).toInstance(TracerFacade.INSTANCE);
        bind(BootStrapContext.class).toInstance(bootstrap);
        bind(BufferAllocator.class).toInstance(bootstrap.getAllocator());
        bind(ExecutorService.class).toInstance(bootstrap.getExecutor());
        bind(LogicalPlanPersistence.class).toInstance(new LogicalPlanPersistence(config.getSabotConfig(), classpathScan));
        bind(OptionValidatorListing.class).toInstance(new OptionValidatorListingImpl(classpathScan));

        // KVStore
        final KVStoreProvider kvStoreProvider = new LocalKVStoreProvider(classpathScan, null, true, true);
        bind(KVStoreProvider.class).toInstance(kvStoreProvider);

        bind(ConnectionReader.class).toInstance(ConnectionReader.of(classpathScan, config.getSabotConfig()));
        // bind default providers.
        bind(MaterializationDescriptorProvider.class).toInstance(MaterializationDescriptorProvider.EMPTY);
        bind(QueryObserverFactory.class).toInstance(QueryObserverFactory.DEFAULT);
        bind(GrpcChannelBuilderFactory.class).toInstance(
          new SingleTenantGrpcChannelBuilderFactory(TracerFacade.INSTANCE, () -> defaultRequestContext,
            () -> Maps.newHashMap() ));

        GrpcServerBuilderFactory gRpcServerBuilderFactory =
          new MultiTenantGrpcServerBuilderFactory(TracerFacade.INSTANCE);
        bind(GrpcServerBuilderFactory.class).toInstance(gRpcServerBuilderFactory);

        // no authentication
        bind(UserService.class).toProvider(Providers.of(null));
        bind(NamespaceService.Factory.class).to(NamespaceServiceImpl.Factory.class);

        final boolean allowPortHunting = true;
        final boolean useIP = false;

        final ConduitServiceRegistry conduitServiceRegistry = new ConduitServiceRegistryImpl();
        bind(ConduitServiceRegistry.class).toInstance(conduitServiceRegistry);

        bind(NodeRegistration.class).asEagerSingleton();

        final MetadataRefreshInfoBroadcaster broadcaster = new MetadataRefreshInfoBroadcaster(
          getProvider(ConduitProvider.class),
          () -> registry.provider(ClusterCoordinator.class)
            .get()
            .getServiceSet(ClusterCoordinator.Role.COORDINATOR)
            .getAvailableEndpoints(),
          () -> registry.provider(SabotContext.class).get().getEndpoint()
        );

        // CatalogService is expensive to create so we eagerly create it
        bind(CatalogService.class).toInstance(new CatalogServiceImpl(
                getProvider(SabotContext.class),
                getProvider(SchedulerService.class),
                getProvider(SystemTablePluginConfigProvider.class),
                getProvider(FabricService.class),
                getProvider(ConnectionReader.class),
                getProvider(BufferAllocator.class),
                getProvider(LegacyKVStoreProvider.class),
                getProvider(DatasetListingService.class),
                getProvider(OptionManager.class),
                () -> broadcaster,
                config,
                EnumSet.allOf(ClusterCoordinator.Role.class)
        ));

        conduitServiceRegistry.registerService(new InformationSchemaServiceImpl(getProvider(CatalogService.class),
          bootstrap::getExecutor));

        // cluster coordinator
        bind(ClusterCoordinator.class).toInstance(clusterCoordinator);

        // RPC Endpoints - eagerly created
        bind(UserServer.class).toInstance(new UserServer(
                config,
                getProvider(ExecutorService.class),
                getProvider(BufferAllocator.class),
                getProvider(UserService.class),
                getProvider(NodeEndpoint.class),
                getProvider(UserWorker.class),
                allowPortHunting,
                TracerFacade.INSTANCE,
                getProvider(OptionValidatorListing.class)
        ));

        // Fabric Service - eagerly created
        final String address = FabricServiceImpl.getAddress(useIP);
        final FabricServiceImpl fabricService = new FabricServiceImpl(
                address,
                45678,
                allowPortHunting,
                config.getSabotConfig().getInt(ExecConstants.BIT_SERVER_RPC_THREADS),
                bootstrap.getAllocator(),
                0,
                Long.MAX_VALUE,
                config.getSabotConfig().getInt(RpcConstants.BIT_RPC_TIMEOUT),
                bootstrap.getExecutor()
        );
        bind(FabricService.class).toInstance(fabricService);

        bind(ConduitServer.class).toInstance(new ConduitServer(getProvider(ConduitServiceRegistry.class), 0,
          Optional.empty()));
        final ConduitProvider conduitProvider = new ConduitProviderImpl(
          getProvider(NodeEndpoint.class), Optional.empty()
        );
        bind(ConduitProvider.class).toInstance(conduitProvider);

        conduitServiceRegistry.registerService(new OptionNotificationService(registry.provider(SystemOptionManager.class)));

        final CoordExecService coordExecService = new CoordExecService(
                bootstrap.getConfig(),
                bootstrap.getAllocator(),
                getProvider(FabricService.class),
                getProvider(com.dremio.exec.service.executor.ExecutorService.class),
                getProvider(ExecToCoordResultsHandler.class),
                getProvider(ExecToCoordStatusHandler.class),
                getProvider(NodeEndpoint.class),
                getProvider(JobTelemetryClient.class)
        );
        bind(CoordExecService.class).toInstance(coordExecService);

        bind(NamespaceService.class).to(NamespaceServiceImpl.class);
        bind(DatasetListingService.class).toInstance(new DatasetListingServiceImpl(
                getProvider(NamespaceService.Factory.class)
        ));

        bind(AccelerationManager.class).toInstance(AccelerationManager.NO_OP);

        // Note: corePoolSize param below should be more than 1 to show any multithreading issues
        final LocalSchedulerService localSchedulerService = new LocalSchedulerService(2);
        bind(SchedulerService.class).toInstance(localSchedulerService);

        bind(AccelerationListManager.class).toProvider(Providers.of(null));

        bind(SystemTablePluginConfigProvider.class).toInstance(new SystemTablePluginConfigProvider());

        bind(ResourceAllocator.class).toInstance(new BasicResourceAllocator(getProvider(ClusterCoordinator.class),
          getProvider(GroupResourceInformation.class)));

        bind(ExecutorSelectorFactory.class).toInstance(new ExecutorSelectorFactoryImpl());

        final ExecutorSelectorProvider executorSelectorProvider = new ExecutorSelectorProvider();
        bind(ExecutorSelectorProvider.class).toInstance(executorSelectorProvider);

        bind(ContextInformationFactory.class).toInstance(new ContextInformationFactory());

        bind(CommandPool.class).toInstance(CommandPoolFactory.INSTANCE.newPool(config, TracerFacade.INSTANCE));

        bind(LocalJobTelemetryServer.class).toInstance(new LocalJobTelemetryServer(
          gRpcServerBuilderFactory, getProvider(LegacyKVStoreProvider.class),
          getProvider(NodeEndpoint.class),
          new GrpcTracerFacade(TracerFacade.INSTANCE)));

        bind(MaestroForwarder.class).toInstance(new NoOpMaestroForwarder());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Provides
    @Singleton
    SystemOptionManager getSystemOptionManager(OptionValidatorListing optionValidatorListing, LogicalPlanPersistence lpp, Provider<LegacyKVStoreProvider> kvStoreProvider, Provider<SchedulerService> schedulerService) throws Exception {
      final OptionChangeBroadcaster systemOptionChangeBroadcaster =
        new OptionChangeBroadcaster(
          registry.provider(ConduitProvider.class),
          () -> registry.provider(ClusterCoordinator.class)
            .get()
            .getServiceSet(ClusterCoordinator.Role.COORDINATOR)
            .getAvailableEndpoints(),
          () -> registry.provider(SabotContext.class).get().getEndpoint());
      return new SystemOptionManager(optionValidatorListing, lpp, kvStoreProvider, schedulerService, systemOptionChangeBroadcaster, false);
    }

    @Provides
    @Singleton
    OptionManager getOptionManager(OptionValidatorListing optionValidatorListing, SystemOptionManager systemOptionManager) throws Exception {
      return OptionManagerWrapper.Builder.newBuilder()
        .withOptionManager(new DefaultOptionManager(optionValidatorListing))
        .withOptionManager(systemOptionManager)
        .build();
    }

    @Provides
    @Singleton
    FragmentWorkManager getFragmentWorkManager(
            Provider<NodeEndpoint> nodeEndpoint,
            Provider<SabotContext> sabotContext,
            Provider<FabricService> fabricService,
            Provider<CatalogService> catalogService,
            Provider<ContextInformationFactory> contextInformationFactory,
            Provider<WorkloadTicketDepot> workloadTicketDepot,
            Provider<TaskPool> taskPool,
            Provider<MaestroClientFactory> maestroServiceClientFactoryProvider,
            Provider<JobTelemetryExecutorClientFactory> jobTelemetryClientFactoryProvider,
            Provider<JobResultsClientFactory> jobResultsSoftwareClientFactoryProvider
    ) {
      return new FragmentWorkManager(
        bootstrap,
        nodeEndpoint,
        sabotContext,
        fabricService,
        catalogService,
        contextInformationFactory,
        workloadTicketDepot,
        taskPool,
        maestroServiceClientFactoryProvider,
        jobTelemetryClientFactoryProvider,
        jobResultsSoftwareClientFactoryProvider
      );
    }

    @Provides
    WorkStats getWorkStats(FragmentWorkManager fragmentWorkManager) {
      return fragmentWorkManager.getWorkStats();
    }

    @Provides
    @Singleton
    ContextService getContextService(
            Provider<ClusterCoordinator> clusterCoordinator,
            Provider<GroupResourceInformation> resourceInformation,
            Provider<WorkStats> workStats,
            Provider<LegacyKVStoreProvider> kvStoreProvider,
            Provider<FabricService> fabricService,
            Provider<ConduitServer> conduitServer,
            Provider<UserServer> userServer,
            Provider<MaterializationDescriptorProvider> materializationDescriptorProvider,
            Provider<QueryObserverFactory> queryObserverFactory,
            Provider<AccelerationManager> accelerationManager,
            Provider<AccelerationListManager> accelerationListManager,
            Provider<NamespaceService.Factory> namespaceServiceFactory,
            Provider<DatasetListingService> datasetListingService,
            Provider<UserService> userService,
            Provider<CatalogService> catalogService,
            Provider<ConduitProvider> conduitProvider,
            Provider<InformationSchemaServiceBlockingStub> informationSchemaStub,
            Provider<SpillService> spillService,
            Provider<ConnectionReader> connectionReader,
            Provider<OptionManager> optionManagerProvider,
            Provider<SystemOptionManager> systemOptionManagerProvider,
            Provider<OptionValidatorListing> optionValidatorListingProvider
    ) {
      return new ContextService(
              bootstrap,
              clusterCoordinator,
              resourceInformation,
              workStats,
              kvStoreProvider,
              fabricService,
              conduitServer,
              userServer,
              materializationDescriptorProvider,
              queryObserverFactory,
              accelerationManager,
              accelerationListManager,
              namespaceServiceFactory,
              datasetListingService,
              userService,
              catalogService,
              conduitProvider,
              informationSchemaStub,
              Providers.of(null),
              spillService,
              connectionReader,
              CredentialsService::new,
              () -> JobResultInfoProvider.NOOP,
              optionManagerProvider,
              systemOptionManagerProvider,
              Providers.of(null),
              Providers.of(null),
              optionValidatorListingProvider,
              allRoles
      );
    }

    @Singleton
    @Provides
    NodeEndpoint getNodeEndpoint(ContextService contextService) {
      return contextService.get().getEndpoint();
    }

    @Singleton
    @Provides
    GroupResourceInformation getGroupResourceInformation(Provider<ClusterCoordinator> coordinatorProvider) {
      return new ClusterResourceInformation(coordinatorProvider);
    }

    @Singleton
    @Provides
    SpillService getSpillService(Provider<OptionManager> optionManager, Provider<SchedulerService> schedulerService) {
      return new SpillServiceImpl(
              config,
              new SpillServiceOptionsImpl(optionManager),
              schedulerService
      );
    }

    @Provides
    @Singleton
    ExecutorSelectionService getExecutorSelectionService(
            Provider<ExecutorSetService> executorSetService,
            Provider<OptionManager> optionManagerProvider,
            Provider<ExecutorSelectorFactory> executorSelectorFactory,
            Provider<ExecutorSelectorProvider> executorSelectorProvider
    ) {
      return new ExecutorSelectionServiceImpl(
              executorSetService,
              optionManagerProvider,
              executorSelectorFactory,
              executorSelectorProvider.get()
      );
    }

    @Provides
    @Singleton
    ForemenWorkManager getForemenWorkManager(
            Provider<FabricService> fabricService,
            Provider<SabotContext> sabotContext,
            Provider<CommandPool> commandPool,
            Provider<MaestroService> maestroService,
            Provider<JobTelemetryClient> jobTelemetryClient,
            Provider<MaestroForwarder> forwarderProvider
    ) {
      return new ForemenWorkManager(
              fabricService,
              sabotContext,
              commandPool,
              maestroService,
              jobTelemetryClient,
              forwarderProvider,
              TracerFacade.INSTANCE
      );
    }

    @Provides
    @Singleton
    ExecutorSetService getExecutorSetService(
            Provider<ClusterCoordinator> clusterCoordinator,
            Provider<OptionManager> optionManagerProvider) {
      return new LocalExecutorSetService(clusterCoordinator,
                                         optionManagerProvider);
    }

    @Provides
    com.dremio.exec.service.executor.ExecutorService getExecutorService(FragmentWorkManager fragmentWorkManager) {
      return fragmentWorkManager.getExecutorService();
    }

    @Provides
    @Singleton
    MaestroService getMaestroService(
            Provider<ExecutorSetService> executorSetService,
            Provider<FabricService> fabricService,
            Provider<SabotContext> sabotContext,
            Provider<ResourceAllocator> resourceAllocator,
            Provider<CommandPool> commandPool,
            Provider<ExecutorSelectionService> executorSelectionService,
            Provider<ExecutorServiceClientFactory> executorServiceClientFactory,
            Provider<JobTelemetryClient> jobTelemetryClient,
            Provider<MaestroForwarder> forwarderProvider
    ) {
      return new MaestroServiceImpl(
              executorSetService,
              fabricService,
              sabotContext,
              resourceAllocator,
              commandPool,
              executorSelectionService,
              executorServiceClientFactory,
              jobTelemetryClient,
              forwarderProvider
              );
    }

    @Provides
    ExecToCoordStatusHandler getExecToCoordStatusHandler(MaestroService maestroService) {
      return maestroService.getExecStatusHandler();
    }

    @Provides
    LocalQueryExecutor getLocalQueryExecutor(ForemenWorkManager fragmentWorkManager) {
      return fragmentWorkManager.getLocalQueryExecutor();
    }

    @Provides
    ExecutorServiceClientFactory getExecutorServiceClientFactory(CoordToExecTunnelCreator tunnelCreator) {
      return new ExecutorServiceProductClientFactory(tunnelCreator);
    }

    @Provides
    ExecToCoordResultsHandler getExecToCoordHandler(ForemenWorkManager fragmentWorkManager) {
      return fragmentWorkManager.getExecToCoordResultsHandler();
    }

    @Provides
    ForemenTool getForemenTool(ForemenWorkManager fragmentWorkManager) {
      return fragmentWorkManager.getForemenTool();
    }

    @Provides
    CoordTunnelCreator getCoordTunnelCreator(ForemenWorkManager fragmentWorkManager) {
      return fragmentWorkManager.getCoordTunnelCreator();
    }

    @Provides
    ExecToCoordTunnelCreator getExecTunnelCreator(Provider<FabricService> fabricService) {
      return new ExecToCoordTunnelCreator(fabricService);
    }

    @Provides
    MaestroClientFactory getMaestroServiceClientFactory(ExecToCoordTunnelCreator tunnelCreator) {
      return new MaestroSoftwareClientFactory(tunnelCreator);
    }

    @Provides
    JobTelemetryExecutorClientFactory getJobTelemetryExecutionClientFactory(ExecToCoordTunnelCreator tunnelCreator) {
      return new JobTelemetrySoftwareClientFactory(tunnelCreator);
    }

    @Provides
    JobResultsClientFactory getJobResultsSoftwareClientFactory(ExecToCoordTunnelCreator tunnelCreator) {
      return new JobResultsSoftwareClientFactory(tunnelCreator);
    }

    @Provides
    MaestroGrpcServerFacade getMaestroAdapter(Provider<ExecToCoordStatusHandler> execToCoordStatusHandlerProvider) {
      return new MaestroGrpcServerFacade(execToCoordStatusHandlerProvider);
    }

    @Provides
    QueryCancelTool getQueryCancelTool(ForemenWorkManager fragmentWorkManager) {
      return fragmentWorkManager.getQueryCancelTool();
    }

    @Provides
    UserWorker getUserWorker(ForemenWorkManager fragmentWorkManager) {
      return fragmentWorkManager.getUserWorker();
    }

    @Provides
    @Singleton
    TaskPoolInitializer getTaskPoolInitializer(Provider<OptionManager> optionManager) {
      return new TaskPoolInitializer(optionManager, config);
    }

    @Provides
    TaskPool getTaskPool(TaskPoolInitializer taskPoolInitializer) {
      return taskPoolInitializer.getTaskPool();
    }

    @Provides
    @Singleton
    WorkloadTicketDepotService getWorkloadTicketDepotService(
            Provider<BufferAllocator> bufferAllocator,
            Provider<TaskPool> taskPool,
            Provider<DremioConfig> dremioConfig
    ) {
      return new WorkloadTicketDepotService(bufferAllocator, taskPool, dremioConfig);
    }

    @Provides
    WorkloadTicketDepot getWorkloadTicketDepot(WorkloadTicketDepotService workloadTicketDepotService) {
      return workloadTicketDepotService.getTicketDepot();
    }

    @Provides
    @Singleton
    SabotContext getSabotContext(ContextService contextService) {
      return contextService.get();
    }

    @Provides
    @Singleton
    JobTelemetryClient getJobTelemetryClient(GrpcChannelBuilderFactory grpcChannelBuilderFactory,
                                             Provider<NodeEndpoint> nodeEndpointProvider) {
      return new JobTelemetryClient(grpcChannelBuilderFactory, nodeEndpointProvider);
    }

    @Provides
    @Singleton
    LegacyKVStoreProvider getLegacyKVStoreProvider(KVStoreProvider kvStoreProvider) {
      return new LegacyKVStoreProviderAdapter(kvStoreProvider);
    }

    @Provides
    InformationSchemaServiceBlockingStub getInformationSchemaServiceBlockingStub(ConduitProvider conduitProvider) {
      return InformationSchemaServiceGrpc.newBlockingStub(conduitProvider.getOrCreateChannelToMaster());
    }
  }
}

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

import com.dremio.authenticator.Authenticator;
import com.dremio.common.DeferredException;
import com.dremio.common.GuiceServiceModule;
import com.dremio.common.StackTrace;
import com.dremio.common.concurrent.CloseableThreadPool;
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
import com.dremio.exec.catalog.ConnectionReaderDecorator;
import com.dremio.exec.catalog.DatasetCatalogServiceImpl;
import com.dremio.exec.catalog.InformationSchemaServiceImpl;
import com.dremio.exec.catalog.MetadataRefreshInfoBroadcaster;
import com.dremio.exec.catalog.VersionedDatasetAdapterFactory;
import com.dremio.exec.exception.NodeStartupException;
import com.dremio.exec.maestro.GlobalKeysService;
import com.dremio.exec.maestro.MaestroForwarder;
import com.dremio.exec.maestro.MaestroService;
import com.dremio.exec.maestro.MaestroServiceImpl;
import com.dremio.exec.maestro.NoOpMaestroForwarder;
import com.dremio.exec.planner.cost.DremioRelMetadataQuery;
import com.dremio.exec.planner.cost.RelMetadataQuerySupplier;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.exec.server.options.OptionChangeBroadcaster;
import com.dremio.exec.server.options.OptionNotificationService;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.server.options.SystemOptionManagerImpl;
import com.dremio.exec.service.executor.ExecutorServiceProductClientFactory;
import com.dremio.exec.service.jobresults.JobResultsSoftwareClientFactory;
import com.dremio.exec.service.jobtelemetry.JobTelemetrySoftwareClientFactory;
import com.dremio.exec.service.maestro.MaestroGrpcServerFacade;
import com.dremio.exec.service.maestro.MaestroSoftwareClientFactory;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.MetadataIOPool;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.statistics.StatisticsAdministrationService;
import com.dremio.exec.store.sys.statistics.StatisticsListManager;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.dremio.exec.store.sys.udf.UserDefinedFunctionService;
import com.dremio.exec.work.WorkStats;
import com.dremio.exec.work.protector.ForemenTool;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.exec.work.rpc.CoordTunnelCreator;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.partitionstats.storeprovider.PartitionStatsCacheStoreProvider;
import com.dremio.resource.ClusterResourceInformation;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.QueryCancelTool;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.RuleBasedEngineSelector;
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
import com.dremio.service.catalog.DatasetCatalogServiceGrpc;
import com.dremio.service.catalog.DatasetCatalogServiceGrpc.DatasetCatalogServiceBlockingStub;
import com.dremio.service.catalog.InformationSchemaServiceGrpc;
import com.dremio.service.catalog.InformationSchemaServiceGrpc.InformationSchemaServiceBlockingStub;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.commandpool.CommandPoolFactory;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.conduit.client.ConduitProviderImpl;
import com.dremio.service.conduit.server.ConduitInProcessChannelProvider;
import com.dremio.service.conduit.server.ConduitServer;
import com.dremio.service.conduit.server.ConduitServiceRegistry;
import com.dremio.service.conduit.server.ConduitServiceRegistryImpl;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.coordinator.ExecutorSetService;
import com.dremio.service.coordinator.LocalExecutorSetService;
import com.dremio.service.coordinator.NoOpClusterCoordinator;
import com.dremio.service.coordinator.SoftwareCoordinatorModeInfo;
import com.dremio.service.embedded.catalog.EmbeddedMetadataPointerService;
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
import com.dremio.service.namespace.catalogpubsub.CatalogEventMessagePublisherProvider;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvents;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.orphanage.OrphanageImpl;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.ModifiableLocalSchedulerService;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.spill.SpillService;
import com.dremio.service.spill.SpillServiceImpl;
import com.dremio.service.users.BasicAuthenticator;
import com.dremio.service.users.LocalUsernamePasswordAuthProvider;
import com.dremio.service.users.SimpleUserService;
import com.dremio.service.users.UserService;
import com.dremio.services.credentials.Cipher;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.CredentialsServiceImpl;
import com.dremio.services.credentials.SecretsCreator;
import com.dremio.services.credentials.SecretsCreatorImpl;
import com.dremio.services.credentials.SystemCipher;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.nessie.grpc.client.GrpcClientBuilder;
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
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import com.google.inject.util.Providers;
import io.opentracing.Tracer;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.zookeeper.Environment;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;

/** Test class to start execution framework without ui. */
public class SabotNode implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SabotNode.class);

  static {
    Environment.logEnv("SabotNode environment: ", logger);
  }

  private boolean isClosed = false;

  private BootStrapContext rootBootstrapContext;
  private Injector injector;

  private ShutdownThread shutdownHook;
  private GuiceServiceModule guiceSingletonHandler;
  private @Nullable List<? extends Module> overrideModules;

  public SabotNode(final SabotConfig config, final ClusterCoordinator clusterCoordinator)
      throws Exception {
    this(config, clusterCoordinator, ClassPathScanner.fromPrescan(config), true);
  }

  @VisibleForTesting
  public SabotNode(
      final SabotConfig config,
      final ClusterCoordinator clusterCoordinator,
      final ScanResult classpathScan,
      boolean allRoles)
      throws Exception {
    this(config, clusterCoordinator, classpathScan, allRoles, null);
  }

  @VisibleForTesting
  public SabotNode(
      final SabotConfig config,
      final ClusterCoordinator clusterCoordinator,
      final ScanResult classpathScan,
      boolean allRoles,
      List<? extends Module> overrideModules)
      throws Exception {
    init(config, clusterCoordinator, classpathScan, allRoles, overrideModules);
  }

  private void init(
      SabotConfig config,
      ClusterCoordinator clusterCoordinator,
      ScanResult classpathScan,
      boolean allRoles,
      List<? extends Module> overrideModules)
      throws Exception {
    Preconditions.checkNotNull(clusterCoordinator);
    this.overrideModules = overrideModules;

    DremioConfig dremioConfig = initDremioConfig(config, allRoles);

    rootBootstrapContext = new BootStrapContext(dremioConfig, classpathScan);
    guiceSingletonHandler = new GuiceServiceModule();
    injector =
        createInjector(
            dremioConfig, classpathScan, clusterCoordinator, allRoles, rootBootstrapContext);
  }

  protected DremioConfig initDremioConfig(SabotConfig config, boolean allRoles) {
    DremioConfig dremioConfig = DremioConfig.create(null, config);
    dremioConfig = dremioConfig.withValue(DremioConfig.ENABLE_COORDINATOR_BOOL, allRoles);
    return dremioConfig;
  }

  protected Injector createInjector(
      DremioConfig config,
      ScanResult classpathScan,
      ClusterCoordinator clusterCoordinator,
      boolean allRoles,
      BootStrapContext bootStrapContext) {
    final SabotModule sabotModule =
        new SabotModule(config, classpathScan, clusterCoordinator, allRoles, bootStrapContext);

    return createInjector(sabotModule);
  }

  protected Injector createInjector(Module sabotModule) {
    if (overrideModules == null || overrideModules.isEmpty()) {
      return Guice.createInjector(guiceSingletonHandler, sabotModule);
    }
    // if overrideModules itself contains overlapping bindings we will fail here with
    // https://github.com/google/guice/wiki/BINDING_ALREADY_SET
    // because we want to force test setup to have easily understandable logic and not a more
    // confusing "last one wins" approach (i.e. overriding sabot injectables multiple times)
    Module sabotModuleWithOverrides = Modules.override(sabotModule).with(overrideModules);
    return Guice.createInjector(guiceSingletonHandler, sabotModuleWithOverrides);
  }

  public void run() throws Exception {
    final Stopwatch w = Stopwatch.createStarted();
    logger.debug("Startup begun.");

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

    try (DeferredException deferred = new DeferredException()) {
      deferred.suppressingClose(() -> guiceSingletonHandler.close());
      deferred.suppressingClose(rootBootstrapContext);
    } catch (Exception e) {
      throw new RuntimeException("Failed to close SabotNode", e);
    }

    logger.info("Shutdown completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
    isClosed = true;
  }

  /**
   * Shutdown hook for SabotNode. Closes the node, and reports on errors that occur during closure,
   * as well as the location the node was started from.
   */
  private static class ShutdownThread extends Thread {
    private static final AtomicInteger idCounter = new AtomicInteger(0);
    private final SabotNode node;
    private final StackTrace stackTrace;

    /**
     * Constructor.
     *
     * @param node the node to close down
     * @param stackTrace the stack trace from where the SabotNode was started; use new StackTrace()
     *     to generate this
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
        synchronized (idCounter) {
          node.doClose();
        }
      } catch (final Exception e) {
        throw new RuntimeException(
            "Caught exception closing SabotNode started from\n" + stackTrace, e);
      }
    }
  }

  public SabotContext getContext() {
    return getInstance(SabotContext.class);
  }

  public NodeEndpoint getEndpoint() {
    return getInstance(NodeEndpoint.class);
  }

  public CatalogService getCatalogService() {
    return getInstance(CatalogService.class);
  }

  public Injector getInjector() {
    return injector;
  }

  public <T> T getInstance(Class<T> type) {
    return getInjector().getInstance(type);
  }

  public <T> Provider<T> getProvider(Class<T> type) {
    return getInjector().getProvider(type);
  }

  public static void main(final String[] cli) throws NodeStartupException {
    final StartupOptions options = StartupOptions.parse(cli);
    startLocally(options);
  }

  public static SabotNode startLocally(StartupOptions options) throws NodeStartupException {
    logger.debug("Starting new SabotNode.");
    SabotConfig config = SabotConfig.create(options.getConfigLocation());
    NoOpClusterCoordinator clusterCoordinator = new NoOpClusterCoordinator();
    ScanResult classpathScan = ClassPathScanner.fromPrescan(config);
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

  /** Guice Module for SabotNode dependancy injection */
  protected class SabotModule extends AbstractModule {
    private final DremioConfig config;
    private final ScanResult classpathScan;
    private final ClusterCoordinator clusterCoordinator;
    private final EnumSet<Role> clusterRoles;
    private final BootStrapContext bootstrap;
    private final RequestContext defaultRequestContext;

    public SabotModule(
        DremioConfig config,
        ScanResult classpathScan,
        ClusterCoordinator clusterCoordinator,
        boolean allRoles,
        BootStrapContext bootstrap) {
      this.config = config;
      this.classpathScan = classpathScan;
      this.clusterCoordinator = clusterCoordinator;
      this.clusterRoles =
          allRoles
              ? EnumSet.allOf(ClusterCoordinator.Role.class)
              : EnumSet.of(ClusterCoordinator.Role.EXECUTOR);
      this.defaultRequestContext =
          RequestContext.empty()
              .with(TenantContext.CTX_KEY, TenantContext.DEFAULT_SERVICE_CONTEXT)
              .with(UserContext.CTX_KEY, UserContext.SYSTEM_USER_CONTEXT);
      this.bootstrap = bootstrap;
    }

    @Override
    protected void configure() {
      try {
        bind(Tracer.class).toInstance(TracerFacade.INSTANCE);
        GrpcTracerFacade grpcTracerFacade = new GrpcTracerFacade(TracerFacade.INSTANCE);
        bind(GrpcTracerFacade.class).toInstance(grpcTracerFacade);

        bind(DremioConfig.class).toInstance(config);
        bind(ScanResult.class).toInstance(classpathScan);
        bind(ClusterCoordinator.class).toInstance(clusterCoordinator);
        bind(RequestContext.class).toInstance(defaultRequestContext);
        bind(GroupResourceInformation.class).to(ClusterResourceInformation.class);
        bind(GlobalKeysService.class).toInstance(GlobalKeysService.NO_OP);

        bind(BootStrapContext.class).toInstance(bootstrap);
        bind(SabotConfig.class).toInstance(bootstrap.getConfig());
        bind(BufferAllocator.class).toInstance(bootstrap.getAllocator());
        bind(ExecutorService.class).toInstance(bootstrap.getExecutor());

        bind(LogicalPlanPersistence.class);
        bind(OptionValidatorListing.class).to(OptionValidatorListingImpl.class);

        // KVStore
        final KVStoreProvider kvStoreProvider =
            new LocalKVStoreProvider(classpathScan, null, true, true);
        bind(KVStoreProvider.class).toInstance(kvStoreProvider);

        bind(Cipher.class).to(SystemCipher.class).in(Singleton.class);
        bind(SecretsCreator.class).to(SecretsCreatorImpl.class).in(Singleton.class);

        bind(ConnectionReader.class)
            .toInstance(
                new ConnectionReaderDecorator(
                    ConnectionReader.of(classpathScan, config.getSabotConfig()),
                    getProvider(CredentialsService.class)));
        // bind default providers.
        bind(MaterializationDescriptorProvider.class)
            .toInstance(MaterializationDescriptorProvider.EMPTY);
        bind(QueryObserverFactory.class).toInstance(QueryObserverFactory.DEFAULT);
        bind(GrpcChannelBuilderFactory.class)
            .toInstance(
                new SingleTenantGrpcChannelBuilderFactory(
                    TracerFacade.INSTANCE,
                    getProvider(RequestContext.class),
                    () -> Maps.newHashMap()));

        GrpcServerBuilderFactory gRpcServerBuilderFactory =
            new MultiTenantGrpcServerBuilderFactory(TracerFacade.INSTANCE);
        bind(GrpcServerBuilderFactory.class).toInstance(gRpcServerBuilderFactory);

        // setup authentication
        final UserServiceTestImpl userServiceTestImpl = new UserServiceTestImpl();
        bind(UserService.class).toInstance(userServiceTestImpl);
        bind(SimpleUserService.class).toInstance(userServiceTestImpl);
        bind(LocalUsernamePasswordAuthProvider.class);
        bind(Authenticator.class).to(BasicAuthenticator.class);

        // needed by UserServer + FabricService
        boolean allowPortHunting = true;
        bind(Boolean.class)
            .annotatedWith(Names.named("allowPortHunting"))
            .toInstance(allowPortHunting);

        bind(Orphanage.Factory.class).to(OrphanageImpl.Factory.class);
        bind(NamespaceService.Factory.class).to(NamespaceServiceImpl.Factory.class);

        final ConduitServiceRegistry conduitServiceRegistry = new ConduitServiceRegistryImpl();
        bind(ConduitServiceRegistry.class).toInstance(conduitServiceRegistry);

        bind(NodeRegistration.class).asEagerSingleton();

        bind(MetadataRefreshInfoBroadcaster.class);

        bind(CatalogStatusEvents.class).toInstance(new CatalogStatusEventsImpl());

        // No-op implementation as executors should not interact with catalog event publishing
        bind(CatalogEventMessagePublisherProvider.class)
            .toInstance(CatalogEventMessagePublisherProvider.NO_OP);

        // CatalogService is expensive to create so we eagerly create it
        bind(CatalogService.class)
            .toInstance(
                new CatalogServiceImpl(
                    getProvider(SabotContext.class),
                    getProvider(SchedulerService.class),
                    getProvider(SystemTablePluginConfigProvider.class),
                    null,
                    getProvider(FabricService.class),
                    getProvider(ConnectionReader.class),
                    getProvider(BufferAllocator.class),
                    getProvider(LegacyKVStoreProvider.class),
                    getProvider(DatasetListingService.class),
                    getProvider(OptionManager.class),
                    getProvider(MetadataRefreshInfoBroadcaster.class),
                    config,
                    clusterRoles,
                    getProvider(ModifiableSchedulerService.class),
                    getProvider(VersionedDatasetAdapterFactory.class),
                    getProvider(CatalogStatusEvents.class),
                    getProvider(ExecutorService.class)));

        conduitServiceRegistry.registerService(
            new InformationSchemaServiceImpl(
                getProvider(CatalogService.class), bootstrap::getExecutor));

        final EmbeddedMetadataPointerService pointerService =
            new EmbeddedMetadataPointerService(getProvider(KVStoreProvider.class));
        pointerService.getGrpcServices().forEach(conduitServiceRegistry::registerService);
        bind(EmbeddedMetadataPointerService.class).toInstance(pointerService);

        conduitServiceRegistry.registerService(
            new DatasetCatalogServiceImpl(
                getProvider(CatalogService.class), getProvider(NamespaceService.Factory.class)));

        conduitServiceRegistry.registerService(
            new OptionNotificationService(getProvider(SystemOptionManager.class)));

        bind(UserServer.class);

        // Fabric Service - eagerly created
        final String address = FabricServiceImpl.getAddress(false);
        final FabricServiceImpl fabricService =
            new FabricServiceImpl(
                address,
                45678,
                allowPortHunting,
                config.getSabotConfig().getInt(ExecConstants.BIT_SERVER_RPC_THREADS),
                bootstrap.getAllocator(),
                0,
                Long.MAX_VALUE,
                config.getSabotConfig().getInt(RpcConstants.BIT_RPC_TIMEOUT),
                bootstrap.getExecutor());
        bind(FabricService.class).toInstance(fabricService);

        // TODO: avoid eagerSingleton? some services need to register their protocol to fabric early
        bind(CoordExecService.class).asEagerSingleton();
        bind(FragmentWorkManager.class);
        bind(WorkloadTicketDepotService.class);

        final String inProcessServerName = UUID.randomUUID().toString();
        bind(ConduitServer.class)
            .toInstance(
                new ConduitServer(
                    getProvider(ConduitServiceRegistry.class),
                    0,
                    Optional.empty(),
                    inProcessServerName));
        final ConduitProvider conduitProvider =
            new ConduitProviderImpl(getProvider(NodeEndpoint.class), Optional.empty());
        bind(ConduitInProcessChannelProvider.class)
            .toInstance(
                new ConduitInProcessChannelProvider(
                    inProcessServerName, getProvider(RequestContext.class)));
        bind(ConduitProvider.class).toInstance(conduitProvider);

        bind(Orphanage.class).to(OrphanageImpl.class);
        bind(NamespaceService.class).to(NamespaceServiceImpl.class);
        bind(DatasetListingService.class).to(DatasetListingServiceImpl.class);

        bind(AccelerationManager.class).toInstance(AccelerationManager.NO_OP);

        // Note: corePoolSize param below should be more than 1 to show any multithreading issues
        final LocalSchedulerService localSchedulerService = new LocalSchedulerService(2);
        bind(SchedulerService.class).toInstance(localSchedulerService);

        bind(AccelerationListManager.class).toProvider(Providers.of(null));

        bind(SystemTablePluginConfigProvider.class);

        bind(SysFlightChannelProvider.class).toInstance(SysFlightChannelProvider.NO_OP);

        bind(SourceVerifier.class).toInstance(SourceVerifier.NO_OP);

        bind(ResourceAllocator.class).to(BasicResourceAllocator.class);

        bind(ExecutorSelectorFactory.class).toInstance(new ExecutorSelectorFactoryImpl());
        bind(ExecutorSelectorProvider.class).toInstance(new ExecutorSelectorProvider());
        bind(ExecutorSelectionService.class).to(ExecutorSelectionServiceImpl.class);

        bind(ExecutorSetService.class).to(LocalExecutorSetService.class);

        bind(ContextInformationFactory.class).toInstance(new ContextInformationFactory());

        bind(MetadataIOPool.class).toInstance(MetadataIOPool.Factory.INSTANCE.newPool(config));

        bind(CommandPool.class)
            .toInstance(CommandPoolFactory.INSTANCE.newPool(config, TracerFacade.INSTANCE));

        bind(LocalJobTelemetryServer.class)
            .toInstance(
                new LocalJobTelemetryServer(
                    getProvider(OptionManager.class),
                    gRpcServerBuilderFactory,
                    getProvider(KVStoreProvider.class),
                    getProvider(LegacyKVStoreProvider.class),
                    getProvider(NodeEndpoint.class),
                    grpcTracerFacade,
                    CloseableThreadPool::newCachedThreadPool));

        bind(MaestroService.class).to(MaestroServiceImpl.class);

        bind(MaestroForwarder.class).to(NoOpMaestroForwarder.class);
        bind(RuleBasedEngineSelector.class).toInstance(RuleBasedEngineSelector.NO_OP);
        bind(StatisticsService.class).toInstance(StatisticsService.MOCK_STATISTICS_SERVICE);
        bind(StatisticsAdministrationService.Factory.class)
            .toInstance((context) -> StatisticsService.MOCK_STATISTICS_SERVICE);
        bind(StatisticsListManager.class).toProvider(Providers.of(null));
        bind(UserDefinedFunctionService.class).toProvider(Providers.of(null));
        bind(PartitionStatsCacheStoreProvider.class).to(MockPartitionStatsStoreProvider.class);
        bind(RelMetadataQuerySupplier.class)
            .toInstance(
                DremioRelMetadataQuery.getSupplier(StatisticsService.MOCK_STATISTICS_SERVICE));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Provides
    @Singleton
    OptionChangeBroadcaster getOptionChangeBroadcaster(
        Provider<ConduitProvider> conduitProviderProvider,
        Provider<ClusterCoordinator> clusterCoordinatorProvider,
        Provider<NodeEndpoint> nodeEndpointProvider) {
      return new OptionChangeBroadcaster(
          conduitProviderProvider, clusterCoordinatorProvider, nodeEndpointProvider);
    }

    @Provides
    @Singleton
    SystemOptionManager getSystemOptionManager(
        OptionValidatorListing optionValidatorListing,
        LogicalPlanPersistence lpp,
        Provider<LegacyKVStoreProvider> kvStoreProvider,
        Provider<SchedulerService> schedulerService,
        OptionChangeBroadcaster optionChangeBroadcaster)
        throws Exception {
      return new SystemOptionManagerImpl(
          optionValidatorListing,
          lpp,
          kvStoreProvider,
          schedulerService,
          optionChangeBroadcaster,
          false);
    }

    @Provides
    @Singleton
    OptionManager getOptionManager(
        OptionValidatorListing optionValidatorListing, SystemOptionManager systemOptionManager)
        throws Exception {
      return OptionManagerWrapper.Builder.newBuilder()
          .withOptionManager(new DefaultOptionManager(optionValidatorListing))
          .withOptionManager(systemOptionManager)
          .build();
    }

    @Provides
    @Singleton
    ModifiableSchedulerService getModifiableMetadataScheduler(
        Provider<OptionManager> optionManagerProvider) throws Exception {
      return new ModifiableLocalSchedulerService(
          2,
          "modifiable-scheduler-",
          ExecConstants.MAX_CONCURRENT_METADATA_REFRESHES,
          optionManagerProvider);
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
        Provider<Orphanage.Factory> orphanageFactory,
        Provider<DatasetListingService> datasetListingService,
        Provider<UserService> userService,
        Provider<CatalogService> catalogService,
        Provider<ConduitProvider> conduitProvider,
        Provider<InformationSchemaServiceBlockingStub> informationSchemaStub,
        Provider<SpillService> spillService,
        Provider<ConnectionReader> connectionReader,
        Provider<OptionManager> optionManagerProvider,
        Provider<SystemOptionManager> systemOptionManagerProvider,
        Provider<OptionValidatorListing> optionValidatorListingProvider,
        Provider<NessieApiV2> nessieApiProvider,
        Provider<StatisticsService> statisticsService,
        Provider<StatisticsAdministrationService.Factory> statisticsAdministrationServiceFactory,
        Provider<StatisticsListManager> statisticsListManagerProvider,
        Provider<UserDefinedFunctionService> userDefinedFunctionListManagerProvider,
        Provider<RelMetadataQuerySupplier> relMetadataQuerySupplier,
        Provider<SimpleJobRunner> jobsRunnerProvider,
        Provider<DatasetCatalogServiceBlockingStub> datasetCatalogStub,
        Provider<GlobalKeysService> globalKeysServiceProvider,
        Provider<CredentialsService> credentialsServiceProvider,
        Provider<ConduitInProcessChannelProvider> conduitInProcessChannelProviderProvider,
        Provider<SysFlightChannelProvider> sysFlightChannelProviderProvider,
        Provider<SourceVerifier> sourceVerifierProvider,
        Provider<SecretsCreator> secretsCreatorProvider,
        Provider<ForemenWorkManager> foremenWorkManagerProvider,
        Provider<MetadataIOPool> metadataIOPoolProvider) {
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
          orphanageFactory,
          datasetListingService,
          userService,
          catalogService,
          conduitProvider,
          informationSchemaStub,
          Providers.of(null),
          spillService,
          connectionReader,
          () -> JobResultInfoProvider.NOOP,
          optionManagerProvider,
          Providers.of(null),
          Providers.of(null),
          optionValidatorListingProvider,
          clusterRoles,
          Providers.of(new SoftwareCoordinatorModeInfo()),
          nessieApiProvider,
          statisticsService,
          statisticsAdministrationServiceFactory,
          statisticsListManagerProvider,
          userDefinedFunctionListManagerProvider,
          relMetadataQuerySupplier,
          jobsRunnerProvider,
          datasetCatalogStub,
          globalKeysServiceProvider,
          credentialsServiceProvider,
          conduitInProcessChannelProviderProvider,
          sysFlightChannelProviderProvider,
          sourceVerifierProvider,
          secretsCreatorProvider,
          foremenWorkManagerProvider,
          metadataIOPoolProvider);
    }

    @Singleton
    @Provides
    CredentialsService getCredentialsServiceProvider(
        DremioConfig dremioConfig,
        ScanResult scanResult,
        Provider<OptionManager> optionManager,
        Provider<ConduitProvider> conduitProvider,
        Provider<Cipher> cipher) {
      return CredentialsServiceImpl.newInstance(
          dremioConfig,
          scanResult,
          optionManager,
          cipher,
          () -> conduitProvider.get().getOrCreateChannelToMaster());
    }

    @Singleton
    @Provides
    NodeEndpoint getNodeEndpoint(SabotContext sabotContext) {
      return sabotContext.getEndpoint();
    }

    @Singleton
    @Provides
    SpillService getSpillService(
        Provider<OptionManager> optionManager, Provider<SchedulerService> schedulerService) {
      return new SpillServiceImpl(
          config, new SpillServiceOptionsImpl(optionManager), schedulerService);
    }

    @Provides
    @Singleton
    ForemenWorkManager getForemenWorkManager(
        Provider<FabricService> fabricService,
        Provider<SabotContext> sabotContext,
        Provider<CommandPool> commandPool,
        Provider<MaestroService> maestroService,
        Provider<JobTelemetryClient> jobTelemetryClient,
        Provider<MaestroForwarder> forwarderProvider,
        Provider<RuleBasedEngineSelector> ruleBasedEngineSelectorProvider,
        Provider<RequestContext> requestContextProvider,
        Provider<PartitionStatsCacheStoreProvider> transientStoreProvider) {
      return new ForemenWorkManager(
          fabricService,
          sabotContext,
          commandPool,
          maestroService,
          jobTelemetryClient,
          forwarderProvider,
          ruleBasedEngineSelectorProvider,
          requestContextProvider,
          transientStoreProvider);
    }

    @Provides
    com.dremio.exec.service.executor.ExecutorService getExecutorService(
        FragmentWorkManager fragmentWorkManager) {
      return fragmentWorkManager.getExecutorService();
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
    ExecutorServiceClientFactory getExecutorServiceClientFactory(
        CoordToExecTunnelCreator tunnelCreator) {
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
    JobTelemetryExecutorClientFactory getJobTelemetryExecutionClientFactory(
        ExecToCoordTunnelCreator tunnelCreator) {
      return new JobTelemetrySoftwareClientFactory(tunnelCreator);
    }

    @Provides
    JobResultsClientFactory getJobResultsSoftwareClientFactory(
        ExecToCoordTunnelCreator tunnelCreator) {
      return new JobResultsSoftwareClientFactory(tunnelCreator);
    }

    @Provides
    MaestroGrpcServerFacade getMaestroAdapter(
        Provider<ExecToCoordStatusHandler> execToCoordStatusHandlerProvider) {
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
    TaskPoolInitializer getTaskPoolInitializer(
        Provider<OptionManager> optionManager, DremioConfig dremioConfig) {
      return new TaskPoolInitializer(optionManager, dremioConfig);
    }

    @Provides
    TaskPool getTaskPool(TaskPoolInitializer taskPoolInitializer) {
      return taskPoolInitializer.getTaskPool();
    }

    @Provides
    WorkloadTicketDepot getWorkloadTicketDepot(
        WorkloadTicketDepotService workloadTicketDepotService) {
      return workloadTicketDepotService.getTicketDepot();
    }

    @Provides
    @Singleton
    SabotContext getSabotContext(ContextService contextService) {
      return contextService.get();
    }

    @Provides
    @Singleton
    JobTelemetryClient getJobTelemetryClient(
        GrpcChannelBuilderFactory grpcChannelBuilderFactory,
        Provider<NodeEndpoint> nodeEndpointProvider) {
      return new JobTelemetryClient(grpcChannelBuilderFactory, nodeEndpointProvider);
    }

    @Provides
    @Singleton
    SimpleJobRunner getSimpleJobService(SabotContext sabotContext) {
      return sabotContext.getJobsRunner().get();
    }

    @Provides
    @Singleton
    LegacyKVStoreProvider getLegacyKVStoreProvider(KVStoreProvider kvStoreProvider) {
      return new LegacyKVStoreProviderAdapter(kvStoreProvider);
    }

    @Provides
    InformationSchemaServiceBlockingStub getInformationSchemaServiceBlockingStub(
        Provider<ConduitProvider> conduitProvider) {
      return InformationSchemaServiceGrpc.newBlockingStub(
          conduitProvider.get().getOrCreateChannelToMaster());
    }

    @Provides
    NessieApiV2 newNessieApi(Provider<ConduitProvider> conduitProvider) {
      String endpoint = config.getString(DremioConfig.NESSIE_SERVICE_REMOTE_URI);
      if (endpoint == null || endpoint.isEmpty()) {
        return GrpcClientBuilder.builder()
            .withChannel(conduitProvider.get().getOrCreateChannelToMaster())
            .build(NessieApiV2.class);
      }
      return NessieClientBuilder.createClientBuilder("HTTP", null)
          .withUri(URI.create(endpoint))
          .build(NessieApiV2.class);
    }

    @Provides
    DatasetCatalogServiceBlockingStub getDatasetCatalogServiceBlockingStub(
        Provider<ConduitProvider> conduitProvider) {
      return DatasetCatalogServiceGrpc.newBlockingStub(
          conduitProvider.get().getOrCreateChannelToMaster());
    }
  }
}

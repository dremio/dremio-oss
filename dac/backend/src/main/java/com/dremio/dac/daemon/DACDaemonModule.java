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
package com.dremio.dac.daemon;

import static com.dremio.config.DremioConfig.WEB_AUTH_TYPE;
import static com.dremio.service.reflection.ReflectionServiceImpl.LOCAL_TASK_LEADER_NAME;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Optional;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.nodes.NodeProvider;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.DACDaemon.ClusterMode;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.server.APIServer;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.DremioServer;
import com.dremio.dac.server.DremioServlet;
import com.dremio.dac.server.LivenessService;
import com.dremio.dac.server.RestServerV2;
import com.dremio.dac.server.WebServer;
import com.dremio.dac.server.tokens.TokenManager;
import com.dremio.dac.server.tokens.TokenManagerImpl;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DACViewCreatorFactory;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.exec.MasterElectionService;
import com.dremio.dac.service.exec.MasterStatusListener;
import com.dremio.dac.service.exec.MasterlessStatusListener;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchService;
import com.dremio.dac.service.search.SearchServiceImpl;
import com.dremio.dac.service.search.SearchServiceInvoker;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.service.users.UserServiceHelper;
import com.dremio.dac.support.SupportService;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.edition.EditionProvider;
import com.dremio.edition.EditionProviderImpl;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.ViewCreatorFactory;
import com.dremio.exec.enginemanagement.proto.EngineManagementProtos.EngineId;
import com.dremio.exec.enginemanagement.proto.EngineManagementProtos.SubEngineId;
import com.dremio.exec.maestro.MaestroService;
import com.dremio.exec.maestro.MaestroServiceImpl;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.server.JobResultInfoProvider;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.NodeRegistration;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.service.executor.ExecutorService;
import com.dremio.exec.service.executor.ExecutorServiceProductClientFactory;
import com.dremio.exec.service.jobresults.JobResultsSoftwareClientFactory;
import com.dremio.exec.service.jobtelemetry.JobTelemetrySoftwareClientFactory;
import com.dremio.exec.service.maestro.MaestroSoftwareClientFactory;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.PDFSService;
import com.dremio.exec.store.dfs.PDFSService.PDFSMode;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.work.WorkStats;
import com.dremio.exec.work.protector.ForemenTool;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.exec.work.rpc.CoordTunnelCreator;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.options.OptionManager;
import com.dremio.provision.service.ProvisioningService;
import com.dremio.provision.service.ProvisioningServiceImpl;
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
import com.dremio.service.InitializerRegistry;
import com.dremio.service.SingletonRegistry;
import com.dremio.service.accelerator.AccelerationListManagerImpl;
import com.dremio.service.accelerator.ReflectionTunnelCreator;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.commandpool.CommandPoolFactory;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ExecutorSetService;
import com.dremio.service.coordinator.LocalExecutorSetService;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.service.coordinator.zk.ZKClusterCoordinator;
import com.dremio.service.execselector.ExecutorSelectionService;
import com.dremio.service.execselector.ExecutorSelectionServiceImpl;
import com.dremio.service.execselector.ExecutorSelectorFactory;
import com.dremio.service.execselector.ExecutorSelectorFactoryImpl;
import com.dremio.service.execselector.ExecutorSelectorProvider;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.dremio.service.grpc.GrpcChannelBuilderFactory;
import com.dremio.service.grpc.GrpcServerBuilderFactory;
import com.dremio.service.jobresults.client.JobResultsClientFactory;
import com.dremio.service.jobs.HybridJobsService;
import com.dremio.service.jobs.JobResultToLogEntryConverter;
import com.dremio.service.jobs.JobResultsStoreConfig;
import com.dremio.service.jobs.JobsServer;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.service.jobtelemetry.client.JobTelemetryExecutorClientFactory;
import com.dremio.service.jobtelemetry.server.LocalJobTelemetryServer;
import com.dremio.service.listing.DatasetListingInvoker;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.listing.DatasetListingServiceImpl;
import com.dremio.service.maestroservice.MaestroClientFactory;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.SplitOrphansCleanerService;
import com.dremio.service.reflection.ExecutorOnlyReflectionService;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.ReflectionStatusServiceImpl;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.spill.SpillService;
import com.dremio.service.spill.SpillServiceImpl;
import com.dremio.service.users.SimpleUserService;
import com.dremio.service.users.UserService;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.base.Throwables;
import com.google.inject.util.Providers;

import io.opentracing.Tracer;

/**
 * DAC module to setup Dremio daemon
 */
public class DACDaemonModule implements DACModule {
  private static final Logger logger = LoggerFactory.getLogger(DACDaemonModule.class);

  public static final String JOBS_STORAGEPLUGIN_NAME = "__jobResultsStore";
  public static final String SCRATCH_STORAGEPLUGIN_NAME = "$scratch";

  public DACDaemonModule() {
  }

  @Override
  public void bootstrap(final Runnable shutdownHook, final SingletonRegistry bootstrapRegistry, ScanResult scanResult, DACConfig dacConfig, boolean isMaster) {
    final DremioConfig config = dacConfig.getConfig();
    final boolean isMasterless = config.isMasterlessEnabled();
    final boolean embeddedZookeeper = config.getBoolean(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL);


    final BootStrapContext bootStrapContext = new BootStrapContext(config, scanResult, bootstrapRegistry);
    bootstrapRegistry.bindSelf(bootStrapContext);
    bootstrapRegistry.bind(BufferAllocator.class, bootStrapContext.getAllocator());

    // Start cluster coordinator before all other services so that non master nodes can poll for master status
    if (dacConfig.getClusterMode() == ClusterMode.LOCAL) {
      bootstrapRegistry.bind(ClusterCoordinator.class, new LocalClusterCoordinator());
    } else {
      // ClusterCoordinator has a runtime dependency on ZooKeeper. If no ZooKeeper server
      // is present, ClusterCoordinator won't start, so this service should be initialized first.
      final Provider<Integer> portProvider;
      if (isMaster && embeddedZookeeper) {
        ZkServer zkServer = new ZkServer(
            config.getString(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_PATH_STRING),
            config.getInt(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_PORT_INT),
            dacConfig.autoPort);
        bootstrapRegistry.bindSelf(zkServer);

        portProvider = dacConfig.autoPort ? new Provider<Integer>(){
          @Override
          public Integer get() {
            return bootstrapRegistry.lookup(ZkServer.class).getPort();
          }} : null;
      } else {
        portProvider = null;
      }

      final ZKClusterCoordinator coord;
      try {
        coord = new ZKClusterCoordinator(config.getSabotConfig(), portProvider);
      } catch (IOException e) {
        throw new RuntimeException("Cannot instantiate the ZooKeeper cluster coordinator", e);
      }
      bootstrapRegistry.bind(ClusterCoordinator.class, coord);
    }

    // Start master election
    if (isMaster && !config.getBoolean(DremioConfig.DEBUG_DISABLE_MASTER_ELECTION_SERVICE_BOOL)) {
      bootstrapRegistry.bindSelf(new MasterElectionService(bootstrapRegistry.provider(ClusterCoordinator.class)));
    }

    final MasterStatusListener masterStatusListener;
    if (!isMasterless) {
      masterStatusListener = new MasterStatusListener(bootstrapRegistry.provider(ClusterCoordinator.class), config.getSabotConfig(), isMaster);
    } else {
      masterStatusListener =
        new MasterlessStatusListener(bootstrapRegistry.provider(ClusterCoordinator.class), isMaster);
    }
    // start master status listener
    bootstrapRegistry.bind(MasterStatusListener.class, masterStatusListener);
    bootstrapRegistry.bindProvider(EngineId.class, Providers.of(null));
    bootstrapRegistry.bindProvider(SubEngineId.class, Providers.of(null));
  }

  @Override
  public void build(final SingletonRegistry bootstrapRegistry, final SingletonRegistry registry, ScanResult scanResult,
      DACConfig dacConfig, boolean isMaster){
    final DremioConfig config = dacConfig.getConfig();
    final SabotConfig sabotConfig = config.getSabotConfig();
    final BootStrapContext bootstrap = bootstrapRegistry.lookup(BootStrapContext.class);

    final boolean isMasterless = config.isMasterlessEnabled();
    final boolean isCoordinator = config.getBoolean(DremioConfig.ENABLE_COORDINATOR_BOOL);
    final boolean isExecutor = config.getBoolean(DremioConfig.ENABLE_EXECUTOR_BOOL);
    final boolean isDistributedCoordinator = isMasterless && isCoordinator;
    final boolean isDistributedMaster = isDistributedCoordinator || isMaster;

    final Provider<NodeEndpoint> masterEndpoint = new Provider<NodeEndpoint>() {
      private final Provider<MasterStatusListener> masterStatusListener =
          registry.provider(MasterStatusListener.class);

      @Override
      public NodeEndpoint get() {
        return masterStatusListener.get().getMasterNode();
      }
    };

    registry.bind(java.util.concurrent.ExecutorService.class, bootstrap.getExecutor());

    EnumSet<ClusterCoordinator.Role> roles = EnumSet.noneOf(ClusterCoordinator.Role.class);
    if (isMaster) {
      roles.add(ClusterCoordinator.Role.MASTER);
    }
    if (isCoordinator) {
      roles.add(ClusterCoordinator.Role.COORDINATOR);
    }
    if (isExecutor) {
      roles.add(ClusterCoordinator.Role.EXECUTOR);
    }

    registry.bindSelf(config);

    registry.bind(ConnectionReader.class, ConnectionReader.of(scanResult, sabotConfig));

    // register default providers.

    registry.bind(MaterializationDescriptorProvider.class, MaterializationDescriptorProvider.EMPTY);
    registry.bind(QueryObserverFactory.class, QueryObserverFactory.DEFAULT);

    // copy bootstrap bindings to the main registry.
    bootstrapRegistry.copyBindings(registry);

    registry.bind(GrpcChannelBuilderFactory.class, new GrpcChannelBuilderFactory(registry.lookup(Tracer.class)));
    registry.bind(GrpcServerBuilderFactory.class, new GrpcServerBuilderFactory(registry.lookup(Tracer.class)));

    // Fabric
    final String fabricAddress;
    try {
      fabricAddress = FabricServiceImpl.getAddress(false);
    } catch (UnknownHostException e) {
      throw new RuntimeException("Cannot get local address", e);
    }

    registry.bind(
        FabricService.class,
        new FabricServiceImpl(
            fabricAddress,
            dacConfig.localPort,
            dacConfig.autoPort,
            sabotConfig.getInt(ExecConstants.BIT_SERVER_RPC_THREADS),
            bootstrap.getAllocator(),
            config.getBytes(DremioConfig.FABRIC_MEMORY_RESERVATION),
            Long.MAX_VALUE,
            sabotConfig.getInt(RpcConstants.BIT_RPC_TIMEOUT),
            bootstrap.getExecutor()
        ));

    registry.bind(
      KVStoreProvider.class,
      KVStoreProviderHelper.newKVStoreProvider(
        dacConfig,
        bootstrap,
        registry.provider(FabricService.class),
        masterEndpoint,
        bootstrapRegistry.lookup(Tracer.class)
      )
    );

    registry.bind(
      LegacyKVStoreProvider.class,
      new LegacyKVStoreProviderAdapter(
        registry.provider(KVStoreProvider.class).get(),
        bootstrap.getClasspathScan(), false)
    );

    registry.bind(
      ViewCreatorFactory.class,
      new DACViewCreatorFactory(
        registry.provider(InitializerRegistry.class),
        registry.provider(LegacyKVStoreProvider.class),
        registry.provider(JobsService.class),
        registry.provider(NamespaceService.Factory.class),
        registry.provider(CatalogService.class),
        registry.provider(ContextService.class),
        () -> bootstrap.getAllocator()
      )
    );

    final boolean isInternalUGS = setupUserService(registry, dacConfig.getConfig(),
        registry.provider(SabotContext.class));
    registry.bind(NamespaceService.Factory.class, NamespaceServiceImpl.Factory.class);
    final DatasetListingService localListing;
    if (isDistributedMaster) {
      localListing = new DatasetListingServiceImpl(registry.provider(NamespaceService.Factory.class));
    } else {
      localListing = DatasetListingService.UNSUPPORTED;
    }

    final Provider<NodeEndpoint> searchEndPoint = () -> {
      // will return master endpoint if it's masterful mode
      Optional<NodeEndpoint> serviceEndPoint =
        registry.provider(SabotContext.class).get().getServiceLeader(SearchServiceImpl.LOCAL_TASK_LEADER_NAME);
      return serviceEndPoint.orElse(null);
    };

    // this is the delegate service for localListing (calls start/close internally)
    registry.bind(DatasetListingService.class,
        new DatasetListingInvoker(
          isDistributedMaster,
          searchEndPoint,
            registry.provider(FabricService.class),
            bootstrap.getAllocator(),
            localListing));

    // RPC Endpoints.

    if (isCoordinator) {
      registry.bindSelf(
        new UserServer(
          config,
          registry.provider(java.util.concurrent.ExecutorService.class),
          registry.provider(BufferAllocator.class),
          registry.provider(UserService.class),
          registry.provider(NodeEndpoint.class),
          registry.provider(UserWorker.class),
          dacConfig.autoPort,
          bootstrapRegistry.lookup(Tracer.class)
        )
      );
    }

    registry.bindSelf(new CoordExecService(
        bootstrap.getConfig(),
        bootstrap.getAllocator(),
        registry.provider(FabricService.class),
        registry.provider(ExecutorService.class),
        registry.provider(ExecToCoordResultsHandler.class),
        registry.provider(ExecToCoordStatusHandler.class),
        registry.provider(NodeEndpoint.class),
        registry.provider(JobTelemetryClient.class)
      ));

    registry.bindSelf(HomeFileTool.class);
    registry.bindSelf(CredentialsService.class);

    // Context Service.
    final ContextService contextService = new ContextService(
      bootstrap,
      registry.provider(ClusterCoordinator.class),
      registry.provider(WorkStats.class),
      registry.provider(LegacyKVStoreProvider.class),
      registry.provider(FabricService.class),
      registry.provider(UserServer.class),
      registry.provider(MaterializationDescriptorProvider.class),
      registry.provider(QueryObserverFactory.class),
      registry.provider(AccelerationManager.class),
      registry.provider(AccelerationListManager.class),
      registry.provider(NamespaceService.Factory.class),
      registry.provider(DatasetListingService.class),
      registry.provider(UserService.class),
      registry.provider(CatalogService.class),
      registry.provider(ViewCreatorFactory.class),
      registry.provider(SpillService.class),
      registry.provider(ConnectionReader.class),
      registry.provider(CredentialsService.class),
      registry.provider(JobResultInfoProvider.class),
      registry.provider(SystemOptionManager.class),
      bootstrapRegistry.provider(EngineId.class),
      bootstrapRegistry.provider(SubEngineId.class),
      roles
    );
    registry.bind(ContextService.class, contextService);
    registry.bindProvider(SabotContext.class, contextService::get);
    registry.bindProvider(NodeEndpoint.class, contextService::getEndpoint);

    final DefaultOptionManager defaultOptionManager = new DefaultOptionManager(scanResult);
    final SystemOptionManager optionManager = new SystemOptionManager(defaultOptionManager, bootstrap.getLpPersistance(), registry.provider(LegacyKVStoreProvider.class), !isCoordinator);
    // Fhe first bind will start the optionManger (since it implements Service), the second will not and simply binds to
    // the base class.
    registry.bindSelf(optionManager);
    registry.bind(OptionManager.class, optionManager);
    registry.bind(ProjectOptionManager.class, optionManager);

    Provider<NodeEndpoint> currentEndPoint =
      () -> registry.provider(SabotContext.class).get().getEndpoint();

    if (isCoordinator && config.getBoolean(DremioConfig.JOBS_ENABLED_BOOL)) {
      registry.bindSelf(new JobsServer(registry.provider(
        LocalJobsService.class), registry.lookup(GrpcServerBuilderFactory.class),
        registry.provider(ExecToCoordStatusHandler.class), registry.provider(ExecToCoordResultsHandler.class),
        bootstrap.getAllocator()));

      registry.bindSelf(new LocalJobTelemetryServer(
          registry.lookup(GrpcServerBuilderFactory.class),
          registry.provider(LegacyKVStoreProvider.class),
          currentEndPoint)
        );
    }

    // Periodic task scheduler service
    registry.bind(SchedulerService.class, new LocalSchedulerService(
      config.getInt(DremioConfig.SCHEDULER_SERVICE_THREAD_COUNT),
      registry.provider(ClusterCoordinator.class), currentEndPoint, isDistributedCoordinator));

    if (isDistributedMaster) {
      // Companion service to clean split orphans
      registry.bind(SplitOrphansCleanerService.class, new SplitOrphansCleanerService(
        registry.provider(SchedulerService.class),
        registry.provider(NamespaceService.Factory.class)));
    }

    if(isExecutor) {
      registry.bind(SpillService.class, new SpillServiceImpl(
          config,
          new SpillServiceOptionsImpl(registry.provider(OptionManager.class)),
          registry.provider(SchedulerService.class)
        )
      );
    }

    final Provider<SabotContext> sabotContextProvider = registry.provider(SabotContext.class);
    final Provider<NodeEndpoint> nodeEndpointProvider = () -> sabotContextProvider.get().getEndpoint();
    final Provider<Iterable<NodeEndpoint>> executorsProvider = () -> sabotContextProvider.get().getExecutors();
    // PDFS depends on fabric.
    registry.bindSelf(new PDFSService(
        registry.provider(FabricService.class),
        nodeEndpointProvider,
        executorsProvider,
        bootstrapRegistry.lookup(Tracer.class),
        sabotConfig,
        bootstrap.getAllocator(),
        isExecutor ? PDFSMode.DATA : PDFSMode.CLIENT
        ));

    registry.bindSelf(new SystemTablePluginConfigProvider());

    registry.bind(CatalogService.class, new CatalogServiceImpl(
        registry.provider(SabotContext.class),
        registry.provider(SchedulerService.class),
        registry.provider(SystemTablePluginConfigProvider.class),
        registry.provider(FabricService.class),
        registry.provider(ConnectionReader.class),
        registry.provider(BufferAllocator.class),
        registry.provider(LegacyKVStoreProvider.class),
        registry.provider(DatasetListingService.class),
        registry.provider(OptionManager.class),
        config,
        roles
    ));

    // Run initializers only on coordinator.
    if (isCoordinator) {
      registry.bindSelf(new InitializerRegistry(bootstrap.getClasspathScan(), registry.getBindingProvider()));
    }

    registry.bind(CommandPool.class, CommandPoolFactory.INSTANCE.newPool(config, bootstrapRegistry.lookup(Tracer.class)));

    final Provider<NamespaceService> namespaceServiceProvider = () -> sabotContextProvider.get().getNamespaceService(SYSTEM_USERNAME);

    registry.bind(JobTelemetryClient.class,
      new JobTelemetryClient(registry.lookup(GrpcChannelBuilderFactory.class),
        registry.provider(NodeEndpoint.class)));

    final LocalJobsService localJobsService = new LocalJobsService(
      registry.provider(LegacyKVStoreProvider.class),
      bootstrap.getAllocator(),
      () -> {
        try {
          final CatalogService storagePluginRegistry = registry.provider(CatalogService.class).get();
          final FileSystemPlugin plugin = storagePluginRegistry.getSource(JOBS_STORAGEPLUGIN_NAME);
          return new JobResultsStoreConfig(plugin.getName(), plugin.getConfig().getPath(), plugin.getSystemUserFS());
        } catch (Exception e) {
          Throwables.throwIfUnchecked(e);
          throw new RuntimeException(e);
        }
      },
      registry.provider(LocalQueryExecutor.class),
      registry.provider(CoordTunnelCreator.class),
      registry.provider(ForemenTool.class),
      registry.provider(NodeEndpoint.class),
      namespaceServiceProvider,
      registry.provider(SystemOptionManager.class),
      registry.provider(AccelerationManager.class),
      registry.provider(SchedulerService.class),
      registry.provider(CommandPool.class),
      registry.provider(JobTelemetryClient.class),
      new JobResultToLogEntryConverter(),
      isDistributedMaster
    );

    registry.bind(LocalJobsService.class, localJobsService);
    registry.replaceProvider(QueryObserverFactory.class, localJobsService::getQueryObserverFactory);

    if (isCoordinator) {
      HybridJobsService hybridJobsService = new HybridJobsService(
        // for now, provide the coordinator service set
        registry.lookup(GrpcChannelBuilderFactory.class),
        () -> bootstrap.getAllocator());
      registry.bind(JobsService.class, hybridJobsService);
      registry.bind(HybridJobsService.class, hybridJobsService);
      registry.bind(JobResultInfoProvider.class, localJobsService);
    } else {
      registry.bind(JobResultInfoProvider.class, JobResultInfoProvider.NOOP);
    }

    if (isCoordinator) {
      // put provisioning service before resource allocator
      final Provider<OptionManager> optionsProvider = () -> sabotContextProvider.get().getOptionManager();
      final Provider<ClusterCoordinator> coordProvider = registry.provider(ClusterCoordinator.class);
      final NodeProvider executionNodeProvider = new NodeProvider() {
        @Override
        public Collection<NodeEndpoint> getNodes() {
          return coordProvider.get().getServiceSet(ClusterCoordinator.Role.EXECUTOR).getAvailableEndpoints();
        }
      };

      EditionProvider editionProvider = new EditionProviderImpl();
      registry.bind(EditionProvider.class, editionProvider);
      registry.bind(ProvisioningService.class, new ProvisioningServiceImpl(
        config,
        registry.provider(LegacyKVStoreProvider.class),
        executionNodeProvider,
        bootstrap.getClasspathScan(),
        optionsProvider,
        registry.provider(EditionProvider.class)
      ));
    }

    registry.bind(ResourceAllocator.class, new BasicResourceAllocator(registry.provider(ClusterCoordinator
      .class)));
    if(isCoordinator){
      final Provider<OptionManager> optionsProvider = () -> sabotContextProvider.get().getOptionManager();

      registry.bind(ExecutorSelectorFactory.class, new ExecutorSelectorFactoryImpl());
      ExecutorSelectorProvider executorSelectorProvider = new ExecutorSelectorProvider();
      registry.bind(ExecutorSelectorProvider.class, executorSelectorProvider);
      registry.bind(ExecutorSelectionService.class,
          new ExecutorSelectionServiceImpl(
              registry.provider(ClusterCoordinator.class),
              optionsProvider,
              registry.provider(ExecutorSelectorFactory.class),
              executorSelectorProvider
              )
          );

      final ExecutorSetService executorSetService =
        new LocalExecutorSetService(registry.provider(ClusterCoordinator.class));
      registry.bind(ExecutorSetService.class, executorSetService);

      CoordToExecTunnelCreator tunnelCreator = new CoordToExecTunnelCreator(registry.provider
              (FabricService.class));
      registry.bind(ExecutorServiceClientFactory.class, new ExecutorServiceProductClientFactory
              (tunnelCreator));

      final MaestroService maestroServiceImpl = new MaestroServiceImpl(
        registry.provider(ExecutorSetService.class),
        registry.provider(FabricService.class),
        registry.provider(SabotContext.class),
        registry.provider(ResourceAllocator.class),
        registry.provider(CommandPool.class),
        registry.provider(ExecutorSelectionService.class),
        registry.provider(ExecutorServiceClientFactory.class),
        registry.provider(JobTelemetryClient.class)
      );
      registry.bind(MaestroService.class, maestroServiceImpl);
      registry.bindProvider(ExecToCoordStatusHandler.class, maestroServiceImpl::getExecStatusHandler);

      final ForemenWorkManager foremenWorkManager = new ForemenWorkManager(
        registry.provider(FabricService.class),
        registry.provider(SabotContext.class),
        registry.provider(CommandPool.class),
        registry.provider(MaestroService.class),
        registry.provider(JobTelemetryClient.class),
        bootstrapRegistry.lookup(Tracer.class));

      registry.bindSelf(foremenWorkManager);
      registry.bindProvider(ExecToCoordResultsHandler.class, foremenWorkManager::getExecToCoordResultsHandler);

      registry.replaceProvider(ForemenTool.class, foremenWorkManager::getForemenTool);

      registry.replaceProvider(CoordTunnelCreator.class, foremenWorkManager::getCoordTunnelCreator);

      registry.replaceProvider(QueryCancelTool.class, foremenWorkManager::getQueryCancelTool);

      // accept enduser rpc requests (replaces noop implementation).
      registry.bindProvider(UserWorker.class, foremenWorkManager::getUserWorker);

      // accept local query execution requests.
      registry.bindProvider(LocalQueryExecutor.class, foremenWorkManager::getLocalQueryExecutor);

    } else {
      registry.bind(ForemenTool.class, ForemenTool.NO_OP);
      registry.bind(QueryCancelTool.class, QueryCancelTool.NO_OP);
    }

    TaskPoolInitializer taskPoolInitializer = null;
    if(isExecutor){
      registry.bindSelf(new ContextInformationFactory());
      taskPoolInitializer = new TaskPoolInitializer(
        registry.provider(OptionManager.class),
        config);
      registry.bindSelf(taskPoolInitializer);
      registry.bindProvider(TaskPool.class, taskPoolInitializer::getTaskPool);

      final WorkloadTicketDepotService workloadTicketDepotService = new WorkloadTicketDepotService(
        registry.provider(BufferAllocator.class),
        registry.provider(TaskPool.class),
        registry.provider(DremioConfig.class)
      );
      registry.bindSelf(workloadTicketDepotService);
      registry.bindProvider(WorkloadTicketDepot.class, workloadTicketDepotService::getTicketDepot);

      ExecToCoordTunnelCreator execToCoordTunnelCreator =
        new ExecToCoordTunnelCreator(registry.provider(FabricService.class));

      registry.bind(MaestroClientFactory.class,
        new MaestroSoftwareClientFactory(execToCoordTunnelCreator));
      registry.bind(JobTelemetryExecutorClientFactory.class,
        new JobTelemetrySoftwareClientFactory(execToCoordTunnelCreator));
      registry.bind(JobResultsClientFactory.class,
        new JobResultsSoftwareClientFactory(execToCoordTunnelCreator));

      final FragmentWorkManager fragmentWorkManager = new FragmentWorkManager(bootstrap,
        registry.provider(NodeEndpoint.class),
        registry.provider(SabotContext.class),
        registry.provider(FabricService.class),
        registry.provider(CatalogService.class),
        registry.provider(ContextInformationFactory.class),
        registry.provider(WorkloadTicketDepot.class),
        registry.provider(TaskPool.class),
        defaultOptionManager,
        registry.provider(MaestroClientFactory.class),
        registry.provider(JobTelemetryExecutorClientFactory.class),
        registry.provider(JobResultsClientFactory.class));

      registry.bindSelf(fragmentWorkManager);

      registry.bindProvider(WorkStats.class, fragmentWorkManager::getWorkStats);

      registry.bindProvider(ExecutorService.class, fragmentWorkManager::getExecutorService);
    } else {
      registry.bind(WorkStats.class, WorkStats.NO_OP);
    }

    registry.bind(AccelerationManager.class, AccelerationManager.NO_OP);

    if (isCoordinator) {
      final ReflectionServiceImpl reflectionService = new ReflectionServiceImpl(
        sabotConfig,
        registry.provider(LegacyKVStoreProvider.class),
        registry.provider(SchedulerService.class),
        registry.provider(JobsService.class),
        registry.provider(CatalogService.class),
        registry.provider(SabotContext.class),
        registry.provider(ReflectionStatusService.class),
        bootstrap.getExecutor(),
        isDistributedMaster,
        bootstrap.getAllocator());
      registry.bind(ReflectionService.class, reflectionService);
      registry.replaceProvider(MaterializationDescriptorProvider.class, reflectionService::getMaterializationDescriptor);
      registry.replaceProvider(AccelerationManager.class, reflectionService::getAccelerationManager);

      final Provider<Collection<NodeEndpoint>> nodeEndpointsProvider = () -> sabotContextProvider.get().getExecutors();

      registry.bind(ReflectionStatusService.class, new ReflectionStatusServiceImpl(
        nodeEndpointsProvider,
        namespaceServiceProvider,
        registry.provider(CatalogService.class),
        registry.provider(LegacyKVStoreProvider.class),
        registry.provider(ReflectionService.class).get().getCacheViewerProvider()
      ));
    } else {
      registry.bind(ReflectionService.class, new ExecutorOnlyReflectionService());
      registry.bind(ReflectionStatusService.class, ReflectionStatusService.NOOP);
    }

    final Provider<Optional<NodeEndpoint>> serviceLeaderProvider = () -> sabotContextProvider.get().getServiceLeader(LOCAL_TASK_LEADER_NAME);
    final AccelerationListManagerImpl accelerationListManager = new AccelerationListManagerImpl(
      registry.provider(LegacyKVStoreProvider.class),
      registry.provider(ReflectionStatusService.class),
      registry.provider(ReflectionService.class),
      registry.provider(FabricService.class),
      registry.provider(BufferAllocator.class),
      () -> config,
      isMaster,
      isCoordinator,
      serviceLeaderProvider);
    registry.bind(AccelerationListManager.class, accelerationListManager);
    registry.bindProvider(ReflectionTunnelCreator.class, accelerationListManager::getReflectionTunnelCreator);

    final Provider<OptionManager> optionsProvider = () -> sabotContextProvider.get().getOptionManager();

    if(isCoordinator) {
      registry.bindSelf(new ServerHealthMonitor(registry.provider(MasterStatusListener.class)));
    }

    registry.bind(SupportService.class, new SupportService(
      dacConfig,
      registry.provider(LegacyKVStoreProvider.class),
      registry.provider(JobsService.class),
      registry.provider(UserService.class),
      registry.provider(ClusterCoordinator.class),
      registry.provider(SystemOptionManager.class),
      namespaceServiceProvider,
      registry.provider(CatalogService.class),
      registry.provider(FabricService.class),
      bootstrap.getAllocator()));

    registry.bindSelf(new NodeRegistration(
        registry.provider(NodeEndpoint.class),
        registry.provider(FragmentWorkManager.class),
        registry.provider(ForemenWorkManager.class),
        registry.provider(ClusterCoordinator.class),
        registry.provider(DremioConfig.class)
        ));

    if(isCoordinator){
      registry.bind(SampleDataPopulatorService.class,
          new SampleDataPopulatorService(
              registry.provider(SabotContext.class),
              registry.provider(LegacyKVStoreProvider.class),
              registry.provider(UserService.class),
              registry.provider(InitializerRegistry.class),
              registry.provider(JobsService.class),
              registry.provider(CatalogService.class),
              registry.provider(ReflectionServiceHelper.class),
              registry.provider(ConnectionReader.class),
              registry.provider(CollaborationHelper.class),
              optionsProvider,
              dacConfig.prepopulate,
              dacConfig.addDefaultUser));

      // search
      final SearchService searchService;
      if (isDistributedMaster) {
        searchService = new SearchServiceImpl(
          namespaceServiceProvider,
          registry.provider(SystemOptionManager.class),
          registry.provider(LegacyKVStoreProvider.class),
          registry.provider(SchedulerService.class),
          bootstrap.getExecutor()
        );
      } else {
        searchService = SearchService.UNSUPPORTED;
      }

      final Provider<Optional<NodeEndpoint>> taskLeaderProvider =
        () -> sabotContextProvider.get().getServiceLeader(SearchServiceImpl.LOCAL_TASK_LEADER_NAME);
      registry.bind(SearchService.class, new SearchServiceInvoker(
        isDistributedMaster,
        registry.provider(NodeEndpoint.class),
        taskLeaderProvider,
        registry.provider(FabricService.class),
        bootstrap.getAllocator(),
        searchService
      ));

      registry.bind(RestServerV2.class, new RestServerV2(bootstrap.getClasspathScan()));
      registry.bind(APIServer.class, new APIServer(bootstrap.getClasspathScan()));

      registry.bind(DremioServlet.class, new DremioServlet(dacConfig.getConfig(),
        registry.provider(ServerHealthMonitor.class),
        optionsProvider,
        registry.provider(SupportService.class)
      ));

      // if we have at least one user registered, disable firstTimeApi and checkNoUser
      // but for userGroupService is not started yet so we cannot check for now
      registry.bind(WebServer.class, new WebServer(registry,
          dacConfig,
          registry.provider(ServerHealthMonitor.class),
          registry.provider(NodeEndpoint.class),
          registry.provider(SabotContext.class),
          registry.provider(RestServerV2.class),
          registry.provider(APIServer.class),
          registry.provider(DremioServer.class),
          bootstrapRegistry.lookup(Tracer.class),
          "ui",
          isInternalUGS));

      registry.bind(TokenManager.class, new TokenManagerImpl(
          registry.provider(LegacyKVStoreProvider.class),
          registry.provider(SchedulerService.class),
          registry.provider(SystemOptionManager.class),
          isDistributedMaster,
          dacConfig));
    }

    LivenessService livenessService = new LivenessService(config);
    registry.bind(LivenessService.class, livenessService);
    if (taskPoolInitializer != null) {
      livenessService.addHealthMonitor(taskPoolInitializer);
    }

    registry.bindSelf(SourceService.class);
    registry.bindSelf(DatasetVersionMutator.class);
    registry.bind(NamespaceService.class, NamespaceServiceImpl.class);
    registry.bindSelf(ReflectionServiceHelper.class);
    registry.bindSelf(CatalogServiceHelper.class);
    registry.bindSelf(CollaborationHelper.class);
    registry.bindSelf(UserServiceHelper.class);

    registry.bind(FirstLoginSetupService.class, OSSFirstLoginSetupService.NOOP_INSTANCE);
  }

  /**
   * Set up the {@link UserService} in registry according to the config.
   * @return True if the internal user management is used.
   */
  protected boolean setupUserService(
      final SingletonRegistry registry,
      final DremioConfig config,
      final Provider<SabotContext> sabotContext
  ){
    final String authType = config.getString(WEB_AUTH_TYPE);

    if ("internal".equals(authType)) {
      registry.bindProvider(UserService.class, () -> new SimpleUserService(registry.provider(LegacyKVStoreProvider.class)));
      logger.info("Internal user/group service is configured.");
      return true;
    }

    logger.error("Unknown value '{}' set for {}. Accepted values are ['internal', 'ldap']", authType, WEB_AUTH_TYPE);
    throw new RuntimeException(
        String.format("Unknown auth type '%s' set in config path '%s'", authType, WEB_AUTH_TYPE));
  }
}

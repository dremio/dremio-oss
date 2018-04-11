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
package com.dremio.dac.daemon;

import static com.dremio.config.DremioConfig.WEB_AUTH_TYPE;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.EnumSet;

import javax.inject.Provider;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.nodes.NodeProvider;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.store.ViewCreatorFactory;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.DACDaemon.ClusterMode;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.server.APIServer;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.RestServerV2;
import com.dremio.dac.server.WebServer;
import com.dremio.dac.server.tokens.TokenManager;
import com.dremio.dac.server.tokens.TokenManagerImpl;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.datasets.DACViewCreatorFactory;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.exec.MasterElectionService;
import com.dremio.dac.service.exec.MasterStatusListener;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.support.SupportService;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.RemoteKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.NodeRegistration;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.PDFSService;
import com.dremio.exec.store.dfs.PDFSService.PDFSMode;
import com.dremio.exec.store.sys.PersistentStoreProvider;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.exec.work.RunningQueryProvider;
import com.dremio.exec.work.WorkStats;
import com.dremio.exec.work.protector.ForemenTool;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.exec.work.rpc.CoordTunnelCreator;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.provision.service.ProvisioningService;
import com.dremio.provision.service.ProvisioningServiceImpl;
import com.dremio.sabot.exec.FragmentWorkManager;
import com.dremio.sabot.exec.context.ContextInformationFactory;
import com.dremio.sabot.rpc.CoordExecService;
import com.dremio.sabot.rpc.CoordToExecHandler;
import com.dremio.sabot.rpc.ExecToCoordHandler;
import com.dremio.sabot.rpc.user.UserServer;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.SingletonRegistry;
import com.dremio.service.accelerator.AccelerationListManagerImpl;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.service.coordinator.zk.ZKClusterCoordinator;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.listing.DatasetListingInvoker;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.listing.DatasetListingServiceImpl;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.reflection.ExecutorOnlyReflectionService;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.ReflectionStatusServiceImpl;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SimpleUserService;
import com.dremio.service.users.UserService;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.base.Throwables;

/**
 * DAC module to setup Dremio daemon
 */
public class DACDaemonModule implements DACModule {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DACDaemonModule.class);

  public static final String JOBS_STORAGEPLUGIN_NAME = "__jobResultsStore";
  public static final String SCRATCH_STORAGEPLUGIN_NAME = "$scratch";

  public DACDaemonModule() {
  }

  @Override
  public void bootstrap(final Runnable shutdownHook, final SingletonRegistry bootstrapRegistry, ScanResult scanResult, DACConfig dacConfig, boolean isMaster) {
    final DremioConfig config = dacConfig.getConfig();
    final boolean embeddedZookeeper = config.getBoolean(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL);

    bootstrapRegistry.bindSelf(new BootStrapContext(config, scanResult));

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

    // start master status listener
    bootstrapRegistry.bindSelf(new MasterStatusListener(bootstrapRegistry.provider(ClusterCoordinator.class), isMaster));
  }

  @Override
  public void build(final SingletonRegistry bootstrapRegistry, final SingletonRegistry registry, ScanResult scanResult,
      DACConfig dacConfig, boolean isMaster) {
    final DremioConfig config = dacConfig.getConfig();
    final SabotConfig sabotConfig = config.getSabotConfig();
    final BootStrapContext bootstrap = bootstrapRegistry.lookup(BootStrapContext.class);

    final boolean isCoordinator = config.getBoolean(DremioConfig.ENABLE_COORDINATOR_BOOL);
    final boolean isExecutor = config.getBoolean(DremioConfig.ENABLE_EXECUTOR_BOOL);

    final Provider<NodeEndpoint> masterEndpoint = new Provider<NodeEndpoint>() {
      private final Provider<MasterStatusListener> masterStatusListener =
          registry.provider(MasterStatusListener.class);

      @Override
      public NodeEndpoint get() {
        return masterStatusListener.get().getMasterNode();
      }
    };

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

    ConnectionReader connectionReader = new ConnectionReader(scanResult);
    registry.bind(ConnectionReader.class, connectionReader);

    // register default providers.

    registry.bind(MaterializationDescriptorProvider.class, MaterializationDescriptorProvider.EMPTY);
    registry.bind(QueryObserverFactory.class, QueryObserverFactory.DEFAULT);

    // copy bootstrap bindings to the main registry.
    bootstrapRegistry.copyBindings(registry);

    { // persistent store provider
      final PersistentStoreProvider storeProvider;
      storeProvider = new KVPersistentStoreProvider(registry.provider(KVStoreProvider.class), !isCoordinator);
      registry.bind(PersistentStoreProvider.class, storeProvider);
    }

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
            sabotConfig.getInt(ExecConstants.BIT_RPC_TIMEOUT),
            sabotConfig.getInt(ExecConstants.BIT_SERVER_RPC_THREADS),
            bootstrap.getExecutor(),
            bootstrap.getAllocator(),
            config.getBytes(DremioConfig.FABRIC_MEMORY_RESERVATION),
            Long.MAX_VALUE));

    { // KVStoreProvider
      final KVStoreProvider provider;
      if(isMaster){
        provider =  new LocalKVStoreProvider(
            bootstrap.getClasspathScan(),
            registry.provider(FabricService.class),
            bootstrap.getAllocator(),
            config.getThisNode(),
            config.getString(DremioConfig.DB_PATH_STRING),
            dacConfig.inMemoryStorage,
            true,
            true,
            false);

      } else {
        final Provider<NodeEndpoint> masterNodeProvider = new Provider<NodeEndpoint>() {
          private final Provider<MasterStatusListener> masterStatusListener = registry.provider(MasterStatusListener.class);
          @Override
          public NodeEndpoint get() {
            return masterStatusListener.get().getMasterNode();
          }
        };
        provider = new RemoteKVStoreProvider(
            bootstrap.getClasspathScan(),
            masterNodeProvider,
            registry.provider(FabricService.class),
            bootstrap.getAllocator(),
            fabricAddress);
      }
      registry.bind(KVStoreProvider.class, provider);
    }

    registry.bind(
      ViewCreatorFactory.class,
      new DACViewCreatorFactory(
        registry.provider(InitializerRegistry.class),
        registry.provider(KVStoreProvider.class),
        registry.provider(JobsService.class),
        registry.provider(NamespaceService.Factory.class),
        registry.provider(ReflectionService.class),
        registry.provider(CatalogService.class)
      )
    );


    final boolean isInternalUGS = setupUserService(registry, dacConfig.getConfig());
    registry.bind(NamespaceService.Factory.class, NamespaceServiceImpl.Factory.class);
    final DatasetListingService localListing;
    if (isMaster) {
      localListing = new DatasetListingServiceImpl(registry.provider(NamespaceService.Factory.class));
    } else {
      localListing = DatasetListingService.UNSUPPORTED;
    }
    // this is the delegate service for localListing (calls start/close internally)
    registry.bind(DatasetListingService.class,
        new DatasetListingInvoker(
            isMaster,
            masterEndpoint,
            registry.provider(FabricService.class),
            bootstrap.getAllocator(),
            localListing));

    // RPC Endpoints.

    if(isCoordinator){
      registry.bindSelf(new UserServer(bootstrap,
          registry.provider(SabotContext.class),
          registry.provider(UserWorker.class),
          null,
          dacConfig.autoPort));
    }

    registry.bindSelf(new CoordExecService(
        bootstrap.getConfig(),
        bootstrap.getAllocator(),
        registry.getBindingCreator(),
        registry.provider(FabricService.class),
        registry.provider(CoordToExecHandler.class),
        registry.provider(ExecToCoordHandler.class)
        ));

    registry.bindSelf(HomeFileTool.class);

    // Context Service.
    registry.bind(ContextService.class, new ContextService(
        registry.getBindingCreator(),
        bootstrap,
        registry.provider(ClusterCoordinator.class),
        registry.provider(PersistentStoreProvider.class),
        registry.provider(WorkStats.class),
        registry.provider(KVStoreProvider.class),
        registry.provider(FabricService.class),
        registry.provider(UserServer.class),
        registry.provider(MaterializationDescriptorProvider.class),
        registry.provider(QueryObserverFactory.class),
        registry.provider(RunningQueryProvider.class),
        registry.provider(AccelerationManager.class),
        registry.provider(AccelerationListManager.class),
        registry.provider(NamespaceService.Factory.class),
        registry.provider(DatasetListingService.class),
        registry.provider(UserService.class),
        registry.provider(CatalogService.class),
        registry.provider(ViewCreatorFactory.class),
        roles
        ));

    // PDFS depends on fabric.
    registry.bindSelf(new PDFSService(
        registry.provider(SabotContext.class),
        registry.provider(FabricService.class),
        sabotConfig,
        bootstrap.getAllocator(),
        isExecutor ? PDFSMode.DATA : PDFSMode.CLIENT
        ));

    registry.bind(SchedulerService.class, new LocalSchedulerService(config.getInt(DremioConfig.SCHEDULER_SERVICE_THREAD_COUNT)));
    registry.bindSelf(new SystemTablePluginConfigProvider());

    registry.bind(CatalogService.class, new CatalogServiceImpl(
        registry.provider(SabotContext.class),
        registry.provider(SchedulerService.class),
        registry.provider(SystemTablePluginConfigProvider.class),
        registry.provider(FabricService.class)
        )
        );


    registry.bindSelf(new InitializerRegistry(bootstrap.getClasspathScan(), registry.getBindingProvider()));

    registry.bind(JobsService.class,
        new LocalJobsService(
            registry.getBindingCreator(),
            registry.provider(KVStoreProvider.class),
            bootstrap.getAllocator(),
            new Provider<FileSystemPlugin>() {
              @Override
              public FileSystemPlugin get() {
                try {
                  CatalogService storagePluginRegistry = registry.provider(CatalogService.class).get();
                  return (FileSystemPlugin) storagePluginRegistry.getSource(JOBS_STORAGEPLUGIN_NAME);
                } catch(Exception e) {
                  throw Throwables.propagate(e);
                }
              }
            },
            registry.provider(LocalQueryExecutor.class),
            registry.provider(CoordTunnelCreator.class),
            registry.provider(ForemenTool.class),
            registry.provider(SabotContext.class),
            registry.provider(SchedulerService.class),
            isMaster
            )
        );

    if(isCoordinator){
      registry.bindSelf(
          new ForemenWorkManager(
              registry.provider(ClusterCoordinator.class),
              registry.provider(FabricService.class),
              registry.provider(SabotContext.class),
              registry.getBindingCreator()
              )
          );
    } else {
      registry.bind(ForemenTool.class, ForemenTool.NO_OP);
      registry.bind(RunningQueryProvider.class, RunningQueryProvider.EMPTY);
    }

    if(isExecutor){
      registry.bindSelf(new ContextInformationFactory());
      registry.bindSelf(
          new FragmentWorkManager(bootstrap,
              config,
              registry.provider(NodeEndpoint.class),
              registry.provider(SabotContext.class),
              registry.provider(FabricService.class),
              registry.provider(CatalogService.class),
              registry.provider(ContextInformationFactory.class),
              registry.getBindingCreator()));
    } else {
      registry.bind(WorkStats.class, WorkStats.NO_OP);
    }

    final Provider<FileSystemPlugin> acceleratorStoragePluginProvider = new Provider<FileSystemPlugin>() {
      @Override
      public FileSystemPlugin get() {
        CatalogService catalogService = registry.provider(CatalogService.class).get();

        try {
          return (FileSystemPlugin) catalogService.getSource(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME);
        } catch(Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };

    registry.bind(AccelerationManager.class, AccelerationManager.NO_OP);

    if (isCoordinator) {
      registry.bind(ReflectionService.class, new ReflectionServiceImpl(
        sabotConfig,
        registry.provider(KVStoreProvider.class),
        registry.provider(SchedulerService.class),
        registry.provider(JobsService.class),
        registry.provider(CatalogService.class),
        acceleratorStoragePluginProvider,
        registry.provider(SabotContext.class),
        registry.provider(ReflectionStatusService.class),
        bootstrap.getExecutor(),
        registry.getBindingCreator(),
        isMaster
      ));
      registry.bind(ReflectionStatusService.class, new ReflectionStatusServiceImpl(
        registry.provider(SabotContext.class),
        registry.provider(CatalogService.class),
        registry.provider(KVStoreProvider.class),
        registry.provider(ReflectionService.class).get().getCacheViewerProvider()
      ));
    } else {
      registry.bind(ReflectionService.class, new ExecutorOnlyReflectionService());
      registry.bind(ReflectionStatusService.class, ReflectionStatusService.NOOP);
    }

    registry.bind(AccelerationListManager.class, new AccelerationListManagerImpl(
      registry.provider(KVStoreProvider.class),
      registry.provider(SabotContext.class),
      registry.provider(ReflectionStatusService.class),
      registry.provider(FabricService.class),
      registry.getBindingCreator()));

    if(isCoordinator){
      final Provider<ClusterCoordinator> coordProvider = registry.provider(ClusterCoordinator.class);
      final NodeProvider executionNodeProvider = new NodeProvider(){
        @Override
        public Collection<NodeEndpoint> getNodes() {
          return coordProvider.get().getServiceSet(ClusterCoordinator.Role.EXECUTOR).getAvailableEndpoints();
        }
      };

      registry.bind(ProvisioningService.class, new ProvisioningServiceImpl(
          registry.provider(KVStoreProvider.class),
          executionNodeProvider,
          bootstrap.getClasspathScan()
          ));
    }

    registry.bind(SupportService.class, new SupportService(
      dacConfig,
      registry.provider(KVStoreProvider.class),
      registry.provider(JobsService.class),
      registry.provider(UserService.class),
      registry.provider(SabotContext.class),
      registry.provider(CatalogService.class)));

    if(isCoordinator){
      registry.bindSelf(new ServerHealthMonitor(registry.provider(MasterStatusListener.class)));
    }

    registry.bindSelf(new NodeRegistration(
        registry.provider(SabotContext.class),
        registry.provider(FragmentWorkManager.class),
        registry.provider(ForemenWorkManager.class),
        registry.provider(ClusterCoordinator.class)
        ));

    if(isCoordinator){
      registry.bind(SampleDataPopulatorService.class,
          new SampleDataPopulatorService(
              registry.provider(SabotContext.class),
              registry.provider(KVStoreProvider.class),
              registry.provider(UserService.class),
              registry.provider(InitializerRegistry.class),
              registry.provider(JobsService.class),
              registry.provider(CatalogService.class),
              registry.provider(ReflectionServiceHelper.class),
              registry.provider(ConnectionReader.class),
              dacConfig.prepopulate,
              dacConfig.addDefaultUser));

      registry.bind(RestServerV2.class, new RestServerV2(bootstrap.getClasspathScan()));
      registry.bind(APIServer.class, new APIServer(bootstrap.getClasspathScan()));

      // if we have at least one user registered, disable firstTimeApi and checkNoUser
      // but for userGroupService is not started yet so we cannot check for now
      registry.bind(WebServer.class, new WebServer(registry,
          dacConfig,
          registry.provider(ServerHealthMonitor.class),
          registry.provider(NodeEndpoint.class),
          registry.provider(SabotContext.class),
          registry.provider(RestServerV2.class),
          registry.provider(APIServer.class),
          "ui",
          isInternalUGS));

      registry.bind(TokenManager.class, new TokenManagerImpl(
          registry.provider(KVStoreProvider.class),
          registry.provider(SchedulerService.class),
          isMaster,
          dacConfig));
    }

    registry.bindSelf(SourceService.class);
    registry.bindSelf(DatasetVersionMutator.class);
    registry.bind(NamespaceService.class, NamespaceServiceImpl.class);
    registry.bindSelf(ReflectionServiceHelper.class);
    registry.bindSelf(CatalogServiceHelper.class);
  }

  /**
   * Set up the {@link UserService} in registry according to the config.
   * @return True if the internal user management is used.
   */
  protected boolean setupUserService(final SingletonRegistry registry, final DremioConfig config) {
    final String authType = config.getString(WEB_AUTH_TYPE);

    if ("internal".equals(authType)) {
      registry.bind(UserService.class, SimpleUserService.class);
      logger.info("Internal user/group service is configured.");
      return true;
    }

    logger.error("Unknown value '{}' set for {}. Accepted values are ['internal', 'ldap']", authType, WEB_AUTH_TYPE);
    throw new RuntimeException(
        String.format("Unknown auth type '%s' set in config path '%s'", authType, WEB_AUTH_TYPE));
  }
}

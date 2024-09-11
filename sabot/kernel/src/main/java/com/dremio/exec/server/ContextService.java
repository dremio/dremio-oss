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

import static com.google.common.base.Throwables.throwIfUnchecked;

import com.dremio.common.AutoCloseables;
import com.dremio.common.VM;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.ViewCreatorFactory;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.enginemanagement.proto.EngineManagementProtos.EngineId;
import com.dremio.exec.enginemanagement.proto.EngineManagementProtos.SubEngineId;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.maestro.GlobalKeysService;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.cost.RelMetadataQuerySupplier;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.LoggedFileSystemWrapper;
import com.dremio.exec.store.dfs.MetadataIOPool;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.statistics.StatisticsAdministrationService;
import com.dremio.exec.store.sys.statistics.StatisticsListManager;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.dremio.exec.store.sys.udf.UserDefinedFunctionService;
import com.dremio.exec.work.WorkStats;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.sabot.rpc.user.UserServer;
import com.dremio.service.Service;
import com.dremio.service.catalog.DatasetCatalogServiceGrpc.DatasetCatalogServiceBlockingStub;
import com.dremio.service.catalog.InformationSchemaServiceGrpc.InformationSchemaServiceBlockingStub;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.conduit.server.ConduitInProcessChannelProvider;
import com.dremio.service.conduit.server.ConduitServer;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.coordinator.CoordinatorModeInfo;
import com.dremio.service.coordinator.ServiceSetDecorator;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.spill.SpillService;
import com.dremio.service.users.UserService;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.SecretsCreator;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.collect.Sets;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Set;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.projectnessie.client.api.NessieApiV2;

/** Service Used to construct the base context used by other services. */
public class ContextService implements Service, Provider<SabotContext> {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ContextService.class);

  private final BootStrapContext bootstrapContext;
  private final Provider<ClusterCoordinator> coordinatorProvider;
  private final Provider<GroupResourceInformation> resourceInformationProvider;
  private final Provider<WorkStats> workStats;
  private final Provider<LegacyKVStoreProvider> kvStoreProvider;
  private final Provider<FabricService> fabric;
  private final Provider<ConduitServer> conduitServer;
  private final Provider<UserServer> userServer;
  private final Provider<MaterializationDescriptorProvider> materializationDescriptorProvider;
  private final Provider<QueryObserverFactory> queryObserverFactory;
  private final Provider<AccelerationManager> accelerationManager;
  private final Provider<StatisticsService> statisticsService;
  private final Provider<AccelerationListManager> accelerationListManager;
  private final Provider<NamespaceService.Factory> namespaceServiceFactoryProvider;
  private final Provider<Orphanage.Factory> orphanageFactoryProvider;
  private final Provider<DatasetListingService> datasetListingServiceProvider;
  private final Provider<UserService> userService;
  private final Provider<CatalogService> catalogService;
  private final Provider<ConduitProvider> masterCoordinatorConduit;
  private final Provider<InformationSchemaServiceBlockingStub> informationSchemaStub;
  private final Provider<SpillService> spillService;
  private final Provider<ConnectionReader> connectionReaderProvider;
  private final Provider<ViewCreatorFactory> viewCreatorFactory;
  private final Provider<OptionManager> optionManagerProvider;
  private final Set<ClusterCoordinator.Role> roles;
  private final Provider<JobResultInfoProvider> jobResultInfoProvider;
  private final Provider<EngineId> engineIdProvider;
  private final Provider<SubEngineId> subEngineIdProvider;
  private final Provider<OptionValidatorListing> optionValidatorProvider;
  private final Provider<CoordinatorModeInfo> coordinatorModeInfoProvider;
  private final Provider<NessieApiV2> nessieApiProvider;
  private final Provider<StatisticsAdministrationService.Factory>
      statisticsAdministrationServiceFactory;
  private final Provider<StatisticsListManager> statisticsListManagerProvider;
  private final Provider<UserDefinedFunctionService> userDefinedFunctionListManagerProvider;
  private final Provider<RelMetadataQuerySupplier> relMetadataQuerySupplier;
  protected BufferAllocator queryPlannerAllocator;
  private final Provider<SimpleJobRunner> jobsRunnerProvider;
  private final Provider<DatasetCatalogServiceBlockingStub> datasetCatalogStub;
  private final Provider<GlobalKeysService> globalCredentailsServiceProvider;
  private final Provider<com.dremio.services.credentials.CredentialsService>
      credentialsServiceProvider;
  private final Provider<ConduitInProcessChannelProvider> conduitInProcessChannelProviderProvider;
  private final Provider<SysFlightChannelProvider> sysFlightChannelProviderProvider;
  private final Provider<SourceVerifier> sourceVerifierProvider;
  private final Provider<SecretsCreator> secretsCreatorProvider;
  private final Provider<ForemenWorkManager> foremenWorkManagerProvider;
  private final Provider<MetadataIOPool> metadataIOPoolProvider;

  private SabotContext context;

  public ContextService(
      BootStrapContext bootstrapContext,
      Provider<ClusterCoordinator> coordinatorProvider,
      Provider<GroupResourceInformation> resourceInformationProvider,
      Provider<WorkStats> workStats,
      Provider<LegacyKVStoreProvider> kvStoreProvider,
      Provider<FabricService> fabric,
      Provider<ConduitServer> conduitServer,
      Provider<UserServer> userServer,
      Provider<MaterializationDescriptorProvider> materializationDescriptorProvider,
      Provider<QueryObserverFactory> queryObserverFactory,
      Provider<AccelerationManager> accelerationManager,
      Provider<AccelerationListManager> accelerationListManager,
      Provider<NamespaceService.Factory> namespaceServiceFactoryProvider,
      Provider<Orphanage.Factory> orphanageFactoryProvider,
      Provider<DatasetListingService> datasetListingServiceProvider,
      Provider<UserService> userService,
      Provider<CatalogService> catalogService,
      Provider<ConduitProvider> conduitProvider,
      Provider<InformationSchemaServiceBlockingStub> informationSchemaStub,
      Provider<ViewCreatorFactory> viewCreatorFactory,
      Provider<SpillService> spillService,
      Provider<ConnectionReader> connectionReaderProvider,
      Provider<JobResultInfoProvider> jobResultInfoProvider,
      Provider<OptionManager> optionManagerProvider,
      Provider<EngineId> engineIdProvider,
      Provider<SubEngineId> subEngineIdProvider,
      Provider<OptionValidatorListing> optionValidatorProvider,
      Set<ClusterCoordinator.Role> roles,
      Provider<CoordinatorModeInfo> coordinatorModeInfoProvider,
      Provider<NessieApiV2> nessieApiProvider,
      Provider<StatisticsService> statisticsService,
      Provider<StatisticsAdministrationService.Factory> statisticsAdministrationServiceFactory,
      Provider<StatisticsListManager> statisticsListManagerProvider,
      Provider<UserDefinedFunctionService> userDefinedFunctionListManagerProvider,
      Provider<RelMetadataQuerySupplier> relMetadataQuerySupplier,
      Provider<SimpleJobRunner> jobsRunnerProvider,
      Provider<DatasetCatalogServiceBlockingStub> datasetCatalogStub,
      Provider<GlobalKeysService> globalCredentailsServiceProvider,
      Provider<CredentialsService> credentialsServiceProvider,
      Provider<ConduitInProcessChannelProvider> conduitInProcessChannelProviderProvider,
      Provider<SysFlightChannelProvider> sysFlightChannelProviderProvider,
      Provider<SourceVerifier> sourceVerifierProvider,
      Provider<SecretsCreator> secretsCreatorProvider,
      Provider<ForemenWorkManager> foremenWorkManagerProvider,
      Provider<MetadataIOPool> metadataIOPoolProvider) {
    this.bootstrapContext = bootstrapContext;
    this.workStats = workStats;
    this.kvStoreProvider = kvStoreProvider;
    this.userServer = userServer;
    this.coordinatorProvider = coordinatorProvider;
    this.resourceInformationProvider = resourceInformationProvider;
    this.fabric = fabric;
    this.conduitServer = conduitServer;
    this.materializationDescriptorProvider = materializationDescriptorProvider;
    this.queryObserverFactory = queryObserverFactory;
    this.accelerationManager = accelerationManager;
    this.accelerationListManager = accelerationListManager;
    this.namespaceServiceFactoryProvider = namespaceServiceFactoryProvider;
    this.orphanageFactoryProvider = orphanageFactoryProvider;
    this.datasetListingServiceProvider = datasetListingServiceProvider;
    this.userService = userService;
    this.catalogService = catalogService;
    this.masterCoordinatorConduit = conduitProvider;
    this.informationSchemaStub = informationSchemaStub;
    this.viewCreatorFactory = viewCreatorFactory;
    this.spillService = spillService;
    this.connectionReaderProvider = connectionReaderProvider;
    this.optionManagerProvider = optionManagerProvider;
    this.roles = Sets.immutableEnumSet(roles);
    this.jobResultInfoProvider = jobResultInfoProvider;
    this.engineIdProvider = engineIdProvider;
    this.subEngineIdProvider = subEngineIdProvider;
    this.optionValidatorProvider = optionValidatorProvider;
    this.coordinatorModeInfoProvider = coordinatorModeInfoProvider;
    this.nessieApiProvider = nessieApiProvider;
    this.statisticsService = statisticsService;
    this.statisticsAdministrationServiceFactory = statisticsAdministrationServiceFactory;
    this.statisticsListManagerProvider = statisticsListManagerProvider;
    this.userDefinedFunctionListManagerProvider = userDefinedFunctionListManagerProvider;
    this.relMetadataQuerySupplier = relMetadataQuerySupplier;
    this.jobsRunnerProvider = jobsRunnerProvider;
    this.datasetCatalogStub = datasetCatalogStub;
    this.globalCredentailsServiceProvider = globalCredentailsServiceProvider;
    this.credentialsServiceProvider = credentialsServiceProvider;
    this.conduitInProcessChannelProviderProvider = conduitInProcessChannelProviderProvider;
    this.sysFlightChannelProviderProvider = sysFlightChannelProviderProvider;
    this.sourceVerifierProvider = sourceVerifierProvider;
    this.secretsCreatorProvider = secretsCreatorProvider;
    this.foremenWorkManagerProvider = foremenWorkManagerProvider;
    this.metadataIOPoolProvider = metadataIOPoolProvider;
  }

  @Override
  public void start() throws Exception {
    queryPlannerAllocator =
        bootstrapContext
            .getAllocator()
            .newChildAllocator("query-planning", 0, bootstrapContext.getAllocator().getLimit());
  }

  protected SabotContext newSabotContext() throws Exception {
    if (queryPlannerAllocator == null) {
      throw new IllegalStateException("Context Service has not been #start'ed");
    }

    final FabricService fabric = this.fabric.get();
    int conduitPort = -1;
    if (conduitServer.get() != null) {
      conduitPort = conduitServer.get().getPort();
    }
    int userport = -1;
    try {
      userport = userServer.get().getPort();
    } catch (RuntimeException ex) {
      if (roles.contains(ClusterCoordinator.Role.COORDINATOR)) {
        throw ex;
      }
    }

    final SabotConfig sConfig = bootstrapContext.getConfig();
    final String rpcBindAddressOpt = sConfig.getString(ExecConstants.REGISTRATION_ADDRESS);
    final String rpcBindAddress =
        (rpcBindAddressOpt.trim().isEmpty()) ? fabric.getAddress() : rpcBindAddressOpt;

    InetAddress[] iFaces = InetAddress.getAllByName(rpcBindAddress);
    logger.info(
        "IFaces {} bound to the host: {}", Arrays.asList(iFaces).toString(), rpcBindAddress);

    DremioConfig dremioConfig = bootstrapContext.getDremioConfig();
    final NodeEndpoint.Builder identityBuilder =
        NodeEndpoint.newBuilder()
            .setAddress(rpcBindAddress)
            .setUserPort(userport)
            .setFabricPort(fabric.getPort())
            .setConduitPort(conduitPort)
            .setStartTime(System.currentTimeMillis())
            .setMaxDirectMemory(VM.getMaxDirectMemory())
            .setAvailableCores(VM.availableProcessors())
            .setRoles(ClusterCoordinator.Role.toEndpointRoles(roles))
            .setDremioVersion(DremioVersionInfo.getVersion())
            .setNodeTag(dremioConfig.getString(DremioConfig.NODE_TAG));

    if (engineIdProvider != null && engineIdProvider.get() != null) {
      identityBuilder.setEngineId(engineIdProvider.get());
    }

    if (subEngineIdProvider != null && subEngineIdProvider.get() != null) {
      identityBuilder.setSubEngineId(subEngineIdProvider.get());
    }

    String containerId = System.getenv("CONTAINER_ID");
    if (containerId != null) {
      identityBuilder.setProvisionId(containerId);
    }

    final NodeEndpoint identity = identityBuilder.build();

    OptionManager optionManager = optionManagerProvider.get();
    ClusterCoordinator coordinator = coordinatorProvider.get();
    BufferAllocator allocator = bootstrapContext.getAllocator();
    ScanResult classpathScan = bootstrapContext.getClasspathScan();
    LogicalPlanPersistence lpPersistance = bootstrapContext.getLpPersistance();

    PhysicalPlanReader physicalPlanReader =
        new PhysicalPlanReader(
            classpathScan, lpPersistance, catalogService, connectionReaderProvider.get());
    FunctionImplementationRegistry functionRegistry =
        FunctionImplementationRegistry.create(sConfig, classpathScan, optionManager, false);
    FunctionImplementationRegistry decimalFunctionImplementationRegistry =
        FunctionImplementationRegistry.create(sConfig, classpathScan, optionManager, true);
    CodeCompiler compiler = new CodeCompiler(sConfig, optionManager);
    FileSystemWrapper fileSystemWrapper =
        new LoggedFileSystemWrapper(
            sConfig.getInstance(
                FileSystemWrapper.FILE_SYSTEM_WRAPPER_CLASS,
                FileSystemWrapper.class,
                (fs, storageId, conf, operatorContext, enableAsync, isMetadataEnabled) -> fs,
                dremioConfig,
                optionManager,
                allocator,
                new ServiceSetDecorator(coordinator.getServiceSet(Role.EXECUTOR)),
                identity),
            optionManager);

    return new SabotContext(
        dremioConfig,
        identity,
        sConfig,
        roles,
        classpathScan,
        lpPersistance,
        allocator,
        coordinator,
        workStats,
        kvStoreProvider.get(),
        namespaceServiceFactoryProvider.get(),
        orphanageFactoryProvider.get(),
        datasetListingServiceProvider.get(),
        userService.get(),
        materializationDescriptorProvider,
        queryObserverFactory,
        accelerationManager,
        accelerationListManager,
        catalogService,
        masterCoordinatorConduit.get(),
        informationSchemaStub,
        viewCreatorFactory,
        queryPlannerAllocator,
        spillService,
        jobResultInfoProvider.get(),
        physicalPlanReader,
        optionManager,
        functionRegistry,
        decimalFunctionImplementationRegistry,
        compiler,
        resourceInformationProvider.get(),
        fileSystemWrapper,
        optionValidatorProvider.get(),
        bootstrapContext.getExecutor(),
        coordinatorModeInfoProvider,
        nessieApiProvider,
        statisticsService,
        statisticsAdministrationServiceFactory,
        statisticsListManagerProvider,
        userDefinedFunctionListManagerProvider,
        relMetadataQuerySupplier,
        jobsRunnerProvider,
        datasetCatalogStub,
        globalCredentailsServiceProvider,
        credentialsServiceProvider,
        conduitInProcessChannelProviderProvider,
        sysFlightChannelProviderProvider,
        sourceVerifierProvider,
        secretsCreatorProvider,
        foremenWorkManagerProvider,
        metadataIOPoolProvider);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(context, queryPlannerAllocator);
  }

  @Override
  public SabotContext get() {
    if (context == null) {
      try {
        context = newSabotContext();
      } catch (Exception e) {
        throwIfUnchecked(e);
        throw new RuntimeException("Failed to create SabotContext", e);
      }
    }

    return context;
  }

  public NodeEndpoint getEndpoint() {
    return get().getEndpoint();
  }
}

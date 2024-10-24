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
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.toIntExact;

import com.dremio.authenticator.Authenticator;
import com.dremio.catalog.CatalogMaintenanceService;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.logging.StructuredLogger;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.server.WebServerInfoProvider;
import com.dremio.config.DremioConfig;
import com.dremio.context.RequestContext;
import com.dremio.context.TenantContext;
import com.dremio.context.UserContext;
import com.dremio.dac.daemon.DACDaemon.ClusterMode;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.DremioServer;
import com.dremio.dac.server.DremioServlet;
import com.dremio.dac.server.LivenessService;
import com.dremio.dac.server.RestApiServerFactory;
import com.dremio.dac.server.WebServer;
import com.dremio.dac.server.WebServerInfoProviderImpl;
import com.dremio.dac.service.admin.KVStoreReportService;
import com.dremio.dac.service.catalog.CatalogMaintenanceRunnableProvider;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.credentials.CredentialsServiceGrpcImpl;
import com.dremio.dac.service.datasets.DACViewCreatorFactory;
import com.dremio.dac.service.datasets.DatasetVersionCleanupHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.exec.MasterElectionService;
import com.dremio.dac.service.exec.MasterStatusListener;
import com.dremio.dac.service.exec.MasterlessStatusListener;
import com.dremio.dac.service.flight.CoordinatorFlightProducer;
import com.dremio.dac.service.flight.FlightCloseableBindableService;
import com.dremio.dac.service.job.JobCountsDatasetDeletionSubscriber;
import com.dremio.dac.service.nodeshistory.NodesHistoryPluginInitializer;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchService;
import com.dremio.dac.service.search.SearchServiceImpl;
import com.dremio.dac.service.search.SearchServiceInvoker;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.service.sysflight.SysFlightTablesProvider.JobsTable;
import com.dremio.dac.service.sysflight.SysFlightTablesProvider.MaterializationsTable;
import com.dremio.dac.service.sysflight.SysFlightTablesProvider.RecentJobsTable;
import com.dremio.dac.service.sysflight.SysFlightTablesProvider.ReflectionDependenciesTable;
import com.dremio.dac.service.sysflight.SysFlightTablesProvider.ReflectionLineageTableFunction;
import com.dremio.dac.service.sysflight.SysFlightTablesProvider.ReflectionsTable;
import com.dremio.dac.service.users.UserServiceHelper;
import com.dremio.dac.support.BasicQueryLogBundleService;
import com.dremio.dac.support.BasicSupportService;
import com.dremio.dac.support.CoordinatorLogService;
import com.dremio.dac.support.QueryLogBundleService;
import com.dremio.dac.support.SupportService;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.edition.EditionProvider;
import com.dremio.edition.EditionProviderImpl;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.CatalogServiceSynchronizer;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.ConnectionReaderDecorator;
import com.dremio.exec.catalog.DatasetCatalogServiceImpl;
import com.dremio.exec.catalog.InformationSchemaServiceImpl;
import com.dremio.exec.catalog.MetadataRefreshInfoBroadcaster;
import com.dremio.exec.catalog.VersionedDatasetAdapterFactory;
import com.dremio.exec.catalog.ViewCreatorFactory;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.catalog.factory.CatalogFactory;
import com.dremio.exec.catalog.factory.CatalogSupplier;
import com.dremio.exec.enginemanagement.proto.EngineManagementProtos.EngineId;
import com.dremio.exec.enginemanagement.proto.EngineManagementProtos.SubEngineId;
import com.dremio.exec.maestro.GlobalKeysService;
import com.dremio.exec.maestro.MaestroForwarder;
import com.dremio.exec.maestro.MaestroService;
import com.dremio.exec.maestro.MaestroServiceImpl;
import com.dremio.exec.maestro.NoOpMaestroForwarder;
import com.dremio.exec.planner.cost.DremioRelMetadataQuery;
import com.dremio.exec.planner.cost.RelMetadataQuerySupplier;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.planner.plancache.CacheRefresherService;
import com.dremio.exec.planner.plancache.CacheRefresherServiceImpl;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.rpc.ssl.SSLConfigurator;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.server.JobResultInfoProvider;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.NodeRegistration;
import com.dremio.exec.server.ResultsCleanupService;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.SimpleJobRunner;
import com.dremio.exec.server.SourceVerifier;
import com.dremio.exec.server.SysFlightChannelProvider;
import com.dremio.exec.server.options.OptionChangeBroadcaster;
import com.dremio.exec.server.options.OptionNotificationService;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.server.options.SystemOptionManagerImpl;
import com.dremio.exec.service.executor.ExecutorService;
import com.dremio.exec.service.executor.ExecutorServiceProductClientFactory;
import com.dremio.exec.service.jobresults.JobResultsSoftwareClientFactory;
import com.dremio.exec.service.jobtelemetry.JobTelemetrySoftwareClientFactory;
import com.dremio.exec.service.maestro.MaestroGrpcServerFacade;
import com.dremio.exec.service.maestro.MaestroSoftwareClientFactory;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.JobResultsStoreConfig;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.MetadataIOPool;
import com.dremio.exec.store.dfs.PDFSService;
import com.dremio.exec.store.dfs.PDFSService.PDFSMode;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePluginConfigFactory;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.statistics.StatisticsAdministrationService;
import com.dremio.exec.store.sys.statistics.StatisticsListManager;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.dremio.exec.store.sys.udf.UserDefinedFunctionService;
import com.dremio.exec.store.sys.udf.UserDefinedFunctionServiceImpl;
import com.dremio.exec.work.WorkStats;
import com.dremio.exec.work.protector.ActiveQueryListService;
import com.dremio.exec.work.protector.ForemenTool;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.exec.work.rpc.CoordTunnelCreator;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.hadoop.security.alias.DremioCredentialProviderFactory;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.partitionstats.storeprovider.PartitionStatsCacheInMemoryStoreProvider;
import com.dremio.partitionstats.storeprovider.PartitionStatsCacheStoreProvider;
import com.dremio.plugins.nodeshistory.NodesHistoryStoreConfig;
import com.dremio.plugins.sysflight.NodesHistoryViewResolver;
import com.dremio.plugins.sysflight.SysFlightPluginConfigProvider;
import com.dremio.provision.service.ProvisioningService;
import com.dremio.provision.service.ProvisioningServiceImpl;
import com.dremio.resource.ClusterResourceInformation;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.QueryCancelTool;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.RuleBasedEngineSelector;
import com.dremio.resource.basic.BasicResourceAllocator;
import com.dremio.sabot.exec.CancelQueryContext;
import com.dremio.sabot.exec.CoordinatorHeapClawBackStrategy;
import com.dremio.sabot.exec.ExecToCoordTunnelCreator;
import com.dremio.sabot.exec.FragmentWorkManager;
import com.dremio.sabot.exec.HeapMonitorManager;
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
import com.dremio.service.InitializerRegistry;
import com.dremio.service.SingletonRegistry;
import com.dremio.service.acceleration.ReflectionDescriptionServiceGrpc;
import com.dremio.service.acceleration.ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceStub;
import com.dremio.service.accelerator.AccelerationListManagerImpl;
import com.dremio.service.accelerator.AccelerationListServiceImpl;
import com.dremio.service.catalog.DatasetCatalogServiceGrpc;
import com.dremio.service.catalog.DatasetCatalogServiceGrpc.DatasetCatalogServiceBlockingStub;
import com.dremio.service.catalog.InformationSchemaServiceGrpc;
import com.dremio.service.catalog.InformationSchemaServiceGrpc.InformationSchemaServiceBlockingStub;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.commandpool.CommandPoolFactory;
import com.dremio.service.conduit.ConduitUtils;
import com.dremio.service.conduit.EmbeddedZooKeeperWatchdog;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.conduit.client.ConduitProviderImpl;
import com.dremio.service.conduit.server.ConduitInProcessChannelProvider;
import com.dremio.service.conduit.server.ConduitServer;
import com.dremio.service.conduit.server.ConduitServiceRegistry;
import com.dremio.service.conduit.server.ConduitServiceRegistryImpl;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterServiceSetManager;
import com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV1;
import com.dremio.service.coordinator.DremioAssumeRoleCredentialsProviderV2;
import com.dremio.service.coordinator.ExecutorSetService;
import com.dremio.service.coordinator.LocalExecutorSetService;
import com.dremio.service.coordinator.NoOpClusterCoordinator;
import com.dremio.service.coordinator.ProjectConfig;
import com.dremio.service.coordinator.ProjectConfigImpl;
import com.dremio.service.coordinator.ProjectConfigStore;
import com.dremio.service.coordinator.ProjectRoleInitializer;
import com.dremio.service.coordinator.SoftwareAssumeRoleCredentialsProvider;
import com.dremio.service.coordinator.SoftwareCoordinatorModeInfo;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.service.coordinator.zk.ZKClusterCoordinator;
import com.dremio.service.embedded.catalog.EmbeddedMetadataPointerService;
import com.dremio.service.execselector.ExecutorSelectionService;
import com.dremio.service.execselector.ExecutorSelectionServiceImpl;
import com.dremio.service.execselector.ExecutorSelectorFactory;
import com.dremio.service.execselector.ExecutorSelectorFactoryImpl;
import com.dremio.service.execselector.ExecutorSelectorProvider;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.dremio.service.flight.DremioFlightAuthProvider;
import com.dremio.service.flight.DremioFlightAuthProviderImpl;
import com.dremio.service.flight.DremioFlightService;
import com.dremio.service.flight.FlightRequestContextDecorator;
import com.dremio.service.grpc.GrpcChannelBuilderFactory;
import com.dremio.service.grpc.GrpcServerBuilderFactory;
import com.dremio.service.grpc.MultiTenantGrpcServerBuilderFactory;
import com.dremio.service.grpc.SingleTenantGrpcChannelBuilderFactory;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.jobcounts.JobCountsClient;
import com.dremio.service.jobcounts.server.LocalJobCountsServer;
import com.dremio.service.jobresults.client.JobResultsClientFactory;
import com.dremio.service.jobresults.server.JobResultsBindableService;
import com.dremio.service.jobresults.server.JobResultsGrpcServerFacade;
import com.dremio.service.jobs.Chronicle;
import com.dremio.service.jobs.HybridJobsService;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobResultsStore;
import com.dremio.service.jobs.JobsFlightProducer;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JobsServiceAdapter;
import com.dremio.service.jobs.JobsStoreCreator;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.jobs.cleanup.JobsAndDependenciesCleaner;
import com.dremio.service.jobs.cleanup.JobsAndDependenciesCleanerImpl;
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
import com.dremio.service.namespace.catalogpubsub.CatalogEventMessagePublisherProvider;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvents;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.catalogstatusevents.events.DatasetDeletionCatalogStatusEvent;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.orphanage.OrphanageImpl;
import com.dremio.service.orphanagecleaner.OrphanageCleanerService;
import com.dremio.service.reflection.AccelerationManagerImpl;
import com.dremio.service.reflection.DatasetEventHub;
import com.dremio.service.reflection.ExecutorOnlyReflectionService;
import com.dremio.service.reflection.ReflectionAdministrationService;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.ReflectionStatusServiceImpl;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.scheduler.ClusteredSingletonTaskScheduler;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import com.dremio.service.scheduler.ModifiableWrappedSchedulerService;
import com.dremio.service.scheduler.RoutingSchedulerService;
import com.dremio.service.scheduler.ScheduleTaskGroup;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.scheduler.SimpleLocalSchedulerService;
import com.dremio.service.script.NoOpScriptServiceImpl;
import com.dremio.service.script.NoOpScriptStoreImpl;
import com.dremio.service.script.ScriptService;
import com.dremio.service.script.ScriptServiceImpl;
import com.dremio.service.script.ScriptStore;
import com.dremio.service.script.ScriptStoreImpl;
import com.dremio.service.spill.SpillService;
import com.dremio.service.spill.SpillServiceImpl;
import com.dremio.service.sqlrunner.SQLRunnerSessionCleanerService;
import com.dremio.service.sqlrunner.SQLRunnerSessionService;
import com.dremio.service.sqlrunner.SQLRunnerSessionServiceImpl;
import com.dremio.service.sqlrunner.SQLRunnerSessionUserDeletionSubscriber;
import com.dremio.service.sqlrunner.store.SQLRunnerSessionStore;
import com.dremio.service.sqlrunner.store.SQLRunnerSessionStoreImpl;
import com.dremio.service.statistics.StatisticsListManagerImpl;
import com.dremio.service.statistics.StatisticsServiceImpl;
import com.dremio.service.sysflight.SysFlightDataProvider;
import com.dremio.service.sysflight.SysFlightProducer;
import com.dremio.service.sysflight.SystemTableManager;
import com.dremio.service.sysflight.SystemTableManagerImpl;
import com.dremio.service.sysflight.SystemTables;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.tokens.TokenManagerImpl;
import com.dremio.service.tokens.TokenManagerImplV2;
import com.dremio.service.tokens.jwks.JWKSetManager;
import com.dremio.service.tokens.jwks.SystemJWKSetManager;
import com.dremio.service.userpreferences.UserPreferenceService;
import com.dremio.service.userpreferences.UserPreferenceServiceImpl;
import com.dremio.service.userpreferences.UserPreferenceStore;
import com.dremio.service.userpreferences.UserPreferenceStoreImpl;
import com.dremio.service.users.BasicAuthenticator;
import com.dremio.service.users.LocalUsernamePasswordAuthProvider;
import com.dremio.service.users.SimpleUserService;
import com.dremio.service.users.UserResolver;
import com.dremio.service.users.UserService;
import com.dremio.service.users.events.UserDeletionEvent;
import com.dremio.service.users.events.UserServiceEvents;
import com.dremio.service.usersessions.UserSessionService;
import com.dremio.service.usersessions.UserSessionServiceImpl;
import com.dremio.service.usersessions.UserSessionServiceOptions;
import com.dremio.service.usersessions.store.UserSessionStoreProvider;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.credentials.Cipher;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.CredentialsServiceImpl;
import com.dremio.services.credentials.NoopCipher;
import com.dremio.services.credentials.NoopSecretsCreator;
import com.dremio.services.credentials.RemoteSecretsCreatorImpl;
import com.dremio.services.credentials.SecretsCreator;
import com.dremio.services.credentials.SecretsCreatorGrpcImpl;
import com.dremio.services.credentials.SecretsCreatorImpl;
import com.dremio.services.credentials.SystemCipher;
import com.dremio.services.credentials.proto.CredentialsServiceGrpc;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.jobresults.common.JobResultsTunnel;
import com.dremio.services.nessie.grpc.client.GrpcClientBuilder;
import com.dremio.services.nodemetrics.CoordinatorMetricsService;
import com.dremio.services.nodemetrics.NodeMetricsService;
import com.dremio.services.nodemetrics.persistence.NodeMetricsPersistenceService;
import com.dremio.services.pubsub.PubSubClient;
import com.dremio.services.pubsub.noop.NoOpPubSubClient;
import com.dremio.services.systemicebergtablesmaintainer.SystemIcebergTablesMaintainerService;
import com.dremio.services.systemicebergtablesmaintainer.SystemIcebergTablesSchemaUpdaterService;
import com.dremio.ssl.SSLEngineFactory;
import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.dremio.telemetry.utils.TracerFacade;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.util.Providers;
import io.grpc.ManagedChannel;
import io.opentelemetry.api.OpenTelemetry;
import io.opentracing.Tracer;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DAC module to setup Dremio daemon */
public class DACDaemonModule implements DACModule {
  private static final Logger logger = LoggerFactory.getLogger(DACDaemonModule.class);

  public static final String JOBS_STORAGEPLUGIN_NAME = "__jobResultsStore";
  public static final String SCRATCH_STORAGEPLUGIN_NAME = "$scratch";

  public DACDaemonModule() {}

  @Override
  public void bootstrap(
      final Runnable shutdownHook,
      final SingletonRegistry bootstrapRegistry,
      ScanResult scanResult,
      DACConfig dacConfig,
      boolean isMaster) {

    bootstrapRegistry.bind(Tracer.class, TracerFacade.INSTANCE);
    bootstrapRegistry.bind(GrpcTracerFacade.class, new GrpcTracerFacade(TracerFacade.INSTANCE));

    final DremioConfig config = dacConfig.getConfig();
    final boolean embeddedZookeeper =
        config.getBoolean(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL);

    final BootStrapContext bootStrapContext = new BootStrapContext(config, scanResult);
    boolean isMasterless = config.isMasterlessEnabled();

    bootstrapRegistry.bindSelf(bootStrapContext);
    bootstrapRegistry.bind(BufferAllocator.class, bootStrapContext.getAllocator());

    // Start cluster coordinator before all other services so that non master nodes can poll for
    // master status
    if (dacConfig.getClusterMode() == ClusterMode.LOCAL) {
      bootstrapRegistry.bind(ClusterCoordinator.class, new LocalClusterCoordinator());
    } else if (config.getBoolean(DremioConfig.NO_OP_CLUSTER_COORDINATOR_ENABLED)) {
      isMasterless = true;
      Preconditions.checkState(!isMaster);
      bootstrapRegistry.bind(ClusterCoordinator.class, new NoOpClusterCoordinator());
    } else {
      // ClusterCoordinator has a runtime dependency on ZooKeeper. If no ZooKeeper server
      // is present, ClusterCoordinator won't start, so this service should be initialized first.
      final Provider<Integer> portProvider;
      if (isMaster && embeddedZookeeper) {
        ZkServer zkServer =
            new ZkServer(
                config.getString(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_PATH_STRING),
                config.getInt(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_PORT_INT),
                dacConfig.autoPort);
        bootstrapRegistry.bindSelf(zkServer);

        portProvider =
            dacConfig.autoPort
                ? new Provider<Integer>() {
                  @Override
                  public Integer get() {
                    return bootstrapRegistry.lookup(ZkServer.class).getPort();
                  }
                }
                : null;
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
      bootstrapRegistry.bindSelf(
          new MasterElectionService(bootstrapRegistry.provider(ClusterCoordinator.class)));
    }

    final MasterStatusListener masterStatusListener;
    final Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider =
        bootstrapRegistry.provider(ClusterCoordinator.class);

    if (!isMasterless) {
      masterStatusListener =
          new MasterStatusListener(
              clusterServiceSetManagerProvider, config.getSabotConfig(), isMaster);
    } else {
      masterStatusListener =
          new MasterlessStatusListener(clusterServiceSetManagerProvider, isMaster);
    }
    // start master status listener
    bootstrapRegistry.bind(MasterStatusListener.class, masterStatusListener);
    bootstrapRegistry.bindProvider(EngineId.class, Providers.of(null));
    bootstrapRegistry.bindProvider(SubEngineId.class, Providers.of(null));

    // Default request Context
    bootstrapRegistry.bind(
        RequestContext.class,
        RequestContext.empty()
            .with(TenantContext.CTX_KEY, TenantContext.DEFAULT_SERVICE_CONTEXT)
            .with(UserContext.CTX_KEY, UserContext.SYSTEM_USER_CONTEXT));
  }

  @Override
  public void build(
      final SingletonRegistry bootstrapRegistry,
      final SingletonRegistry registry,
      ScanResult scanResult,
      DACConfig dacConfig,
      boolean isMaster) {
    final DremioConfig config = dacConfig.getConfig();
    final SabotConfig sabotConfig = config.getSabotConfig();
    final BootStrapContext bootstrap = bootstrapRegistry.lookup(BootStrapContext.class);

    final boolean isMasterless = config.isMasterlessEnabled();
    final boolean isCoordinator = config.getBoolean(DremioConfig.ENABLE_COORDINATOR_BOOL);
    final boolean isExecutor = config.getBoolean(DremioConfig.ENABLE_EXECUTOR_BOOL);
    final boolean isDistributedCoordinator = isMasterless && isCoordinator;
    final boolean isDistributedMaster = isDistributedCoordinator || isMaster;
    final Provider<NodeEndpoint> masterEndpoint =
        () -> registry.lookup(MasterStatusListener.class).getMasterNode();

    final Provider<SabotContext> sabotContextProvider = registry.provider(SabotContext.class);
    final Provider<NodeEndpoint> selfEndpoint = registry.provider(NodeEndpoint.class);

    registry.bind(java.util.concurrent.ExecutorService.class, bootstrap.getExecutor());

    final BufferAllocatorFactory bufferAllocatorFactory =
        new BufferAllocatorFactory(bootstrap.getAllocator(), "WebServer");
    registry.bindSelf(bufferAllocatorFactory);

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

    registry.bind(
        ConnectionReader.class,
        new ConnectionReaderDecorator(
            getConnectionReaderImpl(scanResult, sabotConfig),
            registry.provider(CredentialsService.class)));

    // register default providers.

    registry.bind(MaterializationDescriptorProvider.class, MaterializationDescriptorProvider.EMPTY);
    registry.bind(QueryObserverFactory.class, QueryObserverFactory.DEFAULT);

    // copy bootstrap bindings to the main registry.
    bootstrapRegistry.copyBindings(registry);

    registry.bind(
        GrpcChannelBuilderFactory.class,
        new SingleTenantGrpcChannelBuilderFactory(
            registry.lookup(Tracer.class),
            registry.provider(RequestContext.class),
            () -> {
              return Maps.newHashMap();
            }));
    registry.bind(
        GrpcServerBuilderFactory.class,
        new MultiTenantGrpcServerBuilderFactory(registry.lookup(Tracer.class)));

    final String fabricAddress = getFabricAddress();

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
            bootstrap.getExecutor()));

    if (isCoordinator && isMaster) {
      registry.bind(Cipher.class, SystemCipher.class);
      registry.bind(
          SecretsCreator.class,
          new SecretsCreatorImpl(
              registry.provider(Cipher.class), registry.provider(CredentialsService.class)));
    } else if (isCoordinator) {
      // Non-master coordinator(s) route calls to the master coordinator to encrypt secrets
      registry.bind(Cipher.class, NoopCipher.class);
      registry.bind(
          SecretsCreator.class,
          new RemoteSecretsCreatorImpl(() -> getOrCreateChannelToMaster(registry)));
    } else {
      // Executor(s) are not allowed to encrypt secrets
      registry.bind(Cipher.class, NoopCipher.class);
      registry.bind(SecretsCreator.class, NoopSecretsCreator.class);
    }

    registry.bind(
        CredentialsService.class,
        CredentialsServiceImpl.newInstance(
            config,
            scanResult,
            registry.provider(OptionManager.class),
            registry.provider(Cipher.class),
            () -> getOrCreateChannelToMaster(registry)));

    DremioCredentialProviderFactory.configure(registry.provider(CredentialsService.class));

    final Optional<SSLEngineFactory> conduitSslEngineFactory;
    try {
      final SSLConfigurator conduitSslConfigurator =
          new SSLConfigurator(
              config,
              registry.provider(CredentialsService.class),
              ConduitUtils.CONDUIT_SSL_PREFIX,
              "conduit");

      // TODO DX-66220: Make AzureVaultCredentialsProvider available before resolving secret URIs in
      // SSL config
      conduitSslEngineFactory =
          SSLEngineFactory.create(conduitSslConfigurator.getSSLConfig(false, fabricAddress));
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }

    ConduitServiceRegistry conduitServiceRegistry = null;
    if (isCoordinator) {
      conduitServiceRegistry = new ConduitServiceRegistryImpl();
      registry.bind(ConduitServiceRegistry.class, conduitServiceRegistry);
      final String inProcessServerName = UUID.randomUUID().toString();
      registry.bind(
          ConduitServer.class,
          new ConduitServer(
              registry.provider(ConduitServiceRegistry.class),
              config.getInt(DremioConfig.CONDUIT_PORT_INT),
              conduitSslEngineFactory,
              inProcessServerName));
      // should be after conduit server
      final ConduitInProcessChannelProvider conduitInProcessChannelProvider =
          new ConduitInProcessChannelProvider(
              inProcessServerName, registry.provider(RequestContext.class));
      registry.bind(ConduitInProcessChannelProvider.class, conduitInProcessChannelProvider);
    } else {
      registry.bindProvider(
          ConduitServer.class,
          () -> {
            return null;
          });
    }

    // for masterless case, this defaults to the local conduit server.
    final Provider<NodeEndpoint> conduitEndpoint;
    if (isMasterless) {
      conduitEndpoint = selfEndpoint;
    } else {
      boolean embeddedZooKeeper = config.getBoolean(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL);
      if (isExecutor && embeddedZooKeeper) {
        // Workaround for ZooKeeper issue - see DX-87859
        conduitEndpoint =
            EmbeddedZooKeeperWatchdog.watch(
                masterEndpoint,
                config.getInt(DremioConfig.EMBEDDED_MASTER_ZK_WATCHDOG_FAILURE_COUNT_THRESHOLD),
                config.getString(
                    DremioConfig.EMBEDDED_MASTER_ZK_WATCHDOG_FAILURE_DURATION_THRESHOLD));
      } else {
        conduitEndpoint = masterEndpoint;
      }
    }

    final ConduitProviderImpl conduitProvider =
        new ConduitProviderImpl(conduitEndpoint, conduitSslEngineFactory);
    registry.bind(ConduitProvider.class, conduitProvider);
    registry.bind(ConduitProviderImpl.class, conduitProvider); // this bind manages lifecycle

    registry.bindProvider(
        InformationSchemaServiceBlockingStub.class,
        () -> InformationSchemaServiceGrpc.newBlockingStub(getOrCreateChannelToMaster(registry)));

    registry.bindProvider(
        DatasetCatalogServiceBlockingStub.class,
        () -> DatasetCatalogServiceGrpc.newBlockingStub(getOrCreateChannelToMaster(registry)));

    registry.bindProvider(NessieApiV2.class, () -> createNessieApi(config, registry));

    // Bind gRPC service on master coordinator
    if (isCoordinator && isMaster) {
      // encrypt by system
      conduitServiceRegistry.registerService(
          new SecretsCreatorGrpcImpl(registry.provider(SecretsCreator.class)));
      // decrypt
      conduitServiceRegistry.registerService(
          new CredentialsServiceGrpcImpl(registry.provider(CredentialsService.class)));
    }
    // Make gRPC route available for all nodes
    registry.bindProvider(
        CredentialsServiceGrpc.CredentialsServiceBlockingStub.class,
        () -> CredentialsServiceGrpc.newBlockingStub(getOrCreateChannelToMaster(registry)));

    registry.bind(
        KVStoreProvider.class,
        KVStoreProviderHelper.newKVStoreProvider(
            dacConfig,
            bootstrap,
            registry.provider(FabricService.class),
            masterEndpoint,
            bootstrapRegistry.lookup(Tracer.class)));

    // should be after the kv store
    registry.bind(
        ProjectRoleInitializer.class,
        new ProjectRoleInitializer() {
          @Override
          public void start() throws Exception {
            // NO-OP
          }

          @Override
          public void close() throws Exception {
            // NO-OP
          }
        });

    registry.bind(
        LegacyKVStoreProvider.class,
        new LegacyKVStoreProviderAdapter(registry.provider(KVStoreProvider.class).get()));

    registry.bind(
        ViewCreatorFactory.class,
        new DACViewCreatorFactory(
            registry.provider(LegacyKVStoreProvider.class),
            registry.provider(JobsService.class),
            registry.provider(NamespaceService.Factory.class),
            registry.provider(CatalogService.class),
            registry.provider(SabotContext.class),
            () -> bootstrap.getAllocator(),
            registry.provider(OptionManager.class)));

    // RPC Endpoints.

    if (isCoordinator) {
      registry.bindSelf(
          new LocalUsernamePasswordAuthProvider(registry.provider(SimpleUserService.class)));
      registry.bind(
          Authenticator.class,
          new BasicAuthenticator(registry.provider(LocalUsernamePasswordAuthProvider.class)));
      registry.bindSelf(
          new UserServer(
              config,
              registry.provider(java.util.concurrent.ExecutorService.class),
              registry.provider(BufferAllocator.class),
              registry.provider(Authenticator.class),
              registry.provider(UserService.class),
              registry.provider(NodeEndpoint.class),
              registry.provider(UserWorker.class),
              dacConfig.autoPort,
              bootstrapRegistry.lookup(Tracer.class),
              registry.provider(OptionValidatorListing.class)));
    }

    // Context Service.
    final ContextService contextService =
        new ContextService(
            bootstrap,
            registry.provider(ClusterCoordinator.class),
            registry.provider(GroupResourceInformation.class),
            registry.provider(WorkStats.class),
            registry.provider(LegacyKVStoreProvider.class),
            registry.provider(FabricService.class),
            registry.provider(ConduitServer.class),
            registry.provider(UserServer.class),
            registry.provider(MaterializationDescriptorProvider.class),
            registry.provider(QueryObserverFactory.class),
            registry.provider(AccelerationManager.class),
            registry.provider(AccelerationListManager.class),
            registry.provider(NamespaceService.Factory.class),
            registry.provider(Orphanage.Factory.class),
            registry.provider(DatasetListingService.class),
            registry.provider(UserService.class),
            registry.provider(CatalogService.class),
            registry.provider(ConduitProvider.class),
            registry.provider(InformationSchemaServiceBlockingStub.class),
            registry.provider(ViewCreatorFactory.class),
            registry.provider(SpillService.class),
            registry.provider(ConnectionReader.class),
            registry.provider(JobResultInfoProvider.class),
            registry.provider(OptionManager.class),
            bootstrapRegistry.provider(EngineId.class),
            bootstrapRegistry.provider(SubEngineId.class),
            registry.provider(OptionValidatorListing.class),
            roles,
            () -> new SoftwareCoordinatorModeInfo(),
            registry.provider(NessieApiV2.class),
            registry.provider(StatisticsService.class),
            registry.provider(StatisticsAdministrationService.Factory.class),
            registry.provider(StatisticsListManager.class),
            registry.provider(UserDefinedFunctionService.class),
            registry.provider(RelMetadataQuerySupplier.class),
            registry.provider(SimpleJobRunner.class),
            registry.provider(DatasetCatalogServiceBlockingStub.class),
            registry.provider(GlobalKeysService.class),
            registry.provider(CredentialsService.class),
            registry.provider(ConduitInProcessChannelProvider.class),
            registry.provider(SysFlightChannelProvider.class),
            registry.provider(SourceVerifier.class),
            registry.provider(SecretsCreator.class),
            registry.provider(ForemenWorkManager.class),
            registry.provider(MetadataIOPool.class));

    registry.bind(SysFlightChannelProvider.class, SysFlightChannelProvider.NO_OP);

    registry.bind(SourceVerifier.class, SourceVerifier.NO_OP);

    registry.bind(ContextService.class, contextService);
    registry.bindProvider(SabotContext.class, contextService::get);
    registry.bindProvider(NodeEndpoint.class, contextService::getEndpoint);

    registry.bindProvider(GlobalKeysService.class, () -> GlobalKeysService.NO_OP);

    setupUserService(
        registry, dacConfig, registry.provider(SabotContext.class), isMaster, isCoordinator);
    registry.bind(Orphanage.Factory.class, OrphanageImpl.Factory.class);
    registry.bind(NamespaceService.Factory.class, NamespaceServiceImpl.Factory.class);
    final DatasetListingService localListing;
    if (isDistributedMaster) {
      localListing =
          new DatasetListingServiceImpl(registry.provider(NamespaceService.Factory.class));
    } else {
      localListing = DatasetListingService.UNSUPPORTED;
    }

    final Provider<NodeEndpoint> searchEndPoint =
        () -> {
          // will return master endpoint if it's masterful mode
          Optional<NodeEndpoint> serviceEndPoint =
              registry
                  .provider(SchedulerService.class)
                  .get()
                  .getCurrentTaskOwner(SearchServiceImpl.LOCAL_TASK_LEADER_NAME);
          return serviceEndPoint.orElse(null);
        };

    // this is the delegate service for localListing (calls start/close internally)
    registry.bind(
        DatasetListingService.class,
        new DatasetListingInvoker(
            isDistributedMaster,
            searchEndPoint,
            registry.provider(FabricService.class),
            bootstrap.getAllocator(),
            localListing));

    registry.bindSelf(
        new CoordExecService(
            bootstrap.getConfig(),
            bootstrap.getAllocator(),
            registry.provider(FabricService.class),
            registry.provider(ExecutorService.class),
            registry.provider(ExecToCoordResultsHandler.class),
            registry.provider(ExecToCoordStatusHandler.class),
            registry.provider(NodeEndpoint.class),
            registry.provider(JobTelemetryClient.class)));

    registry.bind(HomeFileTool.HostNameProvider.class, config::getThisNode);
    registry.bindSelf(HomeFileTool.class);

    // Periodic task scheduler service
    final int capacity = config.getInt(DremioConfig.SCHEDULER_SERVICE_THREAD_COUNT);
    final boolean isLeaderlessScheduler =
        config.getBoolean(DremioConfig.SCHEDULER_LEADERLESS_CLUSTERED_SINGLETON);
    if (isLeaderlessScheduler) {
      final ScheduleTaskGroup defaultTaskGroup =
          ScheduleTaskGroup.create("clustered-singleton-default", capacity);
      ModifiableSchedulerService distributedSchedulerService = null;
      if (isDistributedCoordinator) {
        final boolean haltOnZkLost = config.getBoolean(DremioConfig.SCHEDULER_HALT_ON_ZK_LOST);
        distributedSchedulerService =
            new ClusteredSingletonTaskScheduler(
                defaultTaskGroup,
                getRootName(),
                registry.provider(ClusterCoordinator.class),
                registry.provider(NodeEndpoint.class),
                haltOnZkLost);
      }
      final SchedulerService localSchedulerService = new SimpleLocalSchedulerService(capacity);
      ModifiableSchedulerService scheduler;
      scheduler =
          new RoutingSchedulerService(
              registry.provider(ClusterCoordinator.class),
              localSchedulerService,
              distributedSchedulerService);
      registry.bind(SchedulerService.class, scheduler);
      registry.bind(ModifiableSchedulerService.class, scheduler);
    } else {
      registry.bind(
          SchedulerService.class,
          new LocalSchedulerService(
              capacity,
              registry.provider(ClusterCoordinator.class),
              registry.provider(ClusterCoordinator.class),
              registry.provider(NodeEndpoint.class),
              isDistributedCoordinator));
    }
    registry.bindProvider(UserSessionStoreProvider.class, UserSessionStoreProvider::new);

    final OptionValidatorListing optionValidatorListing =
        new OptionValidatorListingImpl(scanResult);
    final DefaultOptionManager defaultOptionManager =
        new DefaultOptionManager(optionValidatorListing);

    // SystemOptionManager must be bound because it must be #start'ed
    SystemOptionManager systemOptionManager =
        buildSystemOptionManager(
            optionValidatorListing,
            bootstrap.getLpPersistance(),
            registry,
            isCoordinator,
            conduitServiceRegistry,
            !isCoordinator);
    registry.bind(SystemOptionManager.class, systemOptionManager);
    registry.bind(OptionValidatorListing.class, optionValidatorListing);
    registry.bind(
        OptionManager.class,
        buildOptionManager(systemOptionManager, optionValidatorListing).build());
    registry.bind(
        ProjectOptionManager.class,
        buildProjectOptionManager(systemOptionManager, defaultOptionManager));

    Provider<Integer> ttlInSecondsProvider =
        () ->
            toIntExact(
                registry
                    .provider(OptionManager.class)
                    .get()
                    .getOption(UserSessionServiceOptions.SESSION_TTL));

    registry.bind(
        UserSessionService.class,
        new UserSessionServiceImpl(
            registry.provider(UserSessionStoreProvider.class), ttlInSecondsProvider));

    if (isDistributedMaster) {
      // Companion service to clean split orphans
      registry.bind(
          SplitOrphansCleanerService.class,
          new SplitOrphansCleanerService(
              registry.provider(SchedulerService.class),
              registry.provider(NamespaceService.Factory.class),
              registry.provider(OptionManager.class)));
    }

    final Provider<Iterable<NodeEndpoint>> executorsProvider =
        () -> registry.lookup(ClusterCoordinator.class).getExecutorEndpoints();
    if (isExecutor) {
      registry.bind(
          SpillService.class,
          new SpillServiceImpl(
              config,
              new SpillServiceOptionsImpl(registry.provider(OptionManager.class)),
              registry.provider(SchedulerService.class),
              selfEndpoint,
              (isMasterless) ? null : executorsProvider));
    }

    registry.bind(
        GroupResourceInformation.class,
        new ClusterResourceInformation(registry.provider(ClusterCoordinator.class)));

    // PDFS depends on fabric.
    registry.bind(
        PDFSService.class,
        new PDFSService(
            registry.provider(FabricService.class),
            selfEndpoint,
            isCoordinator ? executorsProvider : () -> Collections.singleton(selfEndpoint.get()),
            sabotConfig,
            bootstrap.getAllocator(),
            isExecutor ? PDFSMode.DATA : PDFSMode.CLIENT));

    registry.bindSelf(new SystemTablePluginConfigProvider());

    registry.bind(SysFlightPluginConfigProvider.class, new SysFlightPluginConfigProvider());

    final MetadataRefreshInfoBroadcaster metadataRefreshInfoBroadcaster =
        new MetadataRefreshInfoBroadcaster(
            registry.provider(ConduitProvider.class),
            registry.provider(ClusterCoordinator.class),
            registry.provider(NodeEndpoint.class));

    registry.bindSelf(metadataRefreshInfoBroadcaster);

    if (isCoordinator) {
      registry.bind(ProjectConfigStore.class, ProjectConfigStore.NO_OP);
      registry.bind(
          ProjectConfig.class,
          new ProjectConfigImpl(
              registry.provider(DremioConfig.class), registry.provider(ProjectConfigStore.class)));

      // register a no-op assume role provider
      final SoftwareAssumeRoleCredentialsProvider softwareAssumeRoleCredentialsProvider =
          new SoftwareAssumeRoleCredentialsProvider();
      DremioAssumeRoleCredentialsProviderV2.setAssumeRoleProvider(
          () -> {
            return softwareAssumeRoleCredentialsProvider;
          });

      DremioAssumeRoleCredentialsProviderV1.setAssumeRoleProvider(
          () -> {
            return softwareAssumeRoleCredentialsProvider;
          });
    }

    registry.bindSelf(VersionedDatasetAdapterFactory.class);

    CatalogStatusEvents catalogStatusEvents = new CatalogStatusEventsImpl();
    registry.bind(CatalogStatusEvents.class, catalogStatusEvents);

    registry.bind(
        CatalogService.class,
        new CatalogServiceImpl(
            registry.provider(SabotContext.class),
            registry.provider(SchedulerService.class),
            registry.provider(SystemTablePluginConfigProvider.class),
            registry.provider(SysFlightPluginConfigProvider.class),
            registry.provider(FabricService.class),
            registry.provider(ConnectionReader.class),
            registry.provider(BufferAllocator.class),
            registry.provider(LegacyKVStoreProvider.class),
            registry.provider(DatasetListingService.class),
            registry.provider(OptionManager.class),
            () -> metadataRefreshInfoBroadcaster,
            config,
            roles,
            () ->
                getMetadataRefreshModifiableScheduler(
                    registry, isDistributedCoordinator, isLeaderlessScheduler),
            registry.provider(VersionedDatasetAdapterFactory.class),
            registry.provider(CatalogStatusEvents.class),
            registry.provider(java.util.concurrent.ExecutorService.class)));

    registry.bind(CatalogSupplier.class, CatalogFactory.class);

    registerCatalogMaintenanceService(registry, scanResult, isCoordinator);

    if (isCoordinator) {
      registerInformationSchemaService(conduitServiceRegistry, registry, bootstrap);
      conduitServiceRegistry.registerService(
          new CatalogServiceSynchronizer(registry.provider(CatalogService.class)));
      conduitServiceRegistry.registerService(
          new DatasetCatalogServiceImpl(
              registry.provider(CatalogService.class),
              registry.provider(NamespaceService.Factory.class)));
    }

    // Run initializers only on coordinator.
    // Command pool and metadata i/o pool is required only in the coordinator
    if (isCoordinator) {
      registry.bindSelf(
          new InitializerRegistry(bootstrap.getClasspathScan(), registry.getBindingProvider()));
      registry.bind(MetadataIOPool.class, MetadataIOPool.Factory.INSTANCE.newPool(config));
      registry.bind(
          CommandPool.class,
          CommandPoolFactory.INSTANCE.newPool(config, bootstrapRegistry.lookup(Tracer.class)));
    }

    final Provider<NamespaceService> namespaceServiceProvider =
        () -> sabotContextProvider.get().getNamespaceService(SYSTEM_USERNAME);

    if (isCoordinator) {
      registry.bind(
          KVStoreReportService.class,
          new KVStoreReportService(
              registry.provider(LegacyKVStoreProvider.class),
              registry.provider(KVStoreProvider.class),
              namespaceServiceProvider,
              registry.provider(java.util.concurrent.ExecutorService.class)));
    }

    if (isCoordinator && config.getBoolean(DremioConfig.JOBS_ENABLED_BOOL)) {
      registry.bindSelf(
          new LocalJobTelemetryServer(
              registry.provider(OptionManager.class),
              registry.lookup(GrpcServerBuilderFactory.class),
              registry.provider(KVStoreProvider.class),
              registry.provider(LegacyKVStoreProvider.class),
              registry.provider(NodeEndpoint.class),
              bootstrapRegistry.lookup(GrpcTracerFacade.class),
              name ->
                  new CloseableThreadPool(
                      name,
                      12,
                      Integer.MAX_VALUE,
                      60L,
                      TimeUnit.SECONDS,
                      new SynchronousQueue<>())));

      registry.bindSelf(
          new LocalJobCountsServer(
              registry.lookup(GrpcServerBuilderFactory.class),
              registry.provider(KVStoreProvider.class),
              registry.provider(NodeEndpoint.class)));
    }

    registry.bind(
        JobTelemetryClient.class,
        new JobTelemetryClient(
            registry.lookup(GrpcChannelBuilderFactory.class),
            registry.provider(NodeEndpoint.class)));

    registry.bind(
        JobCountsClient.class,
        new JobCountsClient(
            registry.lookup(GrpcChannelBuilderFactory.class),
            registry.provider(NodeEndpoint.class)));

    LocalJobsService localJobsService = null;
    Provider<JobResultsStoreConfig> jobResultsStoreConfigProvider =
        getJobResultsStoreConfigProvider(registry);
    if (isCoordinator) {
      Provider<LegacyKVStoreProvider> kvStoreProviderProvider =
          registry.provider(LegacyKVStoreProvider.class);
      BufferAllocator allocator = getChildBufferAllocator(bootstrap.getAllocator());
      Provider<JobResultsStore> jobResultsStoreProvider =
          getJobResultsStoreProvider(
              jobResultsStoreConfigProvider, kvStoreProviderProvider, allocator);

      JobsAndDependenciesCleaner jobsAndDependenciesCleaner =
          new JobsAndDependenciesCleanerImpl(
              kvStoreProviderProvider,
              registry.provider(OptionManager.class),
              registry.provider(SchedulerService.class),
              registry.provider(JobTelemetryClient.class),
              Collections.singletonList(
                  DatasetVersionCleanupHelper.datasetVersionCleaner(
                      kvStoreProviderProvider.get())));
      registry.bind(JobsAndDependenciesCleaner.class, jobsAndDependenciesCleaner);

      localJobsService =
          new LocalJobsService(
              kvStoreProviderProvider,
              allocator,
              jobResultsStoreConfigProvider,
              jobResultsStoreProvider,
              registry.provider(LocalQueryExecutor.class),
              registry.provider(CoordTunnelCreator.class),
              registry.provider(ForemenTool.class),
              registry.provider(NodeEndpoint.class),
              registry.provider(ClusterCoordinator.class),
              namespaceServiceProvider,
              registry.provider(OptionManager.class),
              registry.provider(AccelerationManager.class),
              registry.provider(SchedulerService.class),
              registry.provider(CommandPool.class),
              registry.provider(JobTelemetryClient.class),
              getJobResultLogger(registry),
              isDistributedMaster,
              registry.provider(ConduitProvider.class),
              registry.provider(UserSessionService.class),
              registry.provider(OptionValidatorListing.class),
              registry.provider(CatalogService.class),
              registry.provider(JobCountsClient.class),
              registry.provider(JobsAndDependenciesCleaner.class),
              config);

      registry.bind(LocalJobsService.class, localJobsService);
      registry.bind(SimpleJobRunner.class, localJobsService);
      registry.replaceProvider(
          QueryObserverFactory.class, localJobsService::getQueryObserverFactory);

      HybridJobsService hybridJobsService =
          new HybridJobsService(
              // for now, provide the coordinator service set
              registry.lookup(GrpcChannelBuilderFactory.class),
              () -> bootstrap.getAllocator(),
              registry.provider(NodeEndpoint.class),
              registry.lookup(ConduitProvider.class));
      registry.bind(JobsService.class, hybridJobsService);
      registry.bind(HybridJobsService.class, hybridJobsService);
      registry.bind(JobResultInfoProvider.class, localJobsService);
    } else {
      registry.bind(JobResultInfoProvider.class, JobResultInfoProvider.NOOP);
    }

    if (isCoordinator) {
      // put provisioning service before resource allocator

      EditionProvider editionProvider = new EditionProviderImpl();
      registry.bind(EditionProvider.class, editionProvider);
      registry.bind(
          ProvisioningService.class, createProvisioningService(config, bootstrap, registry));
    }

    registry.bind(
        ResourceAllocator.class,
        new BasicResourceAllocator(
            registry.provider(ClusterCoordinator.class),
            registry.provider(GroupResourceInformation.class)));
    if (isCoordinator) {

      registry.bind(ExecutorSelectorFactory.class, new ExecutorSelectorFactoryImpl());
      ExecutorSelectorProvider executorSelectorProvider = new ExecutorSelectorProvider();
      registry.bind(ExecutorSelectorProvider.class, executorSelectorProvider);
      registry.bind(
          ExecutorSetService.class,
          new LocalExecutorSetService(
              registry.provider(ClusterCoordinator.class), registry.provider(OptionManager.class)));
      registry.bind(
          ExecutorSelectionService.class,
          new ExecutorSelectionServiceImpl(
              registry.provider(ExecutorSetService.class),
              registry.provider(OptionManager.class),
              registry.provider(ExecutorSelectorFactory.class),
              executorSelectorProvider));

      CoordToExecTunnelCreator tunnelCreator =
          new CoordToExecTunnelCreator(registry.provider(FabricService.class));
      registry.bind(
          ExecutorServiceClientFactory.class,
          new ExecutorServiceProductClientFactory(tunnelCreator));

      registry.bind(MaestroForwarder.class, new NoOpMaestroForwarder());

      final MaestroService maestroServiceImpl =
          new MaestroServiceImpl(
              registry.provider(ExecutorSetService.class),
              registry.provider(SabotContext.class),
              registry.provider(ResourceAllocator.class),
              registry.provider(CommandPool.class),
              registry.provider(ExecutorSelectionService.class),
              registry.provider(ExecutorServiceClientFactory.class),
              registry.provider(JobTelemetryClient.class),
              registry.provider(MaestroForwarder.class));
      registry.bind(MaestroService.class, maestroServiceImpl);
      registry.bindProvider(
          ExecToCoordStatusHandler.class, maestroServiceImpl::getExecStatusHandler);

      registry.bind(RuleBasedEngineSelector.class, RuleBasedEngineSelector.NO_OP);
      registry.bind(
          PartitionStatsCacheStoreProvider.class, new PartitionStatsCacheInMemoryStoreProvider());

      final ForemenWorkManager foremenWorkManager =
          new ForemenWorkManager(
              registry.provider(FabricService.class),
              registry.provider(SabotContext.class),
              registry.provider(CommandPool.class),
              registry.provider(MaestroService.class),
              registry.provider(JobTelemetryClient.class),
              registry.provider(MaestroForwarder.class),
              registry.provider(RuleBasedEngineSelector.class),
              registry.provider(RequestContext.class),
              registry.provider(PartitionStatsCacheStoreProvider.class));

      if (config.getBoolean(DremioConfig.JOBS_ENABLED_BOOL)) {
        registerJobsServices(conduitServiceRegistry, registry, bootstrap);
      }

      registry.bindSelf(foremenWorkManager);
      registry.bindProvider(
          ExecToCoordResultsHandler.class, foremenWorkManager::getExecToCoordResultsHandler);

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
    if (isExecutor) {
      registry.bindSelf(new ContextInformationFactory());
      taskPoolInitializer = new TaskPoolInitializer(registry.provider(OptionManager.class), config);
      registry.bindSelf(taskPoolInitializer);
      registry.bindProvider(TaskPool.class, taskPoolInitializer::getTaskPool);

      final WorkloadTicketDepotService workloadTicketDepotService =
          new WorkloadTicketDepotService(
              registry.provider(BufferAllocator.class),
              registry.provider(TaskPool.class),
              registry.provider(DremioConfig.class));
      registry.bindSelf(workloadTicketDepotService);
      registry.bindProvider(WorkloadTicketDepot.class, workloadTicketDepotService::getTicketDepot);

      ExecToCoordTunnelCreator execToCoordTunnelCreator =
          new ExecToCoordTunnelCreator(registry.provider(FabricService.class));

      registry.bind(
          MaestroClientFactory.class, new MaestroSoftwareClientFactory(execToCoordTunnelCreator));
      registry.bind(
          JobTelemetryExecutorClientFactory.class,
          new JobTelemetrySoftwareClientFactory(execToCoordTunnelCreator));
      registry.bind(
          JobResultsClientFactory.class,
          new JobResultsSoftwareClientFactory(execToCoordTunnelCreator));

      final FragmentWorkManager fragmentWorkManager =
          new FragmentWorkManager(
              bootstrap,
              config.getSabotConfig(),
              registry.provider(NodeEndpoint.class),
              registry.provider(SabotContext.class),
              registry.provider(FabricService.class),
              registry.provider(CatalogService.class),
              registry.provider(ContextInformationFactory.class),
              registry.provider(WorkloadTicketDepot.class),
              registry.provider(TaskPool.class),
              registry.provider(MaestroClientFactory.class),
              registry.provider(JobTelemetryExecutorClientFactory.class),
              registry.provider(JobResultsClientFactory.class));

      registry.bindSelf(fragmentWorkManager);

      registry.bindProvider(WorkStats.class, fragmentWorkManager::getWorkStats);

      registry.bindProvider(ExecutorService.class, fragmentWorkManager::getExecutorService);
      registry.bind(
          ResultsCleanupService.class,
          new ResultsCleanupService(
              registry.provider(SchedulerService.class),
              jobResultsStoreConfigProvider,
              registry.provider(OptionManager.class)));
    } else {
      registry.bind(WorkStats.class, WorkStats.NO_OP);
    }

    registry.bind(AccelerationManager.class, AccelerationManager.NO_OP);
    registry.bind(StatisticsService.class, StatisticsService.NO_OP);
    registry.bind(RelMetadataQuerySupplier.class, DremioRelMetadataQuery.QUERY_SUPPLIER);

    if (isCoordinator) {
      DatasetEventHub datasetEventHub = new DatasetEventHub();
      final ReflectionServiceImpl reflectionService =
          new ReflectionServiceImpl(
              sabotConfig,
              registry.provider(LegacyKVStoreProvider.class),
              registry.provider(SchedulerService.class),
              registry.provider(JobsService.class),
              registry.provider(CatalogService.class),
              registry.provider(SabotContext.class),
              registry.provider(ReflectionStatusService.class),
              bootstrap.getExecutor(),
              isDistributedMaster,
              bootstrap.getAllocator(),
              registry.provider(RequestContext.class),
              datasetEventHub,
              registry.provider(CacheRefresherService.class));
      CacheRefresherService cacheRefresherService =
          new CacheRefresherServiceImpl(
              registry.provider(SabotContext.class),
              registry.provider(ForemenWorkManager.class),
              registry.provider(SchedulerService.class),
              registry.provider(RequestContext.class),
              reflectionService,
              datasetEventHub,
              reflectionService.getRelfectionGoalsStore(),
              reflectionService.getReflectionEntriesStore(),
              bootstrap.getExecutor());

      registry.bind(ReflectionService.class, reflectionService);

      registry.bind(CacheRefresherService.class, cacheRefresherService);
      registry.bind(ReflectionAdministrationService.Factory.class, (context) -> reflectionService);
      registry.replaceProvider(
          MaterializationDescriptorProvider.class, reflectionService::getMaterializationDescriptor);
      registry.replace(
          AccelerationManager.class,
          new AccelerationManagerImpl(
              registry.provider(ReflectionService.class),
              registry.provider(ReflectionAdministrationService.Factory.class),
              registry.provider(CatalogService.class)));

      final StatisticsServiceImpl statisticsService =
          new StatisticsServiceImpl(
              registry.provider(LegacyKVStoreProvider.class),
              registry.provider(SchedulerService.class),
              registry.provider(JobsService.class),
              namespaceServiceProvider,
              registry.provider(BufferAllocator.class),
              registry.provider(SabotContext.class));
      registry.replace(StatisticsService.class, statisticsService);
      registry.bind(StatisticsAdministrationService.Factory.class, (context) -> statisticsService);
      registry.replace(
          RelMetadataQuerySupplier.class, DremioRelMetadataQuery.getSupplier(statisticsService));

      registry.bind(ReflectionUtils.class, new ReflectionUtils());
      registry.bind(
          ReflectionStatusService.class,
          new ReflectionStatusServiceImpl(
              registry.provider(ClusterCoordinator.class),
              registry.provider(CatalogService.class),
              registry.provider(JobsService.class),
              registry.provider(LegacyKVStoreProvider.class),
              reflectionService.getCacheViewerProvider(),
              registry.provider(OptionManager.class),
              registry.provider(ReflectionUtils.class)));
    } else {
      registry.bind(ReflectionService.class, new ExecutorOnlyReflectionService());
      registry.bind(ReflectionStatusService.class, ReflectionStatusService.NOOP);
    }

    final Provider<Optional<NodeEndpoint>> serviceLeaderProvider =
        () ->
            registry
                .provider(SchedulerService.class)
                .get()
                .getCurrentTaskOwner(LOCAL_TASK_LEADER_NAME);

    if (isCoordinator) {
      conduitServiceRegistry.registerService(
          new AccelerationListServiceImpl(
              registry.provider(ReflectionStatusService.class),
              registry.provider(ReflectionService.class),
              registry.provider(ReflectionAdministrationService.Factory.class),
              registry.provider(LegacyKVStoreProvider.class),
              bootstrap::getExecutor));
    }

    final AccelerationListManagerImpl accelerationListManager =
        new AccelerationListManagerImpl(
            registry.provider(LegacyKVStoreProvider.class),
            registry.provider(ReflectionStatusService.class),
            registry.provider(ReflectionService.class),
            () -> config,
            isMaster,
            isCoordinator,
            serviceLeaderProvider,
            registry.provider(ConduitProvider.class));
    registry.bind(AccelerationListManager.class, accelerationListManager);

    final StatisticsListManager statisticsListManager =
        new StatisticsListManagerImpl(
            registry.provider(StatisticsService.class),
            serviceLeaderProvider,
            registry.provider(FabricService.class),
            registry.provider(BufferAllocator.class),
            () -> config,
            isMaster,
            isCoordinator);
    registry.bind(StatisticsListManager.class, statisticsListManager);

    final UserDefinedFunctionService userDefinedFunctionListManager =
        new UserDefinedFunctionServiceImpl(
            namespaceServiceProvider,
            registry.provider(CatalogService.class),
            registry.provider(OptionManager.class),
            serviceLeaderProvider,
            registry.provider(FabricService.class),
            registry.provider(BufferAllocator.class),
            () -> config,
            isMaster,
            isCoordinator);
    registry.bind(UserDefinedFunctionService.class, userDefinedFunctionListManager);

    if (isCoordinator) {
      registry.bindSelf(new ServerHealthMonitor(registry.provider(MasterStatusListener.class)));
      conduitServiceRegistry.registerService(
          new CoordinatorLogService(sabotContextProvider, registry.provider(SupportService.class)));
    }

    registry.bind(
        SupportService.class,
        new BasicSupportService(
            dacConfig,
            registry.provider(LegacyKVStoreProvider.class),
            registry.provider(JobsService.class),
            registry.provider(UserService.class),
            registry.provider(ClusterCoordinator.class),
            registry.provider(OptionManager.class),
            namespaceServiceProvider,
            registry.provider(CatalogService.class),
            registry.provider(FabricService.class),
            bootstrap.getAllocator()));

    registry.bind(
        QueryLogBundleService.class,
        new BasicQueryLogBundleService(
            bootstrap.getDremioConfig(),
            bootstrap.getClasspathScan(),
            registry.provider(NodeEndpoint.class),
            registry.provider(OptionManager.class),
            registry.provider(ClusterCoordinator.class),
            registry.provider(ProjectOptionManager.class),
            registry.provider(SupportService.class),
            registry.provider(JobsService.class),
            registry.provider(ProvisioningService.class),
            registry.provider(ConduitProvider.class)));

    registry.bindSelf(
        new NodeRegistration(
            registry.provider(NodeEndpoint.class),
            registry.provider(FragmentWorkManager.class),
            registry.provider(ForemenWorkManager.class),
            registry.provider(ClusterCoordinator.class),
            registry.provider(DremioConfig.class)));

    if (isCoordinator) {
      // search
      final SearchService searchService;
      if (isDistributedMaster) {
        searchService =
            new SearchServiceImpl(
                namespaceServiceProvider,
                registry.provider(OptionManager.class),
                registry.provider(LegacyKVStoreProvider.class),
                registry.provider(SchedulerService.class),
                bootstrap.getExecutor());
      } else {
        searchService = SearchService.UNSUPPORTED;
      }

      final Provider<Optional<NodeEndpoint>> taskLeaderProvider =
          () ->
              registry
                  .provider(SchedulerService.class)
                  .get()
                  .getCurrentTaskOwner(SearchServiceImpl.LOCAL_TASK_LEADER_NAME);
      registry.bind(
          SearchService.class,
          new SearchServiceInvoker(
              isDistributedMaster,
              registry.provider(NodeEndpoint.class),
              taskLeaderProvider,
              registry.provider(FabricService.class),
              bootstrap.getAllocator(),
              searchService));

      registry.bind(
          RestApiServerFactory.class,
          new RestApiServerFactory(
              dacConfig, bootstrap.getClasspathScan(), ImmutableSet.of("oss")));

      registry.bind(
          DremioServlet.class,
          new DremioServlet(
              dacConfig.getConfig(),
              registry.provider(ServerHealthMonitor.class),
              registry.provider(OptionManager.class),
              registry.provider(SupportService.class)));
    }

    LivenessService livenessService = new LivenessService(config);
    registry.bind(LivenessService.class, livenessService);
    if (taskPoolInitializer != null) {
      livenessService.addHealthMonitor(taskPoolInitializer);
    }

    registry.bind(PubSubClient.class, NoOpPubSubClient.class);

    registry.bind(Clock.class, Clock.systemUTC());
    registry.bindSelf(SourceService.class);
    registry.bindSelf(DatasetVersionMutator.class);
    registry.bind(NamespaceService.class, NamespaceServiceImpl.class);

    if (isCoordinator) {
      registry.bind(OpenTelemetry.class, OpenTelemetry.noop());
    }

    // Search bindings.
    registry.bind(
        CatalogEventMessagePublisherProvider.class, CatalogEventMessagePublisherProvider.NO_OP);
    // end search bindings.

    if (isCoordinator) {
      registry.bind(
          SampleDataPopulatorService.class,
          new SampleDataPopulatorService(
              registry.provider(SabotContext.class),
              registry.provider(LegacyKVStoreProvider.class),
              registry.provider(UserService.class),
              registry.provider(JobsService.class),
              registry.provider(CatalogService.class),
              registry.provider(ConnectionReader.class),
              registry.provider(SearchService.class),
              registry.lookup(CatalogEventMessagePublisherProvider.class),
              registry.provider(OptionManager.class),
              dacConfig.prepopulate,
              dacConfig.addDefaultUser));
    }

    subscribeJobCountsDatasetDeletion(registry);
    registry.bindSelf(ReflectionServiceHelper.class);
    registry.bindSelf(CatalogServiceHelper.class);
    registry.bindSelf(CollaborationHelper.class);
    registry.bindSelf(UserServiceHelper.class);

    registry.bind(FirstLoginSetupService.class, OSSFirstLoginSetupService.NOOP_INSTANCE);

    if (isExecutor) {
      registry.bindSelf(
          new ExprCachePrewarmService(
              sabotContextProvider,
              registry.provider(OptionManager.class),
              bootstrap.getAllocator()));
    }

    if (isCoordinator && config.getBoolean(DremioConfig.NESSIE_SERVICE_ENABLED_BOOLEAN)) {
      EmbeddedMetadataPointerService pointerService =
          new EmbeddedMetadataPointerService(registry.provider(KVStoreProvider.class));
      pointerService.getGrpcServices().forEach(conduitServiceRegistry::registerService);
      registry.bindSelf(pointerService);
    }

    registry.bind(ScriptStore.class, new NoOpScriptStoreImpl());
    registry.bind(ScriptService.class, new NoOpScriptServiceImpl());

    if (isCoordinator) {
      final ScriptStore scriptStore = new ScriptStoreImpl(registry.provider(KVStoreProvider.class));
      registry.replace(ScriptStore.class, scriptStore);

      final ScriptService scriptService =
          new ScriptServiceImpl(
              registry.provider(ScriptStore.class), registry.provider(UserSessionService.class));
      registry.replace(ScriptService.class, scriptService);
    }

    if (isCoordinator) {
      final UserPreferenceStore userPreferenceStore =
          new UserPreferenceStoreImpl(registry.provider(KVStoreProvider.class));
      registry.bind(UserPreferenceStore.class, userPreferenceStore);

      registry.bind(UserPreferenceService.class, UserPreferenceServiceImpl.class);
    }

    if (isCoordinator) {
      SystemTableManager systemTableManager =
          new SystemTableManagerImpl(
              getSystemTableAllocator(bootstrap),
              () -> getSysFlightTableProviders(registry),
              () -> getSysFlightTableFunctionProviders(registry));
      registry.bind(SystemTableManager.class, systemTableManager);
    }

    if (isCoordinator) {
      final SQLRunnerSessionStore sqlRunnerSessionStore =
          new SQLRunnerSessionStoreImpl(registry.provider(KVStoreProvider.class));
      registry.bind(SQLRunnerSessionStore.class, sqlRunnerSessionStore);

      final SQLRunnerSessionService sqlRunnerSessionService =
          new SQLRunnerSessionServiceImpl(
              registry.provider(SQLRunnerSessionStore.class),
              registry.provider(OptionManager.class),
              registry.provider(ScriptService.class));
      registry.bind(SQLRunnerSessionService.class, sqlRunnerSessionService);
      registerUserServiceEvents(registry);

      registry.bind(
          SQLRunnerSessionCleanerService.class,
          new SQLRunnerSessionCleanerService(
              registry.provider(SchedulerService.class),
              registry.provider(SQLRunnerSessionService.class)));
    }

    registerHeapMonitorManager(registry, isCoordinator);

    registerActiveQueryListService(
        registry, isCoordinator, isDistributedMaster, conduitServiceRegistry);

    registerOrphanageCleanerService(registry, isCoordinator, isDistributedMaster);

    registerSystemIcebergTablesMaintainerServices(registry, isCoordinator);

    bindSystemIcebergStoragePluginConfig(registry, isCoordinator);

    if (isCoordinator) {
      registry.bindSelf(
          new NodeMetricsService(
              registry.provider(ClusterCoordinator.class),
              registry.provider(ConduitProvider.class),
              registry.provider(ExecutorServiceClientFactory.class)));
      conduitServiceRegistry.registerService(new CoordinatorMetricsService(sabotContextProvider));
      registry.bindSelf(
          new NodesHistoryPluginInitializer(
              registry.provider(CatalogService.class), registry.provider(SchedulerService.class)));
      registry.bindSelf(new NodesHistoryViewResolver(registry.provider(OptionManager.class)));
      registry.bindSelf(
          new NodeMetricsPersistenceService(
              false,
              registry.provider(NodeMetricsService.class),
              registry.provider(SchedulerService.class),
              registry.provider(OptionManager.class),
              getNodesHistoryStoreConfigProvider(registry)));
    }

    if (isCoordinator && config.getBoolean(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN)) {
      registry.bind(
          DremioFlightAuthProvider.class,
          new DremioFlightAuthProviderImpl(
              registry.provider(DremioConfig.class),
              registry.provider(UserService.class),
              registry.provider(TokenManager.class)));
      registry.bind(FlightRequestContextDecorator.class, FlightRequestContextDecorator.DEFAULT);

      registry.bindSelf(
          new DremioFlightService(
              registry.provider(DremioConfig.class),
              registry.provider(BufferAllocator.class),
              registry.provider(UserWorker.class),
              registry.provider(SabotContext.class),
              registry.provider(TokenManager.class),
              registry.provider(OptionManager.class),
              registry.provider(UserSessionService.class),
              registry.provider(DremioFlightAuthProvider.class),
              registry.provider(FlightRequestContextDecorator.class),
              registry.provider(CredentialsService.class)));
    } else {
      logger.info("Not starting the flight service.");
    }

    // NOTE : Should be last after all other services
    // used as health check to know when to start serving traffic.
    if (isCoordinator) {
      // if we have at least one user registered, disable firstTimeApi and checkNoUser
      // but for userGroupService is not started yet so we cannot check for now
      registry.bind(
          WebServer.class,
          new WebServer(
              registry,
              dacConfig,
              registry.provider(CredentialsService.class),
              registry.provider(RestApiServerFactory.class),
              registry.provider(DremioServer.class),
              new DremioBinder(registry),
              "ui"));

      final TokenManagerImpl tokenManagerImpl =
          new TokenManagerImpl(
              registry.provider(LegacyKVStoreProvider.class),
              registry.provider(SchedulerService.class),
              registry.provider(OptionManager.class),
              isDistributedMaster,
              config);
      registry.bindSelf(tokenManagerImpl);

      final String fallbackHostname =
          String.format(
              "%s://%s:%s",
              dacConfig.webSSLEnabled() ? "https" : "http",
              config.getThisNode(),
              dacConfig.getHttpPort());
      registry.bind(
          WebServerInfoProvider.class,
          new WebServerInfoProviderImpl(
              () -> registry.lookup(SupportService.class).getClusterId().getIdentity(),
              registry.provider(OptionManager.class),
              fallbackHostname));
      if (isMaster) {
        registry.bind(
            JWKSetManager.class,
            new SystemJWKSetManager(
                Clock.systemUTC(),
                registry.provider(WebServerInfoProvider.class),
                registry.provider(UserService.class),
                registry.provider(OptionManager.class),
                registry.provider(SchedulerService.class),
                () -> new ConfigurationStore(registry.provider(LegacyKVStoreProvider.class).get()),
                registry.provider(SecretsCreator.class),
                registry.provider(CredentialsService.class),
                config));
        registry.bind(
            TokenManager.class,
            new TokenManagerImplV2(
                Clock.systemUTC(),
                () -> tokenManagerImpl,
                registry.provider(OptionManager.class),
                registry.provider(WebServerInfoProvider.class),
                registry.provider(UserResolver.class),
                registry.provider(JWKSetManager.class)));
      } else {
        registry.bind(TokenManager.class, tokenManagerImpl);
      }
    }
  }

  protected SystemOptionManager buildOptionManager(
      OptionChangeBroadcaster systemOptionChangeBroadcaster,
      OptionValidatorListing optionValidatorListing,
      LogicalPlanPersistence logicalPlanPersistence,
      boolean isMemory,
      SingletonRegistry registry) {
    return new SystemOptionManagerImpl(
        optionValidatorListing,
        logicalPlanPersistence,
        registry.provider(LegacyKVStoreProvider.class),
        registry.provider(SchedulerService.class),
        systemOptionChangeBroadcaster,
        isMemory);
  }

  protected SystemOptionManager buildSystemOptionManager(
      OptionValidatorListing optionValidatorListing,
      LogicalPlanPersistence logicalPlanPersistence,
      SingletonRegistry registry,
      boolean isCoordinator,
      ConduitServiceRegistry conduitServiceRegistry,
      boolean inMemory) {
    SystemOptionManager systemOptionManager;
    if (isCoordinator) {
      final OptionChangeBroadcaster systemOptionChangeBroadcaster =
          new OptionChangeBroadcaster(
              registry.provider(ConduitProvider.class),
              registry.provider(ClusterCoordinator.class),
              registry.provider(NodeEndpoint.class));
      systemOptionManager =
          new SystemOptionManagerImpl(
              optionValidatorListing,
              logicalPlanPersistence,
              registry.provider(LegacyKVStoreProvider.class),
              registry.provider(SchedulerService.class),
              systemOptionChangeBroadcaster,
              inMemory);
      conduitServiceRegistry.registerService(
          new OptionNotificationService(registry.provider(SystemOptionManager.class)));
    } else {
      systemOptionManager =
          new SystemOptionManagerImpl(
              optionValidatorListing,
              logicalPlanPersistence,
              registry.provider(LegacyKVStoreProvider.class),
              registry.provider(SchedulerService.class),
              null,
              inMemory);
    }
    return systemOptionManager;
  }

  protected OptionManagerWrapper.Builder buildOptionManager(
      SystemOptionManager systemOptionManager, OptionValidatorListing optionValidatorListing) {
    return OptionManagerWrapper.Builder.newBuilder()
        .withOptionValidatorProvider(optionValidatorListing)
        .withOptionManager(new DefaultOptionManager(optionValidatorListing))
        .withOptionManager(systemOptionManager);
  }

  protected ProjectOptionManager buildProjectOptionManager(
      SystemOptionManager systemOptionManager, DefaultOptionManager defaultOptionManager) {
    return new ProjectOptionManagerWrapper(systemOptionManager, defaultOptionManager);
  }

  protected ProvisioningService createProvisioningService(
      DremioConfig config, BootStrapContext bootstrap, SingletonRegistry registry) {
    return new ProvisioningServiceImpl(
        config,
        registry.provider(LegacyKVStoreProvider.class),
        () -> registry.lookup(ClusterCoordinator.class).getExecutorEndpoints(),
        bootstrap.getClasspathScan(),
        registry.provider(OptionManager.class),
        registry.provider(EditionProvider.class));
  }

  private static void subscribeJobCountsDatasetDeletion(SingletonRegistry registry) {
    JobCountsDatasetDeletionSubscriber jobDeletionSubscriber =
        new JobCountsDatasetDeletionSubscriber(registry.provider(JobCountsClient.class));
    registry.bind(JobCountsDatasetDeletionSubscriber.class, jobDeletionSubscriber);
    registry
        .provider(CatalogService.class)
        .get()
        .subscribe(DatasetDeletionCatalogStatusEvent.getEventTopic(), jobDeletionSubscriber);
  }

  protected void registerUserServiceEvents(SingletonRegistry registry) {
    registry.bind(UserServiceEvents.class, registry.provider(UserService.class).get());
    subscribeSQLRunnerSessionUserDeletion(registry);
  }

  private static void subscribeSQLRunnerSessionUserDeletion(SingletonRegistry registry) {
    SQLRunnerSessionUserDeletionSubscriber userDeletionSubscriber =
        new SQLRunnerSessionUserDeletionSubscriber(
            registry.provider(SQLRunnerSessionService.class));
    registry.bind(SQLRunnerSessionUserDeletionSubscriber.class, userDeletionSubscriber);
    registry
        .provider(UserServiceEvents.class)
        .get()
        .subscribe(UserDeletionEvent.getEventTopic(), userDeletionSubscriber);
  }

  private ManagedChannel getOrCreateChannelToMaster(SingletonRegistry registry) {
    return registry.lookup(ConduitProvider.class).getOrCreateChannelToMaster();
  }

  protected BufferAllocator getSystemTableAllocator(final BootStrapContext bootstrap) {
    return bootstrap.getAllocator().newChildAllocator("sysflight-producer", 0, Long.MAX_VALUE);
  }

  private void registerActiveQueryListService(
      SingletonRegistry registry,
      boolean isCoordinator,
      boolean isDistributedMaster,
      ConduitServiceRegistry conduitServiceRegistry) {
    if (isCoordinator) {
      ActiveQueryListService activeQueryListService =
          new ActiveQueryListService(
              registry.provider(SchedulerService.class),
              registry.provider(ExecutorServiceClientFactory.class),
              registry.provider(NodeEndpoint.class),
              registry.provider(ExecutorSetService.class),
              registry.provider(MaestroService.class),
              registry.provider(ClusterCoordinator.class),
              registry.provider(ConduitProvider.class),
              registry.provider(OptionManager.class),
              isDistributedMaster);

      registry.bindSelf(activeQueryListService);
      conduitServiceRegistry.registerService(activeQueryListService);
    }
  }

  private void registerCatalogMaintenanceService(
      SingletonRegistry registry, ScanResult scanResult, boolean isCoordinator) {
    if (isCoordinator) {
      Provider<SabotContext> sabotContextProvider = registry.provider(SabotContext.class);
      registry.bindSelf(
          new CatalogMaintenanceService(
              registry.provider(SchedulerService.class),
              registry.provider(java.util.concurrent.ExecutorService.class),
              new CatalogMaintenanceRunnableProvider(
                      registry.provider(OptionManager.class),
                      registry.provider(KVStoreProvider.class).get(),
                      () -> sabotContextProvider.get().getNamespaceService(SYSTEM_USERNAME),
                      getVersionedSourceTypes(scanResult))
                  .get(0)));
    }
  }

  public static ImmutableSet<String> getVersionedSourceTypes(ScanResult scanResult) {
    var builder = new ImmutableSet.Builder<String>();
    for (Class<?> clazz : scanResult.getAnnotatedClasses(SourceType.class)) {
      SourceType sourceType = clazz.getAnnotation(SourceType.class);
      if (sourceType.isVersioned()) {
        builder.add(sourceType.value());
      }
    }
    return builder.build();
  }

  protected String getRootName() {
    return Path.SEPARATOR + "dremio" + Path.SEPARATOR + "coordinator";
  }

  protected String getFabricAddress() {
    // Fabric
    final String fabricAddress;
    try {
      fabricAddress = FabricServiceImpl.getAddress(false);
    } catch (UnknownHostException e) {
      throw new RuntimeException("Cannot get local address", e);
    }
    return fabricAddress;
  }

  // Registering heap monitor manager as a service,
  // so that it can be closed cleanly when shutdown
  private void registerHeapMonitorManager(SingletonRegistry registry, boolean isCoordinator) {
    if (isCoordinator) {
      Consumer<CancelQueryContext> cancelConsumer =
          (cancelQueryContext) ->
              registry.provider(ForemenWorkManager.class).get().cancel(cancelQueryContext);

      logger.info("Registering heap monitor manager in coordinator as a service.");
      registry.bindSelf(
          new HeapMonitorManager(
              registry.provider(OptionManager.class),
              new CoordinatorHeapClawBackStrategy(cancelConsumer),
              null,
              ClusterCoordinator.Role.COORDINATOR));
    }
  }

  protected BufferAllocator getChildBufferAllocator(BufferAllocator allocator) {
    return checkNotNull(allocator).newChildAllocator("jobs-service", 0, Long.MAX_VALUE);
  }

  private void registerJobsServices(
      final ConduitServiceRegistry conduitServiceRegistry,
      final SingletonRegistry registry,
      final BootStrapContext bootstrap) {
    // 1. job adapter
    conduitServiceRegistry.registerService(
        new JobsServiceAdapter(registry.provider(LocalJobsService.class)));

    // 2. chronicle
    conduitServiceRegistry.registerService(
        new Chronicle(registry.provider(LocalJobsService.class), bootstrap::getExecutor));

    // 3. jobs, sys flight producers registered together as CoordinatorFlightProducer, as individual
    // binding is masking one of them
    final BufferAllocator coordFlightAllocator =
        bootstrap
            .getAllocator()
            .newChildAllocator(CoordinatorFlightProducer.class.getName(), 0, Long.MAX_VALUE);
    final JobsFlightProducer jobsFlightProducer =
        new JobsFlightProducer(registry.provider(LocalJobsService.class), coordFlightAllocator);
    final SysFlightProducer sysFlightProducer =
        new SysFlightProducer(registry.provider(SystemTableManager.class));
    final CoordinatorFlightProducer coordFlightProducer =
        new CoordinatorFlightProducer(jobsFlightProducer, sysFlightProducer);
    conduitServiceRegistry.registerService(
        new FlightCloseableBindableService(coordFlightAllocator, coordFlightProducer, null, null));

    // 4. MaestroGrpcServerFacade
    conduitServiceRegistry.registerService(
        new MaestroGrpcServerFacade(registry.provider(ExecToCoordStatusHandler.class)));

    // 5. jobresults
    JobResultsGrpcServerFacade jobResultsGrpcServerFacade =
        new JobResultsGrpcServerFacade(
            registry.provider(ExecToCoordResultsHandler.class),
            registry.provider(MaestroForwarder.class));

    // Note: To take effect of the change in this option on coordinator side, coordinator needs to
    // be restarted.
    // But the change of option is propagated to executor without restarting executor. Its dynamic
    // change on executor side.
    if (avoidHeapCopy()) {
      logger.info("Using JobResultsBindableService");
      conduitServiceRegistry.registerService(
          new JobResultsBindableService(bootstrap.getAllocator(), jobResultsGrpcServerFacade));
    } else {
      logger.info("Using JobResultsGrpcServerFacade");
      conduitServiceRegistry.registerService(jobResultsGrpcServerFacade);
    }
  }

  private boolean avoidHeapCopy() {
    String property =
        SystemOptionManagerImpl.SYSTEM_OPTION_PREFIX
            + JobResultsTunnel.AVOID_HEAP_COPY_IN_RESULTS_PATH_OPTION_NAME;
    if (System.getProperty(property) != null) {
      return Boolean.getBoolean(property);
    } else {
      // return the default option value.
      return JobResultsTunnel.AVOID_HEAP_COPY_IN_RESULTS_PATH.getDefault().getBoolVal();
    }
  }

  protected LegacyIndexedStore<JobId, JobResult> getIndexedJobsStore(
      Provider<LegacyKVStoreProvider> kvStoreProviderProvider) {
    return kvStoreProviderProvider.get().getStore(JobsStoreCreator.class);
  }

  protected Provider<JobResultsStore> getJobResultsStoreProvider(
      Provider<JobResultsStoreConfig> jobResultsStoreConfigProvider,
      Provider<LegacyKVStoreProvider> kvStoreProviderProvider,
      BufferAllocator allocator) {
    return () -> {
      try {
        return new JobResultsStore(
            jobResultsStoreConfigProvider.get(),
            getIndexedJobsStore(kvStoreProviderProvider),
            allocator);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  protected StructuredLogger<Job> getJobResultLogger(SingletonRegistry registry) {
    return LocalJobsService.createJobResultLogger();
  }

  protected Provider<JobResultsStoreConfig> getJobResultsStoreConfigProvider(
      SingletonRegistry registry) {
    return () -> {
      try {
        final CatalogService storagePluginRegistry = registry.provider(CatalogService.class).get();
        final FileSystemPlugin<?> plugin = storagePluginRegistry.getSource(JOBS_STORAGEPLUGIN_NAME);
        return new JobResultsStoreConfig(
            plugin.getName(), plugin.getConfig().getPath(), plugin.getSystemUserFS());
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    };
  }

  protected Provider<NodesHistoryStoreConfig> getNodesHistoryStoreConfigProvider(
      SingletonRegistry registry) {
    return () -> {
      try {
        final CatalogService storagePluginRegistry = registry.provider(CatalogService.class).get();
        final FileSystemPlugin<?> plugin =
            storagePluginRegistry.getSource(NodesHistoryStoreConfig.STORAGE_PLUGIN_NAME);
        return new NodesHistoryStoreConfig(plugin.getConfig().getPath(), plugin.getSystemUserFS());
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    };
  }

  /**
   * Set up the {@link UserService} in registry according to the config.
   *
   * @return True if the internal user management is used.
   */
  protected boolean setupUserService(
      final SingletonRegistry registry,
      final DACConfig dacConfig,
      final Provider<SabotContext> sabotContext,
      boolean isMaster,
      boolean isCoordinator) {
    if (!isCoordinator) {
      registry.bind(UserService.class, new ExecutorUserService());
      return false;
    }

    if (dacConfig.isInternalUserAuth()) {
      final SimpleUserService simpleUserService =
          new SimpleUserService(registry.provider(LegacyKVStoreProvider.class), isMaster);
      registry.bindProvider(UserService.class, () -> simpleUserService);
      registry.bindSelf(simpleUserService);
      // UserResolver is only needed on Coordinator
      registry.bindProvider(UserResolver.class, () -> simpleUserService);
      logger.info("Internal user/group service is configured.");
      return true;
    }

    String authType = dacConfig.getConfig().getString(WEB_AUTH_TYPE);
    logger.error(
        "Unknown value '{}' set for {}. Accepted values are ['internal', 'ldap']",
        authType,
        WEB_AUTH_TYPE);
    throw new RuntimeException(
        String.format("Unknown auth type '%s' set in config path '%s'", authType, WEB_AUTH_TYPE));
  }

  protected Map<SystemTables, SysFlightDataProvider> getSysFlightTableProviders(
      SingletonRegistry registry) {
    ConduitProvider conduitProvider = registry.lookup(ConduitProvider.class);
    Map<SystemTables, SysFlightDataProvider> tablesMap = Maps.newHashMap();
    ReflectionDescriptionServiceStub reflectionsStub =
        ReflectionDescriptionServiceGrpc.newStub(conduitProvider.getOrCreateChannelToMaster());

    tablesMap.put(
        SystemTableManager.TABLES.JOBS,
        new JobsTable(() -> ChronicleGrpc.newStub(conduitProvider.getOrCreateChannelToMaster())));
    tablesMap.put(
        SystemTableManager.TABLES.REFLECTIONS, new ReflectionsTable(() -> reflectionsStub));
    tablesMap.put(
        SystemTableManager.TABLES.MATERIALIZATIONS,
        new MaterializationsTable(() -> reflectionsStub));
    tablesMap.put(
        SystemTableManager.TABLES.REFLECTION_DEPENDENCIES,
        new ReflectionDependenciesTable(() -> reflectionsStub));
    tablesMap.put(
        SystemTableManager.TABLES.JOBS_RECENT,
        new RecentJobsTable(
            () -> ChronicleGrpc.newStub(conduitProvider.getOrCreateChannelToMaster())));
    return tablesMap;
  }

  protected Map<SystemTableManager.TABLE_FUNCTIONS, SysFlightDataProvider>
      getSysFlightTableFunctionProviders(SingletonRegistry registry) {
    ConduitProvider conduitProvider = registry.provider(ConduitProvider.class).get();
    ReflectionDescriptionServiceStub reflectionsStub =
        ReflectionDescriptionServiceGrpc.newStub(conduitProvider.getOrCreateChannelToMaster());
    Map<SystemTableManager.TABLE_FUNCTIONS, SysFlightDataProvider> tableFunctionsMap =
        Maps.newHashMap();

    tableFunctionsMap.put(
        SystemTableManager.TABLE_FUNCTIONS.REFLECTION_LINEAGE,
        new ReflectionLineageTableFunction(() -> reflectionsStub));
    return tableFunctionsMap;
  }

  private NessieApiV2 createNessieApi(DremioConfig config, SingletonRegistry registry) {
    String endpoint = config.getString(DremioConfig.NESSIE_SERVICE_REMOTE_URI);
    if (endpoint == null || endpoint.isEmpty()) {
      return GrpcClientBuilder.builder()
          .withChannel(getOrCreateChannelToMaster(registry))
          .build(NessieApiV2.class);
    }
    return NessieClientBuilder.createClientBuilder("HTTP", null)
        .withUri(URI.create(endpoint))
        .build(NessieApiV2.class);
  }

  private void registerOrphanageCleanerService(
      SingletonRegistry registry, boolean isCoordinator, boolean isDistributedMaster) {
    if (isCoordinator) {
      OrphanageCleanerService orphanageService =
          new OrphanageCleanerService(
              registry.provider(SchedulerService.class),
              registry.provider(OptionManager.class),
              registry.provider(Orphanage.Factory.class),
              registry.provider(NamespaceService.Factory.class),
              registry.provider(SabotContext.class),
              isDistributedMaster);
      registry.bindSelf(orphanageService);
    }
  }

  protected ModifiableSchedulerService getMetadataRefreshModifiableScheduler(
      SingletonRegistry registry, boolean isDistributedCoordinator, boolean isLeaderless) {
    return new ModifiableWrappedSchedulerService(
        ExecConstants.MAX_CONCURRENT_METADATA_REFRESHES.getDefault().getNumVal().intValue(),
        isDistributedCoordinator,
        "metadata-refresh-modifiable-scheduler-",
        registry.provider(ClusterCoordinator.class),
        registry.provider(ClusterCoordinator.class),
        registry.provider(NodeEndpoint.class),
        ExecConstants.MAX_CONCURRENT_METADATA_REFRESHES,
        registry.provider(OptionManager.class),
        () -> registry.provider(ModifiableSchedulerService.class).get(),
        isLeaderless);
  }

  private void registerSystemIcebergTablesMaintainerServices(
      SingletonRegistry registry, boolean isCoordinator) {
    if (isCoordinator) {
      SystemIcebergTablesSchemaUpdaterService systemIcebergTablesSchemaUpdaterService =
          new SystemIcebergTablesSchemaUpdaterService(
              registry.provider(SchedulerService.class), registry.provider(CatalogService.class));
      registry.bindSelf(systemIcebergTablesSchemaUpdaterService);
      SystemIcebergTablesMaintainerService systemIcebergTablesMaintainerService =
          new SystemIcebergTablesMaintainerService(
              registry.provider(SchedulerService.class),
              registry.provider(OptionManager.class),
              registry.provider(SabotContext.class));
      registry.bindSelf(systemIcebergTablesMaintainerService);
    }
  }

  protected void registerInformationSchemaService(
      ConduitServiceRegistry conduitServiceRegistry,
      SingletonRegistry registry,
      BootStrapContext bootstrap) {
    InformationSchemaServiceImpl informationSchemaService =
        new InformationSchemaServiceImpl(
            registry.provider(CatalogService.class), bootstrap::getExecutor);
    conduitServiceRegistry.registerService(informationSchemaService);
  }

  protected ConnectionReader getConnectionReaderImpl(
      ScanResult scanResult, SabotConfig sabotConfig) {
    return ConnectionReader.of(scanResult, sabotConfig);
  }

  private void bindSystemIcebergStoragePluginConfig(
      SingletonRegistry registry, boolean isCoordinator) {
    if (isCoordinator) {
      SystemIcebergTablesStoragePluginConfigFactory factory =
          new SystemIcebergTablesStoragePluginConfigFactory();
      registry.bindSelf(factory);
    }
  }
}

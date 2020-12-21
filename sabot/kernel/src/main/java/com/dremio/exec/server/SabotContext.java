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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.ViewCreatorFactory;
import com.dremio.exec.catalog.ViewCreatorFactory.ViewCreator;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.expr.fn.DecimalFunctionImplementationRegistry;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.RulesFactory;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.work.WorkStats;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.security.CredentialsService;
import com.dremio.service.catalog.InformationSchemaServiceGrpc.InformationSchemaServiceBlockingStub;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.coordinator.ServiceSetDecorator;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.spill.SpillService;
import com.dremio.service.users.UserService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class SabotContext implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SabotContext.class);

  private final SabotConfig config;
  private final Set<Role> roles;
  private final BufferAllocator allocator;
  private final PhysicalPlanReader reader;
  private final ClusterCoordinator coord;
  private final NodeEndpoint endpoint;
  private final FunctionImplementationRegistry functionRegistry;
  private final DecimalFunctionImplementationRegistry decimalFunctionImplementationRegistry;
  private final OptionManager optionManager;
  private final SystemOptionManager systemOptionManager;
  private final Provider<WorkStats> workStatsProvider;
  private final CodeCompiler compiler;
  private final ScanResult classpathScan;
  private final LogicalPlanPersistence lpPersistence;
  private final Provider<MaterializationDescriptorProvider> materializationProvider;
  private final NamespaceService.Factory namespaceServiceFactory;
  private final DatasetListingService datasetListing;
  private final LegacyKVStoreProvider kvStoreProvider;
  private final UserService userService;
  private final Provider<QueryObserverFactory> queryObserverFactory;
  private final Provider<AccelerationManager> accelerationManager;
  private final Provider<AccelerationListManager> accelerationListManager;
  private final Provider<CatalogService> catalogService;
  private final ConduitProvider conduitProvider;
  private final Provider<InformationSchemaServiceBlockingStub> informationSchemaStub;
  private final Provider<ViewCreatorFactory> viewCreatorFactory;
  private final DremioConfig dremioConfig;
  private final BufferAllocator queryPlanningAllocator;
  private final Provider<SpillService> spillService;
  private final Provider<ConnectionReader> connectionReaderProvider;
  private final GroupResourceInformation clusterInfo;
  private final FileSystemWrapper fileSystemWrapper;
  private final CredentialsService credentialsService;
  private final JobResultInfoProvider jobResultInfoProvider;
  private final List<RulesFactory> rules;
  private final OptionValidatorListing optionValidatorListing;
  private final ExecutorService executorService;
  private final JdbcSchemaFetcherFactoryContext jdbcSchemaFetcherFactoryContext;

  public SabotContext(
      DremioConfig dremioConfig,
      NodeEndpoint endpoint,
      SabotConfig config,
      Collection<Role> roles,
      ScanResult scan,
      LogicalPlanPersistence lpPersistence,
      BufferAllocator allocator,
      ClusterCoordinator coord,
      GroupResourceInformation groupResourceInformation,
      Provider<WorkStats> workStatsProvider,
      LegacyKVStoreProvider kvStoreProvider,
      NamespaceService.Factory namespaceServiceFactory,
      DatasetListingService datasetListing,
      UserService userService,
      Provider<MaterializationDescriptorProvider> materializationProvider,
      Provider<QueryObserverFactory> queryObserverFactory,
      Provider<AccelerationManager> accelerationManager,
      Provider<AccelerationListManager> accelerationListManager,
      Provider<CatalogService> catalogService,
      ConduitProvider conduitProvider,
      Provider<InformationSchemaServiceBlockingStub> informationSchemaStub,
      Provider<ViewCreatorFactory> viewCreatorFactory,
      BufferAllocator queryPlanningAllocator,
      Provider<SpillService> spillService,
      Provider<ConnectionReader> connectionReaderProvider,
      CredentialsService credentialsService,
      JobResultInfoProvider jobResultInfoProvider,
      OptionManager optionManager,
      SystemOptionManager systemOptionManager,
      OptionValidatorListing optionValidatorListing,
      ExecutorService executorService
  ) {
    this.dremioConfig = dremioConfig;
    this.config = config;
    this.roles = ImmutableSet.copyOf(roles);
    this.allocator = allocator;
    this.workStatsProvider = workStatsProvider;
    this.classpathScan = scan;
    this.coord = coord;
    this.clusterInfo = groupResourceInformation;
    this.endpoint = checkNotNull(endpoint);
    this.lpPersistence = lpPersistence;
    this.accelerationManager = accelerationManager;
    this.accelerationListManager = accelerationListManager;
    this.connectionReaderProvider = connectionReaderProvider;

    this.reader = new PhysicalPlanReader(config, classpathScan, lpPersistence, endpoint, catalogService, this);
    this.optionManager = optionManager;
    this.systemOptionManager = systemOptionManager;
    this.functionRegistry = new FunctionImplementationRegistry(config, classpathScan, this.optionManager);
    this.decimalFunctionImplementationRegistry = new DecimalFunctionImplementationRegistry(config, classpathScan, this.optionManager);
    this.compiler = new CodeCompiler(config, this.optionManager);
    this.kvStoreProvider = kvStoreProvider;
    this.namespaceServiceFactory = namespaceServiceFactory;
    this.datasetListing = datasetListing;
    this.userService = userService;
    this.queryObserverFactory = queryObserverFactory;
    this.materializationProvider = materializationProvider;
    this.catalogService = catalogService;
    this.conduitProvider = conduitProvider;
    this.informationSchemaStub = informationSchemaStub;
    this.viewCreatorFactory = viewCreatorFactory;
    this.queryPlanningAllocator = queryPlanningAllocator;
    this.spillService = spillService;
    this.fileSystemWrapper = config.getInstance(
      FileSystemWrapper.FILE_SYSTEM_WRAPPER_CLASS,
      FileSystemWrapper.class,
      (fs, storageId, conf, operatorContext, enableAsync, isMetadataEnabled) -> fs,
      dremioConfig,
      this.optionManager,
      allocator,
      new ServiceSetDecorator(coord.getServiceSet(Role.EXECUTOR)),
      endpoint);
    this.credentialsService = credentialsService;
    this.jobResultInfoProvider = jobResultInfoProvider;
    this.rules = getRulesFactories(scan);
    this.optionValidatorListing = optionValidatorListing;
    this.executorService = executorService;
    this.jdbcSchemaFetcherFactoryContext = new JdbcSchemaFetcherFactoryContext(optionManager, credentialsService);
  }

  private static List<RulesFactory> getRulesFactories(ScanResult scan) {
    ImmutableList.Builder<RulesFactory> factoryBuilder = ImmutableList.builder();
    for (Class<? extends RulesFactory> f : scan.getImplementations(RulesFactory.class)) {
      try {
        factoryBuilder.add(f.newInstance());
      } catch (Exception ex) {
        logger.warn("Failure while configuring rules factory {}", f.getName(), ex);
      }
    }
    return factoryBuilder.build();
  }

  SabotContext(
    DremioConfig dremioConfig,
    NodeEndpoint endpoint,
    SabotConfig config,
    Collection<Role> roles,
    ScanResult scan,
    LogicalPlanPersistence lpPersistence,
    BufferAllocator allocator,
    ClusterCoordinator coord,
    Provider<WorkStats> workStatsProvider,
    LegacyKVStoreProvider kvStoreProvider,
    NamespaceService.Factory namespaceServiceFactory,
    DatasetListingService datasetListing,
    UserService userService,
    Provider<MaterializationDescriptorProvider> materializationProvider,
    Provider<QueryObserverFactory> queryObserverFactory,
    Provider<AccelerationManager> accelerationManager,
    Provider<AccelerationListManager> accelerationListManager,
    Provider<CatalogService> catalogService,
    ConduitProvider conduitProvider,
    Provider<InformationSchemaServiceBlockingStub> informationSchemaStub,
    Provider<ViewCreatorFactory> viewCreatorFactory,
    BufferAllocator queryPlanningAllocator,
    Provider<SpillService> spillService,
    Provider<ConnectionReader> connectionReaderProvider,
    CredentialsService credentialsService,
    JobResultInfoProvider jobResultInfoProvider,
    PhysicalPlanReader physicalPlanReader,
    OptionManager optionManager,
    SystemOptionManager systemOptionManager,
    FunctionImplementationRegistry functionImplementationRegistry,
    DecimalFunctionImplementationRegistry decimalFunctionImplementationRegistry,
    CodeCompiler codeCompiler,
    GroupResourceInformation clusterInfo,
    FileSystemWrapper fileSystemWrapper,
    OptionValidatorListing optionValidatorListing,
    ExecutorService executorService
  ) {
    this.dremioConfig = dremioConfig;
    this.config = config;
    this.roles = ImmutableSet.copyOf(roles);
    this.allocator = allocator;
    this.workStatsProvider = workStatsProvider;
    this.classpathScan = scan;
    this.coord = coord;
    this.endpoint = checkNotNull(endpoint);
    this.lpPersistence = lpPersistence;
    this.accelerationManager = accelerationManager;
    this.accelerationListManager = accelerationListManager;
    this.connectionReaderProvider = connectionReaderProvider;

    // Escaping 'this'
    this.reader = physicalPlanReader;
    this.optionManager = optionManager;
    this.systemOptionManager = systemOptionManager;
    this.functionRegistry = functionImplementationRegistry;
    this.decimalFunctionImplementationRegistry = decimalFunctionImplementationRegistry;
    this.compiler = codeCompiler;

    this.kvStoreProvider = kvStoreProvider;
    this.namespaceServiceFactory = namespaceServiceFactory;
    this.datasetListing = datasetListing;
    this.userService = userService;
    this.queryObserverFactory = queryObserverFactory;
    this.materializationProvider = materializationProvider;
    this.catalogService = catalogService;
    this.conduitProvider = conduitProvider;
    this.informationSchemaStub = informationSchemaStub;
    this.viewCreatorFactory = viewCreatorFactory;
    this.queryPlanningAllocator = queryPlanningAllocator;
    this.spillService = spillService;
    this.clusterInfo = clusterInfo;
    this.fileSystemWrapper = fileSystemWrapper;
    this.credentialsService = credentialsService;
    this.jobResultInfoProvider = jobResultInfoProvider;
    this.rules = getRulesFactories(scan);
    this.optionValidatorListing = optionValidatorListing;
    this.executorService = executorService;
    this.jdbcSchemaFetcherFactoryContext = new JdbcSchemaFetcherFactoryContext(optionManager, credentialsService);
  }

  private void checkIfCoordinator() {
    Preconditions.checkState(roles.contains(Role.COORDINATOR), "this is a coordinator notion");
  }

  // TODO: rationalize which methods are executor only or coordinator only
  public NamespaceService.Factory getNamespaceServiceFactory() {
    return namespaceServiceFactory;
  }

  protected Provider<AccelerationManager> getAccelerationManagerProvider() {
    return accelerationManager;
  }

  protected Provider<AccelerationListManager> getAccelerationListManagerProvider() {
    return accelerationListManager;
  }

  public Provider<CatalogService> getCatalogServiceProvider() {
    return catalogService;
  }

  public Provider<ViewCreatorFactory> getViewCreatorFactoryProvider() {
    return viewCreatorFactory;
  }

  public FunctionImplementationRegistry getFunctionImplementationRegistry() {
    return functionRegistry ;
  }

  public DecimalFunctionImplementationRegistry getDecimalFunctionImplementationRegistry() {
    return decimalFunctionImplementationRegistry;
  }

  public Set<Role> getRoles() {
    return roles;
  }

  /**
   * @return the option manager. It is important to note that this manager only contains options at the
   * "system" level and not "session" level.
   */
  public OptionManager getOptionManager() {
    return optionManager;
  }

  public SystemOptionManager getSystemOptionManager() {
    return systemOptionManager;
  }

  public NodeEndpoint getEndpoint() {
    return endpoint;
  }

  public SabotConfig getConfig() {
    return config;
  }

  public DremioConfig getDremioConfig() {
    return dremioConfig;
  }

  public Collection<NodeEndpoint> getCoordinators() {
    return coord.getServiceSet(ClusterCoordinator.Role.COORDINATOR).getAvailableEndpoints();
  }

  public Optional<NodeEndpoint> getMaster() {
    return Optional.ofNullable(coord.getServiceSet(Role.MASTER).getAvailableEndpoints())
      .flatMap(nodeEndpoints -> nodeEndpoints.stream().findFirst());
  }

  public Collection<NodeEndpoint> getExecutors() {
    return coord.getServiceSet(ClusterCoordinator.Role.EXECUTOR).getAvailableEndpoints();
  }

  /**
   * To return task leader nodeEndpoint if masterless mode is on
   * otherwise return master
   * @param serviceName
   * @return
   */
  public Optional<NodeEndpoint> getServiceLeader(final String serviceName) {
    if (getDremioConfig().isMasterlessEnabled()) {
      return Optional.ofNullable(
        coord.getOrCreateServiceSet(serviceName).getAvailableEndpoints())
        .flatMap(nodeEndpoints -> nodeEndpoints.stream().findFirst());
    }
    return getMaster();
  }

  public GroupResourceInformation getClusterResourceInformation() {
    return clusterInfo;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public BufferAllocator getQueryPlanningAllocator() {
    return queryPlanningAllocator;
  }

  public PhysicalPlanReader getPlanReader() {
    return reader;
  }

  public ClusterCoordinator getClusterCoordinator() {
    return coord;
  }

  public CodeCompiler getCompiler() {
    return compiler;
  }

  public LogicalPlanPersistence getLpPersistence() {
    return lpPersistence;
  }

  public ScanResult getClasspathScan() {
    return classpathScan;
  }

  public NamespaceService getNamespaceService(String userName) {
    // TODO (DX-10053): Add the below check when the ticket is resolved
    // checkIfCoordinator();
    return namespaceServiceFactory.get(userName);
  }

  public DatasetListingService getDatasetListing() {
    return datasetListing;
  }

  public CatalogService getCatalogService() {
    return catalogService.get();
  }

  public ConduitProvider getConduitProvider() {
    return conduitProvider;
  }

  public Provider<InformationSchemaServiceBlockingStub> getInformationSchemaServiceBlockingStubProvider() {
    return informationSchemaStub;
  }

  public InformationSchemaServiceBlockingStub getInformationSchemaServiceBlockingStub() {
    return informationSchemaStub.get();
  }

  public SpillService getSpillService() {
    return spillService.get();
  }

  public Provider<SpillService> getSpillServiceProvider() {
    return spillService;
  }

  public Provider<ConnectionReader> getConnectionReaderProvider() {
    return connectionReaderProvider;
  }

  public LegacyKVStoreProvider getKVStoreProvider() {
    return kvStoreProvider;
  }

  public Provider<MaterializationDescriptorProvider> getMaterializationProvider() {
    return materializationProvider;
  }

  public Provider<QueryObserverFactory> getQueryObserverFactory() {
    return queryObserverFactory;
  }

  public UserService getUserService() {
    Preconditions.checkNotNull(userService, "UserService instance is not set yet.");
    return userService;
  }

  public boolean isUserAuthenticationEnabled() {
    return userService != null;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(fileSystemWrapper);
  }

  public Provider<WorkStats> getWorkStatsProvider() {
    return workStatsProvider;
  }

  public AccelerationManager getAccelerationManager() {
    return accelerationManager.get();
  }

  public AccelerationListManager getAccelerationListManager() {
    return accelerationListManager.get();
  }

  public boolean isCoordinator() {
    return roles.contains(Role.COORDINATOR);
  }

  public boolean isExecutor() {
    return roles.contains(Role.EXECUTOR);
  }

  public boolean isMaster() {
    return roles.contains(Role.MASTER);
  }

  public Collection<RulesFactory> getInjectedRulesFactories() {
    return rules;
  }

  public ViewCreator getViewCreator(String userName) {
    return viewCreatorFactory.get().get(userName);
  }

  public FileSystemWrapper getFileSystemWrapper() {
    return fileSystemWrapper;
  }

  public CredentialsService getCredentialsService() {
    return credentialsService;
  }

  public JobResultInfoProvider getJobResultInfoProvider() {
    return jobResultInfoProvider;
  }

  public OptionValidatorListing getOptionValidatorListing() {
    return optionValidatorListing;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  //TODO(DX-26296): Return JdbcSchemaFetcherFactory
  public JdbcSchemaFetcherFactoryContext getJdbcSchemaFetcherFactoryContext() {
    return jdbcSchemaFetcherFactoryContext;
  }
}

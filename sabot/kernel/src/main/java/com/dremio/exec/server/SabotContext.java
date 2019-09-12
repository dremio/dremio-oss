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
import java.util.Optional;
import java.util.Set;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.ViewCreatorFactory;
import com.dremio.exec.catalog.ViewCreatorFactory.ViewCreator;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.expr.fn.DecimalFunctionImplementationRegistry;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.sys.PersistentStoreProvider;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.work.WorkStats;
import com.dremio.security.CredentialsService;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.coordinator.ServiceSetDecorator;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.spill.SpillService;
import com.dremio.service.users.UserService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

public class SabotContext implements AutoCloseable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SabotContext.class);

  private final SabotConfig config;
  private final Set<Role> roles;
  private final BufferAllocator allocator;
  private final PhysicalPlanReader reader;
  private final ClusterCoordinator coord;
  private final NodeEndpoint endpoint;
  private final FunctionImplementationRegistry functionRegistry;
  private final DecimalFunctionImplementationRegistry decimalFunctionImplementationRegistry;
  private final SystemOptionManager systemOptions;
  private final PersistentStoreProvider provider;
  private final Provider<WorkStats> workStatsProvider;
  private final CodeCompiler compiler;
  private final ScanResult classpathScan;
  private final LogicalPlanPersistence lpPersistence;
  private volatile Provider<MaterializationDescriptorProvider> materializationProvider;
  private final NamespaceService.Factory namespaceServiceFactory;
  private final DatasetListingService datasetListing;
  private final KVStoreProvider kvStoreProvider;
  private final UserService userService;
  private final Provider<QueryObserverFactory> queryObserverFactory;
  private final Provider<AccelerationManager> accelerationManager;
  private final Provider<AccelerationListManager> accelerationListManager;
  private final Provider<CatalogService> catalogService;
  private final Provider<ViewCreatorFactory> viewCreatorFactory;
  private final DremioConfig dremioConfig;
  private final BufferAllocator queryPlanningAllocator;
  private final Provider<SpillService> spillService;
  private final Provider<ConnectionReader> connectionReaderProvider;
  private final ClusterResourceInformation clusterInfo;
  private final FileSystemWrapper fileSystemWrapper;
  private final CredentialsService credentialsService;

  public SabotContext(
      DremioConfig dremioConfig,
      NodeEndpoint endpoint,
      SabotConfig config,
      Collection<Role> roles,
      ScanResult scan,
      LogicalPlanPersistence lpPersistence,
      BufferAllocator allocator,
      ClusterCoordinator coord,
      PersistentStoreProvider provider,
      Provider<WorkStats> workStatsProvider,
      KVStoreProvider kvStoreProvider,
      NamespaceService.Factory namespaceServiceFactory,
      DatasetListingService datasetListing,
      UserService userService,
      Provider<MaterializationDescriptorProvider> materializationProvider,
      Provider<QueryObserverFactory> queryObserverFactory,
      Provider<AccelerationManager> accelerationManager,
      Provider<AccelerationListManager> accelerationListManager,
      Provider<CatalogService> catalogService,
      Provider<ViewCreatorFactory> viewCreatorFactory,
      BufferAllocator queryPlanningAllocator,
      Provider<SpillService> spillService,
      Provider<ConnectionReader> connectionReaderProvider,
      CredentialsService credentialsService
      ) {
    this.dremioConfig = dremioConfig;
    this.config = config;
    this.roles = ImmutableSet.copyOf(roles);
    this.allocator = allocator;
    this.workStatsProvider = workStatsProvider;
    this.classpathScan = scan;
    this.coord = coord;
    this.endpoint = checkNotNull(endpoint);
    this.provider = provider;
    this.lpPersistence = lpPersistence;
    this.accelerationManager = accelerationManager;
    this.accelerationListManager = accelerationListManager;
    this.connectionReaderProvider = connectionReaderProvider;

    // Escaping 'this'
    this.reader = new PhysicalPlanReader(config, classpathScan, lpPersistence, endpoint, catalogService, this);
    this.systemOptions = new SystemOptionManager(classpathScan, lpPersistence, provider);
    this.functionRegistry = new FunctionImplementationRegistry(config, classpathScan, systemOptions);
    this.decimalFunctionImplementationRegistry = new DecimalFunctionImplementationRegistry(config, classpathScan, systemOptions);
    this.compiler = new CodeCompiler(config, systemOptions);

    this.kvStoreProvider = kvStoreProvider;
    this.namespaceServiceFactory = namespaceServiceFactory;
    this.datasetListing = datasetListing;
    this.userService = userService;
    this.queryObserverFactory = queryObserverFactory;
    this.materializationProvider = materializationProvider;
    this.catalogService = catalogService;
    this.viewCreatorFactory = viewCreatorFactory;
    this.queryPlanningAllocator = queryPlanningAllocator;
    this.spillService = spillService;
    this.clusterInfo = new ClusterResourceInformation(coord);
    this.fileSystemWrapper = config.getInstance(
      FileSystemWrapper.FILE_SYSTEM_WRAPPER_CLASS,
      FileSystemWrapper.class,
      (fs, storageId, conf, operatorContext, enableAsync, isMetadataEnabled) -> fs,
      dremioConfig,
      systemOptions,
      allocator,
      new ServiceSetDecorator(coord.getServiceSet(Role.EXECUTOR)),
      endpoint);
    this.credentialsService = credentialsService;
  }

  private void checkIfCoordinator() {
    Preconditions.checkState(roles.contains(Role.COORDINATOR), "this is a coordinator notion");
  }

  // TODO: rationalize which methods are executor only or coordinator only

  protected NamespaceService.Factory getNamespaceServiceFactory() {
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
   * @return the system options manager. It is important to note that this manager only contains options at the
   * "system" level and not "session" level.
   */
  public SystemOptionManager getOptionManager() {
    return systemOptions;
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

  public ClusterResourceInformation getClusterResourceInformation() {
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

  public PersistentStoreProvider getStoreProvider() {
    return provider;
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

  public SpillService getSpillService() {
    return spillService.get();
  }

  public Provider<SpillService> getSpillServiceProvider() {
    return spillService;
  }

  public Provider<ConnectionReader> getConnectionReaderProvider() {
    return connectionReaderProvider;
  }

  public KVStoreProvider getKVStoreProvider() {
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
    AutoCloseables.close(fileSystemWrapper, systemOptions);
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

  public ViewCreator getViewCreator(String userName) {
    return viewCreatorFactory.get().get(userName);
  }

  public FileSystemWrapper getFileSystemWrapper() {
    return fileSystemWrapper;
  }

  public CredentialsService getCredentialsService() {
    return credentialsService;
  }
}

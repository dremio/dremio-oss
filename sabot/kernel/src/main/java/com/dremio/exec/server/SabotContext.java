/*
 * Copyright (C) 2017 Dremio Corporation
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
import java.util.Collections;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.store.ViewCreatorFactory;
import com.dremio.common.store.ViewCreatorFactory.ViewCreator;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.DeferredStoragePluginRegistry;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.sys.PersistentStoreProvider;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.work.RunningQueryProvider;
import com.dremio.exec.work.WorkStats;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.users.UserService;
import com.google.common.base.Preconditions;

public class SabotContext implements AutoCloseable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SabotContext.class);

  private final SabotConfig config;
  private final Collection<Role> roles;
  private final BufferAllocator allocator;
  private final PhysicalPlanReader reader;
  private final ClusterCoordinator coord;
  private final NodeEndpoint endpoint;
  private final FunctionImplementationRegistry functionRegistry;
  private final SystemOptionManager systemOptions;
  private final PersistentStoreProvider provider;
  private final Provider<WorkStats> workStatsProvider;
  private final Provider<RunningQueryProvider> runningQueriesProvider;
  private final CodeCompiler compiler;
  private final ScanResult classpathScan;
  private final LogicalPlanPersistence lpPersistence;
  private volatile Provider<MaterializationDescriptorProvider> materializationProvider;
  private final NamespaceService.Factory namespaceServiceFactory;
  private final KVStoreProvider kvStoreProvider;
  private final UserService userService;
  private final Provider<QueryObserverFactory> queryObserverFactory;
  private final Provider<AccelerationManager> accelerationManager;
  private final Provider<AccelerationListManager> accelerationListManager;
  private final Provider<CatalogService> catalogService;
  private final Provider<ViewCreatorFactory> viewCreatorFactory;

  private final ClusterResourceInformation clusterInfo;

  public SabotContext(
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
      UserService userService,
      Provider<MaterializationDescriptorProvider> materializationProvider,
      Provider<QueryObserverFactory> queryObserverFactory,
      Provider<RunningQueryProvider> runningQueriesProvider,
      Provider<AccelerationManager> accelerationManager,
      Provider<AccelerationListManager> accelerationListManager,
      Provider<CatalogService> catalogService,
      Provider<ViewCreatorFactory> viewCreatorFactory
      ) {
    this.config = config;
    this.roles = roles;
    this.allocator = allocator;
    this.workStatsProvider = workStatsProvider;
    this.classpathScan = scan;
    this.coord = coord;
    this.endpoint = checkNotNull(endpoint);
    this.provider = provider;
    this.lpPersistence = lpPersistence;
    this.accelerationManager = accelerationManager;
    this.accelerationListManager = accelerationListManager;

    // Escaping 'this'
    this.reader = new PhysicalPlanReader(config, classpathScan, lpPersistence, endpoint, new DeferredStoragePluginRegistry(catalogService), this);
    this.systemOptions = new SystemOptionManager(classpathScan, lpPersistence, provider);
    this.functionRegistry = new FunctionImplementationRegistry(config, classpathScan, systemOptions);
    this.compiler = new CodeCompiler(config, systemOptions);

    this.kvStoreProvider = kvStoreProvider;
    this.namespaceServiceFactory = namespaceServiceFactory;
    this.userService = userService;
    this.queryObserverFactory = queryObserverFactory;
    this.materializationProvider = materializationProvider;
    this.runningQueriesProvider = runningQueriesProvider;
    this.catalogService = catalogService;
    this.viewCreatorFactory = viewCreatorFactory;

    this.clusterInfo = new ClusterResourceInformation(coord);
  }

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
    return functionRegistry;
  }

  public Collection<Role> getRoles() {
    return Collections.unmodifiableCollection(roles);
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

  public Collection<NodeEndpoint> getCoordinators() {
    return coord.getServiceSet(ClusterCoordinator.Role.COORDINATOR).getAvailableEndpoints();
  }

  public Collection<NodeEndpoint> getExecutors() {
    return coord.getServiceSet(ClusterCoordinator.Role.EXECUTOR).getAvailableEndpoints();
  }

  public ClusterResourceInformation getClusterResourceInformation() {
    return clusterInfo;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public StoragePluginRegistry getStorage() {
    return new DeferredStoragePluginRegistry(catalogService);
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
    return namespaceServiceFactory.get(userName);
  }

  public CatalogService getCatalogService() {
    return catalogService.get();
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
    AutoCloseables.close(systemOptions);
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

  public Provider<RunningQueryProvider> getRunningQueryProvider() {
    return runningQueriesProvider;
  }

  public boolean isCoordinator() {
    return roles.contains(Role.COORDINATOR);
  }

  public boolean isExecutor() {
    return roles.contains(Role.EXECUTOR);
  }

  public ViewCreator getViewCreator(String userName) {
    return viewCreatorFactory.get().get(userName);
  }
}

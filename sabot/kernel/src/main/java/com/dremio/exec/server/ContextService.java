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


import java.net.InetAddress;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.VM;
import com.dremio.common.config.SabotConfig;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.ViewCreatorFactory;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.sys.PersistentStoreProvider;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.work.WorkStats;
import com.dremio.sabot.rpc.user.UserServer;
import com.dremio.security.CredentialsService;
import com.dremio.service.BindingCreator;
import com.dremio.service.Service;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.spill.SpillService;
import com.dremio.service.users.UserService;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Service Used to construct the base context used by other services.
 */
public class ContextService implements Service, Provider<SabotContext> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ContextService.class);

  private final BindingCreator bindingCreator;
  private final BootStrapContext bootstrapContext;
  private final Provider<ClusterCoordinator> coord;
  private final Provider<WorkStats> workStats;
  private final Provider<PersistentStoreProvider> provider;
  private final Provider<KVStoreProvider> kvStoreProvider;
  private final Provider<FabricService> fabric;
  private final Provider<UserServer> userServer;
  private final Provider<MaterializationDescriptorProvider> materializationDescriptorProvider;
  private final Provider<QueryObserverFactory> queryObserverFactory;
  private final Provider<AccelerationManager> accelerationManager;
  private final Provider<AccelerationListManager> accelerationListManager;
  private final Provider<NamespaceService.Factory> namespaceServiceFactoryProvider;
  private final Provider<DatasetListingService> datasetListingServiceProvider;
  private final Provider<UserService> userService;
  private final Provider<CatalogService> catalogService;
  private final Provider<SpillService> spillService;
  private final Provider<ConnectionReader> connectionReaderProvider;
  private final Provider<ViewCreatorFactory> viewCreatorFactory;
  private final Set<ClusterCoordinator.Role> roles;
  private final Provider<CredentialsService> credentialsService;
  protected BufferAllocator queryPlannerAllocator;

  private SabotContext context;

  public ContextService(
    BindingCreator bindingCreator,
    BootStrapContext bootstrapContext,
    Provider<ClusterCoordinator> coord,
    Provider<PersistentStoreProvider> provider,
    Provider<WorkStats> workStats,
    Provider<KVStoreProvider> kvStoreProvider,
    Provider<FabricService> fabric,
    Provider<UserServer> userServer,
    Provider<MaterializationDescriptorProvider> materializationDescriptorProvider,
    Provider<QueryObserverFactory> queryObserverFactory,
    Provider<AccelerationManager> accelerationManager,
    Provider<AccelerationListManager> accelerationListManager,
    Provider<NamespaceService.Factory> namespaceServiceFactory,
    Provider<DatasetListingService> datasetListingServiceProvider,
    Provider<UserService> userService,
    Provider<CatalogService> catalogService,
    Provider<ViewCreatorFactory> viewCreatorFactory,
    Provider<SpillService> spillService,
    Provider<ConnectionReader> connectionReaderProvider,
    Provider<CredentialsService> credentialsService,
    boolean allRoles) {
    this(bindingCreator, bootstrapContext, coord, provider, workStats, kvStoreProvider, fabric, userServer,
      materializationDescriptorProvider, queryObserverFactory, accelerationManager,
      accelerationListManager, namespaceServiceFactory, datasetListingServiceProvider, userService, catalogService,
      viewCreatorFactory, spillService, connectionReaderProvider, credentialsService,
      allRoles ? EnumSet.allOf(ClusterCoordinator.Role.class) : Sets.newHashSet(ClusterCoordinator.Role.EXECUTOR));
  }

  public ContextService(
    BindingCreator bindingCreator,
    BootStrapContext bootstrapContext,
    Provider<ClusterCoordinator> coord,
    Provider<PersistentStoreProvider> provider,
    Provider<WorkStats> workStats,
    Provider<KVStoreProvider> kvStoreProvider,
    Provider<FabricService> fabric,
    Provider<UserServer> userServer,
    Provider<MaterializationDescriptorProvider> materializationDescriptorProvider,
    Provider<QueryObserverFactory> queryObserverFactory,
    Provider<AccelerationManager> accelerationManager,
    Provider<AccelerationListManager> accelerationListManager,
    Provider<NamespaceService.Factory> namespaceServiceFactoryProvider,
    Provider<DatasetListingService> datasetListingServiceProvider,
    Provider<UserService> userService,
    Provider<CatalogService> catalogService,
    Provider<ViewCreatorFactory> viewCreatorFactory,
    Provider<SpillService> spillService,
    Provider<ConnectionReader> connectionReaderProvider,
    Provider<CredentialsService> credentialsService,
    Set<ClusterCoordinator.Role> roles) {
    this.bindingCreator = bindingCreator;
    this.bootstrapContext = bootstrapContext;
    this.provider = provider;
    this.workStats = workStats;
    this.kvStoreProvider = kvStoreProvider;
    this.userServer = userServer;
    this.coord = coord;
    this.fabric = fabric;
    this.materializationDescriptorProvider = materializationDescriptorProvider;
    this.queryObserverFactory = queryObserverFactory;
    this.accelerationManager = accelerationManager;
    this.accelerationListManager = accelerationListManager;
    this.namespaceServiceFactoryProvider = namespaceServiceFactoryProvider;
    this.datasetListingServiceProvider = datasetListingServiceProvider;
    this.userService = userService;
    this.catalogService = catalogService;
    this.viewCreatorFactory = viewCreatorFactory;
    this.spillService = spillService;
    this.connectionReaderProvider = connectionReaderProvider;
    this.roles = Sets.immutableEnumSet(roles);
    this.credentialsService = credentialsService;
  }

  @Override
  public void start() throws Exception {
    queryPlannerAllocator = bootstrapContext.getAllocator().
      newChildAllocator("query-planning", 0, bootstrapContext.getAllocator().getLimit());

    this.context = newSabotContext();

    SystemOptionManager optionManager = context.getOptionManager();
    optionManager.init();

    bindingCreator.bind(SystemOptionManager.class, optionManager);
    bindingCreator.bindSelf(context.getEndpoint());
    bindingCreator.bind(SabotContext.class, this.context);
  }

  protected SabotContext newSabotContext() throws Exception{
    final FabricService fabric = this.fabric.get();
    int userport = -1;
    try {
      userport = userServer.get().getPort();
    } catch(RuntimeException ex){
      if(roles.contains(ClusterCoordinator.Role.COORDINATOR)){
        throw ex;
      }
    }

    final SabotConfig sConfig = bootstrapContext.getConfig();
    final String rpcBindAddressOpt = sConfig.getString(ExecConstants.REGISTRATION_ADDRESS);
    final String rpcBindAddress = (rpcBindAddressOpt.trim().isEmpty()) ? fabric.getAddress() : rpcBindAddressOpt;

    InetAddress[] iFaces = InetAddress.getAllByName(rpcBindAddress);
    logger.info("IFaces {} bound to the host: {}", Arrays.asList(iFaces).toString(), rpcBindAddress);

    final NodeEndpoint.Builder identityBuilder = NodeEndpoint.newBuilder()
      .setAddress(rpcBindAddress)
      .setUserPort(userport)
      .setFabricPort(fabric.getPort())
      .setStartTime(System.currentTimeMillis())
      .setMaxDirectMemory(VM.getMaxDirectMemory())
      .setAvailableCores(VM.availableProcessors())
      .setRoles(ClusterCoordinator.Role.toEndpointRoles(roles))
      .setNodeTag(bootstrapContext.getDremioConfig().getString(DremioConfig.NODE_TAG));

    String containerId = System.getenv("CONTAINER_ID");
    if(containerId != null){
      identityBuilder.setProvisionId(containerId);
    }

    final NodeEndpoint identity = identityBuilder.build();
    return new SabotContext(
        bootstrapContext.getDremioConfig(),
        identity,
        sConfig,
        roles,
        bootstrapContext.getClasspathScan(),
        bootstrapContext.getLpPersistance(),
        bootstrapContext.getAllocator(),
        coord.get(),
        provider.get(),
        workStats,
        kvStoreProvider.get(),
        namespaceServiceFactoryProvider.get(),
        datasetListingServiceProvider.get(),
        userService.get(),
        materializationDescriptorProvider,
        queryObserverFactory,
        accelerationManager,
        accelerationListManager,
        catalogService,
        viewCreatorFactory,
        queryPlannerAllocator,
        spillService,
        connectionReaderProvider,
        credentialsService.get()
      );
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(context, queryPlannerAllocator);
  }

  @Override
  public SabotContext get() {
    Preconditions.checkNotNull(context, "ContextService must be started before the context can be retrieved.");
    return context;
  }


}

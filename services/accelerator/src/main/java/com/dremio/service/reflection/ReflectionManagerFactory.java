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
package com.dremio.service.reflection;

import com.dremio.context.RequestContext;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.reflection.ReflectionManager.WakeUpCallback;
import com.dremio.service.reflection.ReflectionServiceImpl.DescriptorCache;
import com.dremio.service.reflection.ReflectionServiceImpl.ExpansionHelper;
import com.dremio.service.reflection.refresh.RefreshStartHandler;
import com.dremio.service.reflection.store.DependenciesStore;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationPlanStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.dremio.service.reflection.store.RefreshRequestsStore;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import java.util.concurrent.ExecutorService;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;

// A factory used by ReflectionServiceImpl for creating instances of:
// - ReflectionSettings
// - WakeupHandler
// - ReflectionManager
// - DependencyManager
public class ReflectionManagerFactory {
  static final String REFLECTION_MANAGER_FACTORY =
      "dremio.reflection.reflection-manager-factory.class";
  private final Provider<LegacyKVStoreProvider> storeProvider;
  private final Provider<SabotContext> sabotContext;
  private final Provider<JobsService> jobsService;
  private final Provider<CatalogService> catalogService;
  private final Provider<NamespaceService> namespaceService;
  private final ExecutorService executorService;
  private final ReflectionGoalsStore userStore;
  private final ReflectionEntriesStore internalStore;
  private final ExternalReflectionStore externalReflectionStore;
  private final MaterializationStore materializationStore;
  private final MaterializationPlanStore materializationPlanStore;
  private final WakeUpCallback wakeUpCallback;
  private final Function<Catalog, ExpansionHelper> expansionHelper;
  private final DatasetEventHub datasetEventHub;
  private final RefreshRequestsStore requestsStore;
  private final DependenciesStore dependenciesStore;
  private final BufferAllocator allocator;
  private final Supplier<DescriptorCache> descriptorCache;

  ReflectionManagerFactory(ReflectionManagerFactory that) {
    this.sabotContext = that.sabotContext;
    this.storeProvider = that.storeProvider;
    this.jobsService = that.jobsService;
    this.catalogService = that.catalogService;
    this.namespaceService = that.namespaceService;
    this.executorService = that.executorService;
    this.userStore = that.userStore;
    this.internalStore = that.internalStore;
    this.externalReflectionStore = that.externalReflectionStore;
    this.materializationStore = that.materializationStore;
    this.materializationPlanStore = that.materializationPlanStore;
    this.wakeUpCallback = that.wakeUpCallback;
    this.expansionHelper = that.expansionHelper;
    this.datasetEventHub = that.datasetEventHub;
    this.requestsStore = that.requestsStore;
    this.dependenciesStore = that.dependenciesStore;
    this.allocator = that.allocator;
    this.descriptorCache = that.descriptorCache;
  }

  ReflectionManagerFactory(
      Provider<SabotContext> sabotContext,
      Provider<LegacyKVStoreProvider> storeProvider,
      Provider<JobsService> jobsService,
      Provider<CatalogService> catalogService,
      Provider<NamespaceService> namespaceService,
      ExecutorService executorService,
      ReflectionGoalsStore userStore,
      ReflectionEntriesStore internalStore,
      ExternalReflectionStore externalReflectionStore,
      MaterializationStore materializationStore,
      MaterializationPlanStore materializationPlanStore,
      WakeUpCallback wakeUpCallback,
      Function<Catalog, ExpansionHelper> expansionHelper,
      DatasetEventHub datasetEventHub,
      RefreshRequestsStore requestsStore,
      DependenciesStore dependenciesStore,
      BufferAllocator allocator,
      Supplier<DescriptorCache> descriptorCache) {
    this.sabotContext = sabotContext;
    this.storeProvider = storeProvider;
    this.jobsService = jobsService;
    this.catalogService = catalogService;
    this.namespaceService = namespaceService;
    this.executorService = executorService;
    this.userStore = userStore;
    this.internalStore = internalStore;
    this.externalReflectionStore = externalReflectionStore;
    this.materializationStore = materializationStore;
    this.materializationPlanStore = materializationPlanStore;
    this.wakeUpCallback = wakeUpCallback;
    this.expansionHelper = expansionHelper;
    this.datasetEventHub = datasetEventHub;
    this.requestsStore = requestsStore;
    this.dependenciesStore = dependenciesStore;
    this.allocator = allocator;
    this.descriptorCache = descriptorCache;
  }

  ReflectionSettings newReflectionSettings() {
    return new ReflectionSettingsImpl(
        namespaceService, catalogService, storeProvider, this::getOptionManager);
  }

  DependencyManager newDependencyManager(Provider<RequestContext> requestContextProvider) {
    return new DependencyManager(
        materializationStore,
        internalStore,
        getOptionManager(),
        new DependencyGraph(dependenciesStore));
  }

  ReflectionManager newReflectionManager(
      ReflectionSettings reflectionSettings, Provider<RequestContext> requestContextProvider) {
    final DependencyManager dependencyManager = newDependencyManager(requestContextProvider);
    dependencyManager.start();

    return new ReflectionManager(
        sabotContext.get(),
        jobsService.get(),
        catalogService.get(),
        namespaceService.get(),
        getOptionManager(),
        userStore,
        internalStore,
        externalReflectionStore,
        materializationStore,
        materializationPlanStore,
        dependencyManager,
        descriptorCache.get(),
        wakeUpCallback,
        expansionHelper,
        allocator,
        ReflectionGoalChecker.Instance,
        new RefreshStartHandler(
            catalogService.get(), jobsService.get(), materializationStore, wakeUpCallback),
        new DependencyResolutionContextFactory(
            reflectionSettings, requestsStore, getOptionManager()),
        datasetEventHub);
  }

  ReflectionManagerWakeupHandler newWakeupHandler(
      ExecutorService executor,
      ReflectionManager reflectionManager,
      Provider<RequestContext> requestContextProvider) {
    return new ReflectionManagerWakeupHandler(executor, reflectionManager, requestContextProvider);
  }

  OptionManager getOptionManager() {
    return sabotContext.get().getOptionManager();
  }

  Provider<CatalogService> getCatalogService() {
    return catalogService;
  }

  ReflectionEntriesStore getInternalStore() {
    return internalStore;
  }

  MaterializationStore getMaterializationStore() {
    return materializationStore;
  }

  DependenciesStore getDependenciesStore() {
    return dependenciesStore;
  }
}

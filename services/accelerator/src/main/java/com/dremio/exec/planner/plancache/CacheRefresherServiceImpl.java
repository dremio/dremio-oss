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
package com.dremio.exec.planner.plancache;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.context.RequestContext;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.service.Service;
import com.dremio.service.reflection.DatasetEventHub;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.dremio.service.scheduler.SchedulerService;
import java.util.function.Supplier;
import javax.inject.Provider;

public class CacheRefresherServiceImpl implements Service, CacheRefresherService {

  private final Provider<SabotContext> sabotContextProvider;
  private final DatasetEventHub datasetEventHub;
  private final ReflectionServiceImpl reflectionService;
  private final Provider<SchedulerService> schedulerService;
  private final Provider<RequestContext> requestContextProvider;
  private final ReflectionGoalsStore goalsStore;
  private final ReflectionEntriesStore entriesStore;
  private final Supplier<PlanCacheInvalidationHelper> planCacheInvalidationHelper;

  private CacheRefresher cacheRefresher;

  public CacheRefresherServiceImpl(
      final Provider<SabotContext> sabotContextProvider,
      Provider<ForemenWorkManager> foremenWorkManagerProvider,
      Provider<SchedulerService> schedulerService,
      Provider<RequestContext> requestContextProvider,
      ReflectionServiceImpl reflectionService,
      DatasetEventHub datasetEventHub,
      ReflectionGoalsStore goalsStore,
      ReflectionEntriesStore entriesStore) {
    this.sabotContextProvider = checkNotNull(sabotContextProvider, "sabotContextProvider");
    this.datasetEventHub = checkNotNull(datasetEventHub, "datasetEventHub");
    this.reflectionService = reflectionService;
    this.schedulerService = schedulerService;
    this.requestContextProvider = requestContextProvider;
    this.goalsStore = goalsStore;
    this.entriesStore = entriesStore;

    this.planCacheInvalidationHelper =
        () ->
            new PlanCacheInvalidationHelper(
                sabotContextProvider.get(), foremenWorkManagerProvider.get().getLegacyPlanCache());
  }

  @Override
  public void start() throws Exception {
    PlanCacheSynchronizer planCacheSynchronizer =
        new PlanCacheSynchronizer(goalsStore, entriesStore, planCacheInvalidationHelper);
    cacheRefresher =
        new CacheRefresher(
            reflectionService,
            planCacheSynchronizer,
            sabotContextProvider.get().getOptionManager(),
            schedulerService.get(),
            requestContextProvider);
    cacheRefresher.scheduleNextCacheRefresh(0);
    datasetEventHub.addDatasetRemovedHandler(
        new PlanCacheDatasetRemoveHandler(planCacheInvalidationHelper));
  }

  @Override
  public void close() throws Exception {
    cacheRefresher.close();
  }
}

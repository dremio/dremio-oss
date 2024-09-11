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

import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS;
import static java.time.Instant.ofEpochMilli;
import static org.slf4j.LoggerFactory.getLogger;

import com.dremio.context.RequestContext;
import com.dremio.options.OptionResolver;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.time.Duration;
import java.time.Instant;
import javax.inject.Provider;
import org.slf4j.Logger;

public final class CacheRefresher implements Runnable {
  public static final Logger LOGGER = getLogger(CacheRefresher.class);

  private final ReflectionServiceImpl reflectionService;
  private final PlanCacheSynchronizer planCacheSynchronizer;
  private final OptionResolver optionResolver;
  private final SchedulerService schedulerService;
  private final Provider<RequestContext> requestContextProvider;
  private boolean isClosed = false;

  public CacheRefresher(
      ReflectionServiceImpl reflectionService,
      PlanCacheSynchronizer planCacheSynchronizer,
      OptionResolver optionResolver,
      SchedulerService schedulerService,
      Provider<RequestContext> requestContextProvider) {
    this.reflectionService = reflectionService;
    this.planCacheSynchronizer = planCacheSynchronizer;
    this.optionResolver = optionResolver;
    this.schedulerService = schedulerService;
    this.requestContextProvider = requestContextProvider;
  }

  @Override
  public void run() {
    try {
      if (requestContextProvider != null) {
        requestContextProvider.get().run(this::refreshMaterializationAndPlanCaches);
      } else {
        refreshMaterializationAndPlanCaches();
      }
    } finally {
      long cacheUpdateDelay;
      try {
        cacheUpdateDelay = optionResolver.getOption(MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS);
      } catch (Exception e) {
        LOGGER.warn("Failed to retrieve materialization cache refresh delay", e);
        cacheUpdateDelay = MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS.getDefault().getNumVal();
      }
      scheduleNextCacheRefresh(cacheUpdateDelay);
    }
  }

  public void scheduleNextCacheRefresh(long cacheUpdateDelay) {
    if (isClosed) {
      return;
    }
    schedulerService.schedule(
        Schedule.SingleShotBuilder.at(ofEpochMilli(System.currentTimeMillis() + cacheUpdateDelay))
            .build(),
        this);
  }

  @WithSpan
  private void refreshMaterializationAndPlanCaches() {
    final Instant refreshCacheStart = Instant.now();
    reflectionService.refreshCache();
    long refreshMaterializationCacheDuration =
        Duration.between(refreshCacheStart, Instant.now()).toMillis();
    final Instant planCacheSyncStart = Instant.now();
    this.planCacheSynchronizer.sync();
    long planCacheSyncDuration = Duration.between(planCacheSyncStart, Instant.now()).toMillis();
    LOGGER.info(
        "Materialization cache sync took {} ms.  Plan cache sync took {} ms.",
        refreshMaterializationCacheDuration,
        planCacheSyncDuration);
  }

  public void close() {
    isClosed = true;
  }
}

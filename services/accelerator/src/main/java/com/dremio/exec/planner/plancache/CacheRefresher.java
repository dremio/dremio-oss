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

import static org.slf4j.LoggerFactory.getLogger;

import com.dremio.service.reflection.ReflectionServiceImpl;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;

public final class CacheRefresher implements Runnable {
  public static final Logger LOGGER = getLogger(CacheRefresher.class);

  private final ReflectionServiceImpl reflectionService;
  private final PlanCacheSynchronizer planCacheSynchronizer;

  public CacheRefresher(
      ReflectionServiceImpl reflectionService, PlanCacheSynchronizer planCacheSynchronizer) {
    this.reflectionService = reflectionService;
    this.planCacheSynchronizer = planCacheSynchronizer;
  }

  @Override
  public void run() {
    refreshMaterializationAndPlanCaches();
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
}

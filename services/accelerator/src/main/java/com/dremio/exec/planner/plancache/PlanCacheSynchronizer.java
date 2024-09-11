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

import com.dremio.exec.planner.common.PlannerMetrics;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for invalidating entries in the plan cache based on the changes that
 * have been made to reflections. It looks both at reflection goals and reflections entries to find
 * those that have been changed since the last time #sync has been executed. NOTE: we need to
 * revisit how our overall cache layer works because it relies on enumerating all the db entries In
 * KV databases, that usually means scan operations, which is not a good idea on larger sets of
 * data.
 */
public class PlanCacheSynchronizer {
  private static final Logger logger = LoggerFactory.getLogger(PlanCacheSynchronizer.class);

  private final ReflectionGoalsStore goalsStore;
  private final ReflectionEntriesStore entriesStore;
  private final Supplier<PlanCacheInvalidationHelper> planCacheInvalidationHelper;

  private long lastUpdatedOn;
  private final Meter.MeterProvider<Timer> syncHistogram;

  public PlanCacheSynchronizer(
      ReflectionGoalsStore goalsStore,
      ReflectionEntriesStore entriesStore,
      Supplier<PlanCacheInvalidationHelper> planCacheInvalidationHelper) {
    this.goalsStore = goalsStore;
    this.entriesStore = entriesStore;
    this.planCacheInvalidationHelper = planCacheInvalidationHelper;
    this.lastUpdatedOn = System.currentTimeMillis();
    syncHistogram =
        Timer.builder(
                PlannerMetrics.createName(PlannerMetrics.PREFIX, PlannerMetrics.PLAN_CACHE_SYNC))
            .description("Histogram of plan cache sync times")
            .publishPercentileHistogram()
            .withRegistry(io.micrometer.core.instrument.Metrics.globalRegistry);
  }

  /**
   * Sync logic which queries all updated reflections and invalidates datasets in the plan cache
   * based on that. NOTE: the function never throws. In case of failure, it will retry from the last
   * successful run timestamp
   */
  @WithSpan
  public void sync() {
    logger.debug("Reconciling plan cache with any changes");
    long now = System.currentTimeMillis();

    boolean success = false;
    try (PlanCacheInvalidationHelper helper = planCacheInvalidationHelper.get()) {
      if (!helper.isPlanCacheEnabled()) {
        logger.debug(
            "Plan cache is disabled. Invalidating the plan cache and removing {} plan cache entries",
            helper.getCacheEntryCount());
        helper.invalidatePlanCache();
        return;
      }

      Set<String> datasetsFromEntries = getAffectedDatasetsFromEntries(lastUpdatedOn);
      Set<String> datasetsFromGoals = getAffectedDatasetsFromGoals(lastUpdatedOn);

      Set<String> datasetIds = Sets.union(datasetsFromEntries, datasetsFromGoals);

      logger.info(
          "Cleaning up {} datasets based on entries, {} datasets based on goals. Total (union) processed datasets: {}",
          datasetsFromEntries.size(),
          datasetsFromGoals.size(),
          datasetIds.size());
      Span.current()
          .setAttribute("dremio.plan_cache.datasetsFromEntries", datasetsFromEntries.size());
      Span.current().setAttribute("dremio.plan_cache.datasetsFromGoals", datasetsFromGoals.size());
      Span.current().setAttribute("dremio.plan_cache.datasetsTotal", datasetIds.size());

      long before = helper.getCacheEntryCount();
      for (String datasetId : datasetIds) {
        try {
          helper.invalidateReflectionAssociatedPlanCache(datasetId);
        } catch (Exception ex) {
          logger.warn(
              String.format("Error while invalidating plan cache for dataset %s", datasetId), ex);
        }
      }
      long after = helper.getCacheEntryCount();
      logger.info(
          "Completed plan cache sync.  Cache entries before {}.  Cache entries after {}.",
          before,
          after);
      Span.current().setAttribute("dremio.plan_cache.entryCountBeforeSync", before);
      Span.current().setAttribute("dremio.plan_cache.entryCountAfterSync", after);
      lastUpdatedOn = now;
      success = true;
    } catch (Exception ex) {
      logger.warn("Error when trying to reconcile plan cache entries. Will retry next time", ex);
    } finally {
      PlannerMetrics.withOutcome(syncHistogram, success)
          .record(System.currentTimeMillis() - now, TimeUnit.MILLISECONDS);
    }
  }

  private Set<String> getAffectedDatasetsFromEntries(long onOrAfter) {
    return StreamSupport.stream(entriesStore.find().spliterator(), false)
        .filter(e -> e.getModifiedAt() >= onOrAfter)
        .map(ReflectionEntry::getDatasetId)
        .collect(Collectors.toSet());
  }

  private Set<String> getAffectedDatasetsFromGoals(long onOrAfter) {
    return StreamSupport.stream(
            goalsStore.getModifiedOrCreatedSince(onOrAfter).spliterator(), false)
        .map(ReflectionGoal::getDatasetId)
        .collect(Collectors.toSet());
  }
}

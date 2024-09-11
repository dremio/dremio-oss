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

import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.planner.common.PlannerMetrics;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LegacyPlanCache implements PlanCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(LegacyPlanCache.class);

  private final Cache<String, CachedPlan> cachePlans;
  private final Multimap<String, String> datasetMap;

  public LegacyPlanCache(Cache<String, CachedPlan> cachePlans, Multimap<String, String> map) {
    this.cachePlans = cachePlans;
    this.datasetMap = map;

    Gauge.builder(
            PlannerMetrics.createName(PlannerMetrics.PREFIX, PlannerMetrics.PLAN_CACHE_ENTRIES),
            cachePlans::size)
        .description("Number of plan cache entries")
        .register(Metrics.globalRegistry);
  }

  @Override
  public void putCachedPlan(SqlHandlerConfig config, PlanCacheKey cachedKey, Prel prel) {
    final PlannerCatalog catalog =
        Preconditions.checkNotNull(config.getConverter().getPlannerCatalog());

    boolean addedCacheToDatasetMap = false;
    Iterable<DremioTable> datasets = catalog.getAllRequestedTables();
    for (DremioTable dataset : datasets) {
      DatasetConfig datasetConfig;
      try {
        datasetConfig = dataset.getDatasetConfig();
      } catch (IllegalStateException ignore) {
        LOGGER.debug(
            String.format(
                "Dataset %s is ignored (no dataset config available).", dataset.getPath()),
            ignore);
        continue;
      }
      if (datasetConfig == null) {
        LOGGER.debug(
            String.format(
                "Dataset %s is ignored (no dataset config available).", dataset.getPath()));
        continue;
      }
      if (datasetConfig.getPhysicalDataset() == null) {
        continue;
      }
      synchronized (datasetMap) {
        datasetMap.put(datasetConfig.getId().getId(), cachedKey.getHash());
      }
      addedCacheToDatasetMap = true;
    }
    if (addedCacheToDatasetMap) {
      // Wiping out RelMetadataCache. It will be holding the RelNodes from the prior
      // planning phases.
      prel.getCluster().invalidateMetadataQuery();

      CachedPlan newCachedPlan = CachedPlan.createCachedPlan(prel, prel.getEstimatedSize());
      config.getObserver().addAccelerationProfileToCachedPlan(newCachedPlan);
      cachePlans.put(cachedKey.getHash(), newCachedPlan);
      config.getConverter().dispose();
      LOGGER.debug("Physical plan cache created with cacheKey {}", cachedKey);
    } else {
      LOGGER.debug("Physical plan not cached: Query contains no physical datasets.");
    }
  }

  public Cache<String, CachedPlan> getCachePlans() {
    return cachePlans;
  }

  @Override
  public @Nullable CachedPlan getIfPresentAndValid(
      SqlHandlerConfig sqlHandlerConfig, PlanCacheKey planCacheKey) {
    CatalogService catalogService = sqlHandlerConfig.getContext().getCatalogService();
    final PlannerCatalog catalog =
        Preconditions.checkNotNull(sqlHandlerConfig.getConverter().getPlannerCatalog());

    if (cachePlans == null) {
      return null;
    }
    final CachedPlan cachedPlan = cachePlans.getIfPresent(planCacheKey.getHash());
    if (cachedPlan != null) {
      Iterable<DremioTable> datasets = catalog.getAllRequestedTables();
      for (DremioTable dataset : datasets) {
        try {
          DatasetConfig datasetConfig = dataset.getDatasetConfig();
          if (datasetConfig != null) {
            // DatasetConfig modified
            if (datasetConfig.getLastModified() != null
                && datasetConfig.getLastModified() > cachedPlan.getCreationTime()) {
              // for this case, we can only invalidate this cache entry, other cache entries may
              // still be valid
              cachePlans.invalidate(planCacheKey.getHash());
              LOGGER.debug(
                  "Physical plan cache hit with cacheKey {}: Cache invalidated due to updated dataset {}. datasetTime={} planTime={}",
                  planCacheKey,
                  datasetConfig.getFullPathList(),
                  datasetConfig.getLastModified(),
                  cachedPlan.getCreationTime());
              return null;
            } else {
              // Check if source config is modified and invalidate the cache.
              try {
                ManagedStoragePlugin plugin =
                    catalogService.getManagedSource(dataset.getPath().getRoot());
                if (plugin != null) {
                  SourceConfig sourceConfig = plugin.getConfig();
                  if ((sourceConfig != null)) {
                    long lastModifiedAt =
                        sourceConfig.getLastModifiedAt() != null
                            ? sourceConfig.getLastModifiedAt()
                            : sourceConfig.getCtime();
                    if (lastModifiedAt > cachedPlan.getCreationTime()) {
                      cachePlans.invalidate(planCacheKey.getHash());
                      LOGGER.debug(
                          "Physical plan cache hit with cacheKey {}: Cache invalidated due to updated source {}. sourceTime={} planTime={}",
                          planCacheKey,
                          sourceConfig.getName(),
                          sourceConfig.getLastModifiedAt(),
                          cachedPlan.getCreationTime());
                      return null;
                    }
                  }
                }
              } catch (RuntimeException e) {
                LOGGER.error(
                    "Exception while checking for Source config modification for dataset {}",
                    dataset.getPath().getRoot(),
                    e);
              }
            }
          }
        } catch (IllegalStateException ignore) {
          LOGGER.debug(
              String.format(
                  "Dataset %s is ignored (no dataset config available).", dataset.getPath()),
              ignore);
        }
      }
      LOGGER.debug("Physical plan cache hit with cacheKey {}", planCacheKey);
      return cachedPlan;
    }

    LOGGER.debug("Physical plan cache miss with cacheKey {}", planCacheKey);
    return null;
  }

  @Override
  public void invalidateCacheOnDataset(String datasetId) {
    List<String> affectedCaches = datasetMap.get(datasetId).stream().collect(Collectors.toList());
    for (String cacheId : affectedCaches) {
      cachePlans.invalidate(cacheId);
    }
    if (!affectedCaches.isEmpty()) {
      LOGGER.debug(
          "Physical plan cache invalidated by datasetId {} for cacheKeys {}",
          datasetId,
          affectedCaches);
    }
  }

  @Override
  public void invalidateAll() {
    cachePlans.invalidateAll();
  }

  public void clearDatasetMapOnCacheGC(String cacheId) {
    synchronized (datasetMap) {
      datasetMap.entries().removeIf(datasetMapEntry -> datasetMapEntry.getValue().equals(cacheId));
    }
  }
}

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
package com.dremio.exec.planner;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.Multimap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class PlanCache {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanCache.class);

  private final Cache<String, CachedPlan> cachePlans;
  private static Multimap<String, String> datasetMap;

  public PlanCache(Cache<String, CachedPlan> cachePlans, Multimap<String, String> map) {
    this.cachePlans = cachePlans;
    this.datasetMap = map;
  }

  public Multimap<String, String> getDatasetMap() {
    return datasetMap;
  }

  public Cache<String, CachedPlan> getCachePlans() {
    return cachePlans;
  }

  public void createNewCachedPlan(PlannerCatalog catalog, String cachedKey, String sql,
                                  Prel prel, String textPlan, SqlHandlerConfig config) {
    Preconditions.checkNotNull(catalog);
    boolean addedCacheToDatasetMap = false;
    Iterable<DremioTable> datasets = catalog.getAllRequestedTables();
    for (DremioTable dataset : datasets) {
      DatasetConfig datasetConfig;
      try {
        datasetConfig = dataset.getDatasetConfig();
      } catch (IllegalStateException ignore) {
        logger.debug(String.format("Dataset %s is ignored (no dataset config available).", dataset.getPath()), ignore);
        continue;
      }
      if (datasetConfig == null) {
        logger.debug(String.format("Dataset %s is ignored (no dataset config available).", dataset.getPath()));
        continue;
      }
      if (datasetConfig.getPhysicalDataset() == null) {
        continue;
      }
      synchronized (datasetMap) {
        datasetMap.put(datasetConfig.getId().getId(), cachedKey);
      }
      addedCacheToDatasetMap = true;
    }
    if (addedCacheToDatasetMap) {
      CachedPlan newCachedPlan = CachedPlan.createCachedPlan(sql, prel, textPlan, prel.getEstimatedSize());
      config.getObserver().setCachedAccelDetails(newCachedPlan);
      cachePlans.put(cachedKey, newCachedPlan);
      config.getConverter().dispose();
      logger.debug("Physical plan cache created with cacheKey {}", cachedKey);
    } else {
      logger.debug("Physical plan not cached: Query contains no physical datasets.");
    }
  }

  public static boolean supportPlanCache(PlanCache planCache, SqlHandlerConfig config, SqlNode sqlNode, PlannerCatalog catalog) {
    if (planCache == null || !config.getContext().getPlannerSettings().isPlanCacheEnabled()) {
      logger.debug("Physical plan not cached: Plan cache not enabled.");
      return false;
    }

    for (DremioTable table : catalog.getAllRequestedTables()) {
      if (CatalogUtil.requestedPluginSupportsVersionedTables(table.getPath(), config.getContext().getCatalog())) {
        // Versioned tables don't have a mtime - they have snapshot ids.  Since we don't have a way to invalidate
        // cache entries containing versioned datasets, don't allow these plans to enter the cache.
        logger.debug("Physical plan not cached: Query contains a versioned table.");
        return false;
      }
    }
    if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(sqlNode.toString(), "external_query")) {
      logger.debug("Physical plan not cached: Query contains an external_query.");
      return false;
    }
    if (!config.getConverter().getFunctionContext().getContextInformation().isPlanCacheable()) {
      logger.debug("Physical plan not cached: Query contains dynamic or non-deterministic function.");
      return false;
    }
    return true;
  }

  public static String generateCacheKey(SqlNode sqlNode, RelNode relNode, QueryContext context) {
    Hasher hasher = Hashing.sha256().newHasher();

    hasher
      .putString(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql(), UTF_8)
      .putString(RelOptUtil.toString(relNode), UTF_8)
      .putString(context.getWorkloadType().name(), UTF_8)
      .putString(context.getContextInformation().getCurrentDefaultSchema(), UTF_8);

    if (context.getPlannerSettings().isPlanCacheEnableSecuredUserBasedCaching()){
      hasher.putString(context.getQueryUserName(), UTF_8);
    }

    context.getOptions().getNonDefaultOptions()
        .stream()
        // A sanity filter in case an option with default value is put into non-default options
        .filter(optionValue -> !context.getOptions().getDefaultOptions().contains(optionValue))
        .sorted()
        .forEach((v) -> {
          switch(v.getKind()) {
            case BOOLEAN:
              hasher.putBoolean(v.getBoolVal());
              break;
            case DOUBLE:
              hasher.putDouble(v.getFloatVal());
              break;
            case LONG:
              hasher.putLong(v.getNumVal());
              break;
            case STRING:
              hasher.putString(v.getStringVal(), UTF_8);
              break;
            default:
              throw new AssertionError("Unsupported OptionValue kind: " + v.getKind());
          }
        });

    Optional.ofNullable(context.getGroupResourceInformation())
      .ifPresent(v -> {
        hasher.putInt(v.getExecutorNodeCount());
        hasher.putLong(v.getAverageExecutorCores(context.getOptions()));
      });

    return hasher.hash().toString();
  }

  public CachedPlan getIfPresentAndValid(PlannerCatalog catalog, CatalogService catalogService, String cacheId) {
    if (cachePlans == null) {
      return null;
    }
    final CachedPlan cachedPlan = cachePlans.getIfPresent(cacheId);
    if (cachedPlan != null) {
      Iterable<DremioTable> datasets = catalog.getAllRequestedTables();
      for (DremioTable dataset : datasets) {
        try {
          DatasetConfig config = dataset.getDatasetConfig();
          if (config != null) {
            // DatasetConfig modified
            if (config.getLastModified() != null && config.getLastModified() > cachedPlan.getCreationTime()) {
              // for this case, we can only invalidate this cache entry, other cache entries may still be valid
              cachePlans.invalidate(cacheId);
              logger.debug("Physical plan cache hit with cacheKey {}: Cache invalidated due to updated dataset {}. datasetTime={} planTime={}",
                cacheId, config.getFullPathList(), config.getLastModified(), cachedPlan.getCreationTime());
              return null;
            } else {
              // Check if source config is modified and invalidate the cache.
              try {
                ManagedStoragePlugin plugin = catalogService.getManagedSource(dataset.getPath().getRoot());
                if (plugin != null) {
                  SourceConfig sourceConfig = plugin.getConfig();
                  if ((sourceConfig != null) && sourceConfig.getCtime() > cachedPlan.getCreationTime()) {
                    cachePlans.invalidate(cacheId);
                    logger.debug("Physical plan cache hit with cacheKey {}: Cache invalidated due to updated source {}. sourceTime={} planTime={}",
                      cacheId, sourceConfig.getName(), sourceConfig.getCtime(), cachedPlan.getCreationTime());
                    return null;
                  }
                }
              } catch (RuntimeException e) {
                logger.error("Exception while checking for Source config modification for dataset {}", dataset.getPath().getRoot(), e);
              }
            }
          }
        } catch (IllegalStateException ignore) {
          logger.debug(String.format("Dataset %s is ignored (no dataset config available).", dataset.getPath()), ignore);
        }
      }
      logger.debug("Physical plan cache hit with cacheKey {}", cacheId);
      return cachedPlan;
    }

    logger.debug("Physical plan cache miss with cacheKey {}", cacheId);
    return null;
  }

  public void invalidateCacheOnDataset(String datasetId) {
    List<String> affectedCaches = datasetMap.get(datasetId).stream().collect(Collectors.toList());
    for(String cacheId: affectedCaches) {
      cachePlans.invalidate(cacheId);
    }
    if (!affectedCaches.isEmpty()) {
      logger.debug("Physical plan cache invalidated by datasetId {} for cacheKeys {}", datasetId, affectedCaches);
    }
  }

  public static void clearDatasetMapOnCacheGC(String cacheId) {
    synchronized (datasetMap) {
      datasetMap.entries().removeIf(datasetMapEntry -> datasetMapEntry.getValue().equals(cacheId));
    }
  }
}

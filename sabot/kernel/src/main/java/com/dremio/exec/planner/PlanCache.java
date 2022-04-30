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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.QueryContext;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.Multimap;

public class PlanCache {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanCache.class);

  private final Cache<Long, CachedPlan> cachePlans;
  private static Multimap<String, Long> datasetMap;

  public PlanCache(Cache<Long, CachedPlan> cachePlans, Multimap<String, Long> map) {
    this.cachePlans = cachePlans;
    this.datasetMap = map;
  }

  public Multimap<String, Long> getDatasetMap() {
    return datasetMap;
  }

  public Cache<Long, CachedPlan> getCachePlans() {
    return cachePlans;
  }

  private void addCacheToDatasetMap(String datasetId, Long cacheId) {
    synchronized (datasetMap) {
      datasetMap.put(datasetId, cacheId);
    }
  }

  public boolean addCacheToDatasetMap(Catalog catalog, long cachedKey) {
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
        logger.debug(String.format("Dataset %s is ignored (no physical dataset available).", dataset.getPath()));
        continue;
      }
      addCacheToDatasetMap(datasetConfig.getId().getId(), cachedKey);
      addedCacheToDatasetMap = true;
    }
    return addedCacheToDatasetMap;
  }

  public static long generateCacheKey(String sql, QueryContext context) {
    long result = sql.concat(context.getWorkloadType().name())
      .concat(context.getContextInformation().getCurrentDefaultSchema())
      .hashCode();
    result = 31 * result + generateQueryContextOptionsHash(context);
    return result;
  }

  public static int generateQueryContextOptionsHash(QueryContext context) {
    int result = Objects.hash(context.getOptions().getNonDefaultOptions()
      .stream()
      // A sanity filter in case an option with default value is put into non-default options
      .filter(optionValue -> !context.getOptions().getDefaultOptions().contains(optionValue))
      .sorted()
      .collect(Collectors.toList()));

    GroupResourceInformation resourceInformation = context.getGroupResourceInformation();
    if (resourceInformation != null) {
      result = 31 * result + Objects.hash(resourceInformation.getExecutorNodeCount());
      result = 31 * result + Objects.hash(resourceInformation.getAverageExecutorCores(context.getOptions()));
    }

    return result;
  }

  public CachedPlan getIfPresentAndValid(Catalog catalog, long cacheId) {
    if (cachePlans == null) {
      return null;
    }
    CachedPlan cachedPlan = cachePlans.getIfPresent(cacheId);
    if (cachedPlan != null) {
      Iterable<DremioTable> datasets = catalog.getAllRequestedTables();
      for (DremioTable dataset : datasets) {
        try {
          DatasetConfig config = dataset.getDatasetConfig();
          if (config != null && (config.getLastModified() > cachedPlan.getCreationTime())) {
            // for this case, we can only invalidate this cach entry, other cache entries may still be valid
            cachePlans.invalidate(cacheId);
            return null;
          }
        } catch (IllegalStateException ignore) {
          logger.debug(String.format("Dataset %s is ignored (no dataset config available).", dataset.getPath()), ignore);
        }
      }
    }
    return cachePlans.getIfPresent(cacheId);
  }

  public void invalidateCacheOnDataset(String datasetId) {
    List<Long> affectedCaches = datasetMap.get(datasetId).stream().collect(Collectors.toList());
    for(Long cacheId: affectedCaches) {
      cachePlans.invalidate(cacheId);
    }
  }

  public static void clearDatasetMapOnCacheGC(Long cacheId) {
    synchronized (datasetMap) {
      datasetMap.entries().removeIf(datasetMapEntry -> datasetMapEntry.getValue().equals(cacheId));
    }
  }
}

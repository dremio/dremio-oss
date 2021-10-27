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
import java.util.stream.Collectors;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.cache.Cache;
import com.google.common.collect.Multimap;

public class PlanCache {

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

  public void addCacheToDatasetMap(String datasetId, Long cacheId) {
    synchronized (datasetMap) {
      datasetMap.put(datasetId, cacheId);
    }
  }

  public static long generateCacheKey(String sql, String workLoadType, String defaultSchema) {
    return sql.concat(workLoadType).concat(defaultSchema).hashCode();
  }

  public CachedPlan getIfPresentAndValid(Catalog catalog, long cacheId) {
    if (cachePlans == null) {
      return null;
    }
    CachedPlan cachedPlan = cachePlans.getIfPresent(cacheId);
    if (cachedPlan != null) {
      Iterable<DremioTable> datasets = catalog.getAllRequestedTables();
      for (DremioTable dataset : datasets) {
        if (dataset instanceof NamespaceTable || dataset instanceof ViewTable) {
          DatasetConfig config = dataset.getDatasetConfig();
          if (config.getLastModified() > cachedPlan.getCreationTime()) {
            // for this case, we can only invalidate this cach entry, other cache entries may still be valid
            cachePlans.invalidate(cacheId);
            return null;
          }
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

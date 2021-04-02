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

import java.util.Collection;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.cache.Cache;
import com.google.common.collect.Multimap;

public class PlanCache {

  private final Cache<Long, CachedPlan> cachePlans;
  private final Multimap<PhysicalDataset, Long> datasetMap;

  public PlanCache(Cache<Long, CachedPlan> cachePlans, Multimap<PhysicalDataset, Long> map) {
    this.cachePlans = cachePlans;
    this.datasetMap = map;
  }

  public Multimap<PhysicalDataset, Long> getDatasetMap() {
    return datasetMap;
  }

  public Cache<Long, CachedPlan> getCachePlans() {
    return cachePlans;
  }

  public void addCacheToDatasetMap(PhysicalDataset dataset, Long cacheId) {
    synchronized (datasetMap) {
      datasetMap.put(dataset, cacheId);
    }
  }

  public static long generateCacheKey(String sql, String workLoadType) {
    return sql.concat(workLoadType).hashCode();
  }

  public CachedPlan getIfPresentAndValid(Catalog catalog, long cachedKey) {
    if (cachePlans == null) {
      return null;
    }
    CachedPlan cachedPlan = cachePlans.getIfPresent(cachedKey);
    if (cachedPlan != null) {
      Iterable<DremioTable> datasets = catalog.getAllRequestedTables();
      for (DremioTable dataset : datasets) {
        if (dataset instanceof NamespaceTable) {
          DatasetConfig config = dataset.getDatasetConfig();
          if (config.getLastModified() > cachedPlan.getCreationTime()) {
            invalidateCacheOnDataset(config.getPhysicalDataset());
          }
        }
      }
    }
    return cachePlans.getIfPresent(cachedKey);
  }

  public void invalidateCacheOnDataset(PhysicalDataset dataset) {
    Collection<Long> affectedCaches = datasetMap.get(dataset);
    synchronized (datasetMap) {
      for (Long affectedCache : affectedCaches) {
        cachePlans.invalidate(affectedCache);
      }
    }
  }
}

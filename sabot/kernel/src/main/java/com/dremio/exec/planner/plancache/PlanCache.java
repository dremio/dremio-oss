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

import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;

public interface PlanCache {
  PlanCache EMPTY_CACHE =
      new PlanCache() {
        @Override
        public void putCachedPlan(SqlHandlerConfig config, PlanCacheKey cachedKey, Prel prel) {}

        @Override
        public CachedPlan getIfPresentAndValid(
            SqlHandlerConfig sqlHandlerConfig, PlanCacheKey planCacheKey) {
          return null;
        }

        @Override
        public void invalidateCacheOnDataset(String datasetId) {}

        @Override
        public void invalidateAll() {}
      };

  void putCachedPlan(SqlHandlerConfig config, PlanCacheKey cachedKey, Prel prel);

  CachedPlan getIfPresentAndValid(SqlHandlerConfig sqlHandlerConfig, PlanCacheKey planCacheKey);

  @Deprecated
  void invalidateCacheOnDataset(String datasetId);

  void invalidateAll();
}

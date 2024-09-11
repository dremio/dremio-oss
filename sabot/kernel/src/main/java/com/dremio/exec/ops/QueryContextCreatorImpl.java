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
package com.dremio.exec.ops;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.planner.plancache.LegacyPlanCache;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotQueryContext;
import com.dremio.partitionstats.cache.PartitionStatsCache;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Predicate;

public class QueryContextCreatorImpl implements QueryContextCreator {

  private final SabotQueryContext sabotQueryContext;

  public QueryContextCreatorImpl(SabotQueryContext sabotQueryContext) {
    this.sabotQueryContext = sabotQueryContext;
  }

  @Override
  public QueryContext createNewQueryContext(
      UserSession session,
      UserBitShared.QueryId queryId,
      UserProtos.QueryPriority priority,
      long maxAllocation,
      Predicate<DatasetConfig> datasetValidityChecker,
      LegacyPlanCache planCache,
      PartitionStatsCache partitionStatsCache) {
    return new QueryContext(
        session,
        sabotQueryContext,
        queryId,
        priority,
        maxAllocation,
        datasetValidityChecker,
        planCache,
        partitionStatsCache);
  }

  @Override
  public QueryContext createNewQueryContext(
      UserSession session,
      UserBitShared.QueryId queryId,
      UserProtos.QueryPriority priority,
      long maxAllocation,
      Predicate<DatasetConfig> datasetValidityChecker,
      LegacyPlanCache planCache,
      PartitionStatsCache partitionStatsCache,
      Catalog catalog) {
    return new QueryContext(
        session,
        sabotQueryContext,
        queryId,
        priority,
        maxAllocation,
        datasetValidityChecker,
        planCache,
        partitionStatsCache) {

      @Override
      protected Catalog createCatalog(MetadataRequestOptions options) {
        return catalog;
      }
    };
  }
}

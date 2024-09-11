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
import com.dremio.exec.planner.plancache.LegacyPlanCache;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.partitionstats.cache.PartitionStatsCache;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Predicate;

public interface QueryContextCreator {

  QueryContext createNewQueryContext(
      final UserSession session,
      final UserBitShared.QueryId queryId,
      final UserProtos.QueryPriority priority,
      final long maxAllocation,
      final Predicate<DatasetConfig> datasetValidityChecker,
      final LegacyPlanCache planCache,
      final PartitionStatsCache partitionStatsCache);

  QueryContext createNewQueryContext(
      final UserSession session,
      final UserBitShared.QueryId queryId,
      final UserProtos.QueryPriority priority,
      final long maxAllocation,
      final Predicate<DatasetConfig> datasetValidityChecker,
      final LegacyPlanCache planCache,
      final PartitionStatsCache partitionStatsCache,
      final Catalog catalog);
}

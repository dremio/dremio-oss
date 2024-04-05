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
package com.dremio.service.jobtelemetry.server.store;

import com.dremio.exec.proto.CoordExecRPC.QueryProgressMetrics;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.Service;
import com.dremio.service.jobtelemetry.QueryProgressMetricsMap;
import java.util.Optional;

/** Store used to store and retrieve query progress metrics. */
public interface MetricsStore extends Service {
  /**
   * put query progression metrics for one executor node.
   *
   * @param queryProgressMetrics metrics of one executor.
   */
  void put(
      UserBitShared.QueryId queryId, String nodeAddress, QueryProgressMetrics queryProgressMetrics);

  /**
   * get query progression metrics for all executor nodes.
   *
   * @param queryId queryId
   * @return set of QueryProgressMetrics
   */
  Optional<QueryProgressMetricsMap> get(UserBitShared.QueryId queryId);

  /**
   * Delete the metrics entry for given queryId.
   *
   * @param queryId
   */
  void delete(UserBitShared.QueryId queryId);
}

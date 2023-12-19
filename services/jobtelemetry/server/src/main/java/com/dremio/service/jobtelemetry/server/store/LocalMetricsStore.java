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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.Service;
import com.dremio.service.jobtelemetry.QueryProgressMetricsMap;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Implementation of metrics store, keeps all metrics in-memory.
 */
public class LocalMetricsStore implements MetricsStore, Service {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalMetricsStore.class);
  private Map<String, QueryProgressMetricsMap> map = new ConcurrentHashMap<>();
  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  private Cache<UserBitShared.QueryId, Boolean> deletedQueryIds = CacheBuilder.newBuilder()
    .expireAfterWrite(5, TimeUnit.MINUTES)
    .build();

  public LocalMetricsStore() {
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public synchronized void put(
    UserBitShared.QueryId queryId, String nodeEndPoint,
    CoordExecRPC.QueryProgressMetrics queryNodeProgressMetrics) {

    if (deletedQueryIds.asMap().containsKey(queryId)) {
      return;
    }

    QueryProgressMetricsMap metrics =
      QueryProgressMetricsMap.newBuilder()
        .mergeFrom(get(queryId).orElse(QueryProgressMetricsMap.getDefaultInstance()))
        .putMetricsMap(nodeEndPoint, queryNodeProgressMetrics)
        .build();
    map.put(queryIdToString(queryId), metrics);
  }

  @Override
  public Optional<QueryProgressMetricsMap> get(UserBitShared.QueryId queryId) {
    return Optional.ofNullable(map.get(queryIdToString(queryId)));
  }

  @Override
  public void delete(UserBitShared.QueryId queryId) {
    deletedQueryIds.put(queryId, Boolean.TRUE);
    map.remove(queryIdToString(queryId));
  }

  private String queryIdToString(UserBitShared.QueryId queryId) {
    return QueryIdHelper.getQueryId(queryId);
  }
}

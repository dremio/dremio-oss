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
package com.dremio.service.jobtelemetry.server;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobtelemetry.server.store.MetricsStore;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * Publishes metrics to registered subscribers periodically.
 */
public class ProgressMetricsPublisher implements AutoCloseable {
  private static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(ProgressMetricsPublisher.class);

  private final Multimap<UserBitShared.QueryId,
      Consumer<CoordExecRPC.QueryProgressMetrics>> subscribers =
    Multimaps.synchronizedSetMultimap(HashMultimap.create());
  private final MetricsStore metricsStore;
  private final CloseableSchedulerThreadPool scheduler;

  ProgressMetricsPublisher(MetricsStore metricsStore, int publishFrequencyMillis) {
    this.metricsStore = metricsStore;
    this.scheduler = new CloseableSchedulerThreadPool("metrics-publisher", 1);

    scheduler.scheduleWithFixedDelay(publishAll(), publishFrequencyMillis,
      publishFrequencyMillis, TimeUnit.MILLISECONDS);
  }

  void addSubscriber(UserBitShared.QueryId queryId,
                     Consumer<CoordExecRPC.QueryProgressMetrics> querySubscriber) {
    // subscribe for periodic updates.
    subscribers.put(queryId, querySubscriber);
  }

  void removeSubscriber(UserBitShared.QueryId queryId,
                        Consumer<CoordExecRPC.QueryProgressMetrics> querySubscriber,
                        boolean publishFinalMetrics) {
    // unsubscribe from periodic updates.
    subscribers.remove(queryId, querySubscriber);

    // publish final metrics.
    if (publishFinalMetrics) {
      CoordExecRPC.QueryProgressMetrics metrics = fetchMetricsAndCombine(queryId);
      querySubscriber.accept(metrics);
    }
  }

  private Runnable publishAll() {
    return () -> {
      // extract list of registered queryIds.
      Set<UserBitShared.QueryId> queryList;
      synchronized (subscribers) {
        queryList = new HashSet<>(subscribers.keySet());
      }

      // publish metrics for each registered query.
      queryList.forEach(this::publishForQuery);
    };
  }

  private void publishForQuery(UserBitShared.QueryId queryId) {
    try {
      CoordExecRPC.QueryProgressMetrics metrics = fetchMetricsAndCombine(queryId);
      synchronized (subscribers) {
        for (Consumer<CoordExecRPC.QueryProgressMetrics> observer : subscribers.get(queryId)) {
          observer.accept(metrics);
        }
      }
    } catch (Throwable e) {
      logger.warn("publish failed for queryId " + QueryIdHelper.getQueryId(queryId), e);
    }
  }

  private CoordExecRPC.QueryProgressMetrics fetchMetricsAndCombine(UserBitShared.QueryId queryId) {
    return metricsStore.get(queryId)
      .map(map -> MetricsCombiner.combine(map.getMetricsMapMap().values().stream()))
      .orElse(CoordExecRPC.QueryProgressMetrics.getDefaultInstance());
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(scheduler);
  }
}

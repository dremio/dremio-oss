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

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.concurrent.ScheduledContextMigratingExecutorService;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.CoordExecRPC.QueryProgressMetrics;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobtelemetry.server.store.MetricsStore;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Publishes metrics to registered subscribers periodically. */
public class ProgressMetricsPublisher implements AutoCloseable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ProgressMetricsPublisher.class);

  private final ConcurrentMap<Consumer<QueryProgressMetrics>, SubscriberInfo> subscriberInfoMap =
      new ConcurrentHashMap<>();
  private final MetricsStore metricsStore;
  private final CloseableSchedulerThreadPool scheduler;
  private final ScheduledContextMigratingExecutorService scheduledContextMigratingExecutorService;
  private final int publishFrequencyMillis;

  public ProgressMetricsPublisher(MetricsStore metricsStore, int publishFrequencyMillis) {
    this.metricsStore = metricsStore;
    this.publishFrequencyMillis = publishFrequencyMillis;
    this.scheduler = new CloseableSchedulerThreadPool("metrics-publisher", 1);
    this.scheduler.setRemoveOnCancelPolicy(true);
    scheduledContextMigratingExecutorService =
        new ScheduledContextMigratingExecutorService(scheduler);
  }

  @VisibleForTesting
  public void addSubscriber(
      UserBitShared.QueryId queryId, Consumer<QueryProgressMetrics> querySubscriber) {
    // subscribe for periodic updates.
    ScheduledFuture scheduledFuture =
        scheduledContextMigratingExecutorService.scheduleWithFixedDelay(
            publishToSubcriber(queryId, querySubscriber),
            publishFrequencyMillis,
            publishFrequencyMillis,
            TimeUnit.MILLISECONDS);
    subscriberInfoMap.put(
        querySubscriber, new SubscriberInfo(scheduledFuture, getDefaultMetrics()));
  }

  @VisibleForTesting
  public void removeSubscriber(
      UserBitShared.QueryId queryId,
      Consumer<QueryProgressMetrics> querySubscriber,
      boolean publishFinalMetrics) {
    // unsubscribe from periodic updates.
    if (subscriberInfoMap.containsKey(querySubscriber)) {
      subscriberInfoMap.get(querySubscriber).getPublishTask().cancel(true);
      subscriberInfoMap.remove(querySubscriber);
    }

    try {
      // publish final metrics.
      if (publishFinalMetrics) {
        final QueryProgressMetrics metrics = fetchMetricsAndCombine(queryId);
        querySubscriber.accept(metrics);
      }
    } catch (Throwable e) {
      logger.warn(
          "publishing final metrics failed for queryId " + QueryIdHelper.getQueryId(queryId), e);
    }
  }

  private Runnable publishToSubcriber(
      UserBitShared.QueryId queryId, Consumer<QueryProgressMetrics> querySubscriber) {
    return () -> {
      try {
        final QueryProgressMetrics metrics = fetchMetricsAndCombine(queryId);
        SubscriberInfo info = subscriberInfoMap.get(querySubscriber);
        if (info != null && !metrics.equals(info.getLastPublishedMetric())) {
          querySubscriber.accept(metrics);

          // update LastPublishedMetric for the subscriber
          info.setLastPublishedMetric(metrics);
          subscriberInfoMap.put(querySubscriber, info);
        }
      } catch (Throwable t) {
        logger.warn(
            "publishing metrics failed for queryId " + QueryIdHelper.getQueryId(queryId), t);
        subscriberInfoMap.remove(querySubscriber);
      }
    };
  }

  QueryProgressMetrics fetchMetricsAndCombine(UserBitShared.QueryId queryId) {
    return metricsStore
        .get(queryId)
        .map(map -> MetricsCombiner.combine(() -> map.getMetricsMapMap().values().stream()))
        .orElse(getDefaultMetrics());
  }

  private QueryProgressMetrics getDefaultMetrics() {
    return QueryProgressMetrics.newBuilder().setRowsProcessed(-1).setOutputRecords(-1).build();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(scheduler);
  }

  private static class SubscriberInfo {
    private final ScheduledFuture publishTask;
    private QueryProgressMetrics lastPublishedMetric;

    SubscriberInfo(ScheduledFuture publishTask, QueryProgressMetrics lastPublishedMetric) {
      this.publishTask = publishTask;
      this.lastPublishedMetric = lastPublishedMetric;
    }

    void setLastPublishedMetric(QueryProgressMetrics lastPublishedMetric) {
      this.lastPublishedMetric = lastPublishedMetric;
    }

    ScheduledFuture getPublishTask() {
      return publishTask;
    }

    QueryProgressMetrics getLastPublishedMetric() {
      return lastPublishedMetric;
    }
  }
}

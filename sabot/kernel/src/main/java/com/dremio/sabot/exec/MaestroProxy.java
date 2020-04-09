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
package com.dremio.sabot.exec;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.util.LoadingCacheWithExpiry;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.options.OptionManager;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.jobtelemetry.client.JobTelemetryExecutorClientFactory;
import com.dremio.service.maestroservice.MaestroClientFactory;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;

/**
 * Local Proxy for the maestro service running on the coordinator/job-service.
 */
public class MaestroProxy implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaestroProxy.class);
  private final Provider<MaestroClientFactory> maestroServiceClientFactoryProvider;
  private final Provider<JobTelemetryExecutorClientFactory> jobTelemetryClientFactoryProvider;
  private final ClusterCoordinator clusterCoordinator;
  private final LoadingCacheWithExpiry<QueryId, MaestroProxyQueryTracker> trackers;
  private final CloseableSchedulerThreadPool retryPool =
    new CloseableSchedulerThreadPool("maestro-proxy-retry", 1);
  private final long evictionDelayMillis;

  public MaestroProxy(
    Provider<MaestroClientFactory> maestroServiceClientFactoryProvider,
    Provider<JobTelemetryExecutorClientFactory> jobTelemetryClientFactoryProvider,
    ClusterCoordinator clusterCoordinator,
    NodeEndpoint selfEndpoint,
    OptionManager options) {

    this.maestroServiceClientFactoryProvider = maestroServiceClientFactoryProvider;
    this.jobTelemetryClientFactoryProvider = jobTelemetryClientFactoryProvider;
    this.clusterCoordinator = clusterCoordinator;
    this.evictionDelayMillis = TimeUnit.SECONDS.toMillis(
      options.getOption(ExecConstants.FRAGMENT_CACHE_EVICTION_DELAY_S));

    // Create a cache that evicts expired entries after a delay. This allows for handling
    // duplicate ops and out-of-order ops (eg. cancel arrives before start).
    this.trackers = new LoadingCacheWithExpiry<>("foreman-node-proxy",
      new CacheLoader<QueryId, MaestroProxyQueryTracker>() {
        @Override
        public MaestroProxyQueryTracker load(QueryId queryId) throws Exception {
          return new MaestroProxyQueryTracker(queryId, selfEndpoint,
            evictionDelayMillis, retryPool, clusterCoordinator);
        }
      },
    null, evictionDelayMillis);
  }

  /**
   * Try and start a query.
   *
   * @param queryId queryId
   * @param ticket query ticket
   * @return true if the query was started successfully.
   */
  boolean tryStartQuery(QueryId queryId, QueryTicket ticket) {
    return trackers.getUnchecked(queryId).tryStart(ticket,
      ticket.getForeman(),
      maestroServiceClientFactoryProvider.get().getMaestroClient(ticket.getForeman()),
      jobTelemetryClientFactoryProvider.get().getClient(ticket.getForeman()));
  }

  boolean isQueryStarted(QueryId queryId) {
    return trackers.getUnchecked(queryId).isStarted();
  }

  /**
   * Mark query as cancelled.
   *
   * @param queryId query id.
   */
  void setQueryCancelled(QueryId queryId) {
    trackers.getUnchecked(queryId).setCancelled();
  }

  boolean isQueryCancelled(QueryId queryId) {
    return trackers.getUnchecked(queryId).isCancelled();
  }

  /**
   * Notify the changed status for a fragment.
   *
   * @param fragmentStatus status of one fragment
   */
  public void fragmentStatusChanged(FragmentStatus fragmentStatus) {
    trackers.getUnchecked(fragmentStatus.getHandle().getQueryId()).fragmentStatusChanged(fragmentStatus);
  }

  /**
   * Refresh the status of a fragment.
   *
   * @param fragmentStatus
   */
  public void refreshFragmentStatus(FragmentStatus fragmentStatus) {
    trackers.getUnchecked(fragmentStatus.getHandle().getQueryId()).refreshFragmentStatus(fragmentStatus);
  }

  /**
   * Send the query profile for the query to the control plane.
   *
   * @param queryId
   * @return
   */
  public Optional<ListenableFuture<Empty>> sendQueryProfile(QueryId queryId) {
    return trackers.getUnchecked(queryId).sendQueryProfile();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(trackers, retryPool);
  }
}

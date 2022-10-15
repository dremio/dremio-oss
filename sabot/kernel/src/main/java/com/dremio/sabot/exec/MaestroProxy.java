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

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.nodes.EndpointHelper;
import com.dremio.common.util.LoadingCacheWithExpiry;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.ActiveQueriesOnForeman;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.cursors.FileCursorManagerFactory;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.jobtelemetry.client.JobTelemetryExecutorClientFactory;
import com.dremio.service.maestroservice.MaestroClientFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
  private final LoadingCacheWithExpiry<QueryId, QueryTracker> trackers;
  private final CloseableSchedulerThreadPool retryPool =
    new CloseableSchedulerThreadPool("maestro-proxy-retry", 1);
  private final long evictionDelayMillis;
  private final Set<QueryId> doNotTrackQueries;

  public MaestroProxy(
    Provider<MaestroClientFactory> maestroServiceClientFactoryProvider,
    Provider<JobTelemetryExecutorClientFactory> jobTelemetryClientFactoryProvider,
    ClusterCoordinator clusterCoordinator,
    Provider<NodeEndpoint> selfEndpoint,
    OptionManager options) {

    this.maestroServiceClientFactoryProvider = maestroServiceClientFactoryProvider;
    this.jobTelemetryClientFactoryProvider = jobTelemetryClientFactoryProvider;
    this.clusterCoordinator = clusterCoordinator;
    this.evictionDelayMillis = TimeUnit.SECONDS.toMillis(
      options.getOption(ExecConstants.FRAGMENT_CACHE_EVICTION_DELAY_S));

    this.doNotTrackQueries = ConcurrentHashMap.newKeySet();

    // Create a cache that evicts expired entries after a delay. This allows for handling
    // duplicate ops and out-of-order ops (eg. cancel arrives before start).
    this.trackers = new LoadingCacheWithExpiry<>("foreman-node-proxy",
      new CacheLoader<QueryId, QueryTracker>() {
        @Override
        public QueryTracker load(QueryId queryId) {
          if (doNotTrackQueries.contains(queryId)) {
            return new NoOpQueryTracker();
          }
          return new MaestroProxyQueryTracker(queryId, selfEndpoint,
            evictionDelayMillis, retryPool, clusterCoordinator);
        }
      },
    null, evictionDelayMillis);
  }

  public void doNotTrack(QueryId queryId) {
    doNotTrackQueries.add(queryId);
  }


  /**
   * Try and start a query.
   *
   * @param queryId queryId
   * @param ticket query ticket
   * @param querySentTime
   * @return true if the query was started successfully.
   */
  boolean tryStartQuery(QueryId queryId, QueryTicket ticket, Long querySentTime) {
    QueryTracker queryTracker = trackers.getUnchecked(queryId);
    queryTracker.setQuerySentTime(querySentTime);
    return  queryTracker.tryStart(ticket,
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
   * Initialize the query tracker with the set of fragment handles. This should be done prior to
   * starting the fragments.
   * @param queryId
   * @param pendingFragments
   */
  void initFragmentHandlesForQuery(QueryId queryId,
                                   Set<ExecProtos.FragmentHandle> pendingFragments) {
     trackers.getUnchecked(queryId).initFragmentsForQuery(pendingFragments);
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

  public FileCursorManagerFactory getFileCursorMangerFactory(QueryId queryId) {
    return trackers.getUnchecked(queryId).getFileCursorManagerFactory();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(trackers, retryPool);
  }

  public void markQueryAsDone(QueryId queryId) {
    doNotTrackQueries.remove(queryId);
  }

  /**
   * Determine queries to be cancelled based on activeQueryList and
   * cancel those queries. See implementation for actual algo.
   * @param activeQueryList
   */
  public Set<QueryId> reconcileActiveQueries(CoordExecRPC.ActiveQueryList activeQueryList) {
    return reconcileActiveQueriesHelper(activeQueryList, trackers.asMap());
  }

  @VisibleForTesting
  static Set<QueryId> reconcileActiveQueriesHelper(CoordExecRPC.ActiveQueryList activeQueryList,
                                                   Map<QueryId, QueryTracker> queryTrackerMap) {
    final Set<QueryId> activeQueriesOnExec = queryTrackerMap.entrySet()
                                                            .stream()
                                                            .filter(e -> !e.getValue().isTerminal()) // skip terminal queries
                                                            .map(e -> e.getKey())
                                                            .collect(Collectors.toSet());

    if (activeQueriesOnExec.isEmpty()) {
      logger.info("There are no active queries on this executor. So nothing to reconcile.");
      return Sets.newHashSet();
    }

    logger.debug("ActiveQueryList received: {}", activeQueryList.toString());

    Set<QueryId> activeQueriesFromAQL = activeQueryList.getActiveQueriesOnForemanList()
                                                       .stream()
                                                       .map(x->x.getQueryIdList())
                                                       .flatMap(x->x.stream())
                                                       .collect(Collectors.toSet());

    final Set<QueryId> queryIdsToCheck = Sets.difference(activeQueriesOnExec, activeQueriesFromAQL);

    if (queryIdsToCheck.isEmpty()) {
      logger.info("All queries on executor are active on coordinator. No queries to cancel.");
      return Sets.newHashSet();
    }

    logger.debug("Potential stale queries to be checked:{}", queryIdsToCheck.stream()
                                                                            .map(q->QueryIdHelper.getQueryId(q))
                                                                            .collect(Collectors.joining(", ")));

    // process AQL
    final Map<NodeEndpoint, Long> foremanToTimestampMapFromAQL = Maps.newHashMap();
    final Set<NodeEndpoint> foremanSetFromAQL = Sets.newHashSet();

    for(ActiveQueriesOnForeman activeQueriesOnForeman: activeQueryList.getActiveQueriesOnForemanList()) {
      NodeEndpoint foremanFromAQL = activeQueriesOnForeman.getForeman();
      foremanSetFromAQL.add(EndpointHelper.getMinimalEndpoint(foremanFromAQL));
      foremanToTimestampMapFromAQL.put(foremanFromAQL, activeQueriesOnForeman.getTimestamp());
    }

    final Set<QueryId> queryIdsToCancel = new HashSet<>();

    // For each active query on the executor side
    for (QueryId queryId: queryIdsToCheck) {
      String queryIdString = QueryIdHelper.getQueryId(queryId);
      QueryTracker queryTracker = queryTrackerMap.get(queryId);
      if (queryTracker == null) {
       logger.debug("QueryId:{} is no more active on executor.", queryIdString);
       continue;
      }
      if (queryTracker instanceof NoOpQueryTracker) {
        logger.debug("QueryId:{} is a doNotTrackQuery.", queryIdString);
        continue;
      }
      NodeEndpoint foremanEndpoint = EndpointHelper.getMinimalEndpoint(queryTracker.getForeman());

      // If foreman endpoint is not present in AQL, then cancel query.
      if (!foremanSetFromAQL.contains(foremanEndpoint)) {
        logger.info("Foreman of queryId:{} is not in AQL. Considered for cancelling.", queryIdString);
        queryIdsToCancel.add(queryId);
        continue;
      }

      long queryIdStartTimestamp = queryTracker.getQuerySentTime();
      long aqlTimestamp = foremanToTimestampMapFromAQL.get(foremanEndpoint);

      // If the query start-timestamp >= timestamp in AQL, then do NOT cancel query.
      // It means query was started after the AQL was sent from coordinator.
      if (queryIdStartTimestamp >= aqlTimestamp) {
        continue;
      } else {
        logger.info("Stale queryId:{} running on executor. Considered for cancelling.", queryIdString);
        queryIdsToCancel.add(queryId);
        continue;
      }
    }
    return queryIdsToCancel;
  }

}

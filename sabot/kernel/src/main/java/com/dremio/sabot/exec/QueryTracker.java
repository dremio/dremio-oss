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
import java.util.Set;

import com.dremio.common.util.MayExpire;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.service.jobtelemetry.client.JobTelemetryExecutorClient;
import com.dremio.service.maestroservice.MaestroClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;

/**
 * Tracker for query used in MaestroProxy. Extends {@link MayExpire} so it can be used in {@link com.dremio.common.util.LoadingCacheWithExpiry}
 */
interface QueryTracker extends MayExpire {

  /**
   * Try to start a new query. While the start is in-progress, no completion event will be sent.
   *
   * @param ticket        ticket for the query.
   * @param maestroClient client to maestro service
   * @return true if query can be started.
   */
  boolean tryStart(QueryTicket ticket, CoordinationProtos.NodeEndpoint foreman, MaestroClient maestroClient, JobTelemetryExecutorClient telemetryClient);

  /**
   * @return whether the query is already started
   */
  boolean isStarted();

  /**
   * set query status to CANCELLED
   */
  default void setCancelled() {}

  /**
   * @return whether the query was cancelled
   */
  boolean isCancelled();

  /**
   * Return true if query is one of following
   * 1. completed successfully
   * 2. marked as cancelled
   * 3. expired
   * @return
   */
  boolean isTerminal();

  /**
   * Initialize with the set of fragment handles for the query before
   * starting the query.
   *
   * @param pendingFragments
   */
  default void initFragmentsForQuery(Set<ExecProtos.FragmentHandle> pendingFragments) {}

  /**
   * Handle the status change of one fragment.
   *
   * @param fragmentStatus
   */
  default void fragmentStatusChanged(CoordExecRPC.FragmentStatus fragmentStatus) {}

  /**
   * set new status for a fragment
   *
   * @param newStatus
   */
  default void refreshFragmentStatus(CoordExecRPC.FragmentStatus newStatus) {}

  Optional<ListenableFuture<Empty>> sendQueryProfile();

  CoordinationProtos.NodeEndpoint getForeman();

  /**
   * Return time on foreman side, at which query fragment was sent to executor
   * @return
   */
  long getQuerySentTime();

  /**
   * Set time on foreman side, at which query fragment was sent to executor
   * @return
   */
  void setQuerySentTime(long querySentTime);
}

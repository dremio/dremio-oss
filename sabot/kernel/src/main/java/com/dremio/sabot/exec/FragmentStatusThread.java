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

import java.util.List;

import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryStatus;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;

/**
 * Periodically gather current statistics. {@link Foreman} uses a
 * FragmentStatusListener to maintain changes to state, and should be current.
 * However, we want to collect current statistics about RUNNING queries, such as
 * current memory consumption, number of rows processed, and so on. The
 * FragmentStatusListener only tracks changes to state, so the statistics kept
 * there will be stale; this thread probes for current values.
 */
public class FragmentStatusThread extends Thread implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentStatusThread.class);

  private final static int STATUS_PERIOD_SECONDS = 5;

  private final Iterable<FragmentExecutor> executors;
  private final QueriesClerk clerk;
  private final ExecToCoordTunnelCreator tunnelCreator;

  public FragmentStatusThread(Iterable<FragmentExecutor> executors, QueriesClerk clerk, ExecToCoordTunnelCreator tunnelCreator) {
    super();
    setDaemon(true);
    setName("fragment-status-reporter");
    this.executors = executors;
    this.clerk = clerk;
    this.tunnelCreator = tunnelCreator;
  }

  @Override
  public void run() {

    while (true) {
      final List<RpcFuture<Ack>> futures = Lists.newArrayList();
      try {
        sendFragmentStatuses(futures);
        sendPhaseStatuses(futures);
      } catch (Exception e) {
        // Exception ignored. Status sender thread should not die due to a random exception
      }

      // we'll wait to complete so we don't back up if the cluster is moving slowly.
      try {
        Futures.successfulAsList(futures).get();
      } catch (final Exception ex) {
        logger.info("Failure while sending intermediate fragment status to AttemptManager", ex);
      }

      try {
        Thread.sleep(STATUS_PERIOD_SECONDS * 1000);
      } catch (final InterruptedException e) {
        logger.debug("Status thread exiting.");
        break;
      }
    }
  }

  /**
   * Send the status for all minor fragments currently running on this executor to the coordinator that initiated the
   * minor fragment
   */
  private void sendFragmentStatuses(List<RpcFuture<Ack>> futures) {
    for (final FragmentExecutor fragmentExecutor : executors) {
      final FragmentStatus status = fragmentExecutor.getStatus();
      if (status == null) {
        continue;
      }

      final NodeEndpoint ep = fragmentExecutor.getForeman();
      futures.add(tunnelCreator.getTunnel(ep).sendFragmentStatus(status));
    }
  }

  /**
   * Send the status for all phases (major fragments) of all queries currently running on this executor, to the
   * coordinator that initiated the respective query
   */
  private void sendPhaseStatuses(List<RpcFuture<Ack>> futures) {
    for (final WorkloadTicket workloadTicket : clerk.getWorkloadTickets()) {
      for (final QueryTicket queryTicket : workloadTicket.getActiveQueryTickets()) {
        NodeQueryStatus queryStatus = queryTicket.getStatus();
        final NodeEndpoint ep = queryTicket.getForeman();
        futures.add(tunnelCreator.getTunnel(ep).sendNodeQueryStatus(queryStatus));
      }
    }
  }

  @Override
  public void close() {
    this.interrupt();
  }


}

/*
 * Copyright (C) 2017 Dremio Corporation
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
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.exec.rpc.RpcException;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.google.common.collect.Lists;

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
  private final ExecToCoordTunnelCreator tunnelCreator;

  public FragmentStatusThread(Iterable<FragmentExecutor> executors, ExecToCoordTunnelCreator tunnelCreator) {
    super();
    setDaemon(true);
    setName("fragment-status-reporter");
    this.executors = executors;
    this.tunnelCreator = tunnelCreator;
  }

  @Override
  public void run() {

    while (true) {
      final List<RpcFuture<Ack>> futures = Lists.newArrayList();
      for (final FragmentExecutor fragmentExecutor : executors) {
        final FragmentStatus status = fragmentExecutor.getStatus();
        if (status == null) {
          continue;
        }

        final NodeEndpoint ep = fragmentExecutor.getForeman();
        futures.add(tunnelCreator.getTunnel(ep).sendFragmentStatusFuture(status));
      }

      // we'll wait to complete so we don't backup if the cluster is moving slowly.
      for (final RpcFuture<Ack> future : futures) {
        try {
          future.checkedGet();
        } catch (final RpcException ex) {
          logger.info("Failure while sending intermediate fragment status to AttemptManager", ex);
        }
      }

      try {
        Thread.sleep(STATUS_PERIOD_SECONDS * 1000);
      } catch (final InterruptedException e) {
        logger.debug("Status thread exiting.");
        break;
      }
    }
  }

  @Override
  public void close() {
    this.interrupt();
  }


}

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

import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import java.util.List;
import java.util.Optional;

/**
 * Periodically gather current statistics.
 *
 * <p>We use a thread that runs periodically to collect current statistics about RUNNING queries,
 * such as current memory consumption, number of rows processed, and so on.
 */
public class FragmentStatusThread extends Thread implements AutoCloseable {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(FragmentStatusThread.class);

  private final Iterable<FragmentExecutor> executors;
  private final QueriesClerk clerk;
  private final MaestroProxy maestroProxy;
  private final OptionManager optionManager;
  private volatile boolean isClosing = false;

  public FragmentStatusThread(
      Iterable<FragmentExecutor> executors,
      QueriesClerk clerk,
      MaestroProxy maestroProxy,
      OptionManager optionManager) {
    super();
    setDaemon(true);
    setName("fragment-status-reporter");
    this.executors = executors;
    this.clerk = clerk;
    this.maestroProxy = maestroProxy;
    this.optionManager = optionManager;
  }

  @Override
  public void run() {
    while (!isClosing) {
      final List<ListenableFuture<Empty>> futures = Lists.newArrayList();
      try {
        refreshFragmentStatuses();
        sendQueryProfiles(futures);
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
        Thread.sleep(
            optionManager.getOption(ExecConstants.JOB_PROFILE_EXECUTOR_UPDATE_INTERVAL_SECONDS)
                * 1000L);
      } catch (final InterruptedException e) {
        logger.debug("Status thread exiting.");
        break;
      }
    }
  }

  /** Refresh the status/metrics for all running fragments. */
  private void refreshFragmentStatuses() {
    for (final FragmentExecutor fragmentExecutor : executors) {
      final FragmentStatus status = fragmentExecutor.getStatus();
      if (status == null) {
        continue;
      }

      maestroProxy.refreshFragmentStatus(status);
    }
  }

  /**
   * Send the profiles for all queries currently running on this executor, to the coordinator that
   * initiated the respective query
   */
  private void sendQueryProfiles(List<ListenableFuture<Empty>> futures) {
    for (final WorkloadTicket workloadTicket : clerk.getWorkloadTickets()) {
      for (final QueryTicket queryTicket : workloadTicket.getActiveQueryTickets()) {
        Optional<ListenableFuture<Empty>> future =
            maestroProxy.sendQueryProfile(queryTicket.getQueryId());
        future.ifPresent((x) -> futures.add(x));
      }
    }
  }

  @Override
  public void close() {
    isClosing = true;
    this.interrupt();
  }
}

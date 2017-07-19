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
package com.dremio.sabot.exec.context;

import java.util.List;

import com.google.common.base.Stopwatch;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.google.common.collect.Lists;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Holds statistics of a particular (minor) fragment.
 */
public class FragmentStats {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentStats.class);

  private List<OperatorStats> operators = Lists.newArrayList();
  private final long startTime;
  private long firstRun;
  private final NodeEndpoint endpoint;
  private final BufferAllocator allocator;

  private long sleepingDuration;
  private long blockedDuration;

  private long numRuns;

  private final Stopwatch runWatch = Stopwatch.createUnstarted();
  private final Stopwatch setupWatch = Stopwatch.createUnstarted();
  private final Stopwatch finishWatch = Stopwatch.createUnstarted();

  private boolean notStartedYet = true;

  public FragmentStats(BufferAllocator allocator, NodeEndpoint endpoint) {
    this.startTime = System.currentTimeMillis();
    this.endpoint = endpoint;
    this.allocator = allocator;
  }

  public void addMetricsToStatus(MinorFragmentProfile.Builder prfB) {
    prfB.setStartTime(startTime);
    prfB.setFirstRun(firstRun);
    prfB.setMaxMemoryUsed(allocator.getPeakMemoryAllocation());
    prfB.setEndTime(System.currentTimeMillis());
    prfB.setEndpoint(endpoint);
    for(OperatorStats o : operators){
      prfB.addOperatorProfile(o.getProfile());
    }
    prfB.setSleepingDuration(sleepingDuration);
    prfB.setBlockedDuration(blockedDuration);
    prfB.setRunDuration(runWatch.elapsed(MILLISECONDS));
    prfB.setSetupDuration(setupWatch.elapsed(MILLISECONDS));
    prfB.setFinishDuration(finishWatch.elapsed(MILLISECONDS));
    prfB.setNumRuns(numRuns);
  }

  /**
   * Creates a new holder for operator statistics within this holder for fragment statistics.
   *
   * @param profileDef operator profile definition
   * @param allocator the allocator being used
   * @return a new operator statistics holder
   */
  public OperatorStats newOperatorStats(final OpProfileDef profileDef, final BufferAllocator allocator) {
    final OperatorStats stats = new OperatorStats(profileDef, allocator);
    if(profileDef.operatorType != -1) {
      operators.add(stats);
    }
    return stats;
  }

  public void runStarted() {
    if (notStartedYet) {
      notStartedYet = false;
      firstRun = System.currentTimeMillis();
    }
    runWatch.start();
    numRuns++;
  }

  public void runEnded() {
    runWatch.stop();
  }

  public void setupStarted() {
    setupWatch.start();
  }

  public void setupEnded() {
    setupWatch.stop();
  }

  public void finishStarted() {
    finishWatch.start();
  }

  public void finishEnded() {
    finishWatch.stop();
  }

  public void setSleepingDuration(long sleepingDuration) {
    this.sleepingDuration = sleepingDuration;
  }

  public void setBlockedDuration(long blockedDuration) {
    this.blockedDuration = blockedDuration;
  }
}

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
package com.dremio.sabot.exec.context;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.BlockedResourceDuration;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.sabot.threads.sharedres.SharedResourceType;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

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
  private final FragmentHandle handle;

  private BufferAllocator incomingAllocator;
  private long sleepingDuration;
  private long blockedOnUpstreamDuration;
  private long blockedOnDownstreamDuration;
  private long blockedOnSharedResourceDuration;

  private long numRuns;

  private final Stopwatch runWatch = Stopwatch.createUnstarted();
  private final Stopwatch setupWatch = Stopwatch.createUnstarted();
  private final Stopwatch finishWatch = Stopwatch.createUnstarted();
  private Map<SharedResourceType, Long> perResourceBlockedDurations;
  private final long warnIOTimeThreshold;

  private boolean notStartedYet = true;

  public FragmentStats(BufferAllocator allocator, FragmentHandle handle, NodeEndpoint endpoint, long warnIOTimeThreshold) {
    this.startTime = System.currentTimeMillis();
    this.handle = handle;
    this.endpoint = endpoint;
    this.allocator = allocator;
    this.perResourceBlockedDurations = Collections.synchronizedMap(new EnumMap<SharedResourceType, Long>(SharedResourceType.class));
    this.warnIOTimeThreshold = warnIOTimeThreshold;
  }

  public void addMetricsToStatus(MinorFragmentProfile.Builder prfB) {
    prfB.setStartTime(startTime);
    prfB.setFirstRun(firstRun);
    prfB.setMaxMemoryUsed(allocator.getPeakMemoryAllocation());
    prfB.setMaxIncomingMemoryUsed(getMemoryUsedForIncoming());
    prfB.setEndTime(System.currentTimeMillis());
    prfB.setEndpoint(endpoint);
    for (OperatorStats o : operators) {
      prfB.addOperatorProfile(o.getProfile(true));
    }
    prfB.setSleepingDuration(sleepingDuration);
    prfB.setBlockedDuration(blockedOnUpstreamDuration + blockedOnDownstreamDuration + blockedOnSharedResourceDuration);
    prfB.setBlockedOnUpstreamDuration(blockedOnUpstreamDuration);
    prfB.setBlockedOnDownstreamDuration(blockedOnDownstreamDuration);
    prfB.setBlockedOnSharedResourceDuration(blockedOnSharedResourceDuration);
    for (Map.Entry<SharedResourceType, Long> entry : perResourceBlockedDurations.entrySet()) {
      BlockedResourceDuration duration = BlockedResourceDuration.newBuilder()
        .setResource(entry.getKey().name())
        .setCategory(entry.getKey().getCategory())
        .setDuration(entry.getValue())
        .build();
      prfB.addPerResourceBlockedDuration(duration);
    }
    if (prfB.getPerResourceBlockedDurationList().size() == 0) {
      // add a dummy entry just to distinguish from older profiles that didn't have per-resource splits.
      BlockedResourceDuration duration = BlockedResourceDuration.newBuilder()
        .setResource(SharedResourceType.UNKNOWN.name())
        .setCategory(SharedResourceType.UNKNOWN.getCategory())
        .setDuration(0)
        .build();
      prfB.addPerResourceBlockedDuration(duration);
    }
    prfB.setRunDuration(runWatch.elapsed(MILLISECONDS));
    prfB.setSetupDuration(setupWatch.elapsed(MILLISECONDS));
    prfB.setFinishDuration(finishWatch.elapsed(MILLISECONDS));
    prfB.setNumRuns(numRuns);
  }

  private long getMemoryUsedForIncoming() {
    return incomingAllocator == null ? 0 : incomingAllocator.getPeakMemoryAllocation();
  }

  private void checkAndSaveIncomingAllocator() {
    if (incomingAllocator == null) {
      for (BufferAllocator child : allocator.getChildAllocators()) {
        if (child.getName().startsWith("op:") && child.getName().contains(":incoming")) {
          incomingAllocator = child;
          break;
        }
      }
    }
  }

  /**
   * Creates a new holder for operator statistics within this holder for fragment statistics.
   *
   * @param profileDef operator profile definition
   * @param allocator the allocator being used
   * @return a new operator statistics holder
   */
  public OperatorStats newOperatorStats(final OpProfileDef profileDef, final BufferAllocator allocator) {
    final OperatorStats stats = new OperatorStats(profileDef, allocator, warnIOTimeThreshold);
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
    checkAndSaveIncomingAllocator();
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

  public void setBlockedOnUpstreamDuration(long blockedDuration) {
    this.blockedOnUpstreamDuration = blockedDuration;
  }

  public void setBlockedOnDownstreamDuration(long blockedDuration) {
    this.blockedOnDownstreamDuration = blockedDuration;
  }

  public void addBlockedOnSharedResourceDuration(SharedResourceType resource, long blockedDuration) {
    this.blockedOnSharedResourceDuration += blockedDuration;

    Long oldDuration = perResourceBlockedDurations.get(resource);
    if (oldDuration != null) {
      blockedDuration += oldDuration;
    }
    perResourceBlockedDurations.put(resource, blockedDuration);
  }

}

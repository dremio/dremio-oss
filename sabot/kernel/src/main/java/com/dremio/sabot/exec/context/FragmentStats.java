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

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.BlockedResourceDuration;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.sabot.exec.ExecutionMetrics;
import com.dremio.sabot.threads.sharedres.SharedResourceType;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;

/** Holds statistics of a particular (minor) fragment. */
public class FragmentStats {
  private static final long MIN_RUNTIME_THRESHOLD = MILLISECONDS.toNanos(20);
  private static final long MAX_RUNTIME_THRESHOLD = MILLISECONDS.toNanos(200);
  private static final String PREFIX = "fragment_stats";

  private static final Meter.MeterProvider<Counter> fragmentEvalHeapAllocatedTotal =
      Counter.builder(Joiner.on(".").join(PREFIX, "eval_heap_allocated"))
          .description("Tracks total heap allocation during eval by a minor fragment")
          .withRegistry(Metrics.globalRegistry);

  private static final Meter.MeterProvider<Counter> fragmentSetupHeapAllocatedTotal =
      Counter.builder(Joiner.on(".").join(PREFIX, "setup_heap_allocated"))
          .description("Tracks total heap allocation during setup by a minor fragment")
          .withRegistry(Metrics.globalRegistry);

  private final List<OperatorStats> operators = new ArrayList<>();
  private final long startTime;
  private long firstRun;
  private final NodeEndpoint endpoint;
  private final BufferAllocator allocator;
  private final FragmentHandle handle;

  private BufferAllocator incomingAllocator;
  private long sleepingDuration;
  private long blockedOnUpstreamDuration;
  private long blockedOnDownstreamDuration;
  private long blockedOnMemoryDuration;
  private long blockedOnSharedResourceDuration;

  private long numRuns;
  private long numSlices;
  private long numShortSlices;
  private long numLongSlices;
  private long numInRunQ;
  private long recentSliceStartTime;
  private long lastSliceStartTime;
  private long cancelStartTime;
  private long avgAllocatedHeap;
  private long peakAllocatedHeap;
  private long setupAllocatedHeap;
  private long totalAllocatedHeap;
  private long startHeapAllocation = -1;
  private final Stopwatch runWatch = Stopwatch.createUnstarted();
  private final Stopwatch setupWatch = Stopwatch.createUnstarted();
  private final Stopwatch finishWatch = Stopwatch.createUnstarted();
  private final Map<SharedResourceType, Long> perResourceBlockedDurations;
  private final long warnIOTimeThreshold;
  private boolean notStartedYet = true;

  public FragmentStats(
      BufferAllocator allocator,
      FragmentHandle handle,
      NodeEndpoint endpoint,
      long warnIOTimeThreshold) {
    this.startTime = System.currentTimeMillis();
    this.handle = handle;
    this.endpoint = endpoint;
    this.allocator = allocator;
    this.perResourceBlockedDurations =
        Collections.synchronizedMap(
            new EnumMap<SharedResourceType, Long>(SharedResourceType.class));
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
    prfB.setBlockedDuration(
        blockedOnUpstreamDuration
            + blockedOnDownstreamDuration
            + blockedOnSharedResourceDuration
            + blockedOnMemoryDuration);
    prfB.setBlockedOnUpstreamDuration(blockedOnUpstreamDuration);
    prfB.setBlockedOnDownstreamDuration(blockedOnDownstreamDuration);
    prfB.setBlockedOnSharedResourceDuration(blockedOnSharedResourceDuration);
    prfB.setBlockedOnMemoryDuration(blockedOnMemoryDuration);
    for (Map.Entry<SharedResourceType, Long> entry : perResourceBlockedDurations.entrySet()) {
      BlockedResourceDuration duration =
          BlockedResourceDuration.newBuilder()
              .setResource(entry.getKey().name())
              .setCategory(entry.getKey().getCategory())
              .setDuration(entry.getValue())
              .build();
      prfB.addPerResourceBlockedDuration(duration);
    }
    if (prfB.getPerResourceBlockedDurationList().size() == 0) {
      // add a dummy entry just to distinguish from older profiles that didn't have per-resource
      // splits.
      BlockedResourceDuration duration =
          BlockedResourceDuration.newBuilder()
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
    prfB.setRunQLoad(numInRunQ);
    prfB.setNumSlices(numSlices);
    prfB.setNumLongSlices(numLongSlices);
    prfB.setNumShortSlices(numShortSlices);
    prfB.setRecentSliceStartTime(recentSliceStartTime);
    prfB.setCancelStartTime(cancelStartTime);
  }

  public long getNumSlices() {
    return numSlices;
  }

  public long getNumRuns() {
    return numRuns;
  }

  public long getLastSliceStartTime() {
    return lastSliceStartTime;
  }

  public long getCancelStartTime() {
    return this.cancelStartTime;
  }

  public void setCancelStartTime(final long cancelStartTime) {
    this.cancelStartTime = cancelStartTime;
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
  public OperatorStats newOperatorStats(
      final OpProfileDef profileDef, final BufferAllocator allocator) {
    final OperatorStats stats = new OperatorStats(profileDef, allocator, warnIOTimeThreshold);
    if (profileDef.operatorType != -1) {
      operators.add(stats);
    }
    return stats;
  }

  public void sliceStarted(int runQLoad) {
    numSlices++;
    numInRunQ += runQLoad;
    recentSliceStartTime = System.currentTimeMillis();
    lastSliceStartTime = recentSliceStartTime;
    startHeapAllocation = HeapAllocatedMXBeanWrapper.getCurrentThreadAllocatedBytes();
  }

  public boolean hasRunStarted() {
    return !notStartedYet;
  }

  public void runStarted() {
    if (notStartedYet) {
      notStartedYet = false;
      firstRun = System.currentTimeMillis();
    }
    runWatch.start();
    numRuns++;
  }

  public void sliceEnded(long runTimeNanos) {
    if (runTimeNanos > MAX_RUNTIME_THRESHOLD) {
      ExecutionMetrics.getLongSlicesCounter().increment();
      numLongSlices++;
    } else if (runTimeNanos < MIN_RUNTIME_THRESHOLD) {
      numShortSlices++;
    }
    if (startHeapAllocation >= 0) {
      long currentHeapAllocation = HeapAllocatedMXBeanWrapper.getCurrentThreadAllocatedBytes();
      long lastAllocatedHeap = currentHeapAllocation - startHeapAllocation;
      totalAllocatedHeap += lastAllocatedHeap;
      fragmentEvalHeapAllocatedTotal.withTags().increment(lastAllocatedHeap);
      double avg =
          (double) avgAllocatedHeap
              + ((double) (lastAllocatedHeap - avgAllocatedHeap) / (double) numSlices);
      avgAllocatedHeap = Math.round(avg);
      peakAllocatedHeap = Math.max(lastAllocatedHeap, peakAllocatedHeap);
    }
    recentSliceStartTime = 0;
  }

  public void sliceEndedForRetiredFragments() {
    recentSliceStartTime = 0;
  }

  public void runEnded() {
    runWatch.stop();
  }

  public void setupStarted() {
    checkAndSaveIncomingAllocator();
    setupWatch.start();
  }

  public void setupEnded() {
    if (startHeapAllocation >= 0) {
      final long current = HeapAllocatedMXBeanWrapper.getCurrentThreadAllocatedBytes();
      setupAllocatedHeap = current - startHeapAllocation;
      startHeapAllocation = current;
      fragmentSetupHeapAllocatedTotal.withTags().increment(setupAllocatedHeap);
    }
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

  public void setBlockedOnMemoryDuration(long blockedDuration) {
    this.blockedOnMemoryDuration = blockedDuration;
  }

  public void addBlockedOnSharedResourceDuration(
      SharedResourceType resource, long blockedDuration) {
    this.blockedOnSharedResourceDuration += blockedDuration;

    Long oldDuration = perResourceBlockedDurations.get(resource);
    if (oldDuration != null) {
      blockedDuration += oldDuration;
    }
    perResourceBlockedDurations.put(resource, blockedDuration);
  }

  /**
   * Builds a log friendly string of current fragment stats.
   *
   * <p>Typically used to dump memory usage data under low memory situations
   */
  private static final int KB = 1024;

  private static final String COL_DELIMITER = ",";

  public int fillLogBuffer(
      StringBuilder sb, String id, String state, String taskState, boolean dumpHeapUsage) {
    sb.append(id)
        .append(COL_DELIMITER)
        .append(state)
        .append(COL_DELIMITER)
        .append(taskState)
        .append(COL_DELIMITER)
        .append(numSlices)
        .append(COL_DELIMITER)
        .append(numLongSlices)
        .append(COL_DELIMITER)
        .append(numInRunQ)
        .append(COL_DELIMITER)
        .append(setupWatch.elapsed(MILLISECONDS))
        .append(COL_DELIMITER)
        .append(runWatch.elapsed(MILLISECONDS))
        .append(COL_DELIMITER.repeat(11))
        .append(allocator.getPeakMemoryAllocation() / KB)
        .append(COL_DELIMITER);
    if (startHeapAllocation >= 0 && dumpHeapUsage) {
      sb.append(COL_DELIMITER)
          .append(setupAllocatedHeap / KB)
          .append(COL_DELIMITER)
          .append(avgAllocatedHeap / KB)
          .append(COL_DELIMITER)
          .append(peakAllocatedHeap / KB)
          .append(COL_DELIMITER)
          .append(totalAllocatedHeap / KB);
    } else {
      if (dumpHeapUsage) {
        sb.append(COL_DELIMITER.repeat(4));
      }
    }
    sb.append(System.lineSeparator());
    if (numSlices > 0) {
      // log operator stats if and only if there was a long slice
      for (OperatorStats o : operators) {
        o.fillLogBuffer(sb, id, dumpHeapUsage);
      }
    }
    return operators.size();
  }
}

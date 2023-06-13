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
package com.dremio.dac.server.admin.profile;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dremio.exec.proto.UserBitShared.BlockedResourceDuration;
import com.dremio.exec.proto.UserBitShared.MajorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.NodePhaseProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.StreamProfile;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

/**
 * Wrapper class for a major fragment profile.
 */
public class FragmentWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentWrapper.class);

  private interface FieldAccessor {
    long getValue(MinorFragmentProfile object);
  }

  private static final FieldAccessor firstRunAccessor = new FieldAccessor() {
    @Override
    public long getValue(MinorFragmentProfile p) {
      return p.hasFirstRun() ? (p.getFirstRun() - p.getStartTime()) : 0;
    }
  };

  private static final FieldAccessor wallClockAccessor = new FieldAccessor() {
    @Override
    public long getValue(MinorFragmentProfile p) {
      return p.getEndTime() - p.getStartTime();
    }
  };

  private static final FieldAccessor sleepAccessor = new FieldAccessor() {
    @Override
    public long getValue(MinorFragmentProfile p) {
      return p.getSleepingDuration();
    }
  };

  private static final FieldAccessor blockedAccessor = new FieldAccessor() {
    @Override
    public long getValue(MinorFragmentProfile p) {
      return p.getBlockedDuration();
    }
  };

  private final MajorFragmentProfile major;
  private final long start;
  private final boolean includeDebugColumns;
  private Set<HostProcessingRate> hostProcessingRateSet;

  public FragmentWrapper(
    final MajorFragmentProfile major,
    final long start,
    boolean includeDebugColumns,
    Set<HostProcessingRate> hostProcessingRateSet) {
    this.major = Preconditions.checkNotNull(major);
    this.start = start;
    this.includeDebugColumns = includeDebugColumns;
    this.hostProcessingRateSet = hostProcessingRateSet;
  }

  public String getDisplayName() {
    return String.format("Phase: %s", new OperatorPathBuilder().setMajor(major).build());
  }

  public String getId() {
    return String.format("fragment-%s", major.getMajorFragmentId());
  }

  public static final String[] FRAGMENT_OVERVIEW_COLUMNS = {"Phase", "Weight", "Thread Reporting", "First Start", "Last Start",
    "First End", "Last End", "Min First-run", "Avg First-run", "Max First-run", "Min Wall-clock", "Avg Wall-clock", "Max Wall-clock",
    "Min Sleep", "Avg Sleep", "Max Sleep", "Min Blocked", "Avg Blocked", "Max Blocked", "Last Update", "Last Progress", "Max Peak Memory"};

  // Not including Major Fragment ID, Phase Weight and Minor Fragments Reporting
  public static final int NUM_NULLABLE_OVERVIEW_COLUMNS = FRAGMENT_OVERVIEW_COLUMNS.length - 3;

  public void addSummary(TableBuilder tb) {
    try {
      // Use only minor fragments that have complete profiles
      // Complete iff the fragment profile has at least one operator profile, and start and end times.
      final List<MinorFragmentProfile> complete = new ArrayList<>(
        Collections2.filter(major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));

      tb.appendCell(new OperatorPathBuilder().setMajor(major).build()); // Phase
      tb.appendCell(major.hasPhaseWeight() ? String.valueOf(major.getPhaseWeight()) : "-1");
      tb.appendCell(complete.size() + " / " + major.getMinorFragmentProfileCount()); // Thread Reporting

      // If there are no stats to aggregate, create an empty row
      if (complete.size() < 1) {
        tb.appendRepeated("", NUM_NULLABLE_OVERVIEW_COLUMNS);
        return;
      }

      final MinorFragmentProfile firstStart = Collections.min(complete, Comparators.startTime);
      final MinorFragmentProfile lastStart = Collections.max(complete, Comparators.startTime);
      tb.appendMillis(firstStart.getStartTime() - start); // First Start
      tb.appendMillis(lastStart.getStartTime() - start); // Last Start

      final MinorFragmentProfile firstEnd = Collections.min(complete, Comparators.endTime);
      final MinorFragmentProfile lastEnd = Collections.max(complete, Comparators.endTime);
      tb.appendMillis(firstEnd.getEndTime() - start); // First End
      tb.appendMillis(lastEnd.getEndTime() - start); // Last End

      addFieldStats(tb, complete, firstRunAccessor); // Min, Avg and Max First-run
      addFieldStats(tb, complete, wallClockAccessor); // Min, Avg  and Max Wall-clock
      addFieldStats(tb, complete, sleepAccessor); // Min, Avg and Max Sleep
      addFieldStats(tb, complete, blockedAccessor); // Min, Avg and Max Blocked

      final MinorFragmentProfile lastUpdate = Collections.max(complete, Comparators.lastUpdate);
      tb.appendTime(lastUpdate.getLastUpdate()); // Last Update

      final MinorFragmentProfile lastProgress = Collections.max(complete, Comparators.lastProgress);
      tb.appendTime(lastProgress.getLastProgress()); // Last Progress

      // TODO(DRILL-3494): Names (maxMem, getMaxMemoryUsed) are misleading; the value is peak memory allocated to fragment
      final MinorFragmentProfile maxMem = Collections.max(complete, Comparators.fragmentPeakMemory);
      tb.appendBytes(maxMem.getMaxMemoryUsed()); // Max Peak Memory
    } catch (IOException e) {
      logger.debug("Failed to add summary", e);
    }
  }

  private void addFieldStats(final TableBuilder tb, final Collection<MinorFragmentProfile> fragments,
                             final FieldAccessor accessor) {
    try {
      long total = 0;
      long minValue = Long.MAX_VALUE;
      long maxValue = Long.MIN_VALUE;
      for (final MinorFragmentProfile p : fragments) {
        long value = accessor.getValue(p);
        total += value;
        if (value < minValue) {
          minValue = value;
        }
        if (value > maxValue) {
          maxValue = value;
        }
      }

      tb.appendMillis(minValue); // Min
      tb.appendMillis(total / fragments.size()); // Avg
      tb.appendMillis(maxValue); // Max
    } catch (IOException e) {
      logger.debug("Failed to add field stats", e);
    }
  }

  /* Pre 2.0.2 : no splits in "Blocked"  */
  public static final String[] FRAGMENT_COLUMNS_NO_BLOCKED_SPLITS = {"Thread ID", "Host Name", "Start", "End",
    "Wall-clock time", "First-run", "Setup", "Runtime", "Finish", "Waiting",
    "Blocked", "Num-runs", "Max Records", "Max Batches", "Last Update", "Last Progress", "Peak Memory", "Peak Incoming Memory", "State"};

  // same as above but with extra debug columns: "Diff w OPs"
  public static final String[] FRAGMENT_COLUMNS_DEBUG_NO_BLOCKED_SPLITS = {"Thread ID", "Host Name", "Start", "End",
    "Wall-clock time", "First-run", "Setup", "Runtime", "Finish", "Waiting",
    "Blocked", "Diff w OPs", "Num-runs", "Num Slices", "Num Long Slices", "Num Short Slices", "RunQ Load",
    "Max Records", "Max Batches", "Last Update", "Last Progress", "Peak Memory", "Peak Incoming Memory", "State"};

  public static final String[] FRAGMENT_COLUMNS = {"Thread ID", "Host Name", "Start", "End",
    "Wall-clock time", "First-run", "Setup", "Runtime", "Finish", "Waiting",
    "Blocked On Downstream", "Blocked On Upstream", "Blocked On other",
    "Num-runs", "Num Slices", "Num Long Slices", "Num Short Slices", "RunQ Load", "Max Records", "Max Batches",
    "Last Update", "Last Progress", "Peak Memory", "Peak Incoming Memory", "State"};

  // same as above but with extra debug columns: "Diff w OPs"
  public static final String[] FRAGMENT_COLUMNS_DEBUG = {"Thread ID", "Host Name", "Start", "End",
    "Wall-clock time", "First-run", "Setup", "Runtime", "Finish", "Waiting",
    "Blocked On Downstream", "Blocked On Upstream", "Blocked On other",
    "Diff w OPs", "Num-runs", "Num Slices", "Num Long Slices", "Num Short Slices", "RunQ Load", "Max Records",
    "Max Batches", "Last Update", "Last Progress", "Peak Memory", "Peak Incoming Memory", "State"};

  public static final String[] PHASE_METRICS_COLUMNS = {"Host Name", "Peak Memory", "Num Threads", "Total Max Records",
    "Total Process Time", "Record Processing Rate"};

  public void addFragment(JsonGenerator generator) throws IOException {
    generator.writeFieldName(getId());
    generator.writeStartObject();

    addFragmentInfo(generator);
    addMetrics(generator);

    generator.writeEndObject();
  }

  private void addFragmentInfo(JsonGenerator generator) throws IOException {
    generator.writeFieldName("info");

    boolean withBlockedSplits = false;

    // Use only minor fragments that have complete profiles
    // Complete iff the fragment profile has at least one operator profile, and start and end times.
    final List<MinorFragmentProfile> complete = new ArrayList<>(
      Collections2.filter(major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));
    final List<MinorFragmentProfile> incomplete = new ArrayList<>(
      Collections2.filter(major.getMinorFragmentProfileList(), Filters.missingOperatorsOrTimes));

    Collections.sort(complete, Comparators.minorId);
    if (complete.size() > 0) {
      withBlockedSplits = complete.get(0).getPerResourceBlockedDurationCount() > 0;
    }
    String[] columnHeaders;
    if (includeDebugColumns) {
      columnHeaders = withBlockedSplits ? FRAGMENT_COLUMNS_DEBUG : FRAGMENT_COLUMNS_DEBUG_NO_BLOCKED_SPLITS;
    } else {
      columnHeaders = withBlockedSplits ? FRAGMENT_COLUMNS : FRAGMENT_COLUMNS_NO_BLOCKED_SPLITS;
    }

    final JsonBuilder builder = new JsonBuilder(generator, columnHeaders);

    for (final MinorFragmentProfile minor : complete) {
      builder.startEntry();

      final ArrayList<OperatorProfile> ops = new ArrayList<>(minor.getOperatorProfileList());
      long biggestIncomingRecords = 0;
      long biggestBatches = 0;
      for (final OperatorProfile op : ops) {
        long incomingRecords = 0;
        long batches = 0;
        for (final StreamProfile sp : op.getInputProfileList()) {
          incomingRecords += sp.getRecords();
          batches += sp.getBatches();
        }
        biggestIncomingRecords = Math.max(biggestIncomingRecords, incomingRecords);
        biggestBatches = Math.max(biggestBatches, batches);
      }

      final long wallClockTime = minor.getEndTime() - minor.getStartTime();
      final long waitDuration = minor.getSleepingDuration();
      final long blockedDuration = minor.getBlockedDuration();
      builder.appendString(new OperatorPathBuilder().setMajor(major).setMinor(minor).build()); // Thread ID
      builder.appendString(minor.getEndpoint().getAddress()); // Host name
      builder.appendMillis(minor.getStartTime() - start); // Start
      builder.appendMillis(minor.getEndTime() - start); // End
      builder.appendMillis(wallClockTime); // Wall-clock time
      // prior to 1.0.4 runtime was not stored in the profile but rather computed from the other stats
      final long runtime = minor.hasRunDuration() ? minor.getRunDuration() : wallClockTime - waitDuration - blockedDuration;
      if (minor.hasFirstRun()) {
        builder.appendMillis(minor.getFirstRun() - minor.getStartTime()); // First-run
        builder.appendMillis(minor.getSetupDuration()); // Setup
        builder.appendMillis(minor.getRunDuration()); // Runtime
        builder.appendMillis(minor.getFinishDuration()); // Finish
      } else {
        builder.appendMillis(0); // First-run
        builder.appendMillis(0); // Setup
        builder.appendMillis(runtime); // Runtime
        builder.appendMillis(0); // Finish
      }

      builder.appendMillis(waitDuration); // Waiting

      if (withBlockedSplits) {
        // Categorize each of the blocked durations as upstream/downstream/other.
        long blockedOnDownstreamDuration = minor.getBlockedOnDownstreamDuration();
        long blockedOnUpstreamDuration = minor.getBlockedOnUpstreamDuration();
        long blockedOnOtherDuration = 0;
        for (BlockedResourceDuration resourceDuration : minor.getPerResourceBlockedDurationList()) {
          switch (resourceDuration.getCategory()) {
            case UPSTREAM:
              blockedOnUpstreamDuration += resourceDuration.getDuration();
              break;
            case DOWNSTREAM:
              blockedOnDownstreamDuration += resourceDuration.getDuration();
              break;
            default:
              blockedOnOtherDuration += resourceDuration.getDuration();
              break;
          }
        }
        builder.appendMillis(blockedOnDownstreamDuration); // Blocked On Downstream
        builder.appendMillis(blockedOnUpstreamDuration); // Blocked On Upstream
        builder.appendMillis(blockedOnOtherDuration); // Blocked On other
      } else {
        builder.appendMillis(blockedDuration); // useful for older profiles.
      }

      // compute total setup, process, wait for all operators in minor fragment
      if (includeDebugColumns) {
        List<OperatorProfile> operators = minor.getOperatorProfileList();
        long totalNanos = 0;
        for (OperatorProfile op : operators) {
          totalNanos += op.getSetupNanos();
          totalNanos += op.getProcessNanos();
          totalNanos += op.getWaitNanos();
        }

        builder.appendInteger(runtime - NANOSECONDS.toMillis(totalNanos)); // Diff w OPs
      }

      builder.appendFormattedInteger((minor.hasNumRuns() ? minor.getNumRuns() : -1)); // Num-runs
      builder.appendFormattedInteger((minor.hasNumSlices() ? minor.getNumSlices() : -1)); // Num Slices
      builder.appendFormattedInteger((minor.hasNumLongSlices() ? minor.getNumLongSlices() : -1)); // Num long slices
      builder.appendFormattedInteger((minor.hasNumShortSlices() ? minor.getNumShortSlices() : -1)); // Num short slices
      builder.appendFormattedInteger((minor.hasRunQLoad() ? minor.getRunQLoad() : -1));
      builder.appendFormattedInteger(biggestIncomingRecords); // Max Records
      builder.appendFormattedInteger(biggestBatches); // Max Batches

      builder.appendTime(minor.getLastUpdate()); // Last Update
      builder.appendTime(minor.getLastProgress()); // Last Progress

      builder.appendBytes(minor.getMaxMemoryUsed()); // Peak Memory
      builder.appendBytes(minor.getMaxIncomingMemoryUsed()); // Peak memory for incoming buffers
      builder.appendString(minor.getState().name()); // State

      builder.endEntry();
    }

    for (final MinorFragmentProfile m : incomplete) {
      builder.startEntry();

      builder.appendString(major.getMajorFragmentId() + "-" + m.getMinorFragmentId()); // Thread ID
      builder.appendString(m.getEndpoint().getAddress()); // Host name

      // fill in empty columns with the state
      for (int i = 0; i < columnHeaders.length - 2; i++) {
        builder.appendString(m.getState().toString());
      }

      builder.endEntry();
    }

    builder.end();
  }

  private void addMetrics(JsonGenerator generator) throws IOException {
    if (major.getNodePhaseProfileList() == null || major.getNodePhaseProfileList().isEmpty()) {
      return;
    }

    final List<NodePhaseProfile> nodePhaseProfiles = new ArrayList<>(major.getNodePhaseProfileList());

    Collections.sort(nodePhaseProfiles, Comparators.nodeAddress);
    generator.writeFieldName("metrics");

    final JsonBuilder builder = new JsonBuilder(generator, PHASE_METRICS_COLUMNS);

    Map<String, NodePhaseProfile> map = getHostToNodePhaseProfileMap(nodePhaseProfiles);
    // Display in ascending order of record processing rate.
    for (HostProcessingRate hpr: hostProcessingRateSet) {
      String hostname = hpr.getHostname();
      NodePhaseProfile nodePhaseProfile = map.get(hostname);

      builder.startEntry();
      builder.appendString(hostname); // Host name
      builder.appendBytes(nodePhaseProfile.getMaxMemoryUsed()); // Peak Memory

      builder.appendFormattedInteger(hpr.getNumThreads().longValue());
      builder.appendFormattedInteger(hpr.getNumRecords().longValue());
      builder.appendNanos(hpr.getProcessNanos().longValue());
      builder.appendFormattedInteger(hpr.computeProcessingRate().longValue());

      builder.endEntry();
    }

    builder.end();
  }

  public Map<String, NodePhaseProfile> getHostToNodePhaseProfileMap(List<NodePhaseProfile> nodePhaseProfiles) {
    Map<String, NodePhaseProfile> map = new HashMap<>();
    for(NodePhaseProfile npp: nodePhaseProfiles) {
      map.put(npp.getEndpoint().getAddress(), npp);
    }
    return map;
  }
}

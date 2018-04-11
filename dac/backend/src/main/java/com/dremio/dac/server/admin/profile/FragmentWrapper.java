/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.dremio.exec.proto.UserBitShared.MajorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.NodePhaseProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.StreamProfile;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

/**
 * Wrapper class for a major fragment profile.
 */
public class FragmentWrapper {


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

  public FragmentWrapper(
    final MajorFragmentProfile major,
    final long start,
    boolean includeDebugColumns) {
    this.major = Preconditions.checkNotNull(major);
    this.start = start;
    this.includeDebugColumns = includeDebugColumns;
  }

  public String getDisplayName() {
    return String.format("Phase: %s", new OperatorPathBuilder().setMajor(major).build());
  }

  public String getId() {
    return String.format("fragment-%s", major.getMajorFragmentId());
  }

  public static final String[] FRAGMENT_OVERVIEW_COLUMNS = {"Phase", "Thread Reporting", "First Start", "Last Start",
    "First End", "Last End", "Min First-run", "Avg First-run", "Max First-run", "Min Wall-clock", "Avg Wall-clock", "Max Wall-clock",
    "Min Sleep", "Avg Sleep", "Max Sleep", "Min Blocked", "Avg Blocked", "Max Blocked", "Last Update", "Last Progress", "Max Peak Memory"};

  // Not including Major Fragment ID and Minor Fragments Reporting
  public static final int NUM_NULLABLE_OVERVIEW_COLUMNS = FRAGMENT_OVERVIEW_COLUMNS.length - 2;

  public void addSummary(TableBuilder tb) {
    // Use only minor fragments that have complete profiles
    // Complete iff the fragment profile has at least one operator profile, and start and end times.
    final List<MinorFragmentProfile> complete = new ArrayList<>(
      Collections2.filter(major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));

    tb.appendCell(new OperatorPathBuilder().setMajor(major).build(), null); // Phase
    tb.appendCell(complete.size() + " / " + major.getMinorFragmentProfileCount(), null); // Thread Reporting

    // If there are no stats to aggregate, create an empty row
    if (complete.size() < 1) {
      tb.appendRepeated("", null, NUM_NULLABLE_OVERVIEW_COLUMNS);
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
    tb.appendTime(lastUpdate.getLastUpdate(), null); // Last Update

    final MinorFragmentProfile lastProgress = Collections.max(complete, Comparators.lastProgress);
    tb.appendTime(lastProgress.getLastProgress(), null); // Last Progress

    // TODO(DRILL-3494): Names (maxMem, getMaxMemoryUsed) are misleading; the value is peak memory allocated to fragment
    final MinorFragmentProfile maxMem = Collections.max(complete, Comparators.fragmentPeakMemory);
    tb.appendBytes(maxMem.getMaxMemoryUsed(), null); // Max Peak Memory
  }

  private void addFieldStats(final TableBuilder tb, final Collection<MinorFragmentProfile> fragments,
                             final FieldAccessor accessor) {
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
  }

  public static final String[] FRAGMENT_COLUMNS = {"Thread ID", "Host Name", "Start", "End",
    "Wall-clock time", "First-run", "Setup", "Runtime", "Finish", "Waiting", "Blocked", "Num-runs", "Max Records", "Max Batches", "Last Update", "Last Progress", "Peak Memory", "State"};

  // same as above but with extra debug columns: "Diff w OPs"
  public static final String[] FRAGMENT_COLUMNS_DEBUG = {"Thread ID", "Host Name", "Start", "End",
    "Wall-clock time", "First-run", "Setup", "Runtime", "Finish", "Waiting", "Blocked", "Diff w OPs", "Num-runs", "Max Records", "Max Batches", "Last Update", "Last Progress", "Peak Memory", "State"};

  // Not including minor fragment ID
  private static final int NUM_NULLABLE_FRAGMENTS_COLUMNS = FRAGMENT_COLUMNS.length - 1;

  public String getContent() {
    final TableBuilder builder = new TableBuilder(includeDebugColumns ? FRAGMENT_COLUMNS_DEBUG : FRAGMENT_COLUMNS);

    // Use only minor fragments that have complete profiles
    // Complete iff the fragment profile has at least one operator profile, and start and end times.
    final List<MinorFragmentProfile> complete = new ArrayList<>(
      Collections2.filter(major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));
    final List<MinorFragmentProfile> incomplete = new ArrayList<>(
      Collections2.filter(major.getMinorFragmentProfileList(), Filters.missingOperatorsOrTimes));

    Collections.sort(complete, Comparators.minorId);
    for (final MinorFragmentProfile minor : complete) {
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
      builder.appendCell(new OperatorPathBuilder().setMajor(major).setMinor(minor).build(), null); // Thread ID
      builder.appendCell(minor.getEndpoint().getAddress(), null); // Host name
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
      builder.appendMillis(blockedDuration); // Blocked

      // compute total setup, process, wait for all operators in minor fragment
      if (includeDebugColumns) {
        List<OperatorProfile> operators = minor.getOperatorProfileList();
        long totalNanos = 0;
        for (OperatorProfile op : operators) {
          totalNanos += op.getSetupNanos();
          totalNanos += op.getProcessNanos();
          totalNanos += op.getWaitNanos();
        }

        builder.appendInteger(runtime - NANOSECONDS.toMillis(totalNanos), null); // Diff w OPs
      }

      builder.appendFormattedInteger(minor.hasNumRuns() ? minor.getNumRuns() : -1, null); // Num-runs
      builder.appendFormattedInteger(biggestIncomingRecords, null); // Max Records
      builder.appendFormattedInteger(biggestBatches, null); // Max Batches

      builder.appendTime(minor.getLastUpdate(), null); // Last Update
      builder.appendTime(minor.getLastProgress(), null); // Last Progress

      builder.appendBytes(minor.getMaxMemoryUsed(), null); // Peak Memory
      builder.appendCell(minor.getState().name(), null); // State
    }

    for (final MinorFragmentProfile m : incomplete) {
      builder.appendCell(major.getMajorFragmentId() + "-" + m.getMinorFragmentId(), null);
      builder.appendRepeated(m.getState().toString(), null, NUM_NULLABLE_FRAGMENTS_COLUMNS);
    }
    return builder.build();
  }

  public static final String[] PHASE_METRICS_COLUMNS = {"Host Name", "Peak Memory"};

  public String getMetricsTable() {
    if (major.getNodePhaseProfileList() == null || major.getNodePhaseProfileList().isEmpty()) {
      return "";
    }

    final TableBuilder builder = new TableBuilder(PHASE_METRICS_COLUMNS);
    final List<NodePhaseProfile> nodePhaseProfiles = new ArrayList<>(major.getNodePhaseProfileList());

    Collections.sort(nodePhaseProfiles, Comparators.nodeAddress);

    for (NodePhaseProfile nodePhaseProfile : nodePhaseProfiles) {
      builder.appendCell(nodePhaseProfile.getEndpoint().getAddress(), null); // Host name
      builder.appendBytes(nodePhaseProfile.getMaxMemoryUsed(), null); // Peak Memory
    }

    return builder.build();
  }
}

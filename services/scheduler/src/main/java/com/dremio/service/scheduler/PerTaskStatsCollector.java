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
package com.dremio.service.scheduler;

import static com.dremio.service.scheduler.TaskStatsCollector.BASE_METRIC_NAME;

import java.util.concurrent.TimeUnit;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.telemetry.api.metrics.Counter;
import com.dremio.telemetry.api.metrics.Histogram;
import com.dremio.telemetry.api.metrics.Metrics;
import com.google.common.base.Stopwatch;

/**
 * Collects Per Task stats for the {@code ClusteredSingletonTaskScheduler}
 */
final class PerTaskStatsCollector implements SchedulerEvents.PerTaskEvents {
  // wrapper to a modifiable schedule
  private final PerTaskSchedule schedule;
  // number of schedule modifications so far
  private final Counter totalScheduleModifications;
  private final Counter totalSucceededOwnerQueries;
  private final Counter totalFailedOwnerQueries;
  // task slot booking stats
  private final PerTaskBookingCollector bookingStats;
  // task run stats
  private final PerTaskRunCollector runStats;
  // task load stats
  private final PerTaskLoadCollector loadStats;
  // task recovery stats
  private final PerTaskRecoveryCollector recoveryStats;

  PerTaskStatsCollector(PerTaskSchedule schedule) {
    this.schedule = schedule;
    this.totalScheduleModifications = Metrics.newCounter(getMetricsName("modifications"), Metrics.ResetType.NEVER);
    this.totalSucceededOwnerQueries = Metrics.newCounter(getMetricsName("owner_queries"), Metrics.ResetType.NEVER);
    this.totalFailedOwnerQueries = Metrics.newCounter(getMetricsName("failed_owner_queries"), Metrics.ResetType.NEVER);
    final String perTaskName = getPerTaskName();
    this.bookingStats = new PerTaskBookingCollector(perTaskName);
    this.runStats = new PerTaskRunCollector(perTaskName);
    this.loadStats = new PerTaskLoadCollector(perTaskName);
    this.recoveryStats = new PerTaskRecoveryCollector(perTaskName);
  }

  private String getPerTaskName() {
    return Metrics.join(BASE_METRIC_NAME, schedule.getTaskName());
  }

  private String getMetricsName(String metricName) {
    return Metrics.join(getPerTaskName(), metricName);
  }

  @Override
  public void bookingAttempted() {
    bookingStats.bookingAttempted();
  }

  @Override
  public void bookingAcquired() {
    bookingStats.bookingAcquired();
    runStats.expectRuns();
  }

  @Override
  public void bookingReleased() {
    bookingStats.bookingReleased();
    runStats.runsUnlikely();
  }

  @Override
  public void contractError() {
    runStats.contractError();
  }

  @Override
  public void runStarted() {
    runStats.runStarted();
  }

  @Override
  public void runEnded(boolean success) {
    runStats.runEnded(success);
  }

  @Override
  public void scheduleModified() {
    totalScheduleModifications.increment();
  }

  @Override
  public void taskOwnerQuery(CoordinationProtos.NodeEndpoint nodeEndpoint) {
    totalSucceededOwnerQueries.increment();
  }

  @Override
  public void noTaskOwnerFound() {
    totalFailedOwnerQueries.increment();
  }

  @Override
  public void taskOwnerQueryFailed() {
    totalFailedOwnerQueries.increment();
  }

  @Override
  public void addedToRunSet() {
    loadStats.addedToRunSet();
  }

  @Override
  public void crossedThreshold() {
    loadStats.crossedThreshold();
  }

  @Override
  public void removedFromRunSet(long cTime) {
    loadStats.removedFromRunSet(cTime);
  }

  @Override
  public void recoveryMonitoringStarted() {
    recoveryStats.recoveryMonitoringStarted();
  }

  @Override
  public void recoveryMonitoringStopped() {
    recoveryStats.recoveryMonitoringStopped();
  }

  @Override
  public void recoveryRequested() {
    recoveryStats.recoveryRequested();
  }

  @Override
  public void recoveryRejected(SchedulerEvents.RecoveryRejectReason reason) {
    recoveryStats.recoveryRejected(reason);
  }

  @Override
  public void recovered() {
    recoveryStats.recovered();
  }

  @Override
  public void addedToDeathWatch() {
    recoveryStats.addedToDeathWatch();
  }

  @Override
  public void runOnDeath() {
    recoveryStats.runOnDeath();
  }

  @Override
  public void failedToRunOnDeath() {
    recoveryStats.failedToRunOnDeath();
  }

  @Override
  public String toString() {
      return "Task Name : " + schedule.getTaskName() + System.lineSeparator() +
      "Period : " + schedule.getSchedule().getPeriod() + System.lineSeparator() +
      "Is Single Shot? : " + schedule.getSchedule().isToRunExactlyOnce() + System.lineSeparator() +
      "Total Schedule Modifications : " + totalScheduleModifications + System.lineSeparator() +
      "Per Task Booking Stats : " + System.lineSeparator() + bookingStats + System.lineSeparator() +
      "Per Task Run Stats : " + System.lineSeparator() + runStats + System.lineSeparator() +
      "Per Task Load Stats : " + System.lineSeparator() + loadStats + System.lineSeparator() +
      "Per Task Recovery Stats : " + System.lineSeparator() + recoveryStats + System.lineSeparator();
  }

  /**
   * Booking stats collection.
   */
  private static final class PerTaskBookingCollector {
    // number of attempts made to book the slot by this service instance
    private final Counter totalBookingAttempts;
    // number of successful attempts to book the slot
    private final Counter totalAcquiredBookings;
    // number of explicit release of booked slot
    private final Counter totalReleasedBookings;

    private PerTaskBookingCollector(String baseMetricName) {
      totalBookingAttempts = Metrics.newCounter(bookName(baseMetricName, "attempts"), Metrics.ResetType.NEVER);
      totalAcquiredBookings = Metrics.newCounter(bookName(baseMetricName, "acquired"), Metrics.ResetType.NEVER);
      totalReleasedBookings = Metrics.newCounter(bookName(baseMetricName, "released"), Metrics.ResetType.NEVER);
    }

    private static String bookName(String base, String metric) {
      return Metrics.join(base, "book", metric);
    }

    public void bookingAttempted() {
      totalBookingAttempts.increment();
    }

    public void bookingAcquired() {
      totalAcquiredBookings.increment();
    }

    public void bookingReleased() {
      totalReleasedBookings.increment();
    }

    @Override
    public String toString() {
        return "Total Booking Attempts : " + totalBookingAttempts + System.lineSeparator() +
        "Total Acquired Bookings : " + totalAcquiredBookings + System.lineSeparator() +
        "Total Released Bookings : " + totalReleasedBookings + System.lineSeparator();
    }
  }

  private static final class PerTaskRunCollector {
    // total successful runs so far on this service instance
    private final Counter totalSuccessRunsSoFar;
    // total failed runs so far on this service instance
    private final Counter totalFailedRunsSoFar;
    // distribution of task run time
    private final Histogram taskRuntimeMillis;
    // number of times a run was made without booking (this should be 0 always)
    private final Counter totalContractBreaks;
    // task run time watch, reset after every run
    private final Stopwatch runTimeWatch;
    // whether a run is expected (only if booking is on)
    private volatile boolean runsExpected;

    private PerTaskRunCollector(String baseMetricName) {
      totalSuccessRunsSoFar = Metrics.newCounter(runName(baseMetricName, "success"), Metrics.ResetType.NEVER);
      totalFailedRunsSoFar = Metrics.newCounter(runName(baseMetricName, "failed"), Metrics.ResetType.NEVER);
      taskRuntimeMillis = Metrics.newHistogram(runName(baseMetricName, "runtime_ms"), Metrics.ResetType.NEVER);
      totalContractBreaks = Metrics.newCounter(runName(baseMetricName, "contract_breaks"), Metrics.ResetType.NEVER);
      runTimeWatch = Stopwatch.createUnstarted();
      runsExpected = false;
    }

    private static String runName(String base, String metric) {
      return Metrics.join(base, "runs", metric);
    }

    public void expectRuns() {
      runsExpected = true;
    }

    public void runsUnlikely() {
      runsExpected = false;
    }

    public void runStarted() {
      runTimeWatch.start();
      if (!runsExpected) {
        totalContractBreaks.increment();
      }
    }

    public void runEnded(boolean success) {
      assert runTimeWatch.isRunning();
      if (success) {
        totalSuccessRunsSoFar.increment();
      } else {
        totalFailedRunsSoFar.increment();
      }
      taskRuntimeMillis.update(runTimeWatch.elapsed(TimeUnit.MILLISECONDS));
      runTimeWatch.reset();
    }

    public void contractError() {
      totalContractBreaks.increment();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Total Success Runs : ").append(totalSuccessRunsSoFar).append(System.lineSeparator());
      sb.append("Total Failed Runs : ").append(totalFailedRunsSoFar).append(System.lineSeparator());
      sb.append("Total Contract Breaks : ").append(totalContractBreaks).append(System.lineSeparator());
      sb.append("Current Task State : ");
      // Note: this is an unprotected check and may not be accurate, but for logging it should be fine
      if (runTimeWatch.isRunning()) {
        sb.append("Running since ").append(runTimeWatch.elapsed(TimeUnit.MILLISECONDS))
          .append(" millis").append(System.lineSeparator());
      } else {
        sb.append(" Idle ");
      }
      return sb.toString();
    }

  }

  private static final class PerTaskLoadCollector {
    // total times this task was added to the run q for other service instances to pick due to load on this
    // service instance
    private final Counter totalAddedToRunSet;
    // total times this task was removed from the run q by this service instance
    private final Counter totalRemovedFromRunSet;
    // total time it was in run q before it was picked for running
    private final Histogram timeInRunSet;
    // total times threshold check indicated high load on this service instance
    private final Counter totalFailedThresholdChecks;

    public PerTaskLoadCollector(String baseMetricName) {
      totalAddedToRunSet = Metrics.newCounter(loadName(baseMetricName, "added"), Metrics.ResetType.NEVER);
      totalRemovedFromRunSet = Metrics.newCounter(loadName(baseMetricName, "removed"), Metrics.ResetType.NEVER);
      timeInRunSet = Metrics.newHistogram(loadName(baseMetricName, "q_time_ms"), Metrics.ResetType.NEVER);
      totalFailedThresholdChecks = Metrics.newCounter(loadName(baseMetricName, "threshold_crossed"),
        Metrics.ResetType.NEVER);
    }

    private static String loadName(String base, String metric) {
      return Metrics.join(base, "load", metric);
    }

    public void addedToRunSet() {
      totalAddedToRunSet.increment();
    }

    public void crossedThreshold() {
      totalFailedThresholdChecks.increment();
    }

    public void removedFromRunSet(long cTime) {
      totalRemovedFromRunSet.increment();
      timeInRunSet.update(System.currentTimeMillis() - cTime);
    }

    @Override
    public String toString() {
        return "Total Added to Run Set : " + totalAddedToRunSet + System.lineSeparator() +
        "Total Removed From Run Set : " + totalRemovedFromRunSet + System.lineSeparator() +
        "Number of times load threshold was high : " + totalFailedThresholdChecks + System.lineSeparator();
    }
  }

  private static final class PerTaskRecoveryCollector {
    private static final int REJECT_REASONS_SIZE = SchedulerEvents.RecoveryRejectReason.values().length;
    private final Counter totalMonitoringStarted;
    private final Counter totalMonitoringStopped;
    private final Counter totalTimesRequested;
    private final Counter totalTimesRecovered;
    private final Counter totalAddedToDeathWatch;
    private final Counter totalRunsOnDeath;
    private final Counter totalRunsFailedOnDeath;
    private final Counter[] totalTimesRejected;

    private PerTaskRecoveryCollector(String baseMetricName) {
      totalMonitoringStarted = Metrics.newCounter(recoveryName(baseMetricName, "started"), Metrics.ResetType.NEVER);
      totalMonitoringStopped = Metrics.newCounter(recoveryName(baseMetricName, "stopped"), Metrics.ResetType.NEVER);
      totalTimesRequested = Metrics.newCounter(recoveryName(baseMetricName, "requested"), Metrics.ResetType.NEVER);
      totalTimesRecovered = Metrics.newCounter(recoveryName(baseMetricName, "recovered"), Metrics.ResetType.NEVER);
      totalAddedToDeathWatch = Metrics.newCounter(recoveryName(baseMetricName, "death_watch"), Metrics.ResetType.NEVER);
      totalRunsOnDeath = Metrics.newCounter(recoveryName(baseMetricName, "runs_on_death"), Metrics.ResetType.NEVER);
      totalRunsFailedOnDeath = Metrics.newCounter(recoveryName(baseMetricName, "runs_failed_on_death"),
        Metrics.ResetType.NEVER);
      totalTimesRejected = new Counter[REJECT_REASONS_SIZE];
      final SchedulerEvents.RecoveryRejectReason[] reasons = SchedulerEvents.RecoveryRejectReason.values();
      for (int i = 0; i < REJECT_REASONS_SIZE; i++) {
        totalTimesRejected[i] = Metrics.newCounter(recoveryRejectedName(baseMetricName, reasons[i]),
          Metrics.ResetType.NEVER);
      }
    }

    private static String recoveryName(String base, String metric) {
      return Metrics.join(base, "recovery_monitor", metric);
    }

    private static String recoveryRejectedName(String base, SchedulerEvents.RecoveryRejectReason reason) {
      return Metrics.join(base, "recovery_monitor", "rejected", reason.getName());
    }

    private void recoveryMonitoringStarted() {
      totalMonitoringStarted.increment();
    }

    public void recoveryMonitoringStopped() {
      totalMonitoringStopped.increment();
    }

    public void recoveryRequested() {
      totalTimesRequested.increment();
    }

    public void recovered() {
      totalTimesRecovered.increment();
    }

    public void recoveryRejected(SchedulerEvents.RecoveryRejectReason reason) {
      totalTimesRejected[reason.ordinal()].increment();
    }

    public void addedToDeathWatch() {
      totalAddedToDeathWatch.increment();
    }

    public void runOnDeath() {
      totalRunsOnDeath.increment();
    }

    public void failedToRunOnDeath() {
      totalRunsFailedOnDeath.increment();
    }

    @Override
    public String toString() {
      String ret = "Total times recovery monitoring stopped : " + totalMonitoringStopped + System.lineSeparator() +
        "Total times recovery monitoring started : " + totalMonitoringStarted + System.lineSeparator() +
        "Total times recovery was requested : " + totalTimesRequested + System.lineSeparator() +
        "Total times recovered : " + totalTimesRecovered + System.lineSeparator() +
        "Added to death watch : " + totalAddedToDeathWatch + System.lineSeparator() +
        "Total Runs on death : " + totalRunsOnDeath + System.lineSeparator() +
        "Total Failed Runs on death : " + totalRunsFailedOnDeath + System.lineSeparator();
      StringBuilder sb = new StringBuilder(ret);
      SchedulerEvents.RecoveryRejectReason[] reasons = SchedulerEvents.RecoveryRejectReason.values();
      for (int i = 0; i < REJECT_REASONS_SIZE; i++) {
        sb.append("Total times rejected due to ").append(reasons[i].getName()).append(" : ")
          .append(totalTimesRejected[i]).append(System.lineSeparator());
      }
      return sb.toString();
    }
  }
}

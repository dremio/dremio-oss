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

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.telemetry.api.metrics.CounterWithOutcome;
import com.dremio.telemetry.api.metrics.SimpleCounter;
import com.dremio.telemetry.api.metrics.SimpleDistributionSummary;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.TimeUnit;

/** Collects Per Task stats for the {@code ClusteredSingletonTaskScheduler} */
final class PerTaskStatsCollector implements SchedulerEvents.PerTaskEvents {
  // wrapper to a modifiable schedule
  private final PerTaskSchedule schedule;
  // number of schedule modifications so far
  private final SimpleCounter totalScheduleModifications;
  private final CounterWithOutcome ownerQueriesCounter;
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
    final Tags taskNameTag = getTaskNameTag();
    this.totalScheduleModifications =
        SimpleCounter.of(
            getMetricsName("modifications"),
            "Tracks task's schedule modification count",
            taskNameTag);
    this.ownerQueriesCounter =
        CounterWithOutcome.of(
            getMetricsName("owner_queries"), "Tracks task's get owner node requests", taskNameTag);
    this.bookingStats = new PerTaskBookingCollector(taskNameTag);
    this.runStats = new PerTaskRunCollector(taskNameTag);
    this.loadStats = new PerTaskLoadCollector(taskNameTag);
    this.recoveryStats = new PerTaskRecoveryCollector(taskNameTag);
  }

  private Tags getTaskNameTag() {
    return Tags.of(Tag.of("task_name", schedule.getTaskName().toLowerCase()));
  }

  private static String getMetricsName(String metricName) {
    return Joiner.on(".").join(BASE_METRIC_NAME, "task", metricName);
  }

  private static String getMetricsNameWithPrefix(String prefix, String metricName) {
    return Joiner.on(".").join(BASE_METRIC_NAME, "task", prefix, metricName);
  }

  @Override
  public void bookingAttempted() {
    bookingStats.bookingAttempted();
  }

  @Override
  public void bookingAcquired(long bookingOwnerSessionId) {
    bookingStats.bookingAcquired();
    runStats.expectRuns();
  }

  @Override
  public void bookingReleased() {
    bookingStats.bookingReleased();
    runStats.runsUnlikely();
  }

  @Override
  public void bookingLost() {
    bookingStats.bookingLost();
    runStats.runsUnlikely();
  }

  @Override
  public void bookingRechecked() {
    bookingStats.bookingRechecked();
  }

  @Override
  public void bookingRegained() {
    bookingStats.bookingRegained();
    runStats.expectRuns();
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
    ownerQueriesCounter.succeeded();
  }

  @Override
  public void noTaskOwnerFound() {
    ownerQueriesCounter.errored();
  }

  @Override
  public void taskOwnerQueryFailed() {
    ownerQueriesCounter.errored();
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
  public void weightShed(int weight) {
    loadStats.weightShed(weight);
  }

  @Override
  public void weightGained(int weight) {
    loadStats.weightGained(weight);
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
    return "Task Name : "
        + schedule.getTaskName()
        + System.lineSeparator()
        + "Period : "
        + schedule.getSchedule().getPeriod()
        + System.lineSeparator()
        + "Is Single Shot? : "
        + schedule.getSchedule().isToRunExactlyOnce()
        + System.lineSeparator()
        + "Total Schedule Modifications : "
        + totalScheduleModifications
        + System.lineSeparator()
        + "Per Task Booking Stats : "
        + System.lineSeparator()
        + bookingStats
        + System.lineSeparator()
        + "Per Task Run Stats : "
        + System.lineSeparator()
        + runStats
        + System.lineSeparator()
        + "Per Task Load Stats : "
        + System.lineSeparator()
        + loadStats
        + System.lineSeparator()
        + "Per Task Recovery Stats : "
        + System.lineSeparator()
        + recoveryStats
        + System.lineSeparator();
  }

  /** Booking stats collection. */
  private static final class PerTaskBookingCollector {
    // number of attempts made to book the slot by this service instance
    private final SimpleCounter totalBookingAttempts;
    // number of successful attempts to book the slot
    private final SimpleCounter totalAcquiredBookings;
    // number of explicit release of booked slot
    private final SimpleCounter totalReleasedBookings;
    private final SimpleCounter totalLostBookings;
    private final SimpleCounter totalRecheckedBookings;
    private final SimpleCounter totalRegainedBookings;

    private PerTaskBookingCollector(Tags taskNameTag) {
      totalBookingAttempts =
          SimpleCounter.of(
              bookName("attempts"),
              "Tracks number of booking attempts made to request ownership",
              taskNameTag);
      totalAcquiredBookings =
          SimpleCounter.of(bookName("acquired"), "Tracks ownership acquired count", taskNameTag);
      totalReleasedBookings =
          SimpleCounter.of(
              bookName("released"), "Tracks ownership relinquished voluntarily count", taskNameTag);
      totalLostBookings =
          SimpleCounter.of(
              bookName("lost"), "Tracks ownership lost involuntarily count", taskNameTag);
      totalRecheckedBookings =
          SimpleCounter.of(bookName("rechecked"), "Tracks ownership rechecked count", taskNameTag);
      totalRegainedBookings =
          SimpleCounter.of(bookName("regained"), "Tracks ownership regained count", taskNameTag);
    }

    private static String bookName(String metricName) {
      return getMetricsNameWithPrefix("book", metricName);
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

    public void bookingLost() {
      totalLostBookings.increment();
    }

    public void bookingRechecked() {
      totalRecheckedBookings.increment();
    }

    public void bookingRegained() {
      totalRegainedBookings.increment();
    }

    @Override
    public String toString() {
      return "Total Booking Attempts : "
          + totalBookingAttempts
          + System.lineSeparator()
          + "Total Acquired Bookings : "
          + totalAcquiredBookings
          + System.lineSeparator()
          + "Total Released Bookings : "
          + totalReleasedBookings
          + System.lineSeparator()
          + "Total Lost Bookings : "
          + totalLostBookings
          + System.lineSeparator()
          + "Total Rechecked Bookings : "
          + totalRecheckedBookings
          + System.lineSeparator()
          + "Total Regained Bookings : "
          + totalRegainedBookings
          + System.lineSeparator();
    }
  }

  private static final class PerTaskRunCollector {
    // successful and errored runs so far on this service instance
    private final CounterWithOutcome runsSoFarCounter;
    // distribution of task run time
    private final SimpleDistributionSummary taskRuntimeMillis;
    // number of times a run was made without booking (this should be 0 always)
    private final SimpleCounter totalContractBreaks;
    // task run time watch, reset after every run
    private final Stopwatch runTimeWatch;
    // whether a run is expected (only if booking is on)
    private volatile boolean runsExpected;

    private PerTaskRunCollector(Tags taskNameTag) {
      runsSoFarCounter =
          CounterWithOutcome.of(runName("so_far"), "Tracks task's actual runs", taskNameTag);
      taskRuntimeMillis =
          SimpleDistributionSummary.of(runName("runtime_ms"), "Record task's runtime", taskNameTag);
      totalContractBreaks =
          SimpleCounter.of(
              runName("contract_breaks"),
              "Tracks runs attempted without a booking (should be zero always)",
              taskNameTag);
      runTimeWatch = Stopwatch.createUnstarted();
      runsExpected = false;
    }

    private static String runName(String metric) {
      return getMetricsNameWithPrefix("runs", metric);
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
        runsSoFarCounter.succeeded();
      } else {
        runsSoFarCounter.errored();
      }
      taskRuntimeMillis.recordAmount(runTimeWatch.elapsed(TimeUnit.MILLISECONDS));
      runTimeWatch.reset();
    }

    public void contractError() {
      totalContractBreaks.increment();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Total Success Runs : ")
          .append(runsSoFarCounter.countSucceeded())
          .append(System.lineSeparator());
      sb.append("Total Failed Runs : ")
          .append(runsSoFarCounter.countErrored())
          .append(System.lineSeparator());
      sb.append("Total Contract Breaks : ")
          .append(totalContractBreaks)
          .append(System.lineSeparator());
      sb.append("Current Task State : ");
      // Note: this is an unprotected check and may not be accurate, but for logging it should be
      // fine
      if (runTimeWatch.isRunning()) {
        sb.append("Running since ")
            .append(runTimeWatch.elapsed(TimeUnit.MILLISECONDS))
            .append(" millis")
            .append(System.lineSeparator());
      } else {
        sb.append(" Idle ");
      }
      return sb.toString();
    }
  }

  private static final class PerTaskLoadCollector {
    // total times this task was added to the run q for other service instances to pick due to load
    // on this
    // service instance
    private final SimpleCounter totalAddedToRunSet;
    // total times this task was removed from the run q by this service instance
    private final SimpleCounter totalRemovedFromRunSet;
    // total time it was in run q before it was picked for running
    private final SimpleDistributionSummary timeInRunSet;
    // total times threshold check indicated high load on this service instance
    private final SimpleCounter totalFailedThresholdChecks;
    // total cumulative weight that was shed by the instance for this task
    private final SimpleCounter weightShed;
    // total cumulative weight that was gained by the instance for this task
    private final SimpleCounter weightGained;

    public PerTaskLoadCollector(Tags taskNameTag) {
      totalAddedToRunSet =
          SimpleCounter.of(loadName("added"), "Tracks load shedding requests", taskNameTag);
      totalRemovedFromRunSet =
          SimpleCounter.of(
              loadName("removed"),
              "Tracks task ownership accepted count to balance load",
              taskNameTag);
      timeInRunSet =
          SimpleDistributionSummary.of(
              loadName("q_time_ms"),
              "Records the amount of time the task was in queue waiting to be load balanced",
              taskNameTag);
      totalFailedThresholdChecks =
          SimpleCounter.of(
              loadName("threshold_crossed"), "Tracks load threshold crossed count", taskNameTag);
      weightShed =
          SimpleCounter.of(
              loadName("weight_shed"),
              "Tracks cumulative weight shed by this instance",
              taskNameTag);
      weightGained =
          SimpleCounter.of(
              loadName("weight_gained"),
              "Tracks cumulative weight gained by this instance",
              taskNameTag);
    }

    private static String loadName(String metric) {
      return getMetricsNameWithPrefix("load", metric);
    }

    public void addedToRunSet() {
      totalAddedToRunSet.increment();
    }

    public void crossedThreshold() {
      totalFailedThresholdChecks.increment();
    }

    public void removedFromRunSet(long cTime) {
      totalRemovedFromRunSet.increment();
      timeInRunSet.recordAmount(System.currentTimeMillis() - cTime);
    }

    public void weightShed(int weight) {
      weightShed.increment(weight);
    }

    public void weightGained(int weight) {
      weightGained.increment(weight);
    }

    @Override
    public String toString() {
      return "Total Added to Run Set : "
          + totalAddedToRunSet
          + System.lineSeparator()
          + "Total Removed From Run Set : "
          + totalRemovedFromRunSet
          + System.lineSeparator()
          + "Number of times load threshold was high : "
          + totalFailedThresholdChecks
          + System.lineSeparator()
          + "Weight Shed : "
          + weightShed
          + System.lineSeparator()
          + "Weight Gained : "
          + weightGained
          + System.lineSeparator();
    }
  }

  private static final class PerTaskRecoveryCollector {
    private static final int REJECT_REASONS_SIZE =
        SchedulerEvents.RecoveryRejectReason.values().length;
    private final SimpleCounter totalMonitoringStarted;
    private final SimpleCounter totalMonitoringStopped;
    private final SimpleCounter totalTimesRequested;
    private final SimpleCounter totalTimesRecovered;
    private final SimpleCounter totalAddedToDeathWatch;
    private final CounterWithOutcome totalRunsOnDeath;
    private final SimpleCounter[] totalTimesRejected;

    private PerTaskRecoveryCollector(Tags taskNameTag) {
      totalMonitoringStarted =
          SimpleCounter.of(
              recoveryName("started"), "Tracks recovery monitoring started count", taskNameTag);
      totalMonitoringStopped =
          SimpleCounter.of(
              recoveryName("stopped"), "Tracks recovery monitoring stopped count", taskNameTag);
      totalTimesRequested =
          SimpleCounter.of(
              recoveryName("requested"), "Tracks recovery requested count", taskNameTag);
      totalTimesRecovered =
          SimpleCounter.of(recoveryName("recovered"), "Tracks recovered count", taskNameTag);
      totalAddedToDeathWatch =
          SimpleCounter.of(recoveryName("death_watch"), "Tracks death watch count", taskNameTag);
      totalRunsOnDeath =
          CounterWithOutcome.of(
              recoveryName("runs_on_death"), "Tracks runs on a node death", taskNameTag);
      totalTimesRejected = new SimpleCounter[REJECT_REASONS_SIZE];
      final SchedulerEvents.RecoveryRejectReason[] reasons =
          SchedulerEvents.RecoveryRejectReason.values();
      for (int i = 0; i < REJECT_REASONS_SIZE; i++) {
        totalTimesRejected[i] =
            SimpleCounter.of(
                recoveryName("rejected"),
                "Tracks recovery rejected count",
                taskNameTag.and(Tag.of("reason", reasons[i].getName().toLowerCase())));
      }
    }

    private static String recoveryName(String metric) {
      return getMetricsNameWithPrefix("recovery_monitor", metric);
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
      totalRunsOnDeath.succeeded();
    }

    public void failedToRunOnDeath() {
      totalRunsOnDeath.errored();
    }

    @Override
    public String toString() {
      String ret =
          "Total times recovery monitoring stopped : "
              + totalMonitoringStopped
              + System.lineSeparator()
              + "Total times recovery monitoring started : "
              + totalMonitoringStarted
              + System.lineSeparator()
              + "Total times recovery was requested : "
              + totalTimesRequested
              + System.lineSeparator()
              + "Total times recovered : "
              + totalTimesRecovered
              + System.lineSeparator()
              + "Added to death watch : "
              + totalAddedToDeathWatch
              + System.lineSeparator()
              + "Total Runs on death : "
              + totalRunsOnDeath.countSucceeded()
              + System.lineSeparator()
              + "Total Failed Runs on death : "
              + totalRunsOnDeath.countErrored()
              + System.lineSeparator();
      StringBuilder sb = new StringBuilder(ret);
      SchedulerEvents.RecoveryRejectReason[] reasons =
          SchedulerEvents.RecoveryRejectReason.values();
      for (int i = 0; i < REJECT_REASONS_SIZE; i++) {
        sb.append("Total times rejected due to ")
            .append(reasons[i].getName())
            .append(" : ")
            .append(totalTimesRejected[i])
            .append(System.lineSeparator());
      }
      return sb.toString();
    }
  }
}

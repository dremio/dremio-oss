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

import com.dremio.exec.proto.CoordinationProtos;
import com.google.common.base.Stopwatch;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Does periodic info logging (summary) of events happening every few minute(s).
 *
 * <p>If no events happen for the given period, nothing will be logged. Only important events are
 * tracked and logged. By logging periodically, the danger of info logging while holding a mutex and
 * associated performance problems are eliminated.
 */
final class TaskInfoLogger implements SchedulerEvents, AutoCloseable {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(TaskInfoLogger.class);
  private static final int LOG_INTERVAL_SECONDS = 120;
  private final ClusteredSingletonCommon schedulerCommon;
  private final Map<String, PerTaskInfoLogger> allTasks;
  private final AtomicBoolean membershipChanged;
  private int currentMembership;
  private volatile ScheduledFuture<?> logTask;

  TaskInfoLogger(ClusteredSingletonCommon schedulerCommon) {
    this.schedulerCommon = schedulerCommon;
    this.currentMembership = 0;
    this.allTasks = new ConcurrentHashMap<>();
    this.membershipChanged = new AtomicBoolean(true);
    this.logTask = null;
  }

  void start() {
    logTask =
        schedulerCommon
            .getSchedulePool()
            .scheduleAtFixedRate(
                this::logSummary, LOG_INTERVAL_SECONDS, LOG_INTERVAL_SECONDS, TimeUnit.SECONDS);
  }

  void logSummary() {
    if (membershipChanged.compareAndSet(true, false)) {
      LOGGER.info(
          "Service Info: Name = {}, Version = {}, Endpoint = {}, Instance count = {}, Run Path = {}, Done Path = {},"
              + "Versioned Done Path = {}",
          schedulerCommon.getFqServiceName(),
          schedulerCommon.getServiceVersion(),
          schedulerCommon.getThisEndpoint(),
          currentMembership,
          schedulerCommon.getStealFqPath(),
          schedulerCommon.getUnVersionedDoneFqPath(),
          schedulerCommon.getVersionedDoneFqPath());
    }
    allTasks.values().forEach((x) -> x.logEvents(schedulerCommon.getThisEndpoint().getAddress()));
  }

  @Override
  public PerTaskEvents addTask(PerTaskSchedule schedule) {
    return allTasks.computeIfAbsent(schedule.getTaskName(), (k) -> new PerTaskInfoLogger(schedule));
  }

  @Override
  public void hitUnexpectedError() {
    // nothing to log
  }

  @Override
  public void taskDone(String taskName) {
    allTasks.remove(taskName);
  }

  @Override
  public void membershipChanged(int newCount) {
    currentMembership = newCount;
    // following will guarantee happens before due to volatile write
    membershipChanged.set(true);
  }

  @Override
  public void tasksAddedToMembership(int taskCount) {
    // not logged
  }

  @Override
  public void tasksRemovedFromMembership(int taskCount) {
    // not logged
  }

  @Override
  public void runSetSize(int currentSize) {
    // not logged
  }

  @Override
  public void computedWeight(int currentWeight) {}

  @Override
  public void close() throws Exception {
    if (logTask != null) {
      logTask.cancel(false);
      try {
        if (!logTask.isDone()) {
          logTask.get(5, TimeUnit.SECONDS);
        }
      } catch (Exception e) {
        LOGGER.warn("Unable to close logger for clustered singleton", e);
      }
      logSummary();
      logTask = null;
    }
  }

  private static final class PerTaskInfoLogger implements SchedulerEvents.PerTaskEvents {
    private final PerTaskSchedule schedule;
    private final AtomicLong numRunsSoFar;
    private final Queue<LogEvent> currentLogEventQ;
    private final Stopwatch runTimeWatch;
    private long lastLoggedNumRuns;
    private volatile long averageElapsedTime;

    PerTaskInfoLogger(PerTaskSchedule schedule) {
      this.lastLoggedNumRuns = 0;
      this.averageElapsedTime = 0;
      this.schedule = schedule;
      this.numRunsSoFar = new AtomicLong(0L);
      // These are used only for rare events that needs to be logged. So let us use a lock-free
      // but thread safe queue
      this.currentLogEventQ = new ConcurrentLinkedDeque<>();
      this.runTimeWatch = Stopwatch.createUnstarted();
    }

    @Override
    public void bookingAttempted() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.BOOKING_ATTEMPTED));
    }

    @Override
    public void bookingAcquired(long bookingOwnerSessionId) {
      this.currentLogEventQ.add(new BookingAcquiredEvent(bookingOwnerSessionId));
    }

    @Override
    public void bookingReleased() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.BOOKING_RELEASED));
    }

    @Override
    public void bookingLost() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.BOOKING_LOST));
    }

    @Override
    public void bookingRechecked() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.BOOKING_RECHECK));
    }

    @Override
    public void bookingRegained() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.BOOKING_REGAINED));
    }

    @Override
    public void contractError() {
      // do immediate logging at warning level as this should never happen
      LOGGER.warn(
          "Internal Error: Unexpected run without having a booking for task {}",
          schedule.getTaskName());
    }

    @Override
    public void runStarted() {
      this.numRunsSoFar.incrementAndGet();
      this.runTimeWatch.start();
    }

    @Override
    public void runEnded(boolean success) {
      this.runTimeWatch.stop();
      this.averageElapsedTime =
          this.runTimeWatch.elapsed(TimeUnit.MILLISECONDS) / this.numRunsSoFar.get();
    }

    @Override
    public void scheduleModified() {
      this.currentLogEventQ.add(new ScheduleModifiedEvent(schedule.getSchedule()));
    }

    @Override
    public void taskOwnerQuery(CoordinationProtos.NodeEndpoint nodeEndpoint) {
      this.currentLogEventQ.add(new TaskOwnerQueryEvent(nodeEndpoint));
    }

    @Override
    public void noTaskOwnerFound() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.TASK_NO_OWNER_FOUND));
    }

    @Override
    public void taskOwnerQueryFailed() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.TASK_OWNER_QUERY_FAILED));
    }

    @Override
    public void addedToRunSet() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.ADDED_TO_RUN_SET));
    }

    @Override
    public void crossedThreshold() {
      // not logged
    }

    @Override
    public void removedFromRunSet(long cTime) {
      this.currentLogEventQ.add(new RemovedFromRunSetEvent(System.currentTimeMillis() - cTime));
    }

    @Override
    public void weightShed(int weight) {
      // not logged
    }

    @Override
    public void weightGained(int weight) {
      // not logged
    }

    @Override
    public void recoveryMonitoringStarted() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.RECOVERY_MONITORING_STARTED));
    }

    @Override
    public void recoveryMonitoringStopped() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.RECOVERY_MONITORING_STOPPED));
    }

    @Override
    public void recoveryRequested() {
      // not logged
    }

    @Override
    public void recoveryRejected(RecoveryRejectReason reason) {
      this.currentLogEventQ.add(new RecoveryRejectedEvent(reason));
    }

    @Override
    public void recovered() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.RECOVERED));
    }

    @Override
    public void addedToDeathWatch() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.ADDED_TO_DEATH_WATCH));
    }

    @Override
    public void runOnDeath() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.RUN_ON_DEATH));
    }

    @Override
    public void failedToRunOnDeath() {
      this.currentLogEventQ.add(new DefaultLogEvent(LogEventType.RUN_ON_DEATH_FAILED));
    }

    private void logEvents(String hostAddress) {
      while (true) {
        LogEvent next = this.currentLogEventQ.poll();
        if (next == null) {
          break;
        }
        LOGGER.info(
            "Clustered Singleton Event on host {} : {}",
            hostAddress,
            next.getLogString(schedule.getTaskName()));
      }
      final long numRuns = numRunsSoFar.get();
      // this is a dirty check, but it is ok since it is only for logging
      final boolean running = runTimeWatch.isRunning();
      if (numRuns > lastLoggedNumRuns || running) {
        lastLoggedNumRuns = numRuns;
        LOGGER.info(
            "Clustered Singleton Task {} Run Information: Running = {}, Average Run Time Millis = {}",
            schedule.getTaskName(),
            running,
            averageElapsedTime);
      }
    }
  }

  private abstract static class LogEvent {
    private final LogEventType logEventType;
    // milli second granularity time stamp
    private final long timeStamp;

    private LogEvent(LogEventType type) {
      this.logEventType = type;
      this.timeStamp = System.currentTimeMillis();
    }

    protected LogEventType getLogEventType() {
      return logEventType;
    }

    protected long getEventTimeStamp() {
      return timeStamp;
    }

    String getLogString(String taskName) {
      return String.format(
          logEventType.getFormatString(), Instant.ofEpochMilli(timeStamp), taskName);
    }
  }

  private static final class DefaultLogEvent extends LogEvent {
    private DefaultLogEvent(LogEventType type) {
      super(type);
    }
  }

  private static final class BookingAcquiredEvent extends LogEvent {
    private final long bookingOwnerSessionId;

    private BookingAcquiredEvent(long bookingOwnerSessionId) {
      super(LogEventType.BOOKING_ACQUIRED);
      this.bookingOwnerSessionId = bookingOwnerSessionId;
    }

    @Override
    String getLogString(String taskName) {
      return String.format(
          getLogEventType().getFormatString(),
          Instant.ofEpochMilli(getEventTimeStamp()),
          taskName,
          bookingOwnerSessionId);
    }
  }

  private static final class RemovedFromRunSetEvent extends LogEvent {
    private final long totalTimeInRunSetMillis;

    private RemovedFromRunSetEvent(long totalTimeInRunSetMillis) {
      super(LogEventType.REMOVED_FROM_RUN_SET);
      this.totalTimeInRunSetMillis = totalTimeInRunSetMillis;
    }

    @Override
    String getLogString(String taskName) {
      return String.format(
          getLogEventType().getFormatString(),
          Instant.ofEpochMilli(getEventTimeStamp()),
          taskName,
          totalTimeInRunSetMillis);
    }
  }

  private static final class RecoveryRejectedEvent extends LogEvent {
    private final RecoveryRejectReason reason;

    private RecoveryRejectedEvent(RecoveryRejectReason reason) {
      super(LogEventType.RECOVERY_REJECTED);
      this.reason = reason;
    }

    @Override
    String getLogString(String taskName) {
      return String.format(
          getLogEventType().getFormatString(),
          Instant.ofEpochMilli(getEventTimeStamp()),
          taskName,
          reason.getName());
    }
  }

  private static final class TaskOwnerQueryEvent extends LogEvent {
    private final CoordinationProtos.NodeEndpoint ownerEndpoint;

    private TaskOwnerQueryEvent(CoordinationProtos.NodeEndpoint ownerEndpoint) {
      super(LogEventType.TASK_OWNER_QUERY);
      this.ownerEndpoint = ownerEndpoint;
    }

    @Override
    String getLogString(String taskName) {
      return String.format(
          getLogEventType().getFormatString(),
          Instant.ofEpochMilli(getEventTimeStamp()),
          taskName,
          ownerEndpoint.getAddress(),
          ownerEndpoint.getFabricPort());
    }
  }

  private static final class ScheduleModifiedEvent extends LogEvent {
    private final Schedule newSchedule;

    private ScheduleModifiedEvent(Schedule newSchedule) {
      super(LogEventType.SCHEDULE_MODIFIED);
      this.newSchedule = newSchedule;
    }

    @Override
    String getLogString(String taskName) {
      return String.format(
          getLogEventType().getFormatString(),
          Instant.ofEpochMilli(getEventTimeStamp()),
          taskName,
          newSchedule);
    }
  }

  private enum LogEventType {
    BOOKING_ATTEMPTED("%s: Slot Booking attempted for task %s"),
    BOOKING_ACQUIRED("%s: Slot Booked for task %s. Session id is %s"),
    BOOKING_RELEASED("%s: Slot Released for task %s"),
    BOOKING_LOST("%s: Booking lost for task %s"),
    BOOKING_RECHECK("%s: Checking if booking is still available for task %s"),
    BOOKING_REGAINED("%s: Booking regained for task %s"),
    RECOVERED("%s: Task %s successfully recovered"),
    RECOVERY_REJECTED("%s: Recovery rejected for task %s due to %s "),
    SCHEDULE_MODIFIED("%s: Schedule Modified for task %s. New Scheduled Details : %s"),
    ADDED_TO_RUN_SET(
        "%s: Relinquished control of Task %s and added to run set due to high load locally"),
    REMOVED_FROM_RUN_SET("%s: Gained control of Task %s and removed from run set after %dms"),
    RUN_ON_DEATH("%s: Task %s ran due to death of an instance"),
    TASK_OWNER_QUERY("%s: Current owner for task %s is %s:%d"),
    TASK_NO_OWNER_FOUND("%s: No current owner for task %s"),
    TASK_OWNER_QUERY_FAILED("%s: Could not ascertain current owner for task %s due to bad data"),
    RUN_ON_DEATH_FAILED("%s: Task %s could not get booking to run on death of an instance"),
    ADDED_TO_DEATH_WATCH("%s: Task %s added to death watch set"),
    RECOVERY_MONITORING_STARTED("%s: Recovery monitoring started for task %s"),
    RECOVERY_MONITORING_STOPPED("%s: Recovery monitoring stopped for task %s");

    private final String formatString;

    LogEventType(String formatString) {
      this.formatString = formatString;
    }

    private String getFormatString() {
      return formatString;
    }
  }
}

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

import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.CREATE_EPHEMERAL;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.DELETE;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.PathCommand;
import static com.dremio.service.scheduler.Schedule.SingleShotType.RUN_ONCE_EVERY_MEMBER_DEATH;
import static com.dremio.service.scheduler.TaskDoneHandler.PerTaskDoneInfo;
import static com.dremio.service.scheduler.TaskLoadController.PerTaskLoadInfo;
import static com.dremio.service.scheduler.TaskRecoveryMonitor.PerTaskRecoveryInfo;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.io.file.Path;
import com.dremio.service.coordinator.exceptions.PathExistsException;
import com.dremio.service.coordinator.exceptions.PathMissingException;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Wraps the actual task runner of the schedule in order to recover schedules on failure and ensure the singleton rule
 * on the running of the scheduled tasks across service instances, among other things.
 * <p>
 * Monitors and manages schedules of a single task. Responsibilities include:
 * 1. Keep track of schedules and run the task on time, enforcing cluster wide singleton rule
 * 2. Provide recovery methods for another service instance to take over
 * 3. Allow other instances to steal the task if this instance is heavily loaded
 * 4. Monitor and collect statistics of every task run for improved visibility
 * </p>
 */
final class PerTaskScheduleTracker implements Runnable, Cancellable, PerTaskRecoveryInfo, PerTaskDoneInfo,
  PerTaskLoadInfo {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PerTaskScheduleTracker.class);
  private static final String BOOK_PATH_NAME = "book";
  // max amount of time to wait for completion by another node for immediate one shot lock step schedules
  private static final int WAIT_TIME_FOR_DONE_SECONDS = 60;
  // Amount of time to wait at each iteration to check if a cancelled task is complete
  private static final int MAX_RUN_WAIT_TIME_MILLIS = 1;
  // Keep the booking even after a schedule is cancelled for these many seconds
  private static final int MAX_TIME_TO_WAIT_POST_CANCEL_SECONDS = 90;
  // keep cancelled schedules for these many seconds
  private static final int MAX_AGE_TO_KEEP_CANCELLED_SCHEDULE_SECONDS = 24 * 60 * 60 * 30;

  private final String taskName;
  private final Runnable actualTask;
  private final ClusteredSingletonCommon schedulerCommon;
  private final String taskFqPath;
  private final String bookFqPathLocal;
  private final CloseableThreadPool runningPool;
  private final AtomicReference<Iterator<Instant>> instantsRef;
  private final ReentrantLock cancelLock;
  private final Condition doneCondition;
  private final TaskDoneHandler.PerTaskDoneHandler doneHandler;
  private final TaskRecoveryMonitor.PerTaskRecoveryMonitor recoveryMonitor;
  private final TaskLoadController.PerTaskLoadController loadController;
  private final SchedulerEvents.PerTaskEvents eventsCollector;
  private final Consumer<PerTaskScheduleTracker> cancelHandler;
  // this needs to be volatile as isCancelled check is not under a lock
  private volatile boolean cancelled;
  private volatile boolean bookingOwner;
  private volatile boolean taskDone;
  private volatile Instant lastRun;
  private ScheduledFuture<?> scheduledTaskFuture;
  private Future<?> runningTaskFuture;
  private Instant firstRun;
  private Instant cancelTime;
  private Schedule currentSchedule;

  PerTaskScheduleTracker(Schedule schedule,
                         Runnable task,
                         String taskFqPath,
                         String taskFqPathForBook,
                         boolean bookingOwner,
                         String taskName,
                         CloseableThreadPool runningPool,
                         ClusteredSingletonCommon schedulerCommon,
                         Consumer<PerTaskScheduleTracker> cancelHandler,
                         TaskLoadController loadControllerManager,
                         TaskCompositeEventCollector eventCollectorManager,
                         TaskRecoveryMonitor recoveryMonitorManager,
                         TaskDoneHandler doneHandlerManager) {
    this.schedulerCommon = schedulerCommon;
    this.cancelHandler = cancelHandler;
    this.currentSchedule = schedule;
    this.instantsRef = new AtomicReference<>(schedule.iterator());
    this.actualTask = task;
    this.taskName = taskName;
    this.taskFqPath = taskFqPath;
    this.bookFqPathLocal = taskFqPathForBook + Path.SEPARATOR + BOOK_PATH_NAME;
    this.firstRun = Instant.MIN;
    this.lastRun = Instant.MIN;
    this.cancelled = false;
    this.taskDone = false;
    this.runningTaskFuture = null;
    this.runningPool = runningPool;
    this.cancelLock = new ReentrantLock();
    this.doneCondition = cancelLock.newCondition();
    this.cancelTime = null;
    this.bookingOwner = bookingOwner;
    // Order is important as event call backs may arrive as soon as each of these managers know about this task
    this.eventsCollector = eventCollectorManager.addTask(this);
    this.recoveryMonitor = recoveryMonitorManager.addTask(this, eventsCollector);
    this.loadController = loadControllerManager.addTask(this, eventsCollector);
    this.doneHandler = doneHandlerManager.addTask(this);
  }

  PerTaskScheduleTracker(Schedule schedule,
                         Runnable task,
                         String taskFqPath,
                         CloseableThreadPool runningPool,
                         ClusteredSingletonCommon schedulerCommon,
                         Consumer<PerTaskScheduleTracker> cancelHandler,
                         TaskLoadController loadControllerManager,
                         TaskCompositeEventCollector eventCollectorManager,
                         TaskRecoveryMonitor recoveryMonitorManager,
                         TaskDoneHandler doneHandlerManager) {
    this(schedule, task, taskFqPath, taskFqPath, false, schedule.getTaskName(), runningPool, schedulerCommon,
      cancelHandler, loadControllerManager, eventCollectorManager, recoveryMonitorManager, doneHandlerManager);
  }

  @Override
  public Schedule getSchedule() {
    return currentSchedule;
  }

  @Override
  public String getTaskName() {
    return taskName;
  }

  @Override
  public String getBookFqPathLocal() {
    return bookFqPathLocal;
  }

  @Override
  public String getTaskFqPath() {
    return taskFqPath;
  }

  @Override
  public boolean isBookingOwner() {
    return bookingOwner;
  }

  /**
   * Starts the schedule.
   * <p>
   * Called independently on all service instances, but only one service instance that can create the booking
   * will be able to run and track the schedule.
   * startRun is called at two places as follows:
   * 1. When the schedule is first created on an instance
   * 2. When an attempt is made to recover the schedule {@link this#tryRecover()} on this instance
   * </p>
   * @param recover true if called from recovery path, false otherwise
   */
  void startRun(boolean recover) {
    // try booking. Create the schedule only if we can book successfully.
    final boolean booked = tryBook();
    if (booked) {
      // now we are the task owner and we loose ownership only if we die or explicitly relinquish.
      // remove from run set, if it is still in run set
      loadController.removeFromRunSet();
      // save the last schedule time for recovery of lastRun
      recoveryMonitor.storeScheduleTime();
      // Booking success. Start the schedule
      long delay = ChronoUnit.MILLIS.between(Instant.now(), nextInstant());
      cancelLock.lock();
      try {
        if (!taskDone) {
          scheduledTaskFuture = schedulerCommon.getSchedulePool().schedule(this, delay, TimeUnit.MILLISECONDS);
        } else {
          LOGGER.info("Task {} is done. Ignoring scheduling request", taskName);
        }
      } finally {
        cancelLock.unlock();
      }
    } else {
      recoveryMonitor.setRecoveryWatch();
      if (!recover) {
        awaitImmediateTaskCompletion();
      }
    }
  }

  /**
   * Sets a new schedule.
   * <p>
   * Mainly to support schedule modifications on the fly and/or allow chaining of single-shot schedules.
   * It is assumed that schedule changes are propagated externally to all service instances.
   * </p>
   *
   * @param newSchedule the modified schedule
   */
  void setNewSchedule(Schedule newSchedule) {
    Preconditions.checkArgument(newSchedule != null, "Modified Schedule cannot be null for task %s", this);
    Preconditions.checkArgument(currentSchedule.getTaskName().equals(newSchedule.getTaskName()),
      "Illegal schedule modification. Cannot change task name from %s to %s", currentSchedule.getTaskName(),
      newSchedule.getTaskName());
    Preconditions.checkArgument(newSchedule.isToRunExactlyOnce() == currentSchedule.isToRunExactlyOnce(),
      "Illegal schedule modification. Cannot change between single shot schedule and normal schedule for task %s", this);
    Preconditions.checkArgument(!newSchedule.isToRunExactlyOnce(),
      "Illegal schedule modification. Cannot modify single shot schedules for task %s", this);
    currentSchedule = newSchedule;
    instantsRef.set(newSchedule.iterator());
    if (currentSchedule.getPeriod() != null) {
      // do not record schedule modifications for single shot chains
      eventsCollector.scheduleModified();
    }
  }

  @Override
  public String toString() {
    return taskName;
  }

  @Override
  public void run() {
    Preconditions.checkState(bookingOwner, "Something went wrong. Must be a owner to run task %s", this);
    boolean addToRunSet = false;
    cancelLock.lock();
    try {
      if (isTaskReallyDone()) {
        return;
      }
      this.scheduledTaskFuture = null;
      if (firstRun.equals(Instant.MIN)) {
        firstRun = Instant.now();
      }
      CloseableThreadPool currentPool = (currentSchedule.isSticky() &&
        runningPool.getActiveCount() >= runningPool.getMaximumPoolSize()) ? schedulerCommon.getStickyPool() :
        runningPool;
      this.runningTaskFuture = currentPool.submit(() -> {
        Exception runError = null;
        eventsCollector.runStarted();
        try {
          actualTask.run();
        } catch (Exception e) {
          // just log the error to avoid thread pool depletion
          runError = e;
        } finally {
          eventsCollector.runEnded(runError == null);
        }
        if (schedulerCommon.isActive()) {
          if (taskDone) {
            // if we are here this is a single shot schedule that is done.. now we are running this only on
            // specific events. Release booking immediately after run
            releaseBooking();
          } else {
            // schedule the next run only if the running pool is not shutting down
            scheduleNextRun(runError);
          }
        }
      });
    } catch (RejectedExecutionException e) {
      if (schedulerCommon.isActive()) {
        if (currentSchedule.isSticky()) {
          scheduledTaskFuture = schedulerCommon.getSchedulePool().schedule(this, 10, TimeUnit.MILLISECONDS);
          eventsCollector.crossedThreshold();
        } else {
          addToRunSet = true;
        }
      }
    } catch (Exception e) {
      // TODO: DX-68347 Exit handler for the schedule pool threads that does a fatal exit
      // just log for now
      LOGGER.warn("Ignoring unexpected exception while scheduling task {}", this, e);
    } finally {
      cancelLock.unlock();
      if (addToRunSet) {
        releaseBooking();
        loadController.addToRunSet();
      }
      if (isTaskReallyDone()) {
        LOGGER.info("Skipping execution of the task {} as {}", this,
          (cancelled) ? "the task was cancelled" : "the task is done");
      }
    }
  }

  public Optional<CoordinationProtos.NodeEndpoint> getCurrentTaskOwner() {
    try {
      byte[] addressBytes = schedulerCommon.getTaskStore().getData(bookFqPathLocal);
      CoordinationProtos.NodeEndpoint nodeEndpoint = CoordinationProtos.NodeEndpoint.parseFrom(addressBytes);
      eventsCollector.taskOwnerQuery(nodeEndpoint);
      return Optional.of(nodeEndpoint);
    } catch (PathMissingException e) {
      eventsCollector.noTaskOwnerFound();
      return Optional.empty();
    } catch (InvalidProtocolBufferException e) {
      eventsCollector.taskOwnerQueryFailed();
      return Optional.empty();
    }
  }

  /**
   * Cancel the task locally. Typically done during shutdown or when a schedule task is no longer required (.
   * <p>
   * Note that this does not mean the task is marked done. Recovery for this can happen and another
   * instance could pick up this schedule. However, this instance will never try and pick up this schedule
   * anymore unless it is restarted.
   * </p>
   *
   * @param mayInterruptIfRunning if true, might interrupt the thread if the operation is
   *                              currently running (to use with caution).
   */
  @Override
  public void cancel(boolean mayInterruptIfRunning) {
    boolean propogateCancel = false;
    cancelLock.lock();
    try {
      if (taskDone) {
        return;
      }
      taskDone = true;
      doneCondition.signal();
      propogateCancel = true;
      cancelTime = Instant.now();
      LOGGER.info("Cancelling task {}", this);
      if (scheduledTaskFuture != null) {
        scheduledTaskFuture.cancel(mayInterruptIfRunning);
      }
      if (runningTaskFuture != null && mayInterruptIfRunning) {
        runningTaskFuture.cancel(true);
      }
    } finally {
      cancelled = true;
      cancelLock.unlock();
      // do the next step outside the lock
      if (propogateCancel) {
        // if cancel is done, invoke cancellation handler so that it can monitor the task
        // completion periodically and release booking.
        cancelHandler.accept(this);
      }
    }
  }

  @Override
  public boolean isCancelled() {
    return cancelled;
  }

  @Override
  public boolean isDone() {
    return taskDone;
  }

  @Override
  public boolean isScheduled() {
    return bookingOwner && !taskDone;
  }

  @Override
  public SchedulerEvents.RecoveryRejectReason tryRecover() {
    if (!loadController.isInRunSet()) {
      startRun(true);
      return bookingOwner ? null : SchedulerEvents.RecoveryRejectReason.CANNOT_BOOK;
    }
    return SchedulerEvents.RecoveryRejectReason.IN_RUN_QUEUE;
  }

  @Override
  public void updateLastRun(Instant newValue) {
    lastRun = newValue;
  }

  @Override
  public void markDone() {
    cancelLock.lock();
    try {
      taskDone = true;
      doneCondition.signal();
    } finally {
      cancelLock.unlock();
      if (RUN_ONCE_EVERY_MEMBER_DEATH.equals(currentSchedule.getSingleShotType())) {
        recoveryMonitor.addToDeathWatchLocal();
      }
    }
  }

  @Override
  public ThreadPoolExecutor getTaskRunningPool() {
    return runningPool;
  }

  @Override
  public void runImmediate() {
    // try booking. Create the schedule only if we can book successfully.
    boolean booked = tryBook();
    if (booked) {
      // save the last schedule time for recovery of lastRun
      recoveryMonitor.storeScheduleTime();
      cancelLock.lock();
      try {
        if (!taskDone) {
          // still use schedule instead of submit to make it cancellable once cancel lock is released
          scheduledTaskFuture = schedulerCommon.getSchedulePool().schedule(this, 0, TimeUnit.MILLISECONDS);
        } else {
          LOGGER.info("Task {} is done. Ignoring scheduling request", taskName);
        }
      } finally {
        cancelLock.unlock();
      }
    } else {
      recoveryMonitor.setRecoveryWatch();
    }
  }

  @Override
  public boolean runImmediateForce() {
    // try booking. Create the schedule only if we can book successfully.
    boolean booked = tryBook();
    if (booked) {
      // save the last schedule time for recovery of lastRun
      recoveryMonitor.storeScheduleTime();
      cancelLock.lock();
      try {
        scheduledTaskFuture = schedulerCommon.getSchedulePool().schedule(this, 0, TimeUnit.MILLISECONDS);
      } finally {
        cancelLock.unlock();
      }
    }
    return booked;
  }

  /**
   * Explicitly releases the booking when a task is ended.
   * <p>
   * Done when the schedule is explicitly cancelled locally.
   * This can trigger the watcher for the recovering instance of this task and the task may start running on another
   * instance, unless it is cancelled there as well.
   * </p>
   * <p>
   * Note that the task may not be cleaned up in time locally for re-execution and explicit re-scheduling post cancel.
   * The assumption is that once a task is cancelled, it is only because of one of two reasons:
   * 1. There is a shutdown of the service instance that is prompting schedule cancellation on this node.
   * 2. The task is no longer required (e.g a source deletion) and a new task will never be scheduled with the
   * same task name.
   * </p>
   */
  boolean checkAndReleaseBookingOnCancelCompletion() {
    Preconditions.checkState(cancelTime != null, "Task %s is not cancelled", this);
    boolean completed = true;
    cancelLock.lock();
    try {
      if (runningTaskFuture != null) {
        try {
          runningTaskFuture.get(MAX_RUN_WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          LOGGER.debug("Task {} is not completed yet, despite cancellation", this);
        } catch (Exception e) {
          LOGGER.debug("Task {} execution returned exception", this, e);
        }
        completed = runningTaskFuture.isDone();
      }
      if (completed && scheduledTaskFuture != null) {
        completed = scheduledTaskFuture.isDone();
      }
    } finally {
      cancelLock.unlock();
    }
    long elapsedPostCancelSeconds = ChronoUnit.SECONDS.between(cancelTime, Instant.now());
    try {
      if (!completed) {
        if (elapsedPostCancelSeconds < MAX_TIME_TO_WAIT_POST_CANCEL_SECONDS) {
          return false;
        }
        LOGGER.warn("Task {} still not completed {} seconds after cancel", this, MAX_TIME_TO_WAIT_POST_CANCEL_SECONDS);
      }
      releaseBooking();
    } catch (Exception e) {
      // an exception is not expected here. Log a warning as it is a fatal warning
      LOGGER.warn("Unexpected exception for task {} while releasing the local booking", this, e);
    }
    return elapsedPostCancelSeconds > MAX_AGE_TO_KEEP_CANCELLED_SCHEDULE_SECONDS;
  }

  private void awaitImmediateTaskCompletion() {
    if (!currentSchedule.isInLockStep()) {
      // this pause is only for immediate lock step task schedules that is typically called during dremio startup
      return;
    }
    long waitTimeInNanos = TimeUnit.SECONDS.toNanos(WAIT_TIME_FOR_DONE_SECONDS);
    cancelLock.lock();
    try {
      while (!taskDone && waitTimeInNanos > 0) {
        try {
          final long start = System.nanoTime();
          doneCondition.await(waitTimeInNanos, TimeUnit.NANOSECONDS);
          waitTimeInNanos -= (System.nanoTime() - start);
        } catch (InterruptedException e) {
          break;
        }
      }
    } finally {
      cancelLock.unlock();
    }
    if (!taskDone) {
      // Just log a warning and continue as this is only for immediate schedules typically to coordinate startup
      LOGGER.warn("Task {} is not yet done on another instance", this);
    }
  }

  private void scheduleNextRun(Throwable e) {
    lastRun = Instant.now();
    final Instant instant = checkAndAdjustForScheduleModifications();
    boolean endTask = false;
    cancelLock.lock();
    try {
      this.runningTaskFuture = null;
      if (taskDone) {
        return;
      }
      // if instant == null - no more to schedule
      // need to notify all surviving members that this task is done before removing the booking
      if (instant == null) {
        endTask = true;
        taskDone = true;
        doneCondition.signal();
      } else {
        long delay = ChronoUnit.MILLIS.between(Instant.now(), instant);
        if (e != null) {
          LOGGER.info("Exception occurred during last run for task {}. Will try next run after {} millis",
            taskName, delay, e);
        } else {
          LOGGER.debug("Task {} is scheduled to run in {} milliseconds", taskName, delay);
        }
        scheduledTaskFuture = schedulerCommon.getSchedulePool().schedule(this, delay, TimeUnit.MILLISECONDS);
      }
    } finally {
      cancelLock.unlock();
      if (taskDone) {
        LOGGER.info("Task {} is {}. Task cannot be re-scheduled", this, cancelled ? "cancelled" : "done");
      }
      if (endTask) {
        doneHandler.signalEndTaskByBookingOwner();
        if (RUN_ONCE_EVERY_MEMBER_DEATH.equals(currentSchedule.getSingleShotType())) {
          // ask recovery monitor to keep triggering a run even if task is done
          recoveryMonitor.addToDeathWatchLocal();
          releaseBookingLocalState();
        }
      } else {
        recoveryMonitor.storeScheduleTime();
      }
    }
  }

  private Instant checkAndAdjustForScheduleModifications() {
    final Schedule newSchedule = currentSchedule.getScheduleModifier().apply(currentSchedule);
    if (newSchedule != null) {
      setNewSchedule(newSchedule);
    }
    return nextInstant();
  }

  private Instant nextInstant() {
    Instant result = null;
    if (lastRun.equals(Instant.MIN)) {
      lastRun = Instant.now();
    }
    final Iterator<Instant> instants = instantsRef.get();
    while (instants.hasNext()) {
      result = instants.next();
      if (!result.isBefore(lastRun)) {
        break;
      }
    }
    return result;
  }

  private boolean tryBook() {
    try {
      eventsCollector.bookingAttempted();
      if (!bookingOwner) {
        this.schedulerCommon.getTaskStore().executeSingle(new PathCommand(CREATE_EPHEMERAL, bookFqPathLocal,
          schedulerCommon.getThisEndpoint().toByteArray()));
        eventsCollector.bookingAcquired();
        bookingOwner = true;
      }
      return true;
    } catch (PathExistsException ignored) {
      LOGGER.debug("Booking failed. Ephemeral path {} already exists", bookFqPathLocal);
    } catch (Exception e) {
      LOGGER.info("Random failure while booking to schedule task {}." +
        "The recovery instance for the schedule will monitor and recover later", this, e);
    }
    return false;
  }

  private void releaseBooking() {
    if (bookingOwner) {
      try {
        schedulerCommon.getTaskStore().executeSingle(new PathCommand(DELETE, bookFqPathLocal));
        eventsCollector.bookingReleased();
      } catch (PathExistsException | PathMissingException e) {
        LOGGER.warn("Unexpected error. Booking should have been held by this task {}", this);
        eventsCollector.contractError();
      }
      bookingOwner = false;
    }
  }

  private void releaseBookingLocalState() {
    if (bookingOwner) {
      eventsCollector.bookingReleased();
      bookingOwner = false;
    }
  }

  /**
   * Some single shot task types are marked done globally after it is done once on any instance (to ensure that no other
   * instance picks it up and run when it starts up or recovers). This method allows checks done during the actual
   * run() method to differentiate such single shot tasks that may want to run again on certain events.
   *
   * @return true, if done, false otherwise.
   */
  private boolean isTaskReallyDone() {
    return cancelled || (taskDone && !RUN_ONCE_EVERY_MEMBER_DEATH.equals(currentSchedule.getSingleShotType()));
  }
}

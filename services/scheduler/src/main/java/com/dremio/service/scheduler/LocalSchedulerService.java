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

import static java.lang.String.format;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.TaskLeaderChangeListener;
import com.dremio.service.coordinator.TaskLeaderElection;
import com.google.common.annotations.VisibleForTesting;

/**
 * Simple implementation of {@link SchedulerService}
 *
 */
public class LocalSchedulerService implements SchedulerService {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(LocalSchedulerService.class);
  private static final String THREAD_NAME_PREFIX = "scheduler-";

  private final CloseableSchedulerThreadPool executorService;
  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;
  private final Provider<CoordinationProtos.NodeEndpoint> currentEndPoint;
  private final boolean assumeTaskLeadership;

  private final ConcurrentMap<String, TaskLeaderElection> taskLeaderElectionServiceMap = new ConcurrentHashMap<>();

  /**
   * Creates a new scheduler service.
   *
   * The underlying executor uses a {@link ThreadPoolExecutor}, with a given pool size.
   *
   * @param corePoolSize -- the <b>maximum</b> number of threads used by the underlying {@link ThreadPoolExecutor}
   */
  public LocalSchedulerService(int corePoolSize) {
    this(new CloseableSchedulerThreadPool(THREAD_NAME_PREFIX, corePoolSize),
      null, null, false);
  }

  public LocalSchedulerService(int corePoolSize, String threadNamePrefix) {
    this(new CloseableSchedulerThreadPool(threadNamePrefix, corePoolSize),
      null, null, false);
  }

  public LocalSchedulerService(int corePoolSize,
                               Provider<ClusterCoordinator> clusterCoordinatorProvider,
                               Provider<CoordinationProtos.NodeEndpoint> currentNode,
                               boolean assumeTaskLeadership) {
    this(new CloseableSchedulerThreadPool(THREAD_NAME_PREFIX, corePoolSize), clusterCoordinatorProvider, currentNode,
      assumeTaskLeadership);
  }

  @VisibleForTesting
  LocalSchedulerService(CloseableSchedulerThreadPool executorService,
                        Provider<ClusterCoordinator> clusterCoordinatorProvider,
                        Provider<CoordinationProtos.NodeEndpoint> currentNode,
                        boolean assumeTaskLeadership) {
    this.executorService = executorService;
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
    this.currentEndPoint = currentNode;
    this.assumeTaskLeadership = assumeTaskLeadership;
  }

  @VisibleForTesting
  public CloseableSchedulerThreadPool getExecutorService() {
    return executorService;
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Stopping SchedulerService");
    AutoCloseables.close(AutoCloseables.iter(executorService), taskLeaderElectionServiceMap.values());
    LOGGER.info("Stopped SchedulerService");
  }

  @Override
  public void start() throws Exception {
    LOGGER.info("SchedulerService is up");
  }

  @VisibleForTesting
  Collection<TaskLeaderElection> getTaskLeaderElectionServices() {
    return taskLeaderElectionServiceMap.values();
  }

  private class CancellableTask implements Cancellable, Runnable {
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final Iterator<Instant> instants;
    private final String taskName;
    private final Runnable task;
    private volatile boolean taskState;
    private Instant lastRun = Instant.MIN;
    private TaskLeaderChangeListener taskLeaderChangeListener;

    private final AtomicReference<ScheduledFuture<?>> currentTask = new AtomicReference<>(null);

    public CancellableTask(Schedule schedule, Runnable task) {
      this(schedule, task, null);
    }

    public CancellableTask(Schedule schedule, Runnable task, String taskName) {
      this.instants = schedule.iterator();
      this.task = task;
      this.taskName = taskName;
      this.taskState = false;
      this.taskLeaderChangeListener = new TaskLeaderChangeListener() {
        @Override
        public void onLeadershipGained() {
          if (isDone() && schedule.isToRunOnce()) {
            LOGGER.info("Task {} was already completed", taskName);
            return;
          }
          // start doing work
          // if task was cancelled before due to remote scheduling
          // reinstate it
          // it may run earlier then it's scheduled time
          // due to changed of leadership
          scheduleNext();
        }

        @Override
        public void onLeadershipLost() {
          if (isDone() && schedule.isToRunOnce()) {
            LOGGER.info("Task {} was already completed", taskName);
          }
          // cancel task
          basicCancel(false);
          schedule.getCleanupListener().cleanup();
          // unset cancel - since we will need to come back to it
          cancelled.set(false);
        }

        @Override
        public void onLeadershipRelinquished() {
          if (currentTask.get() == null ||
            currentTask.get().isCancelled() ||
            currentTask.get().isDone()) {
            LOGGER.info("Task {} is not currently running. Relinquishing leadership", task);
            schedule.getCleanupListener().cleanup();
            return;
          }
          // if the task is in flight
          // we can't wait before relinquishing
          // as task could be scheduled to run next time in many hours
          // so cancelling w/o interrupt
          basicCancel(false);
          schedule.getCleanupListener().cleanup();
          // unset cancel - since we will need to come back to it
          cancelled.set(false);
        }
      };
    }

    @Override
    public void run() {
      if (cancelled.get()) {
        return;
      }

      try {
        task.run();
      } catch(Exception e) {
        LOGGER.warn(format("Execution of task %s failed", task.toString()), e);
      }
      lastRun = Instant.now();
      scheduleNext();
    }

    private void scheduleNext() {
      if (cancelled.get()) {
        return;
      }

      Instant instant = nextInstant();
      // if instant == null - it is the end of the scheduling
      // need to remove listener
      if (instant == null) {
        currentTask.set(handleInstantNull());
      } else {
        ScheduledFuture<?> future = executorService.schedule(this, ChronoUnit.MILLIS.between(Instant.now(), instant), TimeUnit.MILLISECONDS);
        currentTask.set(future);
      }

    }

    /**
     * Do not go through executor when instant is null - it will throw NPE that we will never consume
     * earlier we were just lucky to not deal with the exception
     * Remove listener for the task that is actually finished
     * used for tasks scheduled to run once
     */
    private ScheduledFuture<?> handleInstantNull() {
      LOGGER.debug("Handling Instant null");
      ScheduledFuture<?> future = new ScheduledFuture<Object>() {

        @Override
        public int compareTo(Delayed o) {
          return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          return false;
        }

        @Override
        public boolean isCancelled() {
          return false;
        }

        @Override
        public boolean isDone() {
          return true;
        }

        @Override
        public Object get() {
          return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit)  {
          return null;
        }

        @Override
        public long getDelay(TimeUnit unit) {
          return 0;
        }
      };

      taskState = true;
      if (taskName != null && taskLeaderElectionServiceMap.get(taskName) != null) {
        taskLeaderElectionServiceMap.get(taskName).removeListener(taskLeaderChangeListener);
      }
      return future;
    }


    private Instant nextInstant() {
      Instant result = null;
      while(instants.hasNext()) {
        result = instants.next();
        if (!result.isBefore(lastRun)) {
          break;
        }
      }
      return result;
    }

    @Override
    public void cancel(boolean mayInterruptIfRunning) {
      if (taskName != null && taskLeaderElectionServiceMap.get(taskName) != null) {
        taskLeaderElectionServiceMap.get(taskName).removeListener(taskLeaderChangeListener);
      }
      basicCancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return cancelled.get();
    }

    @Override
    public boolean isDone() {
      return taskState;
    }

    private void basicCancel(boolean mayInterruptIfRunning) {
      if (cancelled.getAndSet(true)) {
        // Already cancelled
        return;
      }

      LOGGER.info(format("Cancelling task %s", task.toString()));
      ScheduledFuture<?> future = currentTask.getAndSet(null);
      if (future != null) {
        future.cancel(mayInterruptIfRunning);
      }
    }
  }

  @Override
  public Cancellable schedule(Schedule schedule, Runnable task) {
    if (!assumeTaskLeadership) {
      return plainSchedule(schedule, task);
    }

    if (!schedule.isDistributedSingleton()) {
      return plainSchedule(schedule, task);
    }

    final TaskLeaderElection taskLeaderElection = getTaskLeaderElection(schedule);

    CancellableTask cancellableTask = new CancellableTask(schedule, task, schedule.getTaskName());
    // wait for elections
    taskLeaderElection.getTaskLeader();
    // adding listener after leader is established (not in CancellableTask ctor),
    // otherwise it may be a race condition on first schedule
    // between leader elected executing onLeadershipGained and ctor of CancellableTask
    taskLeaderElection.addListener(cancellableTask.taskLeaderChangeListener);
    // taskLeaderElection.isTaskLeader() is much more definitive then/and
    // comparing current NodeEndPoint to a leader one - as a particular NodeEndPoint
    // becoming a leader or not is a side effect and not a cause
    if (taskLeaderElection.isTaskLeader()) {
      cancellableTask.scheduleNext();
    } else {
      cancellableTask.taskState = true;
    }
    return cancellableTask;
  }

  private Cancellable plainSchedule(Schedule schedule, Runnable task) {
    CancellableTask cancellableTask = new CancellableTask(schedule, task);
    cancellableTask.scheduleNext();

    return cancellableTask;
  }

  private TaskLeaderElection getTaskLeaderElection(final Schedule schedule) {
    final TaskLeaderElection taskLeader = taskLeaderElectionServiceMap.computeIfAbsent(schedule.getTaskName(),
      s -> {
        final String taskName = schedule.getTaskName();
        final Long scheduledLeadershipRelease = schedule.getScheduledOwnershipReleaseInMillis();
        final TaskLeaderElection taskLeaderElection = new TaskLeaderElection(
          taskName,
          clusterCoordinatorProvider,
          scheduledLeadershipRelease,
          currentEndPoint,
          executorService
        );
        try {
          taskLeaderElection.start();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return taskLeaderElection;
      });
    // in case if ownership release time changed compare to last schedule
    taskLeader.updateLeaseExpirationTime(schedule.getScheduledOwnershipReleaseInMillis());
    return taskLeader;
  }

  @VisibleForTesting
  TaskLeaderChangeListener getTaskLeaderChangeListener(Cancellable task) {
    return ((CancellableTask) task).taskLeaderChangeListener;
  }

}

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

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.coordinator.ClusterElectionManager;
import com.dremio.service.coordinator.ClusterServiceSetManager;
import com.dremio.service.coordinator.TaskLeaderChangeListener;
import com.dremio.service.coordinator.TaskLeaderElection;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Provider;

/** Simple implementation of {@link SchedulerService} */
public class LocalSchedulerService implements SchedulerService {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(LocalSchedulerService.class);
  private static final String THREAD_NAME_PREFIX = "scheduler-";

  private final CloseableSchedulerThreadPool executorService;
  private final Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider;
  private final Provider<ClusterElectionManager> clusterElectionManagerProvider;
  private final Provider<CoordinationProtos.NodeEndpoint> currentEndPoint;
  private final boolean assumeTaskLeadership;

  private final ConcurrentMap<String, TaskLeaderElection> taskLeaderElectionServiceMap =
      new ConcurrentHashMap<>();

  /**
   * Creates a new scheduler service.
   *
   * <p>The underlying executor uses a {@link ThreadPoolExecutor}, with a given pool size.
   *
   * @param corePoolSize -- the <b>maximum</b> number of threads used by the underlying {@link
   *     ThreadPoolExecutor}
   */
  public LocalSchedulerService(int corePoolSize) {
    this(corePoolSize, THREAD_NAME_PREFIX);
  }

  public LocalSchedulerService(int corePoolSize, String threadNamePrefix) {
    this(corePoolSize, threadNamePrefix, null, null, null, false);
  }

  public LocalSchedulerService(
      int corePoolSize,
      Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider,
      Provider<ClusterElectionManager> clusterElectionManagerProvider,
      Provider<CoordinationProtos.NodeEndpoint> currentNode,
      boolean assumeTaskLeadership) {
    this(
        corePoolSize,
        THREAD_NAME_PREFIX,
        clusterServiceSetManagerProvider,
        clusterElectionManagerProvider,
        currentNode,
        assumeTaskLeadership);
  }

  public LocalSchedulerService(
      int corePoolSize,
      String threadNamePrefix,
      Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider,
      Provider<ClusterElectionManager> clusterElectionManagerProvider,
      Provider<CoordinationProtos.NodeEndpoint> currentNode,
      boolean assumeTaskLeadership) {
    this(
        new CloseableSchedulerThreadPool(threadNamePrefix, corePoolSize),
        clusterServiceSetManagerProvider,
        clusterElectionManagerProvider,
        currentNode,
        assumeTaskLeadership);
  }

  @VisibleForTesting
  LocalSchedulerService(
      CloseableSchedulerThreadPool executorService,
      Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider,
      Provider<ClusterElectionManager> clusterElectionManagerProvider,
      Provider<CoordinationProtos.NodeEndpoint> currentNode,
      boolean assumeTaskLeadership) {
    this.executorService = executorService;
    this.clusterServiceSetManagerProvider = clusterServiceSetManagerProvider;
    this.clusterElectionManagerProvider = clusterElectionManagerProvider;
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
    AutoCloseables.close(
        AutoCloseables.iter(executorService), taskLeaderElectionServiceMap.values());
    taskLeaderElectionServiceMap.clear();
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
    private Instant scheduleStartInstant = null;
    private final String taskName;
    private final Runnable task;
    private volatile boolean taskState;
    private Instant lastRun = Instant.MIN;
    private TaskLeaderChangeListener taskLeaderChangeListener;
    private final AtomicReference<ScheduledFuture<?>> currentTask;
    private final AtomicBoolean isTaskRunning = new AtomicBoolean(false);

    public CancellableTask(Schedule schedule, Runnable task) {
      this(schedule, task, null);
    }

    public CancellableTask(Schedule schedule, Runnable task, String taskName) {
      this.instants = schedule.iterator();
      this.task = task;
      this.taskName = taskName;
      this.taskState = false;

      this.currentTask = new AtomicReference<>(null);
      this.taskLeaderChangeListener =
          new TaskLeaderChangeListener() {
            @Override
            public void onLeadershipGained() {
              synchronized (CancellableTask.this) {
                LOGGER.info(
                    "onLeadershipGained Event: leadership gained for task {}",
                    CancellableTask.this);
                if (isDone() && schedule.isToRunExactlyOnce()) {
                  LOGGER.info(
                      "onLeadershipGained Event: Task {} was already completed",
                      CancellableTask.this);
                  return;
                }
                // start doing work
                // if task was cancelled before due to remote scheduling
                // reinstate it
                // it may run earlier then it's scheduled time
                // due to changed of leadership

                scheduleNext();
              }
            }

            @Override
            public void onLeadershipLost() {
              synchronized (CancellableTask.this) {
                LOGGER.info(
                    "onLeadershipLost Event: leadership lost for task {}", CancellableTask.this);
                if (isDone() && schedule.isToRunExactlyOnce()) {
                  LOGGER.info(
                      "onLeadershipLost Event: Task {} was already completed",
                      CancellableTask.this);
                }
                // cancel task
                basicCancel(false);
                schedule.getCleanupListener().cleanup();
                // unset cancel - since we will need to come back to it
                cancelled.set(false);
              }
            }

            @Override
            public void onLeadershipRelinquished() {
              synchronized (CancellableTask.this) {
                LOGGER.info(
                    "onLeadershipRelinquished Event: relinquish leadership for task {}",
                    CancellableTask.this);
                if (currentTask.get() == null
                    || currentTask.get().isCancelled()
                    || currentTask.get().isDone()) {
                  LOGGER.info(
                      "onLeadershipRelinquished Event: Task {} is not currently running. Relinquishing leadership",
                      CancellableTask.this);
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
            }
          };
    }

    @Override
    public String toString() {
      if (taskName != null) {
        return taskName;
      }
      return task.toString();
    }

    @Override
    public void run() {
      synchronized (this) {
        // if cancelled (or) lost leadership in between runs
        // if we don't validate leadership, we will not honor
        // that we lost leadership
        boolean lostLeadership =
            taskName != null
                && taskLeaderElectionServiceMap.get(taskName) != null
                && !taskLeaderElectionServiceMap.get(taskName).isTaskLeader();
        if (cancelled.get() || lostLeadership) {
          LOGGER.info(
              "Skipping execution of the task {} as {}",
              this,
              (cancelled.get() ? "the task was cancelled" : "the node lost leadership"));
          return;
        }

        // we are adding a boolean here in order to avoid to re-run the task in the scenario
        // gain-lost-gain leadership,
        // and when executor pool has more than one thread:
        // - Node A gains leadership and long task start running on it
        // - Node A loses leadership, task continues running on A as a running task will not be
        // cancelled
        // - Node A gains leadership and the leadership change callback tries to reschedule the task
        // even when the long-running task is still running on it
        // This boolean can be checked to not reschedule the task if it is already running

        // note: this flag will have no effect in the following scenario:
        // - Node A gains leadership and long task start running on it
        // - Node A loses leadership, task continues running on A
        // - Node B is the leader. Task starts on B
        isTaskRunning.set(true);
      }

      try {
        task.run();
      } catch (Exception e) {
        LOGGER.warn("Execution of task {} failed", this, e);
      }

      synchronized (this) {
        lastRun = Instant.now();
        isTaskRunning.set(false);
        scheduleNext();
      }
    }

    private synchronized void scheduleNext() {
      if (cancelled.get()) {
        LOGGER.info("Task {} was cancelled. Will not be re-scheduled", this);
        return;
      }

      if (isTaskRunning.get()) {
        LOGGER.info(
            "Task {} is being rescheduled while one is already executing. Ignoring the request",
            this);
        return;
      }

      Instant instant = nextInstant();
      if (scheduleStartInstant == null) {
        scheduleStartInstant = instant;
      }
      // if the task was scheduled but never ran because leadership was lost in between
      // reschedule it back
      if (instant == null && lastRun.equals(Instant.MIN)) {
        LOGGER.info(
            "Task {} was cancelled before it could run even once, possibly due to a leadership change. Rescheduling it (with instant value {})",
            this,
            scheduleStartInstant);
        instant = scheduleStartInstant;
      }
      // if instant == null - it is the end of the scheduling
      // need to remove listener
      if (instant == null) {
        LOGGER.debug("This is the end of the task {}", this);
        currentTask.set(handleInstantNull());
      } else {
        long delay = ChronoUnit.MILLIS.between(Instant.now(), instant);
        LOGGER.debug("Task {} is scheduled to run in {} milliseconds", this, delay);
        ScheduledFuture<?> future = executorService.schedule(this, delay, TimeUnit.MILLISECONDS);
        currentTask.set(future);
      }
    }

    /**
     * Do not go through executor when instant is null - it will throw NPE that we will never
     * consume earlier we were just lucky to not deal with the exception Remove listener for the
     * task that is actually finished used for tasks scheduled to run once
     */
    private ScheduledFuture<?> handleInstantNull() {
      LOGGER.debug("Handling Instant null");
      ScheduledFuture<?> future = getDefaultFuture();

      taskState = true;
      if (taskName != null && taskLeaderElectionServiceMap.get(taskName) != null) {
        taskLeaderElectionServiceMap.get(taskName).removeListener(taskLeaderChangeListener);
      }
      return future;
    }

    private ScheduledFuture<Object> getDefaultFuture() {
      return new ScheduledFuture<Object>() {

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
        public Object get(long timeout, TimeUnit unit) {
          return null;
        }

        @Override
        public long getDelay(TimeUnit unit) {
          return 0;
        }
      };
    }

    private Instant nextInstant() {
      Instant result = null;
      while (instants.hasNext()) {
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
        try {
          taskLeaderElectionServiceMap.get(taskName).close();
        } catch (Exception e) {
          LOGGER.error(
              "Exception while trying to close task leader election for task {}", taskName, e);
        }
        taskLeaderElectionServiceMap.remove(taskName);
      }
      basicCancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return cancelled.get();
    }

    @Override
    public boolean isDone() {
      // Note: This is actually a bug in the current local scheduler service to wrongly denote that
      // a task
      // is done, while it may still be running (or scheduled to run) on another node. Since this
      // implementation is going to be decommissioned soon, this bug is not being fixed.
      return taskState;
    }

    @Override
    public boolean isScheduled() {
      return !taskState;
    }

    private void basicCancel(boolean mayInterruptIfRunning) {
      if (cancelled.getAndSet(true)) {
        // Already cancelled
        return;
      }

      LOGGER.info("Cancelling task {}", this);
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
    // if the task is to run once now - as long as the node is the leader go ahead.
    if (!schedule.isToRunExactlyOnce()) {
      // adding listener after leader is established (not in CancellableTask ctor),
      // otherwise it may be a race condition on first schedule
      // between leader elected executing onLeadershipGained and ctor of CancellableTask
      taskLeaderElection.addListener(cancellableTask.taskLeaderChangeListener);
    }
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

  @Override
  public Optional<CoordinationProtos.NodeEndpoint> getCurrentTaskOwner(String taskName) {
    if (assumeTaskLeadership) {
      return Optional.ofNullable(
              clusterServiceSetManagerProvider
                  .get()
                  .getOrCreateServiceSet(taskName)
                  .getAvailableEndpoints())
          .flatMap(nodeEndpoints -> nodeEndpoints.stream().findFirst());
    } else {
      return clusterServiceSetManagerProvider.get().getMasterEndpoint();
    }
  }

  private Cancellable plainSchedule(Schedule schedule, Runnable task) {
    CancellableTask cancellableTask = new CancellableTask(schedule, task);
    cancellableTask.scheduleNext();

    return cancellableTask;
  }

  private TaskLeaderElection getTaskLeaderElection(final Schedule schedule) {
    final TaskLeaderElection taskLeader =
        taskLeaderElectionServiceMap.computeIfAbsent(
            schedule.getTaskName(),
            s -> {
              final String taskName = schedule.getTaskName();
              final Long scheduledLeadershipRelease =
                  schedule.getScheduledOwnershipReleaseInMillis();
              final TaskLeaderElection taskLeaderElection =
                  new TaskLeaderElection(
                      taskName,
                      clusterServiceSetManagerProvider,
                      clusterElectionManagerProvider,
                      scheduledLeadershipRelease,
                      currentEndPoint,
                      executorService);
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

  @VisibleForTesting
  TaskLeaderElection getTaskLeaderElection(Cancellable task) {
    String taskName = ((CancellableTask) task).taskName;
    if (taskName != null) {
      return taskLeaderElectionServiceMap.get(taskName);
    }
    return null;
  }
}

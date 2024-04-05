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

import com.dremio.common.UncaughtExceptionHandlers;
import com.dremio.io.file.Path;
import com.dremio.service.coordinator.LinearizableHierarchicalStore;
import com.dremio.service.coordinator.exceptions.PathExistsException;
import com.dremio.service.coordinator.exceptions.PathMissingException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Deals with load controlling and load management aspects of the {@code
 * ClusteredSingletonTaskScheduler}. The load distribution is 'reactive' and kicks in if and only if
 * a task cannot be run due to load on a service instance just when the task is ready for its next
 * run. If a task is able to run (e.g enough thread pool capacity is available) locally on the
 * service instance that has currently acquired the ownership slot for this task, load distribution
 * and management may never kick in for that task.
 *
 * <p>Load distribution and control is done through a distributed run-list abstraction called
 * 'steal-set'. The task items in this set are names of tasks that ran out of capacity on a given
 * service instance just when it was ready to run (see {@code addToRunSet}).
 *
 * <p>Instead of using the existing queue abstraction from libraries like curator, this
 * implementation relies on a simple set of task names under a 'steal-set' path, where there is only
 * one entry per task name. The assumption is that any entry in the steal_set path will need to be
 * immediately run. The queue primitive directly available in curator like libraries is not
 * immediately used due to the following reasons: 1. Each task name can only have 1 unique entry
 * even if there are multiple 'puts' for the same taskName, thus adhering to a set abstraction
 * rather than a queue abstraction. 2. As of now, there is no order requirement within the run-queue
 * as all tasks that are runnable immediately can be randomly picked, so a simple 'steal-set' path
 * with a list of task names will suffice. 3. The queue abstraction needs us to pick tasks and
 * requeue back in case there is load on the task runner which complicates the use of a curator q
 * primitive for this use case.
 *
 * <p>The load controller puts a watch on the 'steal-set' and picks up tasks on execution, provided
 * the running pool associated with the task has sufficient capacity locally. This allows any one
 * service instance to grab the task, do a run. It will also transfer the ownership of this task to
 * the service instance that successfully grabbed the task.
 */
final class TaskLoadController implements AutoCloseable {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(TaskLoadController.class);
  private final ClusteredSingletonCommon schedulerCommon;
  private final Map<String, PerTaskLoadController> allTasks;
  private final LoadMonitorThread monitorThread;
  private final SchedulerEvents events;

  TaskLoadController(ClusteredSingletonCommon schedulerCommon, SchedulerEvents events) {
    this.schedulerCommon = schedulerCommon;
    this.allTasks = new ConcurrentHashMap<>();
    this.events = events;
    monitorThread = new LoadMonitorThread();
    monitorThread.setUncaughtExceptionHandler(UncaughtExceptionHandlers.processExit());
  }

  void start() {
    // important to start the thread AFTER setting the steal set watcher
    monitorThread.start();
  }

  PerTaskLoadController addTask(
      PerTaskLoadInfo loadInfo, SchedulerEvents.PerTaskLoadEvents loadEventSink) {
    return allTasks.computeIfAbsent(
        loadInfo.getTaskName(), (k) -> new PerTaskLoadController(loadInfo, loadEventSink));
  }

  void removeTask(String taskName) {
    allTasks.remove(taskName);
  }

  @Override
  public void close() {
    allTasks.clear();
    monitorThread.close();
  }

  interface PerTaskLoadInfo extends PerTaskInfo {
    ThreadPoolExecutor getTaskRunningPool();

    void runImmediate();
  }

  final class PerTaskLoadController {
    private static final int POOL_CAPACITY_THRESHOLD_PERCENT = 80;
    private final PerTaskLoadInfo taskLoadInfo;
    private final SchedulerEvents.PerTaskLoadEvents taskLoadStats;
    private final String taskRunSetPath;

    private PerTaskLoadController(
        PerTaskLoadInfo loadInfo, SchedulerEvents.PerTaskLoadEvents taskLoadStats) {
      this.taskLoadInfo = loadInfo;
      this.taskLoadStats = taskLoadStats;
      this.taskRunSetPath =
          schedulerCommon.getStealFqPath() + Path.SEPARATOR + taskLoadInfo.getTaskName();
    }

    boolean removeFromRunSet() {
      try {
        final LinearizableHierarchicalStore.Stats pathStats =
            schedulerCommon.getTaskStore().getStats(taskRunSetPath);
        if (pathStats != null) {
          schedulerCommon.getTaskStore().executeSingle(new PathCommand(DELETE, taskRunSetPath));
          taskLoadStats.removedFromRunSet(pathStats.getCreationTime());
        }
        return true;
      } catch (PathMissingException ignored) {
        LOGGER.debug(
            "Task {} already removed from run set. Full path is {}",
            taskLoadInfo.getTaskName(),
            taskRunSetPath);
        return true;
      } catch (Exception e) {
        LOGGER.info("Random failure while deleting task {} from run set", this, e);
      }
      return false;
    }

    void addToRunSet() {
      try {
        schedulerCommon
            .getTaskStore()
            .executeSingle(new PathCommand(CREATE_EPHEMERAL, taskRunSetPath));
        taskLoadStats.addedToRunSet();
      } catch (PathExistsException ignored) {
        LOGGER.debug("Task {} already added to run set", taskRunSetPath);
      } catch (Exception e) {
        LOGGER.info("Random failure while adding task {} to run set", this, e);
      }
    }

    boolean isInRunSet() {
      return schedulerCommon.getTaskStore().checkExists(taskRunSetPath);
    }

    boolean tryRun() {
      if (!hasRunThresholdCrossed()) {
        // First remove from the run set. Continue the run only if we are able to remove from run
        // set. This
        // ensures atomicity of the run.
        if (removeFromRunSet()) {
          // Run immediately as whatever is in the run set needs to be immediately run.
          // Note that if two service instances are in this loop simultaneously, only one will
          // succeed.
          taskLoadInfo.runImmediate();
        }
        // we should imitate success, even if a particular service instance could not grab the
        // booking slot
        // and run as we know then that some other service instance has started running it. if the
        // task remains
        // in the run set, we will have another chance
        return true;
      }
      taskLoadStats.crossedThreshold();
      return false;
    }

    String getTaskName() {
      return taskLoadInfo.getTaskName();
    }

    private boolean hasRunThresholdCrossed() {
      int poolCapacityThreshold =
          Math.max(
              (POOL_CAPACITY_THRESHOLD_PERCENT
                      * taskLoadInfo.getTaskRunningPool().getMaximumPoolSize())
                  / 100,
              1);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Pool capacity threshold {}, Active count {}",
            poolCapacityThreshold,
            taskLoadInfo.getTaskRunningPool().getActiveCount());
      }
      return taskLoadInfo.getTaskRunningPool().getActiveCount() >= poolCapacityThreshold;
    }
  }

  /**
   * Thread that monitors the run set. Runs on all service instances. Checks if there is available
   * capacity to run the task before running. Also, runs the task only if it can grab the slot.
   *
   * <p><strong>NOTE:</strong> The task is always run on the task running pool and not on this
   * thread.
   */
  final class LoadMonitorThread extends Thread implements AutoCloseable {
    private static final int LONG_PAUSE_TIME_MS = 300000;
    private static final int SHORT_PAUSE_TIME_MS = 5000;
    private final ReentrantLock runListLock;
    private final Condition taskAvailableCondition;
    private volatile boolean isClosing;
    private long runSetReadVersion;
    private long runSetWriteVersion;

    public LoadMonitorThread() {
      super("LoadMonitorThread");
      this.runListLock = new ReentrantLock();
      this.taskAvailableCondition = runListLock.newCondition();
      this.isClosing = false;
      this.runSetReadVersion = 0;
      this.runSetWriteVersion = 1;
    }

    @Override
    public void run() {
      while (!isClosing) {
        try {
          waitForeverForWriteVersionToMove();
          if (isClosing) {
            break;
          }
          // set watcher on any change to the run set
          CompletableFuture<Void> onChildrenChanged = new CompletableFuture<>();
          onChildrenChanged.thenRun(this::wakeupOnLoad);
          List<String> latestRunnable =
              schedulerCommon
                  .getTaskStore()
                  .getChildren(schedulerCommon.getStealFqPath(), onChildrenChanged);
          events.runSetSize(latestRunnable.size());
          processRunSetUntilEmptyOrChanged(latestRunnable);
        } catch (InterruptedException e) {
          // set the interrupt flag back again and exit thread
          isClosing = true;
          Thread.currentThread().interrupt();
        } catch (PathMissingException e) {
          LOGGER.warn("Store Path {} found missing unexpectedly", schedulerCommon.getStealFqPath());
          events.hitUnexpectedError();
        } catch (Throwable t) {
          if (schedulerCommon.isActive() && !isClosing) {
            LOGGER.error("Unexpected exception in Scheduler load monitor", t);
            events.hitUnexpectedError();
            // we need to do a force exit as this is a fatal error
            throw t;
          }
        }
      }
    }

    private void processRunSetUntilEmptyOrChanged(List<String> latestRunnable)
        throws InterruptedException {
      while (!latestRunnable.isEmpty()) {
        final List<String> doneList =
            latestRunnable.stream()
                .map(allTasks::get)
                .filter(Objects::nonNull)
                .map((lc) -> lc.tryRun() ? lc.getTaskName() : null)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        latestRunnable.removeAll(doneList);
        if (!latestRunnable.isEmpty()) {
          // run list is not empty, which means some pools were busy, do a short wait before
          // attempting run for
          // the remaining set
          runListLock.lock();
          try {
            if (runSetReadVersion == runSetWriteVersion) {
              boolean signalled =
                  taskAvailableCondition.await(SHORT_PAUSE_TIME_MS, TimeUnit.MILLISECONDS);
              if (!signalled) {
                LOGGER.debug("{} ms elapsed waiting", SHORT_PAUSE_TIME_MS);
              }
            }
            if (runSetReadVersion < runSetWriteVersion || isClosing) {
              // get out of loop as write version has changed
              break;
            }
          } finally {
            runListLock.unlock();
          }
        }
      }
    }

    private void waitForeverForWriteVersionToMove() throws InterruptedException {
      runListLock.lock();
      try {
        if (isClosing) {
          return;
        }
        while (runSetReadVersion >= runSetWriteVersion) {
          boolean signalled =
              taskAvailableCondition.await(LONG_PAUSE_TIME_MS, TimeUnit.MILLISECONDS);
          if (!signalled) {
            LOGGER.debug("{} ms elapsed waiting", LONG_PAUSE_TIME_MS);
          }
        }
        runSetReadVersion = runSetWriteVersion;
      } finally {
        runListLock.unlock();
      }
    }

    private void wakeupOnLoad() {
      runListLock.lock();
      try {
        runSetWriteVersion++;
        taskAvailableCondition.signal();
      } finally {
        runListLock.unlock();
      }
    }

    @Override
    public void close() {
      isClosing = true;
      runListLock.lock();
      try {
        taskAvailableCondition.signal();
      } finally {
        runListLock.unlock();
      }
    }
  }
}

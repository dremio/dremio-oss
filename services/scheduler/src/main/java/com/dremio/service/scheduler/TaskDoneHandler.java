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
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.CREATE_EPHEMERAL_SEQUENTIAL;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.CREATE_PERSISTENT;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.DELETE;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.PathCommand;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.dremio.io.file.Path;
import com.dremio.service.coordinator.exceptions.PathExistsException;
import com.dremio.service.coordinator.exceptions.PathMissingException;

/**
 * Handles cluster wide task done operation for single shot and limited shot schedules.
 * <p>
 * The following assumptions are made for single shot or limited shot schedules:
 *   1. On a given version of the cluster, the single shot schedule need only run for one cycle, until the
 *      entire cluster is restarted OR until another schedule is established through the schedule API for the same
 *      task when the current task is running.
 *   2. On different versions (e.g. rolling upgrade), the single shot schedule can run again on the first restart
 *      after upgrade.
 * </p>
 */
final class TaskDoneHandler {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TaskDoneHandler.class);
  private static final String COMPLETED_PATH_NAME = "complete";
  private static final String DONE_PREFIX = "done-";
  private static final Function<String, String> TO_DONE_PATH_LOCAL = (rootPath) -> rootPath
    + Path.SEPARATOR + DONE_PREFIX;
  private final ClusteredSingletonCommon schedulerCommon;
  private final Set<String> doneTasks;
  private final Map<String, PerTaskDoneHandler> allTasks;
  private final SchedulerEvents events;

  TaskDoneHandler(ClusteredSingletonCommon schedulerCommon,
                  SchedulerEvents events) {
    this.schedulerCommon = schedulerCommon;
    this.doneTasks = ConcurrentHashMap.newKeySet();
    this.allTasks = new ConcurrentHashMap<>();
    this.events = events;
  }

  void start() {
    addDonePathWatcher();
  }

  PerTaskDoneHandler addTask(PerTaskDoneInfo doneInfo) {
    final PerTaskDoneHandler doneHandler = allTasks.computeIfAbsent(doneInfo.getTaskName(),
      (v) -> new PerTaskDoneHandler(doneInfo));
    doneHandler.createCompletionRootPathIgnoreIfExists();
    if (doneTasks.contains(doneInfo.getTaskName()) || doneHandler.hasDoneChildren()) {
      // task is already done
      doneHandler.processEndTaskSignal();
      allTasks.remove(doneInfo.getTaskName());
    }
    return doneHandler;
  }

  private void addDonePathWatcher() {
    try {
      CompletableFuture<Void> onChildrenChanged = new CompletableFuture<>();
      onChildrenChanged.thenRun(this::wakeupOnDone);
      List<String> latestDone = this.schedulerCommon.getTaskStore().getChildren(schedulerCommon.getVersionedDoneFqPath(),
        onChildrenChanged);
      doneTasks.addAll(latestDone);
    } catch (PathMissingException e) {
      LOGGER.error("Fatal Internal Error: Root done path {} found missing in store",
        schedulerCommon.getVersionedDoneFqPath());
      throw new IllegalStateException(e);
    }
  }

  private void wakeupOnDone() {
    try {
      CompletableFuture<Void> onChildrenChanged = new CompletableFuture<>();
      onChildrenChanged.thenRun(this::wakeupOnDone);
      List<String> latestDone = this.schedulerCommon.getTaskStore().getChildren(schedulerCommon.getVersionedDoneFqPath(),
        onChildrenChanged);
      latestDone.forEach((taskName) -> allTasks.computeIfPresent(taskName, (k, v) -> {
        v.processEndTaskSignal();
        return null;
      }));
      doneTasks.addAll(latestDone);
    } catch (PathMissingException e) {
      LOGGER.warn("Root done path {} found missing in store", schedulerCommon.getVersionedDoneFqPath());
      // TODO: DX-68347 ; do proper error handling as a part of retry handling if we are not in the shutdown path
    } catch (Exception e) {
      LOGGER.warn("Unexpected exception on done path {} in store", schedulerCommon.getVersionedDoneFqPath());
      // TODO: DX-68347 ; do proper error handling as a part of retry handling if we are not in the shutdown path
    }
  }

  interface PerTaskDoneInfo extends PerTaskInfo {
    void markDone();
  }

  final class PerTaskDoneHandler {
    private final PerTaskDoneInfo doneInfo;
    private final String doneFqPathLocalRoot;
    private final String doneFqPathGlobal;

    PerTaskDoneHandler(PerTaskDoneInfo doneInfo) {
      this.doneInfo = doneInfo;
      this.doneFqPathLocalRoot = doneInfo.getTaskFqPath() + Path.SEPARATOR + schedulerCommon.getServiceVersion()
        + Path.SEPARATOR + COMPLETED_PATH_NAME;
      this.doneFqPathGlobal = schedulerCommon.getVersionedDoneFqPath() + Path.SEPARATOR + doneInfo.getTaskName();
    }

    /**
     * Signals the end of task for all instances from this instance.
     * <p>
     * Other instances will create a watcher on the done path and moment the task name appears in the 'done' root,
     * will mark 'done' themselves, so that the task is no longer picked up for execution in the entire cluster,
     * unless the entire cluster is restarted again.
     * </p>
     */
    void signalEndTaskByBookingOwner() {
      LOGGER.info("Signal task done for task {} to all instances", this);
      try {
        // Under one transaction:
        //    1. create task name ephemeral against the global done path, so that other service instance knows this
        //       task is complete and should no longer run until the entire cluster is restarted.
        //    2. create done sequential ephemeral against the task path and version, to signify that this instance is
        //       no longer interested in this task schedule as long as this instance is alive.
        //    3. delete the local booking for this task (for non-lock step schedules, as this instance is no longer
        //    interested in scheduling this task.
        PathCommand[] commands;
        if (doneInfo.getSchedule().isInLockStep()) {
          // do not remove booking path for lock step schedules as the next schedule with the same name uses
          // the same booking
          commands = new PathCommand[2];
          commands[0] = new PathCommand(CREATE_EPHEMERAL, doneFqPathGlobal);
          commands[1] = new PathCommand(CREATE_EPHEMERAL_SEQUENTIAL, TO_DONE_PATH_LOCAL.apply(doneFqPathLocalRoot));
        } else {
          commands = new PathCommand[3];
          commands[0] = new PathCommand(CREATE_EPHEMERAL, doneFqPathGlobal);
          commands[1] = new PathCommand(CREATE_EPHEMERAL_SEQUENTIAL, TO_DONE_PATH_LOCAL.apply(doneFqPathLocalRoot));
          commands[2] = new PathCommand(DELETE, doneInfo.getBookFqPathLocal());
        }
        schedulerCommon.getTaskStore().executeMulti(commands);
        events.taskDone(doneInfo.getTaskName());
      } catch (Exception e) {
        // an exception is not expected here. Log a warning as it is typically a fatal error.
        // but since the task is marked done already, the task will not run on this instance.
        LOGGER.warn("Unexpected exception for task {} while marking it done cluster wide", this, e);
      }
    }

    private void processEndTaskSignal() {
      LOGGER.info("Process end task signal received from another instance for {}", doneInfo.getTaskName());
      try {
        schedulerCommon.getTaskStore().executeSingle(new PathCommand(CREATE_EPHEMERAL_SEQUENTIAL,
          TO_DONE_PATH_LOCAL.apply(doneFqPathLocalRoot)));
        events.taskDone(doneInfo.getTaskName());
      } catch (PathExistsException e) {
        LOGGER.debug("Duplicate done signal for task {}", doneInfo.getTaskName(), e);
      } catch (Exception e) {
        // a fatal exception is not expected. But log a warning and mark the task done anyway
        LOGGER.warn("Unexpected exception processing task done signal for task {}", doneInfo.getTaskName(), e);
      } finally {
        doneInfo.markDone();
      }
    }

    private boolean hasDoneChildren() {
      boolean hasDoneChildren;
      try {
        List<String> donePaths = schedulerCommon.getTaskStore().getChildren(doneFqPathLocalRoot, null);
        hasDoneChildren = !donePaths.isEmpty();
      } catch (PathMissingException e) {
        return false;
      }
      return hasDoneChildren;
    }

    private void createCompletionRootPathIgnoreIfExists() {
      try {
        schedulerCommon.getTaskStore().executeSingle(new PathCommand(CREATE_PERSISTENT, doneFqPathLocalRoot));
      } catch (PathExistsException e) {
        LOGGER.debug("Duplicate done signal for task {}", doneInfo.getTaskName(), e);
      } catch (Exception e) {
        // a fatal exception is not expected.
        LOGGER.warn("Unexpected exception processing task done signal for task {}", doneInfo.getTaskName(), e);
        // TODO: DX-68347 ; do proper error handling as a part of retry handling; we may have to exit the process
      }
    }
  }
}

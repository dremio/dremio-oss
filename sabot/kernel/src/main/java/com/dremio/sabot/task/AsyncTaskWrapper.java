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
package com.dremio.sabot.task;

import java.util.concurrent.TimeUnit;

import com.dremio.sabot.task.TaskManager.TaskHandle;
import com.dremio.sabot.threads.AvailabilityCallback;
import com.dremio.sabot.threads.sharedres.SharedResourceType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Holds an overall asynchronous task, including the priority, the AsyncTask and the
 * final cleaner to be executed once the task is done.
 */
public class AsyncTaskWrapper implements Task {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AsyncTaskWrapper.class);

  enum WatchType {
    NONE,
    SLEEP,
    BLOCKED_ON_UPSTREAM,
    BLOCKED_ON_DOWNSTREAM,
    BLOCKED_ON_SHARED_RESOURCE,
    BLOCKED_ON_MEMORY;

    private static final int Size = values().length;
  }

  private Stopwatch[] watches = new Stopwatch[WatchType.Size];
  private WatchType runningWatch = WatchType.NONE;

  private final class TaskDescriptorImpl implements TaskDescriptor {
    private volatile TaskHandle<AsyncTaskWrapper> taskHandle = null;

    @Override
    public int getThread() {
      return (taskHandle != null) ? taskHandle.getThread(): -1;
    }

    @Override
    public long getSleepDuration() {
      return getDuration(WatchType.SLEEP);
    }

    @Override
    public long getTotalBlockedDuration() {
      return getDuration(WatchType.BLOCKED_ON_DOWNSTREAM) +
        getDuration(WatchType.BLOCKED_ON_UPSTREAM) + getDuration(WatchType.BLOCKED_ON_SHARED_RESOURCE);
    }

    private void setTaskHandle(TaskHandle<AsyncTaskWrapper> taskHandle) {
      this.taskHandle = taskHandle;
    }

    @Override
    public String toString() {
      return taskHandle != null ? taskHandle.toString() : "unknown task";
    }
  }

  private final SchedulingGroup<AsyncTaskWrapper> schedulingGroup;
  private final AsyncTask asyncTask;
  private final AutoCloseable cleaner;
  private final long taskWeight;

  private SharedResourceType blockedOnResource;
  private int lastThread;
  private long lastSleepDuration;
  private long warnMaxTime;

  private final TaskDescriptorImpl taskDescriptor = new TaskDescriptorImpl();

  public AsyncTaskWrapper(long taskWeight, SchedulingGroup<AsyncTaskWrapper> schedulingGroup, AsyncTask asyncTask,
                          AutoCloseable cleaner, int warnMaxTime) {
    super();
    this.taskWeight = taskWeight;
    this.schedulingGroup = Preconditions.checkNotNull(schedulingGroup, "Scheduling group required");
    this.asyncTask = Preconditions.checkNotNull(asyncTask);
    asyncTask.setTaskDescriptor(taskDescriptor);
    this.cleaner = Preconditions.checkNotNull(cleaner);
    this.warnMaxTime = warnMaxTime;

    for (int i = 0; i < WatchType.Size; i++) {
      watches[i] = Stopwatch.createUnstarted();
    }
    stateStarted();
  }

  public SchedulingGroup<AsyncTaskWrapper> getSchedulingGroup() {
    return schedulingGroup;
  }

  @Override
  public long getTaskWeight() {
    return taskWeight;
  }

  public void run() {
    stateEnded();
    try {
      if (getDuration(WatchType.SLEEP) - lastSleepDuration > warnMaxTime) {
        logger.warn("DHL: The task {} has been in sleep for {}ms. The task last ran on thread e{}",
          this, getDuration(WatchType.SLEEP) - lastSleepDuration, lastThread);
      }
      asyncTask.run();
      lastThread = taskDescriptor.getThread();
      lastSleepDuration = getDuration(WatchType.SLEEP);
    } finally {
      stateStarted();
    }
  }

  public AutoCloseable getCleaner() {
    return cleaner;
  }

  public void setAvailabilityCallback(final AvailabilityCallback callback) {
    asyncTask.setWakeupCallback(new AvailabilityCallback() {
      @Override
      public void nowAvailable() {
        unblocked();
        callback.nowAvailable();
      }
    });
  }

  private void unblocked() {
    Preconditions.checkState(isBlocked());
    stateEnded();
    asyncTask.refreshState();
    stateStarted();
  }

  @Override
  public State getState() {
    return asyncTask.getState();
  }

  boolean isBlocked() {
    switch (getState()) {
      case BLOCKED_ON_UPSTREAM:
      case BLOCKED_ON_DOWNSTREAM:
      case BLOCKED_ON_SHARED_RESOURCE:
      case BLOCKED_ON_MEMORY:
        return true;
      default:
        return false;
    }
  }

  private long getDuration(WatchType type) {
    return watches[type.ordinal()].elapsed(TimeUnit.MILLISECONDS);
  }

  private WatchType getWatchTypeForState(State state) {
    switch (state) {
      case RUNNABLE:
        return WatchType.SLEEP;
      case BLOCKED_ON_UPSTREAM:
        return WatchType.BLOCKED_ON_UPSTREAM;
      case BLOCKED_ON_DOWNSTREAM:
        return WatchType.BLOCKED_ON_DOWNSTREAM;
      case BLOCKED_ON_SHARED_RESOURCE:
        return WatchType.BLOCKED_ON_SHARED_RESOURCE;
      case BLOCKED_ON_MEMORY:
        return WatchType.BLOCKED_ON_MEMORY;
      default:
        return WatchType.NONE;
    }
  }

  private void stateStarted() {
    try {
      Preconditions.checkState(runningWatch == WatchType.NONE);
      WatchType wtype = getWatchTypeForState(getState());
      if (wtype != WatchType.NONE) {
        watches[wtype.ordinal()].start();
        runningWatch = wtype;
      }
      if (getState() == State.BLOCKED_ON_SHARED_RESOURCE) {
        // remember the resource that the task is blocked on.
        blockedOnResource = asyncTask.getFirstBlockedResource();
        if (blockedOnResource == null) {
          blockedOnResource = SharedResourceType.UNKNOWN;
        }
      }
    } catch (IllegalStateException e) {
      // we don't want to cause a task to be dropped from execution if we are not tracking this stat correctly
      logger.warn("stateStarted() called in the wrong order : state " + getState().name() + " runningWatch " + runningWatch.name(), e);
    }
  }

  private void stateEnded() {
    try {
      Preconditions.checkState(runningWatch != WatchType.NONE);
      long elapsed = 0;

      WatchType wtype = getWatchTypeForState(getState());
      if (wtype != WatchType.NONE) {
        watches[wtype.ordinal()].stop();
        runningWatch = WatchType.NONE;
        elapsed = getDuration(wtype);
      }

      switch (getState()) {
        case RUNNABLE:
          asyncTask.updateSleepDuration(elapsed);
          break;
        case BLOCKED_ON_DOWNSTREAM:
          asyncTask.updateBlockedOnDownstreamDuration(elapsed);
          break;
        case BLOCKED_ON_UPSTREAM:
          asyncTask.updateBlockedOnUpstreamDuration(elapsed);
          break;
        case BLOCKED_ON_SHARED_RESOURCE:
          watches[wtype.ordinal()].reset(); // differential counter, not cumulative.
          asyncTask.addBlockedOnSharedResourceDuration(blockedOnResource, elapsed);
          blockedOnResource = null;
          break;
        case BLOCKED_ON_MEMORY:
          asyncTask.updateBlockedOnMemoryDuration(elapsed);
          break;
      }
    } catch (IllegalStateException e) {
      // we don't want to cause a task to be dropped from execution if we are not tracking this stat correctly
      logger.warn("stateEnded() called in the wrong order : state " + getState().name() + " runningWatch " + runningWatch.name(), e);
    }
  }

  public void setTaskHandle(final TaskHandle<AsyncTaskWrapper> taskHandle) {
    taskDescriptor.setTaskHandle(taskHandle);
  }

  @Override
  public String toString() {
    return asyncTask.toString();
  }

  @VisibleForTesting
  public AsyncTask getAsyncTask() {
    return asyncTask;
  }
}

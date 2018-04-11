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
package com.dremio.sabot.task;

import java.util.concurrent.TimeUnit;

import com.dremio.exec.proto.CoordExecRPC.FragmentPriority;
import com.dremio.sabot.task.TaskManager.TaskHandle;
import com.dremio.sabot.threads.AvailabilityCallback;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Holds an overall asynchronous task, including the priority, the AsyncTask and the
 * final cleaner to be executed once the task is done.
 */
public class AsyncTaskWrapper implements Task {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AsyncTaskWrapper.class);

  private final class TaskDescriptorImpl implements TaskDescriptor {
    private volatile TaskHandle<AsyncTaskWrapper> taskHandle = null;

    @Override
    public int getThread() {
      return (taskHandle != null) ? taskHandle.getThread(): -1;
    }

    @Override
    public long getSleepDuration() {
      return sleepWatch.elapsed(TimeUnit.MILLISECONDS);
    }

    @Override
    public long getBlockedDuration() {
      return blockedWatch.elapsed(TimeUnit.MILLISECONDS);
    }

    private void setTaskHandle(TaskHandle<AsyncTaskWrapper> taskHandle) {
      this.taskHandle = taskHandle;
    }

    @Override
    public String toString() {
      return taskHandle != null ? taskHandle.toString() : "unknown task";
    }
  }

  private final FragmentPriority priority;
  private final AsyncTask asyncTask;
  private final AutoCloseable cleaner;

  private final Stopwatch sleepWatch = Stopwatch.createUnstarted();
  private final Stopwatch blockedWatch = Stopwatch.createUnstarted();

  private final TaskDescriptorImpl taskDescriptor = new TaskDescriptorImpl();

  public AsyncTaskWrapper(FragmentPriority priority, AsyncTask asyncTask, AutoCloseable cleaner) {
    super();
    Preconditions.checkNotNull(priority);
    Preconditions.checkNotNull(asyncTask);
    Preconditions.checkNotNull(cleaner);
    this.priority = priority;
    this.asyncTask = asyncTask;
    asyncTask.setTaskDescriptor(taskDescriptor);

    this.cleaner = cleaner;

    sleepStarted();
  }

  public FragmentPriority getPriority() {
    return priority;
  }

  public void run() {
    sleepEnded();
    try {
      asyncTask.run();
    } finally {
      if (getState() == State.BLOCKED) {
        blockedStarted();
      } else if (getState() == State.RUNNABLE) {
        sleepStarted();
      }
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
    Preconditions.checkState(getState() == State.BLOCKED);
    asyncTask.refreshState();
    blockedEnded();
    sleepStarted();
  }

  @Override
  public State getState() {
    return asyncTask.getState();
  }

  private void sleepStarted() {
    try {
      Preconditions.checkState(getState() == State.RUNNABLE);
      Preconditions.checkState(!blockedWatch.isRunning());
      sleepWatch.start();
    } catch (IllegalStateException e) {
      // we don't want to cause a task to be dropped from execution if we are not tracking this stat correctly
      logger.warn("sleepStarted() called in the wrong order.", e);
    }
  }

  private void sleepEnded() {
    try {
      sleepWatch.stop();
      asyncTask.updateSleepDuration(sleepWatch.elapsed(TimeUnit.MILLISECONDS));
    } catch (IllegalStateException e) {
      logger.warn("sleepEnded() called in the wrong order.", e);
    }
  }

  private void blockedStarted() {
    try {
      Preconditions.checkState(getState() == State.BLOCKED);
      Preconditions.checkState(!sleepWatch.isRunning());
      blockedWatch.start();
    } catch (IllegalStateException e) {
      logger.warn("blockedStarted() called in the wrong order.", e);
    }
  }

  private void blockedEnded() {
    try {
      blockedWatch.stop();
      asyncTask.updateBlockedDuration(blockedWatch.elapsed(TimeUnit.MILLISECONDS));
    } catch (IllegalStateException e) {
      logger.warn("blockedEnded() called in the wrong order.", e);
    }
  }

  public void setTaskHandle(final TaskHandle<AsyncTaskWrapper> taskHandle) {
    taskDescriptor.setTaskHandle(taskHandle);
  }
}

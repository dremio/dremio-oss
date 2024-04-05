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

/** Observer on the manager's primary events */
public interface Observer<T extends Task> {
  enum Type {
    DUMMY,
    BASIC,
    VERBOSE
  }

  /**
   * Called whenever a new task is added to the manager or to a {@link SchedulingGroup}
   *
   * @param task new task
   * @param thread thread the task was added to
   */
  void addTask(TaskManager.TaskHandle<T> task, int thread);

  /**
   * Called whenever a runnable task is context switched out for a new and more eligible runnable
   * task.
   *
   * @param from Currently running task
   * @param to New task
   * @param thread thread on which the task was switched
   */
  void switchTask(TaskManager.TaskHandle<T> from, TaskManager.TaskHandle<T> to, int thread);

  /**
   * Called whenever a task is dequeued from the run queue as it is blocked.
   *
   * @param task blocked task
   * @param thread thread on which it was running when blocked
   */
  void dequeueTask(TaskManager.TaskHandle<T> task, int thread);

  /**
   * Called whenever a task is unblocked and is ready to be put back in the run queue.
   *
   * @param task runnable task
   * @param oldThread old thread where it was running when last blocked
   * @param newThread new thread where it will run now
   */
  void enqueueTask(TaskManager.TaskHandle<T> task, int oldThread, int newThread);

  /**
   * Called whenever a runnable task is migrated from one thread to another on steal requests.
   *
   * @param task migrated task
   * @param srcThread source thread of migration
   * @param dstThread destination thread of migration
   */
  void rebalance(TaskManager.TaskHandle<T> task, int srcThread, int dstThread);

  /**
   * Called whenever a thread requests more work and it gets rejected. The thread is quite likely
   * going idle
   *
   * @param thread thread whose request was rejected
   */
  void workRequestRejected(int thread);
}

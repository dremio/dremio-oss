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

/**
 * Observer on the manager's primary events
 */
public interface Observer<T extends Task> {

  /**
   * Called whenever a new task is added to the manager or to a {@link SchedulingGroup}
   *
   * @param task new task
   * @param thread thread the task was added to
   */
  void addTask(TaskManager.TaskHandle<T> task, int thread);

  /**
   * Called whenever a runnable task is migrated from one thread to another
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

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

/**
 * The main purpose of load balancing is to keep all CPUs (executing threads) busy.<br>
 * <br>
 * Each executing thread has it's own runqueue and associated scheduler.<br>
 * Ideally when a task yields we want to pick up the task with the smallest vruntime in all runqueues.<br>
 * In practice, each scheduler selects the best task from it's own runqueue, and the load balancer moves
 * tasks between the threads whenever there is an imbalance between their runqueues.
 */
public interface TaskManager<T extends Task> extends GroupManager<T> {

  long MAX_WEIGHT = 1000; // using higher values that MAX_WEIGHT will cause vRuntime overflow to occur sooner than can be handled by the system

  /**
   * Listener called whenever a task has been added to a thread. Useful to wake up the
   * corresponding executing thread if it was idle.
   */
  interface WakeUpListener {
    void wakeUpIfIdle();
  }

  /**
   * When a task is added to the load balancer, an entity instance is returned
   * to the caller so the task can be woken up if it gets blocked.
   * TaskHandle also exposes information and stats about the task
   */
  interface TaskHandle<T extends Task> {

    T getTask();

    /**
     * Called when the task become runnable after it was previously blocked.
     */
    void reEnqueue();

    /**
     * @return which thread the task is assigned to
     */
    int getThread();
  }

  /**
   * Scheduling interface specific to a single executing thread.<br>
   * The methods of this interface are expected to always be called from the same thread
   */
  interface TaskProvider<T extends Task> {

    /**
     * @param time elapsed time since last call in nanoseconds
     * @return task selected to run next
     */
    TaskHandle<T> getTask(long time);

    int getNumTasks();

    int getNumStaged();

    int getNumWorkRequests();
  }

  /**
   * add task to the root (no parent group)
   *
   * @param task new task
   * @param weight task's priority
   *
   * @return corresponding task handle
   */
  TaskHandle<T> addTask(T task, long weight);

  /**
   * retrieve task provider for a thread
   *
   * @param thread thread index
   * @param listener wake up listener
   *
   * @return task provider
   */
  TaskProvider<T> getTaskProvider(int thread, WakeUpListener listener);
}

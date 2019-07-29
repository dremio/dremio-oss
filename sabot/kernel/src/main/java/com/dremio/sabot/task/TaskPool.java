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

import java.util.Collections;

public interface TaskPool extends AutoCloseable {

  void execute(AsyncTaskWrapper task);

  default Iterable<ThreadInfo> getSlicingThreads() {
    return Collections.emptyList();
  }

  /**
   * Determines if all threads created by the taskpool are alive
   * @return true, if all threads are alive; false otherwise
   */
  default boolean areAllThreadsAlive() { return true; }

  GroupManager<AsyncTaskWrapper> getGroupManager();

  class ThreadInfo {
    /** current Java thread name */
    public final String threadName;
    /** slicing thread Id */
    public final int slicingThreadId;
    /** OS thread Id */
    public final int osThreadId;
    /** cpu id (core) */
    public final int cpuId;

    public final int numTasks;
    public final int numStagedTasks;
    public final int numRequestedWork;

    /** Java thread id**/
    public final long threadId;


    public ThreadInfo(String threadName, int slicingThreadId, int osThreadId, int cpuId, int numTasks, int numStagedTasks,
               int numRequestedWork, long threadId) {
      this.threadName = threadName;
      this.slicingThreadId = slicingThreadId;
      this.osThreadId = osThreadId;
      this.cpuId = cpuId;
      this.numTasks = numTasks;
      this.numStagedTasks = numStagedTasks;
      this.numRequestedWork = numRequestedWork;
      this.threadId = threadId;
    }

  }}

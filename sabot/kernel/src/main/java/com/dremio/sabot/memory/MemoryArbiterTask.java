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
package com.dremio.sabot.memory;

/**
 * Tasks managed by the MemoryArbiter should implement this interface. This
 * interface contains methods that the MemoryArbiter needs to manage tasks
 */
public interface MemoryArbiterTask {
  /**
   * Obtains a unique taskId for each task
   * @return
   */
  String getTaskId();

  /**
   * Returns the memory used by the task
   * @return
   */
  long getUsedMemory();

  /**
   * Returns the memory grant in bytes assigned to this task
   * @return
   */
  long getMemoryGrant();

  /**
   * Sets the memory grant in bytes
   *
   * @param memoryGrantInBytes Memory grant assigned in bytes
   */
  void setMemoryGrant(long memoryGrantInBytes);
}

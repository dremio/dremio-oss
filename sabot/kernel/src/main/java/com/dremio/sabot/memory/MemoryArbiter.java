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

import com.dremio.common.config.SabotConfig;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.FragmentExecutors;
import com.dremio.sabot.exec.QueriesClerk;
import org.apache.arrow.memory.RootAllocator;

/**
 * This interface defines the MemoryArbiter. The MemoryArbiter allows sharing of memory by various
 * tasks
 */
public interface MemoryArbiter extends AutoCloseable {
  static MemoryArbiter newInstance(
      SabotConfig sabotConfig,
      RootAllocator rootAllocator,
      FragmentExecutors fragmentExecutors,
      QueriesClerk clerk,
      final OptionManager options) {
    MemoryArbiterFactory memoryArbiterFactory =
        sabotConfig.getInstance(
            MemoryArbiterFactory.DREMIO_MEMORY_ARBITER_FACTORY_CLASS,
            MemoryArbiterFactory.class,
            DefaultMemoryArbiter.Factory.class);

    return memoryArbiterFactory.newInstance(
        sabotConfig, rootAllocator, fragmentExecutors, clerk, options);
  }

  /**
   * This method notifies that the task is done
   *
   * @param memoryArbiterTask The task that is done
   */
  void taskDone(MemoryArbiterTask memoryArbiterTask);

  /**
   * This method notifies that the task starts
   *
   * @param memoryArbiterTask The task that is starting
   */
  void startTask(MemoryArbiterTask memoryArbiterTask);

  /**
   * Acquires a grant of memory to run the task
   *
   * @param memoryArbiterTask The task that is requesting the grant
   * @param memoryGrantInBytes The memory size in bytes
   * @return true, if the request has been granted
   */
  boolean acquireMemoryGrant(MemoryArbiterTask memoryArbiterTask, long memoryGrantInBytes);

  /**
   * Releases the memory grant previously acquired
   *
   * @param memoryArbiterTask The task that acquired the grant
   */
  void releaseMemoryGrant(MemoryArbiterTask memoryArbiterTask);

  /**
   * Removes the task from blocked tasks
   *
   * @param memoryArbiterTask The task that needs to be removed from blocked tasks
   */
  boolean removeFromBlocked(MemoryArbiterTask memoryArbiterTask);

  void removeFromSpilling(MemoryTaskAndShrinkableOperator memoryTaskAndShrinkableOperator);

  void addTaskToQueue(MemoryArbiterTask memoryArbiterTask);

  public static interface MemoryArbiterFactory {
    String DREMIO_MEMORY_ARBITER_FACTORY_CLASS = "dremio.memory.arbiter.factory.class";

    MemoryArbiter newInstance(
        SabotConfig sabotConfig,
        RootAllocator rootAllocator,
        FragmentExecutors fragmentExecutors,
        QueriesClerk clerk,
        OptionManager options);
  }
}

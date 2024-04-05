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

import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.sabot.op.spi.Operator;
import java.util.List;

/**
 * Tasks managed by the MemoryArbiter should implement this interface. This interface contains
 * methods that the MemoryArbiter needs to manage tasks
 */
public interface MemoryArbiterTask {
  /**
   * Obtains a unique taskId for each task
   *
   * @return
   */
  String getTaskId();

  /**
   * Returns the memory used by the task
   *
   * @return
   */
  long getUsedMemory();

  /**
   * Returns the memory grant in bytes assigned to this task
   *
   * @return
   */
  long getMemoryGrant();

  /**
   * Sets the memory grant in bytes
   *
   * @param memoryGrantInBytes Memory grant assigned in bytes
   */
  void setMemoryGrant(long memoryGrantInBytes);

  /**
   * @return Return a non-empty list shrinkable operators associated with this task. If there are no
   *     shrinkable operators, return an empty list
   */
  List<MemoryTaskAndShrinkableOperator> getShrinkableOperators();

  /**
   * Request the shrinkable operator to shrink memory usage
   *
   * @param shrinkableOperator The operator that needs to shrink memory usage
   * @param currentShrinkableMemory The amount of shrinkable memory reported by the shrinkable
   *     operator
   * @throws Exception
   */
  void shrinkMemory(Operator.ShrinkableOperator shrinkableOperator, long currentShrinkableMemory)
      throws Exception;

  /**
   * Returns true if the shrinkable operator is already shrinking memory
   *
   * @param shrinkableOperator The operator that is being queried
   * @return
   */
  default boolean isOperatorShrinkingMemory(Operator.ShrinkableOperator shrinkableOperator) {
    return false;
  }

  /** This task is blocked on memory */
  default void blockOnMemory() {}

  /** Memory may be available and this task is now unblocked */
  default void unblockOnMemory() {}

  default long getBlockedOnMemoryStartTime() {
    return 0;
  }

  default QueryId getQueryId() {
    return null;
  }
}

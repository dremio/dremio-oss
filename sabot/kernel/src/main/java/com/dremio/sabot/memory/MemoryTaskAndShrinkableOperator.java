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

import com.dremio.sabot.op.spi.Operator;
import java.util.Comparator;

/** A utility class tracking the memory task and the shrinkable operator */
public class MemoryTaskAndShrinkableOperator {
  private final MemoryArbiterTask memoryArbiterTask;
  private final Operator.ShrinkableOperator shrinkableOperator;
  private String stringToDisplay = null;
  private long currentShrinkableMemory = 0L;

  public MemoryTaskAndShrinkableOperator(
      MemoryArbiterTask memoryArbiterTask, Operator.ShrinkableOperator shrinkableOperator) {
    this.memoryArbiterTask = memoryArbiterTask;
    this.shrinkableOperator = shrinkableOperator;
  }

  public Operator.ShrinkableOperator getShrinkableOperator() {
    return shrinkableOperator;
  }

  public boolean hasShrinkableMemoryChanged() {
    long newShrinkableMemory = this.shrinkableOperator.shrinkableMemory();
    return (currentShrinkableMemory != newShrinkableMemory);
  }

  public long updateShrinkableMemory() {
    this.currentShrinkableMemory = this.shrinkableOperator.shrinkableMemory();
    return this.currentShrinkableMemory;
  }

  public long getShrinkableMemory() {
    return this.currentShrinkableMemory;
  }

  public void shrinkMemory(long currentShrinkableMemory) throws Exception {
    memoryArbiterTask.shrinkMemory(shrinkableOperator, currentShrinkableMemory);
  }

  public boolean isOperatorShrinkingMemory() {
    return memoryArbiterTask.isOperatorShrinkingMemory(shrinkableOperator);
  }

  public boolean canUseTooMuchMemoryInAPump() {
    return this.shrinkableOperator.canUseTooMuchMemoryInAPump();
  }

  public void setLimit(long limit) {
    this.shrinkableOperator.setLimit(limit);
  }

  public long getAllocatedMemory() {
    return this.shrinkableOperator.getAllocatedMemory();
  }

  public static Comparator<MemoryTaskAndShrinkableOperator> getComparator() {
    return Comparator.comparingLong(MemoryTaskAndShrinkableOperator::getShrinkableMemory)
        .reversed();
  }

  @Override
  public String toString() {
    if (stringToDisplay == null) {
      final StringBuffer buffer = new StringBuffer();
      buffer
          .append(memoryArbiterTask.getTaskId())
          .append(":")
          .append(shrinkableOperator.getOperatorId())
          .append("(shrinkable memory: ")
          .append(shrinkableOperator.shrinkableMemory())
          .append(")")
          .append("(operator state: ")
          .append(shrinkableOperator.getOperatorStateToPrint())
          .append(")");
      stringToDisplay = buffer.toString();
    }

    return stringToDisplay;
  }
}

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

package com.dremio.sabot.op.aggregate.vectorized.arrayagg;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;

public final class IntArrayAggAccumulatorHolder extends ArrayAggAccumulatorHolder<Integer> {
  private final IntVector vector;

  public IntArrayAggAccumulatorHolder(final BufferAllocator allocator, int initialCapacity) {
    super(allocator, initialCapacity);
    vector = new IntVector("array_agg IntArrayAggAccumulatorHolder", allocator);
    vector.allocateNew(initialCapacity);
  }

  @Override
  public long getSizeInBytes() {
    return vector.getDataBuffer().getActualMemoryConsumed()
        + vector.getValidityBuffer().getActualMemoryConsumed()
        + super.getSizeInBytes();
  }

  @Override
  public void close() {
    super.close();
    vector.close();
  }

  @Override
  public void addItemToVector(Integer data, int index) {
    vector.set(index, data);
    vector.setValueCount(vector.getValueCount() + 1);
  }

  @Override
  public Integer getItem(int index) {
    return vector.get(index);
  }

  @Override
  public double getSizeOfElement(Integer element) {
    return IntVector.TYPE_WIDTH;
  }

  @Override
  public void reAllocIfNeeded(Integer data) {
    super.reAllocIfNeeded(data);
    if (getValueCount() + 1 >= vector.getValueCapacity()) {
      vector.reAlloc();
    }
  }
}

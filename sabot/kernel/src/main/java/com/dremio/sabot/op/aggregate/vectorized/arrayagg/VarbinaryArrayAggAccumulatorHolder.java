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

import com.dremio.exec.expr.fn.impl.ByteArrayWrapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;

public final class VarbinaryArrayAggAccumulatorHolder
    extends ArrayAggAccumulatorHolder<ByteArrayWrapper> {
  private final VarBinaryVector vector;
  private double accumulatedBytes = 0;

  public VarbinaryArrayAggAccumulatorHolder(final BufferAllocator allocator, int initialCapacity) {
    super(allocator, initialCapacity);
    vector = new VarBinaryVector("array_agg VarbinaryArrayAggAccumulatorHolder", allocator);
    vector.allocateNew(initialCapacity);
  }

  @Override
  public long getSizeInBytes() {
    return vector.getDataBuffer().getActualMemoryConsumed()
        + vector.getValidityBuffer().getActualMemoryConsumed()
        + vector.getOffsetBuffer().getActualMemoryConsumed()
        + super.getSizeInBytes();
  }

  @Override
  public void close() {
    super.close();
    vector.close();
  }

  @Override
  public void addItemToVector(ByteArrayWrapper data, int index) {
    vector.set(index, data.getBytes());
    accumulatedBytes += getSizeOfElement(data);
  }

  @Override
  public ByteArrayWrapper getItem(int index) {
    byte[] element = vector.get(index);
    return new ByteArrayWrapper(element);
  }

  @Override
  public double getSizeOfElement(ByteArrayWrapper element) {
    return element.getLength();
  }

  @Override
  public void reAllocIfNeeded(ByteArrayWrapper data) {
    super.reAllocIfNeeded(data);
    if (accumulatedBytes + getSizeOfElement(data) > vector.getByteCapacity()) {
      vector.reAlloc();
    }
  }
}

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
import org.apache.arrow.vector.VarBinaryVector;

import com.dremio.exec.expr.fn.impl.ByteArrayWrapper;

public final class VarbinaryArrayAggAccumulatorHolder extends BaseArrayAggAccumulatorHolder<ByteArrayWrapper, VarBinaryVector> {
  private final VarBinaryVector vector;
  public VarbinaryArrayAggAccumulatorHolder(int maxValuesPerBatch, final BufferAllocator allocator) {
    super(maxValuesPerBatch, allocator);
    vector = new VarBinaryVector("array_agg VarbinaryArrayAggAccumulatorHolder", allocator);
    vector.allocateNew(maxValuesPerBatch);
  }

  @Override
  public long getSizeInBytes() {
    return vector.getDataBuffer().getActualMemoryConsumed() +
      vector.getValidityBuffer().getActualMemoryConsumed() +
      vector.getOffsetBuffer().getActualMemoryConsumed() +
      super.getSizeInBytes();
  }

  @Override
  public void close() {
    super.close();
    vector.close();
  }

  @Override
  public void addItemToVector(ByteArrayWrapper data, int index) {
    vector.set(index, data.getBytes());
  }

  @Override
  public ByteArrayWrapper getItem(int index) {
    byte[] element = vector.get(index);
    return new ByteArrayWrapper(element);
  }
}

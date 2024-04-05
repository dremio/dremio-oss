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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;

public final class VarcharArrayAggAccumulatorHolder
    extends BaseArrayAggAccumulatorHolder<Text, VarCharVector> {
  private final VarCharVector vector;

  public VarcharArrayAggAccumulatorHolder(int maxValuesPerBatch, final BufferAllocator allocator) {
    super(maxValuesPerBatch, allocator);
    vector = new VarCharVector("array_agg VarcharArrayAggAccumulatorHolder", allocator);
    vector.allocateNew(maxValuesPerBatch);
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
  public void addItemToVector(Text data, int index) {
    vector.set(index, data);
  }

  @Override
  public Text getItem(int index) {
    return vector.getObject(index);
  }
}

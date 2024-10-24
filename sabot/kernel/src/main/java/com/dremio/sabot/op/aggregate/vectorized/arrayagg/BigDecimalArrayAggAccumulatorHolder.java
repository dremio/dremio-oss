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

import java.math.BigDecimal;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;

public final class BigDecimalArrayAggAccumulatorHolder
    extends ArrayAggAccumulatorHolder<BigDecimal> {
  private final DecimalVector vector;

  public BigDecimalArrayAggAccumulatorHolder(
      final BufferAllocator allocator, DecimalVector inputVector, int initialCapacity) {
    super(allocator, initialCapacity);
    vector =
        new DecimalVector(
            "array_agg BigDecimalArrayAggAccumulatorHolder",
            allocator,
            inputVector.getPrecision(),
            inputVector.getScale());
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
  public void addItemToVector(BigDecimal data, int index) {
    vector.set(index, data);
    vector.setValueCount(vector.getValueCount() + 1);
  }

  @Override
  public BigDecimal getItem(int index) {
    return vector.getObject(index);
  }

  @Override
  public double getSizeOfElement(BigDecimal element) {
    return DecimalVector.TYPE_WIDTH;
  }

  @Override
  public void reAllocIfNeeded(BigDecimal data) {
    super.reAllocIfNeeded(data);
    if (getValueCount() + 1 >= vector.getValueCapacity()) {
      vector.reAlloc();
    }
  }
}

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

import io.netty.util.internal.PlatformDependent;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;

/** Accumulator for ARRAY_AGG for BIGINT type */
public final class BigIntArrayAggAccumulator extends ArrayAggAccumulator<Long> {
  public BigIntArrayAggAccumulator(
      FieldVector input,
      FieldVector transferVector,
      BaseValueVector tempAccumulatorHolder,
      BufferAllocator allocator,
      int maxFieldSizeBytes,
      int initialVectorSize) {
    super(
        input,
        transferVector,
        tempAccumulatorHolder,
        allocator,
        maxFieldSizeBytes,
        initialVectorSize);
  }

  @Override
  protected int getFieldWidth() {
    return BigIntVector.TYPE_WIDTH;
  }

  @Override
  protected void writeItem(UnionListWriter writer, Long item) {
    writer.writeBigInt(item);
  }

  @Override
  protected ArrayAggAccumulatorHolder<Long> getAccumulatorHolder(
      int maxFieldSizeBytes, BufferAllocator allocator, int initialCapacity) {
    return new BigIntArrayAggAccumulatorHolder(allocator, initialCapacity);
  }

  @Override
  protected Long getElement(
      long baseAddress, int itemIndex, ArrowBuf dataBuffer, ArrowBuf offsetBuffer) {
    long offHeapMemoryAddress = getOffHeapAddressForFixedWidthTypes(baseAddress, itemIndex);
    return PlatformDependent.getLong(offHeapMemoryAddress);
  }
}

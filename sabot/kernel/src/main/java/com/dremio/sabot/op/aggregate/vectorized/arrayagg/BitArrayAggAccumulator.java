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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;

public final class BitArrayAggAccumulator extends ArrayAggAccumulator<Integer> {
  private final int maxFieldSizeBytes;

  public BitArrayAggAccumulator(
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
    this.maxFieldSizeBytes = maxFieldSizeBytes;
  }

  @Override
  public int getDataBufferSize() {
    return maxFieldSizeBytes;
  }

  @Override
  protected int getFieldWidth() {
    // One byte of BitVector corresponds to 8 rows of data. To technical the field width is 1/8
    // bytes.
    // However, fractional field widths are not useful for where `getFieldWidth` is used (Data
    // buffer
    // size estimation, evaluating offHeapMemoryAddress of Nth field). Therefore, we are performing
    // those
    // through override. getFieldWidth for BIT is expected not to get called.
    throw new UnsupportedOperationException("Field width is not defined for BIT type.");
  }

  @Override
  protected void writeItem(UnionListWriter writer, Integer item) {
    writer.writeBit(item);
  }

  @Override
  protected ArrayAggAccumulatorHolder<Integer> getAccumulatorHolder(
      int maxFieldSizeBytes, BufferAllocator allocator, int initialCapacity) {
    return new BitArrayAggAccumulatorHolder(allocator, initialCapacity);
  }

  @Override
  protected Integer getElement(
      long baseAddress, int itemIndex, ArrowBuf dataBuffer, ArrowBuf offsetBuffer) {
    // Incorporating the fact that every single bit in BitVector corresponds to one data item.
    // Bit 0 (index 0 bit 1) for row 0, bit 10 (index 1 bit 2) for row 10, etc.
    // For index N, last 3 bits represent offset within the byte block and higher bits represent
    // byte index.
    return PlatformDependent.getByte(baseAddress + (itemIndex >>> 3)) >>> (itemIndex & 7) & 1;
  }
}

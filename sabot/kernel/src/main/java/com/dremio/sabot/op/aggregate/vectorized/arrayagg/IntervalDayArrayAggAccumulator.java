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
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;

public final class IntervalDayArrayAggAccumulator
    extends ArrayAggAccumulator<NullableIntervalDayHolder> {
  public IntervalDayArrayAggAccumulator(
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
    return IntervalDayVector.TYPE_WIDTH;
  }

  @Override
  protected void writeItem(UnionListWriter writer, NullableIntervalDayHolder item) {
    writer.writeIntervalDay(item.days, item.milliseconds);
  }

  @Override
  protected ArrayAggAccumulatorHolder<NullableIntervalDayHolder> getAccumulatorHolder(
      int maxFieldSizeBytes, BufferAllocator allocator, int initialCapacity) {
    return new IntervalDayArrayAggAccumulatorHolder(allocator, initialCapacity);
  }

  @Override
  protected NullableIntervalDayHolder getElement(
      long baseAddress, int itemIndex, ArrowBuf dataBuffer, ArrowBuf offsetBuffer) {
    long offHeapMemoryAddress = getOffHeapAddressForFixedWidthTypes(baseAddress, itemIndex);
    long element = PlatformDependent.getLong(offHeapMemoryAddress);
    /* first 4 bytes are the number of days (in little endian, that's the bottom 32 bits)
    second 4 bytes are the number of milliseconds (in little endian, that's the top 32 bits)
    See com.dremio.sabot.op.aggregate.vectorized.MinAccumulators.IntervalDayMinAccumulator
    */
    NullableIntervalDayHolder ret = new NullableIntervalDayHolder();
    ret.days = (int) element;
    ret.milliseconds = (int) (element >>> 32);
    ret.isSet = 1;
    return ret;
  }
}

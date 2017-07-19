/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.aggregate.vectorized;

import java.util.List;

import org.apache.arrow.vector.FieldVector;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

public class SumZeroAccumulators {

  private SumZeroAccumulators(){}

  public static class IntSumZeroAccumulator extends BaseSingleAccumulator {

    private static final int WIDTH = 4;

    public IntSumZeroAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomingBit = buffers.get(0).memoryAddress();
      final long incomingValue = buffers.get(1).memoryAddress();
      final long[] valueAddresses = this.valueAddresses;

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final int newVal = PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH)) * bitVal;
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        final long sumAddr = valueAddresses[tableIndex >>> LBlockHashTable.BITS_IN_CHUNK] + (tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK) * 8;
        PlatformDependent.putLong(sumAddr, PlatformDependent.getLong(sumAddr) + newVal);
      }
    }
  }

  public static class FloatSumZeroAccumulator extends BaseSingleAccumulator {

    private static final int WIDTH = 4;

    public FloatSumZeroAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomingBit = buffers.get(0).memoryAddress();
      final long incomingValue = buffers.get(1).memoryAddress();
      final long[] valueAddresses = this.valueAddresses;

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final float newVal = Float.intBitsToFloat(PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH)) * bitVal);
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        final long sumAddr = valueAddresses[tableIndex >>> LBlockHashTable.BITS_IN_CHUNK] + (tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK) * 8;
        PlatformDependent.putLong(sumAddr, Double.doubleToLongBits(Double.longBitsToDouble(PlatformDependent.getLong(sumAddr)) + newVal));
      }
    }
  }

  public static class BigIntSumZeroAccumulator extends BaseSingleAccumulator {

    private static final int WIDTH = 8;
    public BigIntSumZeroAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomingBit = buffers.get(0).memoryAddress();
      final long incomingValue = buffers.get(1).memoryAddress();
      final long[] valueAddresses = this.valueAddresses;

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final long newVal = PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH)) * bitVal;
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        final long sumAddr = valueAddresses[tableIndex >>> LBlockHashTable.BITS_IN_CHUNK] + (tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK) * 8;
        PlatformDependent.putLong(sumAddr, PlatformDependent.getLong(sumAddr) + newVal);
      }
    }
  }

  public static class DoubleSumZeroAccumulator extends BaseSingleAccumulator {

    private static final int WIDTH = 8;

    public DoubleSumZeroAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomingBit = buffers.get(0).memoryAddress();
      final long incomingValue = buffers.get(1).memoryAddress();
      final long[] valueAddresses = this.valueAddresses;

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final double newVal = Double.longBitsToDouble(PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH)) * bitVal);
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        final long sumAddr = valueAddresses[tableIndex >>> LBlockHashTable.BITS_IN_CHUNK] + (tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK) * 8;
        PlatformDependent.putLong(sumAddr, Double.doubleToLongBits(Double.longBitsToDouble(PlatformDependent.getLong(sumAddr)) + newVal));
      }
    }
  }

}

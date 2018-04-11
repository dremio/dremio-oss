/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

public class MinAccumulators {

  private MinAccumulators(){};

  public static class IntMinAccumulator extends BaseSingleAccumulator {
    private static final long INIT = 0x7fffffff7fffffffl;
    private static final int WIDTH = 4;

    public IntMinAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomingBit = buffers.get(0).memoryAddress();
      final long incomingValue = buffers.get(1).memoryAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int newVal = PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTable.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK;
        final long minAddr = valueAddresses[chunkIndex] + (chunkOffset) * 4;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putInt(minAddr, min(PlatformDependent.getInt(minAddr), newVal, bitVal));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class FloatMinAccumulator extends BaseSingleAccumulator {
    private static final long INIT = 0x7f7fffff7f7fffffl;
    private static final int WIDTH = 4;

    public FloatMinAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomingBit = buffers.get(0).memoryAddress();
      final long incomingValue = buffers.get(1).memoryAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final float newVal = Float.intBitsToFloat(PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH)));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTable.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK;
        final long minAddr = valueAddresses[chunkIndex] + (chunkOffset) * 4;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putInt(minAddr, Float.floatToIntBits(min(Float.intBitsToFloat(PlatformDependent.getInt(minAddr)), newVal, bitVal)));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class BigIntMinAccumulator extends BaseSingleAccumulator {

    private static final int WIDTH = 8;

    public BigIntMinAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, Long.MAX_VALUE);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomingBit = buffers.get(0).memoryAddress();
      final long incomingValue = buffers.get(1).memoryAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){

        final long newVal = PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTable.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK;
        final long minAddr = valueAddresses[chunkIndex] + (chunkOffset) * 8;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putLong(minAddr, min(PlatformDependent.getLong(minAddr), newVal, bitVal));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class DoubleMinAccumulator extends BaseSingleAccumulator {

    private static final long INIT = Double.doubleToLongBits(Double.MAX_VALUE);
    private static final int WIDTH = 8;

    public DoubleMinAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomingBit = buffers.get(0).memoryAddress();
      final long incomingValue = buffers.get(1).memoryAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final double newVal = Double.longBitsToDouble(PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH)));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTable.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK;
        final long minAddr = valueAddresses[chunkIndex] + (chunkOffset) * 8;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putLong(minAddr, Double.doubleToLongBits(min(Double.longBitsToDouble(PlatformDependent.getLong(minAddr)), newVal, bitVal)));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  private static final long min(long a, long b, int bitVal){
    // update the incoming value to either be the max (if the incoming is null) or keep as is (if the value is not null)
    b = b * bitVal + Long.MAX_VALUE * (bitVal ^ 1);
    return Math.min(a,b);
  }

  private static final int min(int a, int b, int bitVal){
    b = b * bitVal + Integer.MAX_VALUE * (bitVal ^ 1);
    return Math.min(a,b);
  }

  private static final double min(double a, double b, int bitVal){
    if(bitVal == 1){
      return Math.min(a, b);
    }
    return a;
  }

  private static final float min(float a, float b, int bitVal){
    if(bitVal == 1){
      return Math.min(a, b);
    }
    return a;
  }
}

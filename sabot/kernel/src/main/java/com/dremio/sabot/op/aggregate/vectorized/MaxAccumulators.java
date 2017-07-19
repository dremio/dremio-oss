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

public class MaxAccumulators {

  private MaxAccumulators(){};

  public static class IntMaxAccumulator extends BaseSingleAccumulator {
    private static final long INIT = 0x8000000080000000l;
    private static final int WIDTH = 4;

    public IntMaxAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxMemAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomaxgBit = buffers.get(0).memoryAddress();
      final long incomaxgValue = buffers.get(1).memoryAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;

      int incomaxgIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxMemAddr; ordinalAddr += 4, incomaxgIndex++){
        final int newVal = PlatformDependent.getInt(incomaxgValue + (incomaxgIndex * WIDTH));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTable.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK;
        final long maxAddr = valueAddresses[chunkIndex] + (chunkOffset) * 4;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomaxgBit + ((incomaxgIndex >>> 3))) >>> (incomaxgIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putInt(maxAddr, max(PlatformDependent.getInt(maxAddr), newVal, bitVal));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class FloatMaxAccumulator extends BaseSingleAccumulator {
    private static final long INIT = 0x0000000100000001l;
    private static final int WIDTH = 4;

    public FloatMaxAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxMemAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomaxgBit = buffers.get(0).memoryAddress();
      final long incomaxgValue = buffers.get(1).memoryAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;

      int incomaxgIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxMemAddr; ordinalAddr += 4, incomaxgIndex++){
        final float newVal = Float.intBitsToFloat(PlatformDependent.getInt(incomaxgValue + (incomaxgIndex * WIDTH)));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTable.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK;
        final long maxAddr = valueAddresses[chunkIndex] + (chunkOffset) * 4;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomaxgBit + ((incomaxgIndex >>> 3))) >>> (incomaxgIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putInt(maxAddr, Float.floatToIntBits(max(Float.intBitsToFloat(PlatformDependent.getInt(maxAddr)), newVal, bitVal)));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class BigIntMaxAccumulator extends BaseSingleAccumulator {

    private static final int WIDTH = 8;

    public BigIntMaxAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, Long.MIN_VALUE);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxMemAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomaxgBit = buffers.get(0).memoryAddress();
      final long incomaxgValue = buffers.get(1).memoryAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;

      int incomaxgIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxMemAddr; ordinalAddr += 4, incomaxgIndex++){

        final long newVal = PlatformDependent.getLong(incomaxgValue + (incomaxgIndex * WIDTH));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTable.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK;
        final long maxAddr = valueAddresses[chunkIndex] + (chunkOffset) * 8;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomaxgBit + ((incomaxgIndex >>> 3))) >>> (incomaxgIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putLong(maxAddr, max(PlatformDependent.getLong(maxAddr), newVal, bitVal));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class DoubleMaxAccumulator extends BaseSingleAccumulator {

    private static final long INIT = Double.doubleToLongBits(Double.MIN_VALUE);
    private static final int WIDTH = 8;

    public DoubleMaxAccumulator(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxMemAddr = memoryAddr + count * 4;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomaxgBit = buffers.get(0).memoryAddress();
      final long incomaxgValue = buffers.get(1).memoryAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;

      int incomaxgIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxMemAddr; ordinalAddr += 4, incomaxgIndex++){
        final double newVal = Double.longBitsToDouble(PlatformDependent.getLong(incomaxgValue + (incomaxgIndex * WIDTH)));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTable.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK;
        final long maxAddr = valueAddresses[chunkIndex] + (chunkOffset) * 8;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomaxgBit + ((incomaxgIndex >>> 3))) >>> (incomaxgIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putLong(maxAddr, Double.doubleToLongBits(max(Double.longBitsToDouble(PlatformDependent.getLong(maxAddr)), newVal, bitVal)));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  private static final long max(long a, long b, int bitVal){
    // update the incomaxg value to either be the max (if the incomaxg is null) or keep as is (if the value is not null)
    b = b * bitVal + Long.MIN_VALUE * (bitVal ^ 1);
    return Math.max(a,b);
  }

  private static final int max(int a, int b, int bitVal){
    b = b * bitVal + Integer.MIN_VALUE * (bitVal ^ 1);
    return Math.max(a,b);
  }

  private static final double max(double a, double b, int bitVal){
    if(bitVal == 1){
      return Math.max(a, b);
    }
    return a;
  }

  private static final float max(float a, float b, int bitVal){
    if(bitVal == 1){
      return Math.max(a, b);
    }
    return a;
  }
}

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
package com.dremio.sabot.op.aggregate.vectorized.nospill;

import java.math.BigDecimal;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.MutableVarcharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.util.ByteFunctionHelpers;

import com.dremio.exec.util.DecimalUtils;
import com.dremio.sabot.op.common.ht2.LBlockHashTableNoSpill;

import io.netty.util.internal.PlatformDependent;

public class MaxAccumulatorsNoSpill {

  private MaxAccumulatorsNoSpill(){};

  public static class IntMaxAccumulatorNoSpill extends BaseSingleAccumulatorNoSpill {
    private static final long INIT = 0x8000000080000000l;
    private static final int WIDTH = 4;

    public IntMaxAccumulatorNoSpill(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxMemAddr = memoryAddr + count * 4;
      final long incomaxgBit = getInput().getValidityBufferAddress();
      final long incomaxgValue =  getInput().getDataBufferAddress();

      int incomaxgIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxMemAddr; ordinalAddr += 4, incomaxgIndex++){
        final int newVal = PlatformDependent.getInt(incomaxgValue + (incomaxgIndex * WIDTH));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;
        final long maxAddr = getValueAddress(chunkIndex) + (chunkOffset * 4);
        final long bitUpdateAddr = getBitAddress(chunkIndex) + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomaxgBit + ((incomaxgIndex >>> 3))) >>> (incomaxgIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putInt(maxAddr, max(PlatformDependent.getInt(maxAddr), newVal, bitVal));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class VarLenMaxAccumulatorNoSpill extends BaseVarBinaryAccumulatorNoSpill {
    NullableVarCharHolder holder = new NullableVarCharHolder();

    public VarLenMaxAccumulatorNoSpill(FieldVector input, FieldVector output) {
      super(input, output);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      final long incomingBit = getInput().getValidityBufferAddress();

      int incomingIndex = 0;

      final ArrowBuf inputOffsetBuf = getInput().getOffsetBuffer();
      final ArrowBuf inputBuf = getInput().getDataBuffer();

      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;

        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        //get the offset of incoming record
        final int startOffset = inputOffsetBuf.getInt(incomingIndex * BaseVariableWidthVector.OFFSET_WIDTH);
        final int endOffset = inputOffsetBuf.getInt((incomingIndex + 1) * BaseVariableWidthVector.OFFSET_WIDTH);

        //get the proper chunk from the ordinal
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        final int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        final int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;

        MutableVarcharVector mv = (MutableVarcharVector) this.accumulators[chunkIndex];
        holder.isSet = 0;
        boolean swap = false;

        if(mv.isIndexSafe(chunkOffset)) {
          mv.get(chunkOffset, holder);
        }

        if (holder.isSet == 0) {
          //current max value is null, just replace it with incoming record
          swap = true;
        } else {
          //compare incoming with currently running max record
          final int cmp = ByteFunctionHelpers.compare(inputBuf, startOffset, endOffset, holder.buffer, holder.start, holder.end);
          swap = (cmp == 1);
        }

        if (swap) {
          mv.setSafe(chunkOffset, startOffset, (endOffset - startOffset), inputBuf);
        }
      } //for
    } //accumulate
  }

  public static class FloatMaxAccumulatorNoSpill extends BaseSingleAccumulatorNoSpill {
    private static final int INIT = Float.floatToRawIntBits(-Float.MAX_VALUE);
    private static final int WIDTH = 4;

    public FloatMaxAccumulatorNoSpill(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxMemAddr = memoryAddr + count * 4;
      final long incomaxgBit = getInput().getValidityBufferAddress();
      final long incomaxgValue =  getInput().getDataBufferAddress();

      int incomaxgIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxMemAddr; ordinalAddr += 4, incomaxgIndex++){
        final float newVal = Float.intBitsToFloat(PlatformDependent.getInt(incomaxgValue + (incomaxgIndex * WIDTH)));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;
        final long maxAddr = getValueAddress(chunkIndex) + (chunkOffset * 4);
        final long bitUpdateAddr = getBitAddress(chunkIndex) + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomaxgBit + ((incomaxgIndex >>> 3))) >>> (incomaxgIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putInt(maxAddr, Float.floatToRawIntBits(max(Float.intBitsToFloat(PlatformDependent.getInt(maxAddr)), newVal, bitVal)));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class BigIntMaxAccumulatorNoSpill extends BaseSingleAccumulatorNoSpill {

    private static final int WIDTH = 8;

    public BigIntMaxAccumulatorNoSpill(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, Long.MIN_VALUE);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxMemAddr = memoryAddr + count * 4;
      final long incomaxgBit = getInput().getValidityBufferAddress();
      final long incomaxgValue =  getInput().getDataBufferAddress();

      int incomaxgIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxMemAddr; ordinalAddr += 4, incomaxgIndex++){

        final long newVal = PlatformDependent.getLong(incomaxgValue + (incomaxgIndex * WIDTH));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;
        final long maxAddr = getValueAddress(chunkIndex) + (chunkOffset * 8);
        final long bitUpdateAddr = getBitAddress(chunkIndex) + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomaxgBit + ((incomaxgIndex >>> 3))) >>> (incomaxgIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putLong(maxAddr, max(PlatformDependent.getLong(maxAddr), newVal, bitVal));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class DoubleMaxAccumulatorNoSpill extends BaseSingleAccumulatorNoSpill {

    private static final long INIT = Double.doubleToRawLongBits(-Double.MAX_VALUE);
    private static final int WIDTH = 8;

    public DoubleMaxAccumulatorNoSpill(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxMemAddr = memoryAddr + count * 4;
      final long incomaxgBit = getInput().getValidityBufferAddress();
      final long incomaxgValue =  getInput().getDataBufferAddress();

      int incomaxgIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxMemAddr; ordinalAddr += 4, incomaxgIndex++){
        final double newVal = Double.longBitsToDouble(PlatformDependent.getLong(incomaxgValue + (incomaxgIndex * WIDTH)));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;
        final long maxAddr = getValueAddress(chunkIndex) + (chunkOffset * 8);
        final long bitUpdateAddr = getBitAddress(chunkIndex) + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomaxgBit + ((incomaxgIndex >>> 3))) >>> (incomaxgIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putLong(maxAddr, Double.doubleToRawLongBits(max(Double.longBitsToDouble(PlatformDependent.getLong(maxAddr)), newVal, bitVal)));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class DecimalMaxAccumulatorNoSpill extends BaseSingleAccumulatorNoSpill {
    private static final long INIT = Double.doubleToLongBits(-Double.MAX_VALUE);
    private static final int WIDTH_ORDINAL = 4;     // int ordinal #s
    private static final int WIDTH_INPUT = 16;      // decimal inputs
    private static final int WIDTH_ACCUMULATOR = 8; // double accumulators
    byte[] valBuf = new byte[WIDTH_INPUT];

    public DecimalMaxAccumulatorNoSpill(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxMemAddr = memoryAddr + count * WIDTH_ORDINAL;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final int scale = ((DecimalVector)inputVector).getScale();

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxMemAddr; ordinalAddr += WIDTH_ORDINAL, incomingIndex++){
        java.math.BigDecimal newVal = DecimalAccumulatorUtilsNoSpill.getBigDecimal(incomingValue + (incomingIndex * WIDTH_INPUT), valBuf, scale);
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;
        final long maxAddr = getValueAddress(chunkIndex) + (chunkOffset * WIDTH_ACCUMULATOR);
        final long bitUpdateAddr = getBitAddress(chunkIndex) + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putLong(maxAddr, Double.doubleToLongBits(max(Double.longBitsToDouble(PlatformDependent.getLong(maxAddr)), newVal.doubleValue(), bitVal)));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class DecimalMaxAccumulatorNoSpillV2 extends BaseSingleAccumulatorNoSpill {
    private static final BigDecimal INIT = DecimalUtils.MIN_DECIMAL;
    private static final int WIDTH_ORDINAL = 4;     // int ordinal #s
    private static final int WIDTH_INPUT = 16;      // decimal inputs
    private static final int WIDTH_ACCUMULATOR = 16; // decimal accumulators

    public DecimalMaxAccumulatorNoSpillV2(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxMemAddr = memoryAddr + count * WIDTH_ORDINAL;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxMemAddr; ordinalAddr += WIDTH_ORDINAL, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        if (bitVal == 0) {
          continue;
        }
        /* get the corresponding data from input vector -- source data for accumulation */
        long addressOfInput = incomingValue + (incomingIndex * WIDTH_INPUT);
        long newValLow = PlatformDependent.getLong(addressOfInput);
        long newValHigh = PlatformDependent.getLong(addressOfInput + DecimalUtils.LENGTH_OF_LONG);
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;
        final long maxAddr = getValueAddress(chunkIndex) + (chunkOffset * WIDTH_ACCUMULATOR);
        final long bitUpdateAddr = getBitAddress(chunkIndex) + ((chunkOffset >>> 5) * 4);

        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        /* store the accumulated values(new max or existing) at the target location of accumulation vector */
        long curValLow = PlatformDependent.getLong(maxAddr);
        long curValHigh = PlatformDependent.getLong(maxAddr + DecimalUtils.LENGTH_OF_LONG);
        int compare = DecimalUtils.compareDecimalsAsTwoLongs(newValHigh, newValLow, curValHigh, curValLow);

        if (compare > 0) {
          /* store the accumulated values(new max or existing) at the target location of
          accumulation vector */
          PlatformDependent.putLong(maxAddr, newValLow);
          PlatformDependent.putLong(maxAddr + 8, newValHigh);
          PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
        }
      }
    }
  }

  public static class BitMaxAccumulatorNoSpill extends BaseSingleAccumulatorNoSpill {
    private static final int WIDTH_LONG = 8;        // operations done on long boundaries
    private static final int BITS_PER_LONG_SHIFT = 6;  // (1<<6) bits per long
    private static final int BITS_PER_LONG = (1<<BITS_PER_LONG_SHIFT);
    private static final int WIDTH_ORDINAL = 4;     // int ordinal #s

    public BitMaxAccumulatorNoSpill(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndZero(vector);
    }

    public void accumulate(final long memoryAddr, final int count) {
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomingBit = buffers.get(0).memoryAddress();
      final long incomingValue = buffers.get(1).memoryAddress();

      final long numWords = (count + (BITS_PER_LONG-1)) >>> BITS_PER_LONG_SHIFT; // rounded up number of words that cover 'count' bits
      final long maxInputAddr = incomingValue + numWords * WIDTH_LONG;
      final long maxOrdinalAddr = memoryAddr + count * WIDTH_ORDINAL;

      // Like every accumulator, the code below essentially implements:
      //   accumulators[ordinals[i]] += inputs[i]
      // with the only complication that both accumulators and inputs are bits.
      // There's nothing we can do about the locality of the accumulators, but inputs can be processed a word at a time.
      // Algorithm:
      // - get 64 bits worth of inputs, until all inputs exhausted. For each long:
      //   - find the accumulator word it pertains to
      //   - read/update/write the accumulator bit
      // In the code below:
      // - input* refers to the data values in the incoming batch
      // - ordinal* refers to the temporary table that hashAgg passes in, identifying which hash table entry each input matched to
      // - min* refers to the accumulator
      for (long inputAddr = incomingValue, inputBitAddr = incomingBit, batchCount = 0;
           inputAddr < maxInputAddr;
           inputAddr += WIDTH_LONG, inputBitAddr += WIDTH_LONG, batchCount++) {
        final long inputBatch = PlatformDependent.getLong(inputAddr);
        final long inputBits = PlatformDependent.getLong(inputBitAddr);
        long ordinalAddr = memoryAddr + (batchCount << BITS_PER_LONG_SHIFT);
        for (long bitNum = 0; bitNum < BITS_PER_LONG && ordinalAddr < maxOrdinalAddr; bitNum++, ordinalAddr += WIDTH_ORDINAL) {
          final int tableIndex = PlatformDependent.getInt(ordinalAddr);
          int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
          int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;
          final long minBitUpdateAddr = getBitAddress(chunkIndex) + ((chunkOffset >>> 5) * 4);
          // Update rules:
          // max of two boolean values boils down to doing a bitwise OR on the two
          // If the input bit is set, we update both the accumulator value and its bit
          //    -- the accumulator is OR-ed with the value of the input bit
          //    -- the accumulator bit is OR-ed with 1 (since the input is valid)
          // If the input bit is not set, we update neither the accumulator nor its bit
          //    -- the accumulator is OR-ed with a 0 (thus remaining unchanged)
          //    -- the accumulator bit is OR-ed with 0 (thus remaining unchanged)
          // Thus, the logical function for updating the accumulator is: oldVal OR (inputBit AND inputValue)
          // Thus, the logical function for updating the accumulator is: oldBitVal OR inputBit
          // Because the operations are all done in a word length (and not on an individual bit), the AND value for
          // updating the accumulator must have all its other bits set to 1
          final int inputBitVal = (int)((inputBits >>> bitNum) & 0x01);
          final int inputVal = (int)((inputBatch >>> bitNum) & 0x01);
          final int minBitUpdateVal = inputBitVal << (chunkOffset & 31);
          int minUpdateVal = (inputBitVal & inputVal) << (chunkOffset & 31);
          final long minAddr = getValueAddress(chunkIndex) + ((chunkOffset >>> 5) * 4);
          PlatformDependent.putInt(minAddr, PlatformDependent.getInt(minAddr) | minUpdateVal);
          PlatformDependent.putInt(minBitUpdateAddr, PlatformDependent.getInt(minBitUpdateAddr) | minBitUpdateVal);
        }
      }
    }
  }

  public static class IntervalDayMaxAccumulatorNoSpill extends BaseSingleAccumulatorNoSpill {
    private static final long INIT = 0x8000000080000000l;
    private static final int WIDTH_ORDINAL = 4;     // int ordinal #s
    private static final int WIDTH_INPUT = 8;       // pair-of-ints inputs
    private static final int WIDTH_ACCUMULATOR = 8; // pair-of-ints pair accumulators

    public IntervalDayMaxAccumulatorNoSpill(FieldVector input, FieldVector output) {
      super(input, output);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * WIDTH_ORDINAL;
      List<ArrowBuf> buffers = getInput().getFieldBuffers();
      final long incomingBit = buffers.get(0).memoryAddress();
      final long incomingValue = buffers.get(1).memoryAddress();

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += WIDTH_ORDINAL, incomingIndex++){
        final long newVal = PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH_INPUT));
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;
        final long minAddr = getValueAddress(chunkIndex) + (chunkOffset * WIDTH_ACCUMULATOR);
        final long bitUpdateAddr = getBitAddress(chunkIndex) + ((chunkOffset >>> 5) * 4);
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        // first 4 bytes are the number of days (in little endian, that's the bottom 32 bits)
        // second 4 bytes are the number of milliseconds (in little endian, that's the top 32 bits)
        final int newDays = (int) newVal;
        final int newMillis = (int)(newVal >> 32);
        // To compare the pairs of day/milli, we swap them, with days getting the most significant bits
        // The incoming value is updated to either be MAX (if incoming is null), or keep as is (if the value is not null)
        final long newSwappedVal = ((((long)newDays) << 32) | newMillis) * bitVal + Long.MAX_VALUE * (bitVal ^ 1);
        final long maxVal = PlatformDependent.getLong(minAddr);
        final int maxDays = (int) maxVal;
        final int maxMillis = (int)(maxVal >> 32);
        final long maxSwappedVal = (((long)maxDays) << 32) | maxMillis;
        PlatformDependent.putLong(minAddr, (maxSwappedVal > newSwappedVal) ? maxVal : newVal);
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  private static final long max(long a, long b, int bitVal){
    // update the incoming value to either be the max (if the incoming is null) or keep as is (if the value is not null)
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

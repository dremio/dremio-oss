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
package com.dremio.sabot.op.aggregate.vectorized;

import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.HTORDINAL_OFFSET;
import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.KEYINDEX_OFFSET;
import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH;

import java.math.BigDecimal;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.MutableVarcharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.exec.util.DecimalUtils;

import io.netty.util.internal.PlatformDependent;

public class MaxAccumulators {

  private MaxAccumulators(){};

  public static class IntMaxAccumulator extends BaseSingleAccumulator {
    private static final long INIT = 0x8000000080000000l;
    private static final int WIDTH_INPUT = 4;         // int inputs
    private static final int WIDTH_ACCUMULATOR = 4;

    public IntMaxAccumulator(FieldVector input, FieldVector output,
                             FieldVector transferVector, int maxValuesPerBatch,
                             BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.MAX, maxValuesPerBatch,
            computationVectorAllocator);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxMemAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxMemAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int newVal = PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH_INPUT));
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        /* get the target addresses of accumulation vector */
        final long maxAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        /* store the accumulated values(new max or existing) at the target location of accumulation vector */
        PlatformDependent.putInt(maxAddr, max(PlatformDependent.getInt(maxAddr), newVal, bitVal));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class FloatMaxAccumulator extends BaseSingleAccumulator {
    private static final int INIT = Float.floatToRawIntBits(-Float.MAX_VALUE);
    private static final int WIDTH_INPUT = 4;       // float inputs
    private static final int WIDTH_ACCUMULATOR = 4; // float accumulators

    public FloatMaxAccumulator(FieldVector input, FieldVector output,
                               FieldVector transferVector, int maxValuesPerBatch,
                               BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.MAX, maxValuesPerBatch,
            computationVectorAllocator);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxMemAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxMemAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final float newVal = Float.intBitsToFloat(PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH_INPUT)));
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        /* get the target addresses of accumulation vector */
        final long maxAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        /* store the accumulated values(new max or existing) at the target location of accumulation vector */
        PlatformDependent.putInt(maxAddr, Float.floatToRawIntBits(max(Float.intBitsToFloat(PlatformDependent.getInt(maxAddr)), newVal, bitVal)));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class BigIntMaxAccumulator extends BaseSingleAccumulator {
    private static final int WIDTH_INPUT = 8;         // long inputs
    private static final int WIDTH_ACCUMULATOR = 8;

    public BigIntMaxAccumulator(FieldVector input, FieldVector output,
                                FieldVector transferVector, int maxValuesPerBatch,
                                BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.MAX, maxValuesPerBatch,
            computationVectorAllocator);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, Long.MIN_VALUE);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxMemAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxMemAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final long newVal = PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH_INPUT));
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        /* get the target addresses of accumulation vector */
        final long maxAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        /* store the accumulated values(new max or existing) at the target location of accumulation vector */
        PlatformDependent.putLong(maxAddr, max(PlatformDependent.getLong(maxAddr), newVal, bitVal));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class DoubleMaxAccumulator extends BaseSingleAccumulator {

    private static final long INIT = Double.doubleToRawLongBits(-Double.MAX_VALUE);
    private static final int WIDTH_INPUT = 8;       // double inputs
    private static final int WIDTH_ACCUMULATOR = 8; // double accumulators

    /**
     * Used during operator setup for creating accumulator vectors
     * for the first time.
     * @param input input vector with values to be accumulated
     * @param output vector in outgoing container used for making a transfer pair
     * @param transferVector output vector in outgoing container used for transferring the contents from
     *                       accumulator vector
     * @param maxValuesPerBatch maximum values in a single hashtable batch
     * @param computationVectorAllocator allocator used for allocating
     *                                   accumulators that store computed values
     */
    public DoubleMaxAccumulator(FieldVector input, FieldVector output,
                                FieldVector transferVector, int maxValuesPerBatch,
                                BufferAllocator computationVectorAllocator) {
      this(input, output, transferVector, maxValuesPerBatch, computationVectorAllocator,
           null, null, null);
    }

    /*
     * private constructor that does the initialization.
     */
    private DoubleMaxAccumulator(final FieldVector input, final FieldVector output,
                                  final FieldVector transferVector, final int maxValuesPerBatch,
                                  final BufferAllocator computationVectorAllocator,
                                  final long[] bitAddresses, final long[] valueAddresses,
                                  final FieldVector[] accumulators) {
      super(input, output, transferVector,
            AccumulatorBuilder.AccumulatorType.MAX,
            maxValuesPerBatch, computationVectorAllocator,
            bitAddresses,
            valueAddresses,
            accumulators);
    }

    /**
     * Used during post-spill processing to convert a float
     * sum accumulator to a post-spill double sum accumulator
     *
     * @param decimalMaxAccumulator pre-spill float sum accumulator
     * @param input input vector with values to be accumulated
     * @param maxValuesPerBatch max values in a hash table batch
     * @param computationVectorAllocator allocator used for allocating
     *                                   accumulators that store computed values
     */
    DoubleMaxAccumulator(final MaxAccumulators.DecimalMaxAccumulator decimalMaxAccumulator,
                         final FieldVector input, final int maxValuesPerBatch,
                         final BufferAllocator computationVectorAllocator) {
      this(input, decimalMaxAccumulator.getOutput(),
           decimalMaxAccumulator.getTransferVector(),
           maxValuesPerBatch, computationVectorAllocator,
           decimalMaxAccumulator.getBitAddresses(),
           decimalMaxAccumulator.getValueAddresses(),
           decimalMaxAccumulator.getAccumulators()
      );
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxMemAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxMemAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final double newVal = Double.longBitsToDouble(PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH_INPUT)));
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        /* get the target addresses of accumulation vector */
        final long maxAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        PlatformDependent.putLong(maxAddr, Double.doubleToRawLongBits(max(Double.longBitsToDouble(PlatformDependent.getLong(maxAddr)), newVal, bitVal)));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class DecimalMaxAccumulator extends BaseSingleAccumulator {
    private static final long INIT = Double.doubleToLongBits(-Double.MAX_VALUE);
    private static final int WIDTH_INPUT = 16;      // decimal inputs
    private static final int WIDTH_ACCUMULATOR = 8; // double accumulators
    private byte[] valBuf = new byte[WIDTH_INPUT];

    public DecimalMaxAccumulator(FieldVector input, FieldVector output,
                                 FieldVector transferVector, int maxValuesPerBatch,
                                 BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.MAX, maxValuesPerBatch,
            computationVectorAllocator);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxMemAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;
      final int scale = ((DecimalVector)inputVector).getScale();
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxMemAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        java.math.BigDecimal newVal = DecimalUtils.getBigDecimalFromLEBytes(incomingValue + (incomingIndex * WIDTH_INPUT), valBuf, scale);
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        /* get the target addresses of accumulation vector */
        final long maxAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        /* store the accumulated values(new max or existing) at the target location of accumulation vector */
        PlatformDependent.putLong(maxAddr, Double.doubleToLongBits(max(Double.longBitsToDouble(PlatformDependent.getLong(maxAddr)), newVal.doubleValue(), bitVal)));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class DecimalMaxAccumulatorV2 extends BaseSingleAccumulator {
    private static final BigDecimal INIT = DecimalUtils.MIN_DECIMAL;
    private static final int WIDTH_INPUT = 16;      // decimal inputs
    private static final int WIDTH_ACCUMULATOR = 16; // decimal accumulators

    public DecimalMaxAccumulatorV2(FieldVector input, FieldVector output,
                                   FieldVector transferVector, int maxValuesPerBatch,
                                   BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.MAX, maxValuesPerBatch,
        computationVectorAllocator);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxMemAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxMemAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        // no point continuing.
        if (bitVal == 0) {
          continue;
        }
        /* get the corresponding data from input vector -- source data for accumulation */
        long addressOfInput = incomingValue + (incomingIndex * WIDTH_INPUT);
        long newValLow = PlatformDependent.getLong(addressOfInput);
        long newValHigh = PlatformDependent.getLong(addressOfInput + DecimalUtils.LENGTH_OF_LONG);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        /* get the target addresses of accumulation vector */
        final long maxAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        /* store the accumulated values(new max or existing) at the target location of accumulation vector */
        long curValLow = PlatformDependent.getLong(maxAddr);
        long curValHigh = PlatformDependent.getLong(maxAddr + DecimalUtils.LENGTH_OF_LONG);
        int compare = DecimalUtils.compareDecimalsAsTwoLongs(newValHigh, newValLow, curValHigh,
          curValLow);

        if (compare > 0) {
          /* store the accumulated values(new max or existing) at the target location of
          accumulation vector */
          PlatformDependent.putLong(maxAddr, newValLow);
          PlatformDependent.putLong(maxAddr + DecimalUtils.LENGTH_OF_LONG, newValHigh);
          PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
        }
      }
    }
  }

  public static class BitMaxAccumulator extends BaseSingleAccumulator {
    private static final int BITS_PER_BYTE_SHIFT = 3;  // (1<<3) bits per byte
    private static final int BITS_PER_BYTE = (1 << BITS_PER_BYTE_SHIFT);

    public BitMaxAccumulator(FieldVector input, FieldVector output,
                             FieldVector transferVector, int maxValuesPerBatch,
                             BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.MAX, maxValuesPerBatch,
            computationVectorAllocator);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndZero(vector);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;

      final long maxOrdinalAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      // Like every accumulator, the code below essentially implements:
      //   accumulators[ordinals[i]] += inputs[i]
      // with the only complication that both accumulators and inputs are bits.
      // There's nothing we can do about the locality of the accumulators, but inputs can be processed a word at a time.
      // Algorithm:
      // - get 64 bits worth of inputs, until all inputs exhausted. For each long:
      //   - find the accumulator word it pertains to
      //   - read/update/write the accumulator bit
      // Unfortunately, there is no locality: the incoming partition+ordinal array has been ordered by partition, which
      // removes the ability to process input bits a word at a time
      // In the code below:
      // - input* refers to the data values in the incoming batch
      // - ordinal* refers to the temporary table that hashAgg passes in, identifying which hash table entry each input matched to
      // - min* refers to the accumulator
      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxOrdinalAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        final long maxBitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);  // 32-bit read-update-write
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
        final int inputBitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> BITS_PER_BYTE_SHIFT))) >>> (incomingIndex & (BITS_PER_BYTE - 1))) & 1;
        final int inputVal = (PlatformDependent.getByte(incomingValue + ((incomingIndex >>> BITS_PER_BYTE_SHIFT))) >>> (incomingIndex & (BITS_PER_BYTE - 1))) & 1;
        final int maxBitUpdateVal = inputBitVal << (chunkOffset & 31);
        int minUpdateVal = (inputBitVal & inputVal) << (chunkOffset & 31);
        final long maxAddr = valueAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        PlatformDependent.putInt(maxAddr, PlatformDependent.getInt(maxAddr) | minUpdateVal);
        PlatformDependent.putInt(maxBitUpdateAddr, PlatformDependent.getInt(maxBitUpdateAddr) | maxBitUpdateVal);
      }
    }
  }

  public static class IntervalDayMaxAccumulator extends BaseSingleAccumulator {
    private static final long INIT = 0x8000000080000000l;
    private static final int WIDTH_INPUT = 8;       // pair-of-ints inputs
    private static final int WIDTH_ACCUMULATOR = 8; // pair-of-ints pair accumulators

    public IntervalDayMaxAccumulator(FieldVector input, FieldVector output,
                                     FieldVector transferVector, int maxValuesPerBatch,
                                     BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.MAX, maxValuesPerBatch,
            computationVectorAllocator);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndValue(vector, INIT);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxMemAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxMemAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final long newVal = PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH_INPUT));
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        /* get the target addresses of accumulation vector */
        final long maxAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        // first 4 bytes are the number of days (in little endian, that's the bottom 32 bits)
        // second 4 bytes are the number of milliseconds (in little endian, that's the top 32 bits)
        final int newDays = (int) newVal;
        final int newMillis = (int)(newVal >>> 32);
        // To compare the pairs of day/milli, we swap them, with days getting the most significant bits
        // The incoming value is updated to either be MAX (if incoming is null), or keep as is (if the value is not null)
        final long newSwappedVal = ((((long)newDays) << 32) | newMillis) * bitVal + Long.MAX_VALUE * (bitVal ^ 1);
        final long maxVal = PlatformDependent.getLong(maxAddr);
        final int maxDays = (int) maxVal;
        final int maxMillis = (int)(maxVal >>> 32);
        final long maxSwappedVal = (((long)maxDays) << 32) | maxMillis;
        /* store the accumulated values(new min or existing) at the target location of accumulation vector */
        PlatformDependent.putLong(maxAddr, (maxSwappedVal > newSwappedVal) ? maxVal : newVal);
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class VarLenMaxAccumulator extends BaseVarBinaryAccumulator {
    NullableVarCharHolder holder = new NullableVarCharHolder();

    public VarLenMaxAccumulator(FieldVector input, FieldVector output,
                                FieldVector transferVector, int maxValuesPerBatch,
                                BufferAllocator computationVectorAllocator,
                                int varLenAccumulatorCapacity, BaseVariableWidthVector tempAccumulatorHolder) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.MAX, maxValuesPerBatch,
        computationVectorAllocator, varLenAccumulatorCapacity, tempAccumulatorHolder);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final FieldVector inputVector = getInput();
      final long inputValidityBufAddr = inputVector.getValidityBufferAddress();
      final ArrowBuf inputOffsetBuf = inputVector.getOffsetBuffer();
      final ArrowBuf inputDataBuf = inputVector.getDataBuffer();

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        // get the index of data in input vector
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);

        final int bitVal = (PlatformDependent.getByte(inputValidityBufAddr + (incomingIndex >>> 3)) >>> (incomingIndex & 7)) & 1;
        // incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        // get the hash table ordinal
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);

        // get the offset of incoming record
        final int startOffset = inputOffsetBuf.getInt(incomingIndex * BaseVariableWidthVector.OFFSET_WIDTH);
        final int endOffset = inputOffsetBuf.getInt((incomingIndex + 1) * BaseVariableWidthVector.OFFSET_WIDTH);

        // get the hash table batch index
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;

        MutableVarcharVector mv = (MutableVarcharVector) this.accumulators[chunkIndex];
        holder.isSet = 0;

        if (mv.isIndexSafe(chunkOffset)) {
          mv.get(chunkOffset, holder);
        }

        if (holder.isSet == 0 || // current max value is null, just replace it with incoming record
            ByteFunctionHelpers.compare(inputDataBuf, startOffset, endOffset, holder.buffer, holder.start, holder.end) > 0) {
          mv.set(chunkOffset, startOffset, (endOffset - startOffset), inputDataBuf);
        }
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

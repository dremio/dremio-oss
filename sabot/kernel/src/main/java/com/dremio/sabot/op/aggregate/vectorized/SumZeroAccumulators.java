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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;

import com.dremio.exec.util.DecimalUtils;

import io.netty.util.internal.PlatformDependent;

public class SumZeroAccumulators {

  private static final int WIDTH_ACCUMULATOR = 8;
  private SumZeroAccumulators(){}

  public static class IntSumZeroAccumulator extends BaseSingleAccumulator {
    private static final int WIDTH_INPUT = 4;       // int inputs

    public IntSumZeroAccumulator(FieldVector input, FieldVector output,
                                 FieldVector transferVector, int maxValuesPerBatch,
                                 BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.SUM0, maxValuesPerBatch,
            computationVectorAllocator);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();
      final long[] valueAddresses = this.valueAddresses;
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final int newVal = PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH_INPUT)) * bitVal;
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        /* get the target address of accumulation vector */
        final long sumAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        /* store the accumulated values at the target location of accumulation vector */
        PlatformDependent.putLong(sumAddr, PlatformDependent.getLong(sumAddr) + newVal);
      }
    }
  }

  public static class FloatSumZeroAccumulator extends BaseSingleAccumulator {
    private static final int WIDTH_INPUT = 4;       // float inputs

    public FloatSumZeroAccumulator(FieldVector input, FieldVector output,
                                   FieldVector transferVector, int maxValuesPerBatch,
                                   BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.SUM0, maxValuesPerBatch,
            computationVectorAllocator);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();
      final long[] valueAddresses = this.valueAddresses;
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final float newVal = Float.intBitsToFloat(PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH_INPUT)) * bitVal);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        /* get the target address of accumulation vector */
        final long sumAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        /* store the accumulated values at the target location of accumulation vector */
        PlatformDependent.putLong(sumAddr, Double.doubleToLongBits(Double.longBitsToDouble(PlatformDependent.getLong(sumAddr)) + newVal));
      }
    }
  }

  public static class BigIntSumZeroAccumulator extends BaseSingleAccumulator {
    private static final int WIDTH_INPUT = 8;       // long inputs

    public BigIntSumZeroAccumulator(FieldVector input, FieldVector output,
                                    FieldVector transferVector, int maxValuesPerBatch,
                                    BufferAllocator computationVectorAllocator) {
      this(input, output, transferVector, maxValuesPerBatch,
           computationVectorAllocator, null, null, null);
    }

    private BigIntSumZeroAccumulator(final FieldVector input, final FieldVector output,
                                     final FieldVector transferVector, final int maxValuesPerBatch,
                                     final BufferAllocator computationVectorAllocator,
                                     final long[] bitAddresses, final long[] valueAddresses,
                                     final FieldVector[] accumulators) {
      super(input, output, transferVector,
            AccumulatorBuilder.AccumulatorType.SUM0,
            maxValuesPerBatch, computationVectorAllocator,
            bitAddresses,
            valueAddresses,
            accumulators);
    }

    BigIntSumZeroAccumulator(final SumZeroAccumulators.IntSumZeroAccumulator intSumZeroAccumulator,
                             final FieldVector input, final int maxValuesPerBatch,
                             final BufferAllocator computationVectorAllocator) {
      this(input, intSumZeroAccumulator.getOutput(),
           intSumZeroAccumulator.getTransferVector(),
           maxValuesPerBatch, computationVectorAllocator,
           intSumZeroAccumulator.getBitAddresses(),
           intSumZeroAccumulator.getValueAddresses(),
           intSumZeroAccumulator.getAccumulators());
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();
      final long[] valueAddresses = this.valueAddresses;
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final long newVal = PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH_INPUT)) * bitVal;
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
         /* get the target address of accumulation vector */
        final long sumAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
         /* store the accumulated values at the target location of accumulation vector */
        PlatformDependent.putLong(sumAddr, PlatformDependent.getLong(sumAddr) + newVal);
      }
    }
  }

  public static class DoubleSumZeroAccumulator extends BaseSingleAccumulator {
    private static final int WIDTH_INPUT = 8;       // double inputs

    /**
     * Used during operator setup for creating accumulator vectors
     * for the first time.
     * @param input input vector with values to be accumulated
     * @param output vector in outgoing container used for making a transfer pair
     * @param transferVector output vector in outgoing container used for transferring the contents from accumulator vector
     * @param maxValuesPerBatch maximum values in a single hashtable batch
     * @param computationVectorAllocator allocator used for allocating
     *                                   accumulators that store computed values
     */
    public DoubleSumZeroAccumulator(FieldVector input, FieldVector output,
                                    FieldVector transferVector, int maxValuesPerBatch,
                                    BufferAllocator computationVectorAllocator) {
      this(input, output, transferVector, maxValuesPerBatch,
            computationVectorAllocator, null, null, null);
    }

    /*
     * private constructor that does the initialization.
     */
    private DoubleSumZeroAccumulator(final FieldVector input, final FieldVector output,
                                      final FieldVector transferVector, final int maxValuesPerBatch,
                                      final BufferAllocator computationVectorAllocator,
                                      final long[] bitAddresses, final long[] valueAddresses,
                                      final FieldVector[] accumulators) {
      super(input, output, transferVector,
            AccumulatorBuilder.AccumulatorType.SUM0,
            maxValuesPerBatch, computationVectorAllocator,
            bitAddresses,
            valueAddresses,
            accumulators);
    }

    /**
     * Used during post-spill processing to convert a float
     * sumzero accumulator to a post-spill double sumzero accumulator
     *
     * @param floatSumZeroAccumulator pre-spill float sumzero accumulator
     * @param input input vector with values to be accumulated
     * @param maxValuesPerBatch max values in a hash table batch
     * @param computationVectorAllocator allocator used for allocating
     *                                   accumulators that store computed values
     */
    DoubleSumZeroAccumulator(final SumZeroAccumulators.FloatSumZeroAccumulator floatSumZeroAccumulator,
                             final FieldVector input, final int maxValuesPerBatch,
                             final BufferAllocator computationVectorAllocator) {
      this(input, floatSumZeroAccumulator.getOutput(),
           floatSumZeroAccumulator.getTransferVector(),
           maxValuesPerBatch, computationVectorAllocator,
           floatSumZeroAccumulator.getBitAddresses(),
           floatSumZeroAccumulator.getValueAddresses(),
           floatSumZeroAccumulator.getAccumulators()
      );
    }

    /**
     * Used during post-spill processing to convert a decimal
     * sumzero accumulator to a post-spill double sumzero accumulator
     *
     * @param decimalSumZeroAccumulator pre-spill decimal sum accumulator
     * @param input input vector with values to be accumulated
     * @param maxValuesPerBatch max values in a hash table batch
     * @param computationVectorAllocator allocator used for allocating
     *                                   accumulators that store computed values
     */
    DoubleSumZeroAccumulator(final SumZeroAccumulators.DecimalSumZeroAccumulator decimalSumZeroAccumulator,
                             final FieldVector input, final int maxValuesPerBatch,
                             final BufferAllocator computationVectorAllocator) {
      this(input, decimalSumZeroAccumulator.getOutput(),
           decimalSumZeroAccumulator.getTransferVector(),
           maxValuesPerBatch, computationVectorAllocator,
           decimalSumZeroAccumulator.getBitAddresses(),
           decimalSumZeroAccumulator.getValueAddresses(),
           decimalSumZeroAccumulator.getAccumulators()
      );
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();
      final long[] valueAddresses = this.valueAddresses;
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        final double newVal = Double.longBitsToDouble(PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH_INPUT)) * bitVal);
         /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
         /* get the target address of accumulation vector */
        final long sumAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
         /* store the accumulated values at the target location of accumulation vector */
        PlatformDependent.putLong(sumAddr, Double.doubleToLongBits(Double.longBitsToDouble(PlatformDependent.getLong(sumAddr)) + newVal));
      }
    }
  }

  public static class DecimalSumZeroAccumulator extends BaseSingleAccumulator {
    private static final int WIDTH_INPUT = 16;      // decimal inputs
    private static final int WIDTH_ACCUMULATOR = 8; // double accumulators
    private byte[] valBuf = new byte[WIDTH_INPUT];

    public DecimalSumZeroAccumulator(FieldVector input, FieldVector output,
                                     FieldVector transferVector, int maxValuesPerBatch,
                                     BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.SUM0, maxValuesPerBatch,
            computationVectorAllocator);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] valueAddresses = this.valueAddresses;
      final int scale = ((DecimalVector)inputVector).getScale();
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        java.math.BigDecimal newVal = DecimalUtils.getBigDecimalFromLEBytes(incomingValue + (incomingIndex * WIDTH_INPUT), valBuf, scale);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        /* get the target address of accumulation vector */
        final long sumAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        /* store the accumulated values at the target location of accumulation vector */
        PlatformDependent.putLong(sumAddr, Double.doubleToLongBits(Double.longBitsToDouble(PlatformDependent.getLong(sumAddr)) + newVal.doubleValue() * bitVal));
      }
    }
  }

  public static class DecimalSumZeroAccumulatorV2 extends BaseSingleAccumulator {
    private static final int WIDTH_INPUT = 16;      // decimal inputs
    private static final int WIDTH_ACCUMULATOR = 16; // decimal accumulators
    private byte[] valBuf = new byte[WIDTH_INPUT];

    public DecimalSumZeroAccumulatorV2(FieldVector input, FieldVector output,
                                       FieldVector transferVector, int maxValuesPerBatch,
                                       BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.SUM0, maxValuesPerBatch,
        computationVectorAllocator);
    }

    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] valueAddresses = this.valueAddresses;
      final int scale = ((DecimalVector)inputVector).getScale();
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        long addressOfInput = incomingValue + (incomingIndex * WIDTH_INPUT);
        long newValLow = PlatformDependent.getLong(addressOfInput);
        long newValHigh = PlatformDependent.getLong(addressOfInput + DecimalUtils.LENGTH_OF_LONG);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;
        /* get the target address of accumulation vector */
        final long sumAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        /* store the accumulated values at the target location of accumulation vector */
        long curValLow = PlatformDependent.getLong(sumAddr);
        long curValHigh = PlatformDependent.getLong(sumAddr + DecimalUtils.LENGTH_OF_LONG);
        DecimalUtils.addSignedDecimals(sumAddr, newValLow, newValHigh, curValLow, curValHigh);
      }
    }
  }
}

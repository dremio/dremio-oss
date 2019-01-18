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

import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.HTORDINAL_OFFSET;
import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.KEYINDEX_OFFSET;
import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;

import io.netty.util.internal.PlatformDependent;

public class SumAccumulators {

  private static final int WIDTH_ACCUMULATOR = 8;
  private SumAccumulators(){};

  public static class IntSumAccumulator extends BaseSingleAccumulator {

    private static final int WIDTH_INPUT = 4;     /* int input vector */

    public IntSumAccumulator(FieldVector input, FieldVector output,
                             FieldVector transferVector, int maxValuesPerBatch,
                             BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.SUM, maxValuesPerBatch,
            computationVectorAllocator);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndZero(vector);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
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
        final int chunkIndex = getChunkIndexForOrdinal(tableIndex, maxValuesPerBatch);
        final int chunkOffset = getOffsetInChunkForOrdinal(tableIndex, maxValuesPerBatch);
        /* get the target addresses of accumulation vector */
        final long sumAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        /* store the accumulated values at the target location of accumulation vector */
        PlatformDependent.putLong(sumAddr, PlatformDependent.getLong(sumAddr) + newVal);
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class FloatSumAccumulator extends BaseSingleAccumulator {

    private static final int WIDTH_INPUT = 4;    /* float input vector */

    public FloatSumAccumulator(FieldVector input, FieldVector output,
                               FieldVector transferVector, int maxValuesPerBatch,
                               BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.SUM, maxValuesPerBatch,
            computationVectorAllocator);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndZero(vector);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
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
        final int chunkIndex = getChunkIndexForOrdinal(tableIndex, maxValuesPerBatch);
        final int chunkOffset = getOffsetInChunkForOrdinal(tableIndex, maxValuesPerBatch);
        /* get the target addresses of accumulation vector */
        final long sumAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        /* store the accumulated values at the target location of accumulation vector */
        PlatformDependent.putLong(sumAddr, Double.doubleToLongBits(Double.longBitsToDouble(PlatformDependent.getLong(sumAddr)) + newVal));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }

  public static class BigIntSumAccumulator extends BaseSingleAccumulator {
    private static final int WIDTH_INPUT = 8;     /* long input vector */

    public BigIntSumAccumulator(final FieldVector input, final FieldVector output,
                                final FieldVector transferVector, final int maxValuesPerBatch,
                                final BufferAllocator computationVectorAllocator) {
      this(input, output, transferVector, maxValuesPerBatch,
           computationVectorAllocator, null, null, null);
    }

    private BigIntSumAccumulator(final FieldVector input, final FieldVector output,
                                 final FieldVector transferVector, final int maxValuesPerBatch,
                                 final BufferAllocator computationVectorAllocator,
                                 final long[] bitAddresses, final long[] valueAddresses,
                                 final FieldVector[] accumulators) {
      super(input, output, transferVector,
            AccumulatorBuilder.AccumulatorType.SUM,
            maxValuesPerBatch, computationVectorAllocator,
            bitAddresses,
            valueAddresses,
            accumulators);
    }

    /**
     * Create a BigIntSumAccumulator from an IntSumAccumulator. This is
     * used for post-spill processing.
     * @param intSumAccumulator int sum accumulator
     * @param input new input vector (read from spilled batch)
     * @param maxValuesPerBatch batch size
     * @param computationVectorAllocator accumulator vector allocator
     */
    BigIntSumAccumulator(final IntSumAccumulator intSumAccumulator,
                         final FieldVector input, final int maxValuesPerBatch,
                         final BufferAllocator computationVectorAllocator) {
      this(input, intSumAccumulator.getOutput(), intSumAccumulator.getTransferVector(),
           maxValuesPerBatch, computationVectorAllocator,
           intSumAccumulator.getBitAddresses(),
           intSumAccumulator.getValueAddresses(),
           intSumAccumulator.getAccumulators());
    }

    /**
     * Create a BigIntSumAccumulator from a Count accumulator. This is
     * used for post-spill processing.
     * @param countColumnAccumulator count accumulator
     * @param input new input vector (read from spilled batch)
     * @param maxValuesPerBatch batch size
     * @param computationVectorAllocator accumulator vector allocator
     */
    BigIntSumAccumulator(final CountColumnAccumulator countColumnAccumulator,
                         final FieldVector input, final int maxValuesPerBatch,
                         final BufferAllocator computationVectorAllocator) {
      this(input, countColumnAccumulator.getOutput(), countColumnAccumulator.getTransferVector(),
           maxValuesPerBatch, computationVectorAllocator,
           countColumnAccumulator.getBitAddresses(),
           countColumnAccumulator.getValueAddresses(),
           countColumnAccumulator.getAccumulators());
    }

    /**
     * Create a BigIntSumAccumulator from a CountOne accumulator. This is
     * used for post-spill processing.
     * @param countOneAccumulator countone accumulator
     * @param input new input vector (read from spilled batch)
     * @param maxValuesPerBatch batch size
     * @param computationVectorAllocator accumulator vector allocator
     */
    BigIntSumAccumulator(final CountOneAccumulator countOneAccumulator,
                         final FieldVector input, final int maxValuesPerBatch,
                         final BufferAllocator computationVectorAllocator) {
      this(input, countOneAccumulator.getOutput(),
           countOneAccumulator.getTransferVector(),
           maxValuesPerBatch, computationVectorAllocator,
           countOneAccumulator.getBitAddresses(),
           countOneAccumulator.getValueAddresses(),
           countOneAccumulator.getAccumulators());
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndZero(vector);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
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
        final int chunkIndex = getChunkIndexForOrdinal(tableIndex, maxValuesPerBatch);
        final int chunkOffset = getOffsetInChunkForOrdinal(tableIndex, maxValuesPerBatch);
        /* get the target addresses of accumulation vector */
        final long sumAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        /* store the accumulated values at the target location of accumulation vector */
        PlatformDependent.putLong(sumAddr, PlatformDependent.getLong(sumAddr) + newVal);
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }


  public static class DoubleSumAccumulator extends BaseSingleAccumulator {
    private static final int WIDTH_INPUT = 8;       /* double input vector */

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
    public DoubleSumAccumulator(FieldVector input, FieldVector output,
                                FieldVector transferVector, int maxValuesPerBatch,
                                BufferAllocator computationVectorAllocator) {
      this(input, output, transferVector, maxValuesPerBatch, computationVectorAllocator,
           null, null, null);
    }

    /*
     * private constructor that does the initialization.
     */
    private DoubleSumAccumulator(final FieldVector input, final FieldVector output,
                                 final FieldVector transferVector, final int maxValuesPerBatch,
                                 final BufferAllocator computationVectorAllocator,
                                 final long[] bitAddresses, final long[] valueAddresses,
                                 final FieldVector[] accumulators) {
      super(input, output, transferVector,
            AccumulatorBuilder.AccumulatorType.SUM,
            maxValuesPerBatch, computationVectorAllocator,
            bitAddresses,
            valueAddresses,
            accumulators);
    }

    /**
     * Used during post-spill processing to convert a float
     * sum accumulator to a post-spill double sum accumulator
     *
     * @param floatSumAccumulator pre-spill float sum accumulator
     * @param input input vector with values to be accumulated
     * @param maxValuesPerBatch max values in a hash table batch
     * @param computationVectorAllocator allocator used for allocating
     *                                   accumulators that store computed values
     */
    DoubleSumAccumulator(final FloatSumAccumulator floatSumAccumulator,
                         final FieldVector input, final int maxValuesPerBatch,
                         final BufferAllocator computationVectorAllocator) {
      this(input, floatSumAccumulator.getOutput(),
           floatSumAccumulator.getTransferVector(),
           maxValuesPerBatch, computationVectorAllocator,
           floatSumAccumulator.getBitAddresses(),
           floatSumAccumulator.getValueAddresses(),
           floatSumAccumulator.getAccumulators()
      );
    }

    /**
     * Used during post-spill processing to convert a decimal
     * sum accumulator to a post-spill double sum accumulator
     *
     * @param decimalSumAccumulator pre-spill decimal sum accumulator
     * @param input input vector with values to be accumulated
     * @param maxValuesPerBatch max values in a hash table batch
     * @param computationVectorAllocator allocator used for allocating
     *                                   accumulators that store computed values
     */
    DoubleSumAccumulator(final DecimalSumAccumulator decimalSumAccumulator,
                         final FieldVector input, final int maxValuesPerBatch,
                         final BufferAllocator computationVectorAllocator) {
      this(input, decimalSumAccumulator.getOutput(),
           decimalSumAccumulator.getTransferVector(),
           maxValuesPerBatch, computationVectorAllocator,
           decimalSumAccumulator.getBitAddresses(),
           decimalSumAccumulator.getValueAddresses(),
           decimalSumAccumulator.getAccumulators()
      );
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndZero(vector);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
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
        final int chunkIndex = getChunkIndexForOrdinal(tableIndex, maxValuesPerBatch);
        final int chunkOffset = getOffsetInChunkForOrdinal(tableIndex, maxValuesPerBatch);
        /* get the target addresses of accumulation vector */
        final long sumAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        /* store the accumulated values at the target location of accumulation vector */
        PlatformDependent.putLong(sumAddr, Double.doubleToLongBits(Double.longBitsToDouble(PlatformDependent.getLong(sumAddr)) + newVal));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }
  public static class DecimalSumAccumulator extends BaseSingleAccumulator {
    private static final int WIDTH_INPUT = 16;      // decimal inputs
    private static final int WIDTH_ACCUMULATOR = 8; // double accumulators
    private byte[] valBuf = new byte[WIDTH_INPUT];

    public DecimalSumAccumulator(FieldVector input, FieldVector output,
                                 FieldVector transferVector, int maxValuesPerBatch,
                                 BufferAllocator computationVectorAllocator) {
      super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.SUM, maxValuesPerBatch,
            computationVectorAllocator);
    }

    @Override
    void initialize(FieldVector vector) {
      setNullAndZero(vector);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final long[] bitAddresses = this.bitAddresses;
      final long[] valueAddresses = this.valueAddresses;
      final int scale = ((DecimalVector) inputVector).getScale();
      final int maxValuesPerBatch = super.maxValuesPerBatch;

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        java.math.BigDecimal newVal = DecimalAccumulatorUtils.getBigDecimal(incomingValue + (incomingIndex * WIDTH_INPUT), valBuf, scale);
        /* get the hash table batch index */
        final int chunkIndex = getChunkIndexForOrdinal(tableIndex, maxValuesPerBatch);
        final int chunkOffset = getOffsetInChunkForOrdinal(tableIndex, maxValuesPerBatch);
        /* get the target addresses of accumulation vector */
        final long sumAddr = valueAddresses[chunkIndex] + (chunkOffset) * WIDTH_ACCUMULATOR;
        final long bitUpdateAddr = bitAddresses[chunkIndex] + ((chunkOffset >>> 5) * 4);
        final int bitUpdateVal = bitVal << (chunkOffset & 31);
        /* store the accumulated values at the target location of accumulation vector */
        PlatformDependent.putLong(sumAddr, Double.doubleToLongBits(Double.longBitsToDouble(PlatformDependent.getLong(sumAddr)) + newVal.doubleValue() * bitVal));
        PlatformDependent.putInt(bitUpdateAddr, PlatformDependent.getInt(bitUpdateAddr) | bitUpdateVal);
      }
    }
  }
}

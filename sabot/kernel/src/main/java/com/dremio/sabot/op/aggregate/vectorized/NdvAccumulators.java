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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.datasketches.hll.HllSketch;

import com.google.common.base.Preconditions;

import io.netty.util.internal.PlatformDependent;

public final class NdvAccumulators {

  private NdvAccumulators(){}

  public static class IntNdvAccumulators extends BaseNdvAccumulator {
    private static final int WIDTH_INPUT = 4;

    public IntNdvAccumulators(FieldVector input, FieldVector transferVector, final int maxValuesPerBatch,
                              final BufferAllocator computationVectorAllocator, BaseValueVector tempAccumulatorHolder) {
      super(input, transferVector, maxValuesPerBatch, tempAccumulatorHolder);
    }

    @Override
    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue = getInput().getDataBufferAddress();

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        final int newVal = PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH_INPUT));

        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;

        update(chunkIndex, chunkOffset, newVal);
      }
    }
  }

  public static class VarLenNdvAccumulators extends BaseNdvAccumulator {

    public VarLenNdvAccumulators(FieldVector input, FieldVector transferVector, final int maxValuesPerBatch,
                                 final BufferAllocator computationVectorAllocator, BaseValueVector tempAccumulatorHolder) {
      super(input, transferVector, maxValuesPerBatch, tempAccumulatorHolder);
    }

    @Override
    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final ArrowBuf incomingOffsetBuf = getInput().getOffsetBuffer();
      final ArrowBuf incomingDataBuf = getInput().getDataBuffer();

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;

        //get the offset of incoming record
        final int startOffset = incomingOffsetBuf.getInt(incomingIndex * BaseVariableWidthVector.OFFSET_WIDTH);
        final int endOffset = incomingOffsetBuf.getInt((incomingIndex + 1) * BaseVariableWidthVector.OFFSET_WIDTH);
        final int len = endOffset - startOffset;
        byte[] bytes = new byte[len];
        incomingDataBuf.getBytes(startOffset, bytes);

        update(chunkIndex, chunkOffset, bytes);
      }
    }
  }

  public static class FloatNdvAccumulator extends BaseNdvAccumulator {
    private static final int WIDTH_INPUT = 4;

    public FloatNdvAccumulator(FieldVector input, FieldVector transferVector, final int maxValuesPerBatch,
                               final BufferAllocator computationVectorAllocator, BaseValueVector tempAccumulatorHolder) {
      super(input, transferVector, maxValuesPerBatch, tempAccumulatorHolder);
    }

    @Override
    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue = getInput().getDataBufferAddress();

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }
        final float newVal = Float.intBitsToFloat(PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH_INPUT)));

        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;

        update(chunkIndex, chunkOffset, newVal);
      }
    }
  }

  public static class BigIntNdvAccumulator extends BaseNdvAccumulator {
    private static final int WIDTH_INPUT = 8;

    public BigIntNdvAccumulator(FieldVector input, FieldVector transferVector, final int maxValuesPerBatch,
                                final BufferAllocator computationVectorAllocator, BaseValueVector tempAccumulatorHolder) {
      super(input, transferVector, maxValuesPerBatch, tempAccumulatorHolder);
    }

    @Override
    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue = getInput().getDataBufferAddress();

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }
        final long newVal = PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH_INPUT));

        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;

        update(chunkIndex, chunkOffset, newVal);
      }
    }
  }

  public static class DoubleNdvAccumulator extends BaseNdvAccumulator {
    private static final int WIDTH_INPUT = 8;

    public DoubleNdvAccumulator(FieldVector input, FieldVector transferVector, final int maxValuesPerBatch,
                                final BufferAllocator computationVectorAllocator, BaseValueVector tempAccumulatorHolder) {
      super(input, transferVector, maxValuesPerBatch, tempAccumulatorHolder);
    }

    @Override
    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue = getInput().getDataBufferAddress();

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        final double newVal = Double.longBitsToDouble(PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH_INPUT)));

        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;

        update(chunkIndex, chunkOffset, newVal);
      }
    }
  }

  public static class DecimalNdvAccumulator extends BaseNdvAccumulator {
    private static final int WIDTH_INPUT = 16;      // decimal inputs
    byte[] valBuf = new byte[WIDTH_INPUT];

    public DecimalNdvAccumulator(FieldVector input, FieldVector transferVector, final int maxValuesPerBatch,
                                 final BufferAllocator computationVectorAllocator, BaseValueVector tempAccumulatorHodler) {
      super(input, transferVector, maxValuesPerBatch, tempAccumulatorHodler);
    }

    @Override
    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue = getInput().getDataBufferAddress();
      final int scale = ((DecimalVector)getInput()).getScale();

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        java.math.BigDecimal newVal = DecimalAccumulatorUtils.getBigDecimal(incomingValue + (incomingIndex * WIDTH_INPUT), valBuf, scale);

        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;

        update(chunkIndex, chunkOffset, newVal.doubleValue());
      }
    }
  }

  public static class DecimalNdvAccumulatorV2 extends BaseNdvAccumulator {
    private static final int WIDTH_INPUT = 16;      // decimal inputs

    public DecimalNdvAccumulatorV2(FieldVector input, FieldVector transferVector, final int maxValuesPerBatch,
                                   final BufferAllocator computationVectorAllocator, BaseValueVector tempAccumulatorHolder) {
      super(input, transferVector, maxValuesPerBatch, tempAccumulatorHolder);
    }

    @Override
    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue = getInput().getDataBufferAddress();

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;

        byte[] bytes = new byte[WIDTH_INPUT];
        getInput().getDataBuffer().getBytes(incomingIndex * WIDTH_INPUT, bytes);

        update(chunkIndex, chunkOffset, bytes);
      }
    }
  }

  public static class BitNdvAccumulator extends BaseNdvAccumulator {
    private static final int WIDTH_LONG = 8;        // operations done on long boundaries
    private static final int BITS_PER_LONG_SHIFT = 6;  // (1<<6) bits per long
    private static final int BITS_PER_LONG = (1 << BITS_PER_LONG_SHIFT);

    public BitNdvAccumulator(FieldVector input, FieldVector transferVector, final int maxValuesPerBatch,
                             final BufferAllocator computationVectorAllocator, BaseValueVector tempAccumulatorHolder) {
      super(input, transferVector, maxValuesPerBatch, tempAccumulatorHolder);
    }

    @Override
    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue = getInput().getDataBufferAddress();

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
      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }
        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;

        final int newVal = (PlatformDependent.getByte(incomingValue + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;

        /* XXX: Ramesh: How to update the newVal. Updating single bit value is not correct */
        update(chunkIndex, chunkOffset, newVal);
      }
    }
  }

  public static class IntervalDayNdvAccumulator extends BaseNdvAccumulator {
    private static final int WIDTH_INPUT = 8;       // pair-of-ints inputs

    public IntervalDayNdvAccumulator(FieldVector input, FieldVector transferVector, final int maxValuesPerBatch,
                                     final BufferAllocator computationVectorAllocator, BaseValueVector tempAccumulatorHolder) {
      super(input, transferVector, maxValuesPerBatch, tempAccumulatorHolder);
    }

    @Override
    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue = getInput().getDataBufferAddress();

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }
        final long newVal = PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH_INPUT));

        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;

        update(chunkIndex, chunkOffset, newVal);
      }
    }
  }

  public static class NdvUnionAccumulators extends BaseNdvUnionAccumulator {
    public NdvUnionAccumulators(FieldVector input, FieldVector transferVector, final int maxValuesPerBatch,
                                final BufferAllocator computationVectorAllocator, BaseValueVector tempAccumulatorHolder) {
      super(input, transferVector, maxValuesPerBatch, tempAccumulatorHolder, null);
    }

    public NdvUnionAccumulators(BaseNdvAccumulator baseNdvAccum, FieldVector input,
                                final int maxValuesPerBatch, final BufferAllocator computationVectorAllocator) {
      super(input, baseNdvAccum.getOutput(), maxValuesPerBatch,
         baseNdvAccum.getTempAccumulatorHolder(), baseNdvAccum.getDataBuffer());
    }

    @Override
    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {
      final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseNdvUnionAccumulator.class);
      final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
      final long incomingBit = getInput().getValidityBufferAddress();
      final ArrowBuf inputOffsetBuf = getInput().getOffsetBuffer();
      final ArrowBuf inputBuf = getInput().getDataBuffer();

      for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
        /* get the index of data in input vector */
        final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + (incomingIndex >>> 3)) >>> (incomingIndex & 7)) & 1;
        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        /* get the hash table ordinal */
        final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
        /* get the hash table batch index */
        final int chunkIndex = tableIndex >>> bitsInChunk;
        final int chunkOffset = tableIndex & chunkOffsetMask;

        final int startOffset = inputOffsetBuf.getInt(incomingIndex * BaseVariableWidthVector.OFFSET_WIDTH);
        final int endOffset = inputOffsetBuf.getInt((incomingIndex + 1) * BaseVariableWidthVector.OFFSET_WIDTH);
        if (endOffset <= startOffset) {
          Preconditions.checkArgument(endOffset > startOffset,
            "Invalid size({} {}) at accumarray[{}], batchIndex: {}, batchOffset: {}",
            endOffset, startOffset, (partitionAndOrdinalAddr - memoryAddr) / PARTITIONINDEX_HTORDINAL_WIDTH,
            chunkIndex, chunkOffset);
        }
        byte[] bytes = new byte[endOffset - startOffset];
        inputBuf.getBytes(startOffset, bytes);
        final HllSketch sketch = HllSketch.heapify(bytes);

        update(chunkIndex, chunkOffset, sketch);
      }
    }
  }
}

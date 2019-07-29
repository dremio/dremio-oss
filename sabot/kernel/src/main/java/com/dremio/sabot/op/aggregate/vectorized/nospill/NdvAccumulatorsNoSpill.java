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


import java.nio.ByteBuffer;

import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;

import com.dremio.exec.util.DecimalUtils;
import com.dremio.sabot.op.common.ht2.LBlockHashTableNoSpill;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.Union;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;


public class NdvAccumulatorsNoSpill {

  private NdvAccumulatorsNoSpill(){};

  public static class IntNdvAccumulatorsNoSpill extends BaseNdvAccumulatorNoSpill {
    private static final int WIDTH = 4;

    public IntNdvAccumulatorsNoSpill(FieldVector input, FieldVector output, BufferManager bufferManager) {
      super(input, output, bufferManager);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;

        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        final int newVal = PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH));

        //get the proper chunk from the ordinal
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;

        final HllAccumHolder ah =  this.accumulators[chunkIndex];
        final HllSketch sketch = ah.getAccums()[chunkOffset];
        sketch.update(newVal);
      }
    }
  }

  public static class VarLenNdvAccumulatorsNoSpill extends BaseNdvAccumulatorNoSpill {

    public VarLenNdvAccumulatorsNoSpill(FieldVector input, FieldVector output, BufferManager bufferManager) {
      super(input, output, bufferManager);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();

      int incomingIndex = 0;

      final ArrowBuf inputOffsetBuf = getInput().getOffsetBuffer();
      final ArrowBuf inputBuf = getInput().getDataBuffer();

      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;

        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        //get the proper chunk from the ordinal
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        //System.out.println("record idx: " + incomingIndex + " ordinal: " + tableIndex);
        final int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        final int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;

        final HllAccumHolder ah =  this.accumulators[chunkIndex];
        final HllSketch sketch = ah.getAccums()[chunkOffset];

        //get the offset of incoming record
        final int startOffset = inputOffsetBuf.getInt(incomingIndex * BaseVariableWidthVector.OFFSET_WIDTH);
        final int endOffset = inputOffsetBuf.getInt((incomingIndex + 1) * BaseVariableWidthVector.OFFSET_WIDTH);
        final int len = endOffset - startOffset;
        final ByteBuffer buffer = inputBuf.nioBuffer(startOffset, len);

        //apply the update
        sketch.update(Memory.wrap(buffer), 0, len);
      } //for
    } //accumulate
  }

  public static class FloatNdvAccumulatorNoSpill extends BaseNdvAccumulatorNoSpill {
    private static final long INIT = 0x7f7fffff7f7fffffl;
    private static final int WIDTH = 4;

    public FloatNdvAccumulatorNoSpill(FieldVector input, FieldVector output, BufferManager bufferManager) {
      super(input, output, bufferManager);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;

        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        final float newVal = Float.intBitsToFloat(PlatformDependent.getInt(incomingValue + (incomingIndex * WIDTH)));

        //get the proper chunk from the ordinal
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;

        final HllAccumHolder ah =  this.accumulators[chunkIndex];
        final HllSketch sketch = ah.getAccums()[chunkOffset];
        sketch.update(newVal);
      }
    }
  }

  public static class BigIntNdvAccumulatorNoSpill extends BaseNdvAccumulatorNoSpill {

    private static final int WIDTH = 8;

    public BigIntNdvAccumulatorNoSpill(FieldVector input, FieldVector output, BufferManager bufferManager) {
      super(input, output, bufferManager);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;

        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        final long newVal = PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH));

        //get the proper chunk from the ordinal
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;

        final HllAccumHolder ah =  this.accumulators[chunkIndex];
        final HllSketch sketch = ah.getAccums()[chunkOffset];

        sketch.update(newVal);
      }
    }
  }

  public static class DoubleNdvAccumulatorNoSpill extends BaseNdvAccumulatorNoSpill {

    private static final long INIT = Double.doubleToLongBits(Double.MAX_VALUE);
    private static final int WIDTH = 8;

    public DoubleNdvAccumulatorNoSpill(FieldVector input, FieldVector output, BufferManager bufferManager) {
      super(input, output, bufferManager);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;

        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        final double newVal = Double.longBitsToDouble(PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH)));

        //get the proper chunk from the ordinal
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;

        final HllAccumHolder ah =  this.accumulators[chunkIndex];
        final HllSketch sketch = ah.getAccums()[chunkOffset];
        sketch.update(newVal);
      }
    }
  }

  public static class DecimalNdvAccumulatorNoSpill extends BaseNdvAccumulatorNoSpill {
    private static final int WIDTH_ORDINAL = 4;     // int ordinal #s
    private static final int WIDTH_INPUT = 16;      // decimal inputs
    byte[] valBuf = new byte[WIDTH_INPUT];

    public DecimalNdvAccumulatorNoSpill(FieldVector input, FieldVector output, BufferManager bufferManager) {
      super(input, output, bufferManager);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * WIDTH_ORDINAL;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final int scale = ((DecimalVector)inputVector).getScale();

      int incomingIndex = 0;
      for (long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += WIDTH_ORDINAL, incomingIndex++) {
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;

        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        java.math.BigDecimal newVal = DecimalAccumulatorUtilsNoSpill.getBigDecimal(incomingValue + (incomingIndex * WIDTH_INPUT), valBuf, scale);

        //get the proper chunk from the ordinal
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;

        final HllAccumHolder ah =  this.accumulators[chunkIndex];
        final HllSketch sketch = ah.getAccums()[chunkOffset];
        sketch.update(newVal.doubleValue());
      }
    }
  }

  public static class DecimalNdvAccumulatorNoSpillV2 extends BaseNdvAccumulatorNoSpill {

    private static final BigDecimal INIT = DecimalUtils.MAX_DECIMAL;
    private static final int WIDTH_ORDINAL = 4;     // int ordinal #s
    private static final int WIDTH_INPUT = 16;      // decimal inputs

    public DecimalNdvAccumulatorNoSpillV2(FieldVector input, FieldVector output, BufferManager bufferManager) {
      super(input, output, bufferManager);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * WIDTH_ORDINAL;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
      final int scale = ((DecimalVector) inputVector).getScale();

      int incomingIndex = 0;
      for (long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += WIDTH_ORDINAL, incomingIndex++) {
        /* get the corresponding data from input vector -- source data for accumulation */
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
        // without if we would need to do a multiply which is slow.
        if (bitVal == 0) {
          continue;
        }

        final ByteBuffer buffer = inputVector.getDataBuffer().nioBuffer(incomingIndex * WIDTH_INPUT, WIDTH_INPUT);

        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;

        final HllAccumHolder ah =  this.accumulators[chunkIndex];
        final HllSketch sketch = ah.getAccums()[chunkOffset];

        sketch.update(Memory.wrap(buffer), 0, WIDTH_INPUT);
      } //for
    }
  }

  public static class BitNdvAccumulatorNoSpill extends BaseNdvAccumulatorNoSpill {
    private static final long INIT = -1l;           // == 0xffffffffffffffff
    private static final int WIDTH_LONG = 8;        // operations done on long boundaries
    private static final int BITS_PER_LONG_SHIFT = 6;  // (1<<6) bits per long
    private static final int BITS_PER_LONG = (1<<BITS_PER_LONG_SHIFT);
    private static final int WIDTH_ORDINAL = 4;     // int ordinal #s

    public BitNdvAccumulatorNoSpill(FieldVector input, FieldVector output, BufferManager bufferManager) {
      super(input, output, bufferManager);
    }

    public void accumulate(final long memoryAddr, final int count) {
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();
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

          final int inputBitVal = (int)((inputBits >>> bitNum) & 0x01);
          final int inputVal = (int)((inputBatch >>> bitNum) & 0x01);
          final int minBitUpdateVal = inputBitVal << (chunkOffset & 31);

          final HllAccumHolder ah =  this.accumulators[chunkIndex];
          final HllSketch sketch = ah.getAccums()[chunkOffset];
          sketch.update(inputBitVal);
        }

      }
    }
  }

  public static class IntervalDayNdvAccumulatorNoSpill extends BaseNdvAccumulatorNoSpill {
    private static final int WIDTH_ORDINAL = 4;     // int ordinal #s
    private static final int WIDTH_INPUT = 8;       // pair-of-ints inputs

    public IntervalDayNdvAccumulatorNoSpill(FieldVector input, FieldVector output, BufferManager bufferManager) {
      super(input, output, bufferManager);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * WIDTH_ORDINAL;
      FieldVector inputVector = getInput();
      final long incomingBit = inputVector.getValidityBufferAddress();
      final long incomingValue = inputVector.getDataBufferAddress();

      int incomingIndex = 0;
      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += WIDTH_ORDINAL, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;

        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        final long newVal = PlatformDependent.getLong(incomingValue + (incomingIndex * WIDTH_INPUT));

        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;

        final HllAccumHolder ah =  this.accumulators[chunkIndex];
        final HllSketch sketch = ah.getAccums()[chunkOffset];
        sketch.update(newVal);
      }
    }
  }


  public static class NdvUnionAccumulatorsNoSpill extends BaseNdvUnionAccumulatorNoSpill{

    public NdvUnionAccumulatorsNoSpill(FieldVector input, FieldVector output, BufferManager bufferManager) {
      super(input, output, bufferManager);
    }

    public void accumulate(final long memoryAddr, final int count) {
      final long maxAddr = memoryAddr + count * 4;
      final long incomingBit = getInput().getValidityBufferAddress();
      final long incomingValue =  getInput().getDataBufferAddress();

      int incomingIndex = 0;

      final ArrowBuf inputOffsetBuf = getInput().getOffsetBuffer();
      final ArrowBuf inputBuf = getInput().getDataBuffer();

      for(long ordinalAddr = memoryAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
        final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;

        //incoming record is null, skip it
        if (bitVal == 0) {
          continue;
        }

        //get the proper chunk from the ordinal
        final int tableIndex = PlatformDependent.getInt(ordinalAddr);
        //System.out.println("record idx: " + incomingIndex + " ordinal: " + tableIndex);
        final int chunkIndex = tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK;
        final int chunkOffset = tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK;

        //get the offset of incoming record
        final int startOffset = inputOffsetBuf.getInt(incomingIndex * BaseVariableWidthVector.OFFSET_WIDTH);
        final int endOffset = inputOffsetBuf.getInt((incomingIndex + 1) * BaseVariableWidthVector.OFFSET_WIDTH);
        final int len = endOffset - startOffset;
        ByteBuffer buffer = inputBuf.nioBuffer(startOffset, len);

        //apply the update
        final HllUnionAccumHolder ah =  this.accumulators[chunkIndex];
        final Union unionSketch = ah.getAccums()[chunkOffset];
        final HllSketch sketch = HllSketch.wrap(Memory.wrap(buffer));
        unionSketch.update(sketch);
      } //for
    } //accumulate
  }

}

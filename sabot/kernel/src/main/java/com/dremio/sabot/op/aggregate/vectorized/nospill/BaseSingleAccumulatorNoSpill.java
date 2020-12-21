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
import java.util.stream.IntStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.common.ht2.LBlockHashTableNoSpill;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

/**
 * A base accumulator that manages the basic concepts of expanding the array of
 * accumulation vectors associated with the current aggregation.
 */
abstract class BaseSingleAccumulatorNoSpill implements AccumulatorNoSpill {

  private static final long OFF = 0;
  private static final long ON = 0xFFFFFFFFFFFFFFFFl;

  private final FieldVector input;
  private final FieldVector output;
  private FieldVector[] accumulators;
  private TransferPair[] pairs;
  long[] bitAddresses;
  long[] valueAddresses;
  int batches;

  public BaseSingleAccumulatorNoSpill(FieldVector input, FieldVector output){
    this.input = input;
    this.output = output;
    initArrs(0);
    batches = 0;
  }

  FieldVector getInput(){
    return input;
  }

  private void initArrs(int size){
    this.accumulators = new FieldVector[size];
    this.pairs = new TransferPair[size];
    this.bitAddresses = new long[size];
    this.valueAddresses = new long[size];
  }

  @Override
  public void resized(int newCapacity) {
    final int oldBatches = accumulators.length;
    final int currentCapacity = oldBatches * LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH;
    if(currentCapacity >= newCapacity){
      return;
    }

    // save old references.
    final FieldVector[] oldAccumulators = this.accumulators;
    final TransferPair[] oldPairs = this.pairs;
    final long[] oldBitAddresses = this.bitAddresses;
    final long[] oldValueAddresses = this.valueAddresses;

    final int newBatches = (int) Math.ceil( newCapacity / (LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH * 1.0d) );
    initArrs(newBatches);

    System.arraycopy(oldAccumulators, 0, this.accumulators, 0, oldBatches);
    System.arraycopy(oldPairs, 0, this.pairs, 0, oldBatches);
    System.arraycopy(oldBitAddresses, 0, this.bitAddresses, 0, oldBatches);
    System.arraycopy(oldValueAddresses, 0, this.valueAddresses, 0, oldBatches);

    for(int i = oldAccumulators.length; i < newBatches; i++){
      FieldVector vector = (FieldVector) output.getTransferPair(output.getAllocator()).getTo();
      TransferPair tp = vector.makeTransferPair(output);
      ((FixedWidthVector) vector).allocateNew(LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH);
      // need to clear the data since allocate new doesn't do so and we want to start with.
      initialize(vector);
      pairs[i] = tp;
      accumulators[i] = vector;
      bitAddresses[i] = vector.getValidityBufferAddress();
      valueAddresses[i] = vector.getDataBufferAddress();
      /* bump counter every time after successfully allocating and adding a batch,
       * if we fail in the middle, we know the point till which the non-null accumulator vectors
       * have been added.
       */
      ++batches;
    }
  }

  void initialize(FieldVector vector){
    // default initialization
    setNotNullAndZero(vector);
  }

  @Override
  public void output(int batchIndex) {
    pairs[batchIndex].transfer();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void close() throws Exception {
    final FieldVector[] accumulatorsToClose = new FieldVector[batches];
    for (int i = 0; i < batches; i++) {
      /* if we earlier failed to resize and hit OOM, we would have NULL(s)
       * towards the end of accumulator array and we need to ignore all of
       * them else the close() call will hit NPE. this is why we loop only
       * until batches.
       */
      accumulatorsToClose[i] = accumulators[i];
    }
    AutoCloseables.close(ImmutableList.copyOf(accumulatorsToClose));
  }

  public static void writeWordwise(long addr, long length, long value) {
    if (length == 0) {
      return;
    }

    long nLong = length >>> 3;
    int nBytes = (int) length & 7;
    for (long i = nLong; i > 0; i--) {
      PlatformDependent.putLong(addr, value);
      addr += 8;
    }
    if (nBytes == 4) {
      PlatformDependent.putInt(addr, (int) value);
      addr += 4;
    } else if (nBytes < 4) {
      for (int i = nBytes; i > 0; i--) {
        PlatformDependent.putByte(addr, (byte) value);
        addr++;
      }
    } else {
      PlatformDependent.putInt(addr, (int) value);
      addr += 4;
      for (int i = nBytes - 4; i > 0; i--) {
        PlatformDependent.putByte(addr, (byte) value);
        addr++;
      }
    }
  }

  public static void writeWordwise(ArrowBuf buffer, long length, BigDecimal value) {
    if (length == 0) {
      return;
    }
    int numberOfDecimals = (int) length >>>4;
    IntStream.range(0, numberOfDecimals).forEach( (index) -> {
      DecimalUtility.writeBigDecimalToArrowBuf(value, buffer, index, DecimalVector.TYPE_WIDTH);
    });
  }

  public static void fillInts(long addr, long length, int value) {
    if (length == 0) {
      return;
    }

    Preconditions.checkArgument((length & 3) == 0, "Error: length should be aligned at 4-byte boundary");
    /* optimize by writing word at a time */
    long valueAsLong = (((long)value) << 32) | (value & 0xFFFFFFFFL);
    long nLong = length >>>3;
    int remaining = (int) length & 7;
    for (long i = nLong; i > 0; i--) {
      PlatformDependent.putLong(addr, valueAsLong);
      addr += 8;
    }
    if (remaining > 0) {
      /* assert is not necessary but just in case */
      Preconditions.checkArgument(remaining == 4, "Error: detected incorrect remaining length");
      PlatformDependent.putInt(addr, value);
    }
  }

  public static void setNullAndValue(FieldVector vector, long value){
    List<ArrowBuf> buffers = vector.getFieldBuffers();
    ArrowBuf bits = buffers.get(0);
    writeWordwise(bits.memoryAddress(), bits.capacity(), OFF);
    ArrowBuf values = buffers.get(1);
    writeWordwise(values.memoryAddress(), values.capacity(), value);
  }

  public static void setNullAndValue(FieldVector vector, int value){
    List<ArrowBuf> buffers = vector.getFieldBuffers();
    ArrowBuf bits = buffers.get(0);
    writeWordwise(bits.memoryAddress(), bits.capacity(), OFF);
    ArrowBuf values = buffers.get(1);
    fillInts(values.memoryAddress(), values.capacity(), value);
  }

  public static void setNullAndZero(FieldVector vector){
    List<ArrowBuf> buffers = vector.getFieldBuffers();
    ArrowBuf bits = buffers.get(0);
    writeWordwise(bits.memoryAddress(), bits.capacity(), OFF);
    ArrowBuf values = buffers.get(1);
    writeWordwise(values.memoryAddress(), values.capacity(), OFF);
  }

  public static void setNotNullAndZero(FieldVector vector){
    List<ArrowBuf> buffers = vector.getFieldBuffers();
    ArrowBuf bits = buffers.get(0);
    writeWordwise(bits.memoryAddress(), bits.capacity(), ON);
    ArrowBuf values = buffers.get(1);
    writeWordwise(values.memoryAddress(), values.capacity(), OFF);
  }

  public static void setNullAndValue(FieldVector vector, BigDecimal value){
    List<ArrowBuf> buffers = vector.getFieldBuffers();
    ArrowBuf bits = buffers.get(0);
    writeWordwise(bits.memoryAddress(), bits.capacity(), OFF);
    ArrowBuf values = buffers.get(1);
    writeWordwise(values, values.capacity(), value);
  }
}

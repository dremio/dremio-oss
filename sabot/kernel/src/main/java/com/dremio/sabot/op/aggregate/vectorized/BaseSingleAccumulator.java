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
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.google.common.collect.FluentIterable;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * A base accumulator that manages the basic concepts of expanding the array of
 * accumulation vectors associated with the current aggregation.
 */
abstract class BaseSingleAccumulator implements Accumulator {

  private static final long OFF = 0;
  private static final long ON = 0xFFFFFFFFFFFFFFFFl;

  private final FieldVector input;
  private final FieldVector output;
  private FieldVector[] accumulators;
  private TransferPair[] pairs;
  long[] bitAddresses;
  long[] valueAddresses;

  public BaseSingleAccumulator(FieldVector input, FieldVector output){
    this.input = input;
    this.output = output;
    initArrs(0);
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
    final int currentCapacity = oldBatches * LBlockHashTable.MAX_VALUES_PER_BATCH;
    if(currentCapacity >= newCapacity){
      return;
    }

    // save old references.
    final FieldVector[] oldAccumulators = this.accumulators;
    final TransferPair[] oldPairs = this.pairs;
    final long[] oldBitAddresses = this.bitAddresses;
    final long[] oldValueAddresses = this.valueAddresses;

    final int newBatches = (int) Math.ceil( newCapacity / (LBlockHashTable.MAX_VALUES_PER_BATCH * 1.0d) );
    initArrs(newBatches);

    System.arraycopy(oldAccumulators, 0, this.accumulators, 0, oldBatches);
    System.arraycopy(oldPairs, 0, this.pairs, 0, oldBatches);
    System.arraycopy(oldBitAddresses, 0, this.bitAddresses, 0, oldBatches);
    System.arraycopy(oldValueAddresses, 0, this.valueAddresses, 0, oldBatches);

    for(int i = oldAccumulators.length; i < newBatches; i++){
      FieldVector vector = (FieldVector) output.getTransferPair(output.getAllocator()).getTo();
      TransferPair tp = vector.makeTransferPair(output);
      ((FixedWidthVector) vector).allocateNew(LBlockHashTable.MAX_VALUES_PER_BATCH);
      // need to clear the data since allocate new doesn't do so and we want to start with.
      initialize(vector);
      pairs[i] = tp;
      accumulators[i] = vector;
      bitAddresses[i] = vector.getFieldBuffers().get(0).memoryAddress();
      valueAddresses[i] = vector.getFieldBuffers().get(1).memoryAddress();
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
    AutoCloseables.close((Iterable<AutoCloseable>) (Object) FluentIterable.of(accumulators).toList());
  }

  public static void writeWordwise(long addr, int length, long value) {
    if (length == 0) {
      return;
    }

    int nLong = length >>> 3;
    int nBytes = length & 7;
    for (int i = nLong; i > 0; i--) {
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

  public static void setNullAndValue(FieldVector vector, long value){
    List<ArrowBuf> buffers = vector.getFieldBuffers();
    ArrowBuf bits = buffers.get(0);
    writeWordwise(bits.memoryAddress(), bits.capacity(), OFF);
    ArrowBuf values = buffers.get(1);
    writeWordwise(values.memoryAddress(), values.capacity(), value);
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

}

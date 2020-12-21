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

import static java.util.Arrays.asList;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

/**
 * A base accumulator that manages the basic concepts of expanding the array of
 * accumulation vectors associated with the current aggregation.
 */
abstract class BaseSingleAccumulator implements Accumulator {

  private static final long OFF = 0;
  private static final long ON = 0xFFFFFFFFFFFFFFFFl;

  private FieldVector input;
  private final FieldVector output;
  private final FieldVector transferVector;
  private FieldVector[] accumulators;
  private final AccumulatorBuilder.AccumulatorType type;
  final int maxValuesPerBatch;
  long[] bitAddresses;
  long[] valueAddresses;

  private boolean resizeAttempted;
  private int batches;
  private final BufferAllocator computationVectorAllocator;
  private final int validityBufferSize;
  private final int dataBufferSize;
  private final int totalBufferSize;

  // serialized field for loading vector with buffers during addBatch()
  private final SerializedField serializedField;

  /**
   * Accumulator for each partition will transfer the accumulated data from
   * accumulation vector into the corresponding transferVector part of outgoing container
   * @param input
   * @param output
   * @param transferVector
   */
  public BaseSingleAccumulator(final FieldVector input, final FieldVector output,
                               final FieldVector transferVector, final AccumulatorBuilder.AccumulatorType type,
                               final int maxValuesPerBatch, final BufferAllocator computationVectorAllocator){
    this (input, output, transferVector, type, maxValuesPerBatch,
          computationVectorAllocator, null, null, null);
  }

  /**
   * This is used to recreate the accumulator for post-spill processing
   * @param input source vector containing data to be accumulated (vector read from spilled batch)
   * @param output vector vector in outgoing container
   * @param transferVector vector in outgoing container that will hold the accumulated results when operator outputs data
   * @param type accumulator type
   * @param maxValuesPerBatch maximum records in a hashtable batch/block
   * @param computationVectorAllocator allocator
   * @param bitAddresses validity buffer addresses of accumulator vectors retrieved from pre-spill iteration
   * @param valueAddresses data buffer addresses of accumulator vectors retrieved from pre-spill iteration
   * @param accumulators empty accumulator vectors from pre-spill iteration that store computed values for each batch
   */
  public BaseSingleAccumulator(final FieldVector input, final FieldVector output,
                               final FieldVector transferVector, final AccumulatorBuilder.AccumulatorType type,
                               final int maxValuesPerBatch, final BufferAllocator computationVectorAllocator,
                               final long[] bitAddresses, final long[] valueAddresses,
                               final FieldVector[] accumulators){
    /* todo:
     * explore removing output vector. it is probably redundant and we only need
     * input and transfer vectors
     */
    this.input = input;
    this.output = output;
    this.transferVector = transferVector;
    this.type = type;
    this.maxValuesPerBatch = maxValuesPerBatch;
    this.resizeAttempted = false;
    if (bitAddresses != null) {
      Preconditions.checkArgument(valueAddresses != null && accumulators != null,
                                  "Error: expecting non-null address array and accumulator vectors");
      Preconditions.checkArgument(bitAddresses.length == 1, "Error: incorrect length");
      this.accumulators = accumulators;
      this.bitAddresses = bitAddresses;
      this.valueAddresses = valueAddresses;
      this.batches = bitAddresses.length;
    } else {
      Preconditions.checkArgument(valueAddresses == null && accumulators == null,
                                  "Error: expecting null address array and accumulator vectors");
      initArrs(0);
      this.batches = 0;
    }
    this.computationVectorAllocator = computationVectorAllocator;
    // buffer sizes
    this.validityBufferSize = getValidityBufferSizeFromCount(maxValuesPerBatch);
    this.totalBufferSize = output.getBufferSizeFor(maxValuesPerBatch);
    this.dataBufferSize = totalBufferSize - validityBufferSize;

    final SerializedField field = TypeHelper.getMetadata(output);
    final SerializedField.Builder serializedFieldBuilder = field.toBuilder();
    // top level value count and total buffer size
    serializedFieldBuilder.setValueCount(maxValuesPerBatch);
    serializedFieldBuilder.setBufferLength(totalBufferSize);
    serializedFieldBuilder.clearChild();
    // add validity child
    serializedFieldBuilder.addChild(field.getChild(0).toBuilder().setValueCount(maxValuesPerBatch).setBufferLength(validityBufferSize));
    // add data child
    serializedFieldBuilder.addChild(field.getChild(1).toBuilder().setValueCount(maxValuesPerBatch).setBufferLength(totalBufferSize - validityBufferSize));
    // this serialized field will be used for adding all batches (new accumulator vectors) and loading them
    this.serializedField = serializedFieldBuilder.build();
  }

  AccumulatorBuilder.AccumulatorType getType() {
    return type;
  }

  /**
   * HashTable and accumulator always run parallel -- when we add
   * a block/batch to hashtable, we also a new block/batch
   * to accumulators. This function is used to verify state
   * is consistent across these data structures.
   * @param batches number of blocks/batches in hashtable
   */
  @Override
  public void verifyBatchCount(final int batches) {
    Preconditions.checkArgument(this.batches == batches, "Error: Detected incorrect batch count in accumulator");
  }

  /**
   * Get the input vector which has source data
   * to be accumulated.
   *
   * @return input vector
   */
  @Override
  public FieldVector getInput() {
    return input;
  }

  /**
   * Set the input vector. This is used by {@link VectorizedHashAggOperator}
   * when processing spilled partitions. Once an operator
   * reads a spilled batch, the accumulator vectors
   * from the batch now become as new input vectors
   * for post-spill processing where we restart the
   * aggregation algorithm.
   *
   * @param inputVector new input vector
   */
  @Override
  public void setInput(final FieldVector inputVector) {
    this.input = inputVector;
  }

  private void initArrs(int size){
    this.accumulators = new FieldVector[size];
    this.bitAddresses = new long[size];
    this.valueAddresses = new long[size];
  }

  public int getBatchCount() {
    return batches;
  }

  public List<ArrowBuf> getBuffers(final int batchIndex) {
    return accumulators[batchIndex].getFieldBuffers();
  }

  public FieldVector getAccumulatorVector(final int batchIndex) {
    return accumulators[batchIndex];
  }

  public void setValueCount(final int batchIndex, final int valueCount) {
    accumulators[batchIndex].setValueCount(valueCount);
  }

  @Override
  public void addBatch(final ArrowBuf dataBuffer, final ArrowBuf validityBuffer) {
    try {
      FieldVector[] oldAccumulators;
      long[] oldBitAddresses;
      long[] oldValueAddresses;
      if (batches == accumulators.length) {
        /* save old references */
        oldAccumulators = this.accumulators;
        oldBitAddresses = this.bitAddresses;
        oldValueAddresses = this.valueAddresses;
        /* provision more to avoid copy in the next call to addBatch */
        initArrs((batches == 0) ? 1 : batches * 2);
        System.arraycopy(oldAccumulators, 0, this.accumulators, 0, batches);
        System.arraycopy(oldBitAddresses, 0, this.bitAddresses, 0, batches);
        System.arraycopy(oldValueAddresses, 0, this.valueAddresses, 0, batches);
      }
      /* add a single batch */
      addBatchHelper(dataBuffer, validityBuffer);
    } catch (Exception e) {
      /* this will be caught by LBlockHashTable and subsequently handled by VectorizedHashAggOperator */
      Throwables.propagate(e);
    }
  }

  private void addBatchHelper(final ArrowBuf dataBuffer, final ArrowBuf validityBuffer) {
    resizeAttempted = true;
    /* the memory for accumulator vector comes from the operator's allocator that manages
     * memory for all data structures required for hash agg processing.
     * in other words, we should not use output.getAllocator() here as that allocator is
     * _only_ for for managing memory of output batch.
     */
    FieldVector vector = (FieldVector) output.getTransferPair(computationVectorAllocator).getTo();
    /* store the new vector and increment batches before allocating memory */
    accumulators[batches] = vector;
    final int oldBatches = batches;
    batches++;
    /* if this step or memory allocation inside any child of NestedAccumulator fails,
     * we have captured enough info to rollback the operation.
     */
    loadAccumulatorForNewBatch(vector, dataBuffer, validityBuffer);
    /* need to clear the data since allocate new doesn't do so and we want to start with clean memory */
    initialize(vector);
    bitAddresses[oldBatches] = vector.getValidityBufferAddress();
    valueAddresses[oldBatches] = vector.getDataBufferAddress();

    checkNotNull();
  }

  /**
   * When LBlockHashTable decides to add a new batch/block,
   * to all the accumulators under AccumulatorSet, the latter
   * does memory allocation for accumulators together using an algorithm
   * that aims for optimal direct and heap memory usage. AccumulatorSet
   * allocates joint buffers by grouping accumulators into different power of
   * 2 buckets. So here all we need to do is to load the new accumulator vector
   * for the new batch with new buffers. To load data into vector from ArrowBufs
   * we reused the TypeHelper.load() methods which just require the vector structure
   * and metadata in the form of SerializedField.
   *
   * The SerializedField was built exactly once for each type of child accumulator
   * (of type BaseSingleAccumulator) under AccumulatorSet. Subsequently when we add a
   * new batch to child accumulator we just create an instance of FieldVector and load it
   * with new buffers
   *
   * @param vector instance of FieldVector (not yet allocated) representing the new accumulator vector for the next batch
   * @param dataBuffer data buffer for this accumulator vector
   * @param validityBuffer validity buffer for this accumulator vector
   */
  private void loadAccumulatorForNewBatch(final FieldVector vector, final ArrowBuf dataBuffer, final ArrowBuf validityBuffer) {
    TypeHelper.loadFromValidityAndDataBuffers(vector, serializedField, dataBuffer, validityBuffer);
  }

  private static int getValidityBufferSizeFromCount(final int valueCount) {
    return (int) Math.ceil(valueCount / 8.0);
  }

  @Override
  public int getValidityBufferSize() {
    return validityBufferSize;
  }

  @Override
  public int getDataBufferSize() {
    return dataBufferSize;
  }

  @Override
  public void revertResize() {
    if (!resizeAttempted) {
      /* because this is invoked for all accumulators under NestedAccumulator,
       * it will be a NO-OP for some accumulators if we failed in the middle
       * of NestedAccumulator.
       */
      return;
    }

    this.accumulators[batches - 1].close();
    /* not really necessary */
    this.accumulators[batches - 1] = null;
    this.bitAddresses[batches - 1] = 0;
    this.valueAddresses[batches - 1] = 0L;
    resizeAttempted = false;

    batches--;

    checkNotNull();
  }

  @Override
  public void commitResize() {
    this.resizeAttempted = false;
  }

  /**
   * Used to get the size of target accumulator vector
   * that stores the computed values. Arrow code
   * already has a way to get the exact size (in bytes)
   * from a vector by looking at the value count and type
   * of the vector. The returned size accounts both
   * validity and data buffers in the vector.
   *
   * We use this method when computing the size
   * of {@link VectorizedHashAggPartition} as part
   * of choosing a victim partition.
   *
   * @return size of vector (in bytes)
   */
  @Override
  public long getSizeInBytes() {
    long size = 0;
    for (int i = 0; i < batches; i++) {
      final FieldVector accumulatorVector = accumulators[i];
      size += accumulatorVector.getBufferSize();
    }
    return size;
  }

  private void checkNotNull() {
    for (int i = 0; i < accumulators.length; i++) {
      if (i < batches) {
        Preconditions.checkArgument(accumulators[i] != null, "Error: expecting a valid accumulator");
      } else {
        Preconditions.checkArgument(accumulators[i] == null, "Error: expecting a null accumulator");
      }
    }
  }

  @Override
  public void resetToMinimumSize() throws Exception {
    if (accumulators.length <= 1) {
      resetFirstAccumulatorVector();
      return;
    }
    final FieldVector[] oldAccumulators = this.accumulators;
    accumulators = Arrays.copyOfRange(oldAccumulators, 0, 1);
    bitAddresses =  Arrays.copyOfRange(bitAddresses, 0, 1);
    valueAddresses =  Arrays.copyOfRange(valueAddresses, 0, 1);

    resetFirstAccumulatorVector();
    batches = 1;

    AutoCloseables.close(asList(Arrays.copyOfRange(oldAccumulators, 1, oldAccumulators.length)));
  }

  private void resetFirstAccumulatorVector() {
    Preconditions.checkArgument(accumulators.length == 1, "Error: incorrect number of batches in accumulator");
    final FieldVector vector = accumulators[0];
    Preconditions.checkArgument(vector != null, "Error: expecting a valid accumulator");
    final ArrowBuf validityBuffer = vector.getValidityBuffer();
    final ArrowBuf dataBuffer = vector.getDataBuffer();
    validityBuffer.readerIndex(0);
    validityBuffer.writerIndex(0);
    dataBuffer.readerIndex(0);
    dataBuffer.writerIndex(0);
    initialize(vector);
    vector.setValueCount(0);
  }

  @Override
  public void releaseBatch(final int batchIdx)
  {
    //the 0th batch memory is never released, only reset.
    if (batchIdx == 0) {
      resetFirstAccumulatorVector();
      return;
    }

    Preconditions.checkArgument(batchIdx < accumulators.length, "Error: incorrect batch index to release");

    final FieldVector vector = accumulators[batchIdx];
    vector.close();
  }

  void initialize(FieldVector vector){
    // default initialization
    setNotNullAndZero(vector);
  }

  /**
   * Take the accumulator vector (the vector that stores computed values)
   * for a particular batch (identified by batchIndex) and output its contents.
   * Output is done by transferring the contents from accumulator vector
   * to its counterpart in outgoing container. As part of transfer,
   * the memory ownership (along with data) is transferred from source
   * vector's allocator to target vector's allocator and source vector's
   * memory is released.
   *
   * While the transfer is good as it essentially
   * avoids copy, we still want the memory associated with
   * allocator of source vector because of post-spill processing
   * where this accumulator vector will still continue to store
   * the computed values as we start treating spilled batches
   * as new input into the operator.
   *
   * This is why we need to immediately allocate
   * the accumulator vector after transfer is done. However we do this
   * for a singe batch only as once we are done outputting a partition,
   * we anyway get rid of all but 1 batch.
   *
   * @param batchIndex batch to output
   */
  @Override
  public void output(final int batchIndex) {
    final FieldVector accumulationVector = accumulators[batchIndex];
    final TransferPair transferPair = accumulationVector.makeTransferPair(transferVector);
    transferPair.transfer();
    if (batchIndex == 0) {
      ((FixedWidthVector) accumulationVector).allocateNew(maxValuesPerBatch);
      accumulationVector.setValueCount(0);
      initialize(accumulationVector);
      bitAddresses[batchIndex] = accumulationVector.getValidityBufferAddress();
      valueAddresses[batchIndex] = accumulationVector.getDataBufferAddress();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void close() throws Exception {
    final FieldVector[] accumulatorsToClose = new FieldVector[batches];
    for (int i = 0; i < accumulators.length; i++) {
      if (i < batches) {
        Preconditions.checkArgument(accumulators[i] != null, "Error: expecting a valid accumulator");
        accumulatorsToClose[i] = accumulators[i];
      } else {
        Preconditions.checkArgument(accumulators[i] == null, "Error: expecting a null accumulator");
      }
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
    byte [] valueInLEBytes = value.unscaledValue().toByteArray();
    IntStream.range(0, numberOfDecimals).forEach( (index) -> {
      DecimalUtility.writeByteArrayToArrowBuf(valueInLEBytes, buffer, index, DecimalVector.TYPE_WIDTH);
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

  /**
   * Get the target vector that stores the computed
   * values for the accumulator.
   *
   * @return target vector
   */
  public FieldVector getOutput() {
    return output;
  }

  /**
   * Get the destination vector where accumulated data
   * is transferred when {@link VectorizedHashAggOperator}
   * starts to output data.
   * The destination vector is always a part of outgoing
   * {@link com.dremio.exec.record.VectorContainer} for
   * the operator.
   *
   * @return desination vector
   */
  public FieldVector getTransferVector() {
    return transferVector;
  }

  long[] getBitAddresses() {
    return bitAddresses;
  }

  long[] getValueAddresses() {
    return valueAddresses;
  }

  FieldVector[] getAccumulators() {
    return accumulators;
  }
}

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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.MutableVarcharVector;
import org.apache.arrow.vector.VariableWidthVector;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

/**
 * A base accumulator that manages the basic concepts of expanding the array of
 * accumulation vectors associated with min/max of varlength columns
 */
abstract class BaseVarBinaryAccumulator implements Accumulator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseVarBinaryAccumulator.class);

  private FieldVector input;
  private final FieldVector transferVector;
  protected FieldVector[] accumulators;
  private final AccumulatorBuilder.AccumulatorType type;
  final int maxValuesPerBatch;

  private boolean resizeAttempted;
  private int batches;
  private final BufferAllocator computationVectorAllocator;
  private final BaseVariableWidthVector tempAccumulatorHolder;
  private final int maxVarWidthVecUsagePercent;
  private int estimatedVariableWidthKeySize;
  private final int maxVariableWidthKeySize;
  private long runtimeVarLenColumnSize = 0;
  private int runtimeVarLenEntries = 0;
  private final int accumIndex;
  private final VectorizedHashAggOperator.VarLenVectorResizer varLenVectorResizer;

  /**
   * @param input
   * @param transferVector
   */
  public BaseVarBinaryAccumulator(final FieldVector input, final FieldVector transferVector,
                                  final AccumulatorBuilder.AccumulatorType type, final int maxValuesPerBatch,
                                  final BufferAllocator computationVectorAllocator, int estimatedVariableWidthKeySize,
                                  int maxVariableWidthKeySize, int maxVarWidthVecUsagePercent,
                                  int accumIndex, BaseVariableWidthVector tempAccumulatorHolder,
                                  VectorizedHashAggOperator.VarLenVectorResizer varLenVectorResizer) {
    this.input = input;
    this.transferVector = transferVector;
    this.type = type;
    this.maxValuesPerBatch = maxValuesPerBatch;
    this.resizeAttempted = false;
    initArrs(0);
    this.batches = 0;
    this.computationVectorAllocator = computationVectorAllocator;
    this.estimatedVariableWidthKeySize = estimatedVariableWidthKeySize;
    this.maxVariableWidthKeySize = maxVariableWidthKeySize;
    this.maxVarWidthVecUsagePercent = maxVarWidthVecUsagePercent;
    this.accumIndex = accumIndex;
    this.tempAccumulatorHolder = tempAccumulatorHolder;
    this.varLenVectorResizer = varLenVectorResizer;
  }

  public void updateRunTimeVarLenColumnSize(int varLenColumnSize) {
    runtimeVarLenColumnSize += varLenColumnSize;
    ++runtimeVarLenEntries;
  }

  @Override
  public AccumulatorBuilder.AccumulatorType getType() {
    return type;
  }

  /**
   * HashTable and accumulator always run parallel -- when we add a block/batch to
   * hashtable, we also add new block/batch to accumulators. This function is used
   * to verify state is consistent across these data structures.
   * @param batches number of blocks/batches in hashtable
   */
  @Override
  public void verifyBatchCount(final int batches) {
    Preconditions.checkArgument(this.batches == batches,
      "Error: Detected incorrect batch count ({}: expected:{}, found:{}) in accumulator",
      this, batches, this.batches);
  }

  /**
   * Get the input vector which has source data to be accumulated.
   *
   * @return input vector
   */
  @Override
  public FieldVector getInput() {
    return input;
  }

  /**
   * Set the input vector. This is used by {@link VectorizedHashAggOperator}
   * when processing spilled partitions. Once an operator reads a spilled batch,
   * the accumulator vectors from the batch now become as new input vectors for
   * post-spill processing where we restart the aggregation algorithm.
   *
   * @param inputVector new input vector
   */
  @Override
  public void setInput(final FieldVector inputVector) {
    this.input = inputVector;
  }

  private void initArrs(int size){
    this.accumulators = new FieldVector[size];
  }

  @Override
  public int getBatchCount() {
    return batches;
  }

  @Override
  public List<ArrowBuf> getBuffers(final int batchIndex, final int recordsInBatch) {
    final MutableVarcharVector mv = (MutableVarcharVector)accumulators[batchIndex];

    tempAccumulatorHolder.reset();
    mv.copyToVarWidthVec(tempAccumulatorHolder, recordsInBatch, 0);

    return tempAccumulatorHolder.getFieldBuffers();
  }

  @Override
  public void addBatch(final ArrowBuf dataBuffer, final ArrowBuf validityBuffer) {
    try {
      if (batches == accumulators.length) {
        FieldVector[] oldAccumulators = this.accumulators;

        /* provision more to avoid copy in the next call to addBatch */
        initArrs((batches == 0) ? 1 : batches * 2);
        System.arraycopy(oldAccumulators, 0, this.accumulators, 0, batches);
      }
      /* add a single batch */
      addBatchHelper(dataBuffer, validityBuffer);
    } catch (Exception e) {
      /* this will be caught by LBlockHashTable and subsequently handled by VectorizedHashAggOperator */
      Throwables.propagate(e);
    }
  }

  private void addBatchHelper(final ArrowBuf dataBuffer, final ArrowBuf validityBuffer) {
    /* store the new vector and increment batches before allocating memory */
    FieldVector vector = new MutableVarcharVector(input.getField().getName(),
      computationVectorAllocator, 0.2, maxVarWidthVecUsagePercent);
    accumulators[batches++] = vector;
    resizeAttempted = true;

    /* if this step or memory allocation inside any child of NestedAccumulator fails,
     * we have captured enough info to rollback the operation.
     */
    loadAccumulatorForNewBatch(vector, dataBuffer, validityBuffer);

    /* need to clear the data since allocate new doesn't do so and we want to start with clean memory */
    vector.reset();

    checkNotNull();
  }

  /**
   * When LBlockHashTable decides to add a new batch/block, to all the
   * accumulators under AccumulatorSet, the latter does memory allocation
   * for accumulators together using an algorithm that aims for optimal
   * direct and heap memory usage. AccumulatorSet allocates joint buffers
   * by grouping accumulators into different power of 2 buckets. So here
   * all we need to do is to load the new accumulator vector for the new
   * batch with new buffers. To load data into vector from ArrowBufs usually
   * the TypeHelper.load() methods are used, which just require the vector
   * structure and metadata in the form of SerializedField, however here
   * loading done locally.
   *
   * @param vector instance of FieldVector (not yet allocated) representing the new accumulator vector for the next batch
   * @param dataBuffer data buffer for this accumulator vector
   * @param validityBuffer validity buffer for this accumulator vector
   */
  private void loadAccumulatorForNewBatch(final FieldVector vector, final ArrowBuf dataBuffer, final ArrowBuf validityBuffer) {
    MutableVarcharVector mv = (MutableVarcharVector) vector;
    mv.loadBuffers(maxValuesPerBatch, estimatedVariableWidthKeySize * maxValuesPerBatch,
      dataBuffer, validityBuffer);
  }

  @Override
  public int getValidityBufferSize() {
    return MutableVarcharVector.getValidityBufferSizeFromCount(maxValuesPerBatch);
  }

  @Override
  public int getDataBufferSize() {

    /*
     * AccumulatorSet queries this before estimating and/or creating new vector. If at least 10% of
     * entries for a batch are sampled, then use it to estimate new estimatedVariableWidthKeySize
     */
    if (runtimeVarLenEntries >= maxValuesPerBatch / 10 &&
        /* estimatedVariableWidthKeySize value cannot do down */
        estimatedVariableWidthKeySize < runtimeVarLenColumnSize / runtimeVarLenEntries) {
      int newEstimatedVariableWidthKeySize = Math.min((int)(runtimeVarLenColumnSize / runtimeVarLenEntries), maxVariableWidthKeySize);
      if (varLenVectorResizer.tryResize(accumIndex, newEstimatedVariableWidthKeySize * maxValuesPerBatch)) {
        estimatedVariableWidthKeySize = newEstimatedVariableWidthKeySize;
      }
      Preconditions.checkArgument(tempAccumulatorHolder.getByteCapacity() >= estimatedVariableWidthKeySize * maxValuesPerBatch);
    }
    runtimeVarLenColumnSize = 0;
    runtimeVarLenEntries = 0;

    return MutableVarcharVector.getDataBufferSizeFromCount(maxValuesPerBatch,
      estimatedVariableWidthKeySize * maxValuesPerBatch);
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
    this.accumulators[batches - 1] = null;
    --batches;
    resizeAttempted = false;

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
      MutableVarcharVector mv = (MutableVarcharVector) accumulators[i];
      size += mv.getSizeInBytes();
    }
    return size;
  }

  private void checkNotNull() {
    for (int i = 0; i < batches; ++i) {
      Preconditions.checkArgument(accumulators[i] != null, "Error: expecting a valid accumulator");
    }
    for (int i = batches; i < accumulators.length; ++i) {
      Preconditions.checkArgument(accumulators[i] == null, "Error: expecting a null accumulator");
    }
  }

  @Override
  public void resetToMinimumSize() throws Exception {
    Preconditions.checkArgument(batches > 0);
    if (batches == 1) {
      resetFirstAccumulatorVector();
      return;
    }

    final FieldVector[] oldAccumulators = this.accumulators;
    accumulators = Arrays.copyOfRange(oldAccumulators, 0, 1);

    resetFirstAccumulatorVector();
    batches = 1;

    AutoCloseables.close(asList(Arrays.copyOfRange(oldAccumulators, 1, oldAccumulators.length)));
  }

  private void resetFirstAccumulatorVector() {
    final FieldVector vector = accumulators[0];
    Preconditions.checkArgument(vector != null, "Error: expecting a valid accumulator");
    vector.reset();
  }

  /**
   * Take the accumulator vector (the vector that stores computed values)
   * for a particular batch (identified by batchIndex) and output its contents.
   * Output is done by copying the contents from accumulator vector to its
   * counterpart in outgoing container. Copy is done after allocating new memory
   * region in the outgoing container. Unlike fixed length accumulators, we
   * cannot transfer the contents as backing MutableVarcharVector does not
   * have the data stored in the index order. Even if requested to output multiple
   * batches, the process is same.
   *
   * We still want the memory associated with allocator of source vector because
   * of post-spill processing where this accumulator vector will still continue
   * to store the computed values as we start treating spilled batches as new
   * input into the operator. However we do this for a singe batch only as once
   * we are done outputting a partition, we anyway get rid of all but 1 batch.
   */
  @Override
  public void output(final int startBatchIndex, int[] recordsInBatches) {
    int numRecords = 0;
    int usedByteCapacity = 0;

    for (int i = 0; i < recordsInBatches.length; i++) {
      usedByteCapacity += getUsedByteCapacity(startBatchIndex + i);
      numRecords += recordsInBatches[i];
    }

    /* trasferVector is always empty as after output, the buffers are transferred. */
    ((VariableWidthVector) transferVector).allocateNew(usedByteCapacity, numRecords);
    transferVector.reset();

    numRecords = 0;
    for (int i = 0; i < recordsInBatches.length; i++) {
      final MutableVarcharVector mv = (MutableVarcharVector) accumulators[startBatchIndex + i];
      mv.copyToVarWidthVec((BaseVariableWidthVector) transferVector, recordsInBatches[i], numRecords);
      numRecords += recordsInBatches[i];

      releaseBatch(startBatchIndex + i);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void close() throws Exception {
    /*
     * tempAccumulatorHolder is not closed here , as it is referenced from the operator & is closed
     * during the operator 'close'.
     */
    final FieldVector[] accumulatorsToClose = new FieldVector[batches];
    for (int i = 0; i < batches; i++) {
      Preconditions.checkArgument(accumulators[i] != null, "Error: expecting a valid accumulator");
      accumulatorsToClose[i] = accumulators[i];
    }
    for (int i = batches; i < accumulators.length; i++) {
      Preconditions.checkArgument(accumulators[i] == null, "Error: expecting a null accumulator");
    }
    AutoCloseables.close(ImmutableList.copyOf(accumulatorsToClose));
  }

  /**
   * Get the target vector that stores the computed
   * values for the accumulator.
   *
   * @return target vector
   */
  @Override
  public FieldVector getOutput() {
    return transferVector;
  }

  @Override
  public SerializedField getSerializedField(int batchIndex, int recordCount) {
    /*
     * HashAggPartitionWritableBatch.java:getNextWritableBatch() will call getBuffers()
     * followed by getSerializedField().
     * In the current context, the tempAccumulatorHolder already has the data saved,
     * especially the valueCount hence directly calling the TypeHelper.getMetadata()
     * is sufficient.
     */
    Preconditions.checkArgument(tempAccumulatorHolder.getValueCount() == recordCount);
    return TypeHelper.getMetadata(tempAccumulatorHolder);
  }

  private int getUsedByteCapacity(final int batchIndex) {
    final MutableVarcharVector mv = (MutableVarcharVector)accumulators[batchIndex];
    return mv.getUsedByteCapacity();
  }

  @Override
  public void releaseBatch(final int batchIdx) {
    Preconditions.checkArgument(batchIdx < batches, "Error: incorrect batch index to release");
    if (batchIdx == 0) {
      // 0th batch memory is never released, only reset.
      resetFirstAccumulatorVector();
    } else {
      accumulators[batchIdx].clear();
    }
  }

  @Override
  public boolean hasSpace(final int space, int batchIndex) {
    final MutableVarcharVector mv = (MutableVarcharVector)accumulators[batchIndex];
    return mv.checkHasAvailableSpace(space);
  }

  @Override
  public void moveValuesAndFreeSpace(int srcBatchIndex, int dstBatchIndex,
                                     int srcStartIndex, int dstStartIndex, int numRecords) {
    ((MutableVarcharVector)accumulators[srcBatchIndex]).moveValuesAndFreeSpace(
      srcStartIndex, dstStartIndex, numRecords, accumulators[dstBatchIndex]);
  }

  @Override
  public long getCompactionTime(TimeUnit unit) {
    long result = 0;
    for (int i = 0; i < batches; ++i) {
      final MutableVarcharVector mv = (MutableVarcharVector)accumulators[i];
      result += mv.getCompactionTime(unit);
    }
    return result;
  }

  @Override
  public int getNumCompactions() {
    int result = 0;
    for (int i = 0; i < batches; ++i) {
      final MutableVarcharVector mv = (MutableVarcharVector) accumulators[i];
      result += mv.getNumCompactions();
    }
    return result;
  }

  @Override
  public int getMaxVarLenKeySize() {
    return estimatedVariableWidthKeySize;
  }
}

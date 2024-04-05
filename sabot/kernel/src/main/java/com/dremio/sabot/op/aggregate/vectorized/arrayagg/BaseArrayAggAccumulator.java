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

package com.dremio.sabot.op.aggregate.vectorized.arrayagg;

import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.HTORDINAL_OFFSET;
import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.KEYINDEX_OFFSET;
import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.op.aggregate.vectorized.Accumulator;
import com.dremio.sabot.op.aggregate.vectorized.AccumulatorBuilder;
import com.google.common.base.Preconditions;
import io.netty.util.internal.PlatformDependent;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;

/**
 * Baseline Accumulator that is responsible for holding accumulated items of ARRAY_AGG. {@link
 * com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator} adds batches of data buffers
 * and passes data items for accumulation. {@link BaseArrayAggAccumulator} is responsible for
 * holding accumulated items by group and outputting batches when requested.
 *
 * @param <ElementType> Type of individual element being accumulated
 * @param <VectorType> Container Arrow vector being used for holding accumulated items
 */
public abstract class BaseArrayAggAccumulator<ElementType, VectorType extends FieldVector>
    implements Accumulator {
  private FieldVector input;
  private final FieldVector transferVector;
  private final int maxValuesPerBatch;
  private final ListVector tempAccumulatorHolder;
  private LinkedList<BaseArrayAggAccumulatorHolder<ElementType, VectorType>> accumulatedBatches;
  private boolean resizeInProgress;
  private final BufferAllocator allocator;

  protected BaseArrayAggAccumulator(
      FieldVector input,
      FieldVector transferVector,
      int maxValuesPerBatch,
      BaseValueVector tempAccumulatorHolder,
      BufferAllocator allocator) {
    this.input = input;
    this.transferVector = transferVector;
    this.tempAccumulatorHolder = (ListVector) tempAccumulatorHolder;
    this.maxValuesPerBatch = maxValuesPerBatch;
    this.accumulatedBatches = new LinkedList<>();
    resizeInProgress = false;
    this.allocator = allocator;
  }

  protected abstract int getFieldWidth();

  protected abstract void writeItem(UnionListWriter writer, ElementType item);

  protected abstract BaseArrayAggAccumulatorHolder<ElementType, VectorType> getAccumulatorHolder(
      int maxValuesPerBatch, BufferAllocator allocator);

  protected abstract ElementType getElement(
      long baseAddress, int itemIndex, ArrowBuf dataBuffer, ArrowBuf offsetBuffer);

  protected BufferAllocator getAllocator() {
    return allocator;
  }

  protected long getOffHeapAddressForFixedWidthTypes(long dataBufferBaseAddress, long index) {
    return dataBufferBaseAddress + index * getFieldWidth();
  }

  @Override
  public void accumulate(long offsetAddr, int count, int bitsInChunk, int chunkOffsetMask) {
    final long maxAddr = offsetAddr + (long) count * PARTITIONINDEX_HTORDINAL_WIDTH;
    final FieldVector inputVector = getInput();
    final long incomingBit = inputVector.getValidityBufferAddress();
    final long incomingValue = inputVector.getDataBufferAddress();
    final ArrowBuf incomingDataBuf = inputVector.getDataBuffer();
    final ArrowBuf incomingOffsetBuf =
        inputVector instanceof BaseVariableWidthVector ? getInput().getOffsetBuffer() : null;

    for (long partitionAndOrdinalAddr = offsetAddr;
        partitionAndOrdinalAddr < maxAddr;
        partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
      /* get the index of data in input vector */
      final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
      /* get the corresponding data from input vector -- source data for accumulation */
      final int bitVal =
          (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7))
              & 1;
      /* incoming record is null, skip it */
      if (bitVal == 0) {
        continue;
      }

      final ElementType newVal =
          getElement(incomingValue, incomingIndex, incomingDataBuf, incomingOffsetBuf);

      /* get the hash table ordinal */
      final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
      /* get the hash table batch index */
      final int chunkIndex = tableIndex >>> bitsInChunk;
      final int chunkOffset = tableIndex & chunkOffsetMask;

      accumulatedBatches.get(chunkIndex).addItem(newVal, chunkOffset);
    }
  }

  @Override
  public void output(int startBatchIndex, int[] recordsInBatches) {
    transferVector.allocateNew();
    transferVector.reset();

    int numRecords = 0;
    for (int i = 0; i < recordsInBatches.length; i++) {
      prepareTransferVector((ListVector) transferVector, startBatchIndex + i, numRecords);
      numRecords += recordsInBatches[i];
      releaseBatch(startBatchIndex + i);
    }
  }

  private void prepareTransferVector(
      ListVector transferVector, int batchIndex, final int targetIndex) {
    transferVector.clear();
    UnionListWriter writer = transferVector.getWriter();
    BaseArrayAggAccumulatorHolder<ElementType, VectorType> batch =
        accumulatedBatches.get(batchIndex);
    Iterator<BaseArrayAggAccumulatorHolder<ElementType, VectorType>.ElementsGroup> groups =
        batch.getGroupsIterator();
    if (!groups.hasNext()) {
      writer.startList();
      writer.endList();
    }
    int groupCount = 0;
    while (groups.hasNext()) {
      BaseArrayAggAccumulatorHolder<ElementType, VectorType>.ElementsGroup group = groups.next();
      writer.startList();
      writer.setPosition(targetIndex + groupCount);
      int elementsInChunk = 0;
      while (group.hasNext()) {
        writeItem(writer, group.next());
        elementsInChunk++;
      }
      writer.endList();
      writer.setValueCount(elementsInChunk);
      groupCount++;
    }
    transferVector.setValueCount(groupCount);
  }

  @Override
  public FieldVector getOutput() {
    return transferVector;
  }

  @Override
  public int getValidityBufferSize() {
    return 0;
  }

  @Override
  public int getDataBufferSize() {
    return maxValuesPerBatch * getFieldWidth();
  }

  @Override
  public void revertResize() {
    if (!resizeInProgress) {
      /*
       * Because this is invoked for all accumulators under NestedAccumulator,
       * it will be a NO-OP for some accumulators if we failed in the middle
       * of NestedAccumulator.
       */
      return;
    }

    accumulatedBatches.removeLast();
    resizeInProgress = false;
  }

  @Override
  public void verifyBatchCount(int batches) {
    Preconditions.checkArgument(
        batches == this.accumulatedBatches.size(),
        "Error: Detected incorrect batch count in ArrayAgg Accumulator");
  }

  @Override
  public void releaseBatch(int batchIdx) {
    accumulatedBatches.get(batchIdx).close();
  }

  @Override
  public void resetToMinimumSize() throws Exception {
    accumulatedBatches.forEach(
        x -> {
          if (x != null) {
            x.close();
          }
        });
    accumulatedBatches = new LinkedList<>();
  }

  @Override
  public int getBatchCount() {
    return accumulatedBatches.size();
  }

  @Override
  public AccumulatorBuilder.AccumulatorType getType() {
    return AccumulatorBuilder.AccumulatorType.ARRAY_AGG;
  }

  @Override
  public List<ArrowBuf> getBuffers(int batchIndex, int numRecords) {
    tempAccumulatorHolder.reset();
    prepareTransferVector(tempAccumulatorHolder, batchIndex, 0);
    return tempAccumulatorHolder.getFieldBuffers();
  }

  @Override
  public void addBatch(ArrowBuf dataBuffer, ArrowBuf validityBuffer) {
    accumulatedBatches.add(getAccumulatorHolder(maxValuesPerBatch, allocator));
    resizeInProgress = true;
  }

  @Override
  public long getSizeInBytes() {
    long dataBufferSize =
        accumulatedBatches.stream()
            .map(BaseArrayAggAccumulatorHolder::getSizeInBytes)
            .reduce(0L, Long::sum);
    return dataBufferSize + (long) accumulatedBatches.size() * getValidityBufferSize();
  }

  @Override
  public void commitResize() {
    resizeInProgress = false;
  }

  @Override
  public void setInput(FieldVector inputVector) {
    this.input = inputVector;
  }

  @Override
  public UserBitShared.SerializedField getSerializedField(int batchIndex, int recordCount) {
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

  @Override
  public FieldVector getInput() {
    return input;
  }

  @Override
  public void compact(int batchIndex, int nextRecSize) {}

  @Override
  public void close() throws Exception {
    this.resetToMinimumSize();
  }
}

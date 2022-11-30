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
import static java.util.Arrays.asList;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedListVarcharVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.UserBitShared;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

/*
 * This is the aggregator for LISTAGG, similar to other aggregate accumulators.
 * This accumulator is used when there is single phase HashAgg is used.
 */
public class ListAggAccumulator implements Accumulator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ListAggAccumulator.class);

  private FieldVector input;
  /*
   * transferVector type is ListVector for ListAggAccumulator however BaseVariableWidthVector
   * for ListAggMergeAccumulator, which extends the ListAggAccumulator.
   */
  protected final FieldVector transferVector;
  protected FieldVector[] accumulators;
  private final int maxValuesPerBatch;
  private final int maxListAggSize;
  private final boolean distinct;
  private final String delimiter;
  private final boolean orderby;
  private final boolean asc;

  private boolean resizeAttempted;
  private int batches;
  private final BufferAllocator computationVectorAllocator;
  private final ListVector tempAccumulator;
  private final AccumStats accumStats;

  public ListAggAccumulator(FieldVector incomingValues, FieldVector transferVector, int maxValuesPerBatch, BufferAllocator computationVectorAllocator,
                            AccumulatorBuilder.ListAggParams listAggParams, BaseValueVector tempAccumulator) {
    this(incomingValues, transferVector, maxValuesPerBatch, computationVectorAllocator,
      listAggParams, tempAccumulator, null, null);
  }

  public ListAggAccumulator(FieldVector incomingValues, FieldVector transferVector, int maxValuesPerBatch, BufferAllocator computationVectorAllocator,
                            AccumulatorBuilder.ListAggParams listAggParams, BaseValueVector tempAccumulator, FieldVector firstAccumulator, AccumStats accumStats) {
    this.input = incomingValues;
    this.transferVector = transferVector;
    if (firstAccumulator != null) {
      initArrs(1);
      this.accumulators[0] = firstAccumulator;
      this.batches = 1;
      firstAccumulator.reset();
    } else {
      initArrs(0);
      this.batches = 0;
    }
    this.maxValuesPerBatch = maxValuesPerBatch;
    this.maxListAggSize = listAggParams.listAggSize;
    this.distinct = listAggParams.distinct;
    this.delimiter = listAggParams.delimiter;
    this.orderby = listAggParams.orderby;
    this.asc = listAggParams.asc;
    this.resizeAttempted = false;
    this.computationVectorAllocator = computationVectorAllocator;
    this.tempAccumulator = (ListVector)tempAccumulator;
    if (accumStats == null) {
      this.accumStats = new AccumStats();
    } else {
      this.accumStats = accumStats;
    }
  }

  private void initArrs(int size) {
    this.accumulators = new FieldVector[size];
  }

  /*
   * Used by HashAgg to determine total memory used by each partition, by summing
   * its hashtable and accumulators total memory usage.
   */
  @Override
  public long getSizeInBytes() {
    long size = 0;
    for (int i = 0; i < batches; i++) {
      FixedListVarcharVector flv = (FixedListVarcharVector) accumulators[i];
      size += flv.getSizeInBytes();
    }
    return size;
  }

  /* Used by HashAgg when to change the input vector, that read from disk */
  @Override
  public void setInput(FieldVector inputVector) {
    input = inputVector;
  }

  @Override
  public FieldVector getInput() {
    return input;
  }

  /* Mostly used to determine the type of the output vector. We do not transfer the actual vector during output */
  @Override
  public FieldVector getOutput() {
    return transferVector;
  }

  /* Used by AccumulatorSet to determine the space needed for validity bits */
  @Override
  public int getValidityBufferSize() {
    return FixedListVarcharVector.getValidityBufferSize(maxValuesPerBatch);
  }

  /* Used by AccumulatorSet to determine the space needed for data */
  @Override
  public int getDataBufferSize() {
    int dataBufferSize = FixedListVarcharVector.getDataBufferSize(maxValuesPerBatch);
    Preconditions.checkState(dataBufferSize + getValidityBufferSize() ==
      FixedListVarcharVector.FIXED_LISTVECTOR_SIZE_TOTAL);
    return dataBufferSize;
  }

  @Override
  public void addBatch(ArrowBuf dataBuffer, ArrowBuf validityBuffer) {
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
    FixedListVarcharVector vector = new FixedListVarcharVector(input.getField().getName(), computationVectorAllocator, maxValuesPerBatch,
      delimiter, maxListAggSize, distinct, orderby, asc,
      accumStats, tempAccumulator);

    accumulators[batches++] = vector;
    resizeAttempted = true;

    vector.loadBuffers(maxValuesPerBatch, dataBuffer, validityBuffer);

    checkNotNull();
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
    resizeAttempted = false;
  }

  @Override
  public void verifyBatchCount(int batches) {
    Preconditions.checkArgument(this.batches == batches,
      "Error: Detected incorrect batch count ({}: expected:{}, found:{}) in accumulator",
      this, batches, this.batches);
  }

  private void resetFirstAccumulatorVector() {
    final FieldVector vector = accumulators[0];
    Preconditions.checkArgument(vector != null, "Error: expecting a valid accumulator");
    vector.reset();
  }

  @Override
  public void releaseBatch(int batchIdx) {
    Preconditions.checkArgument(batchIdx < batches, "Error: incorrect batch index to release");
    if (batchIdx == 0) {
      // 0th batch memory is never released, only reset.
      resetFirstAccumulatorVector();
    } else {
      accumulators[batchIdx].close();
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

  @Override
  public int getBatchCount() {
    return batches;
  }

  @Override
  public AccumulatorBuilder.AccumulatorType getType() {
    return AccumulatorBuilder.AccumulatorType.LISTAGG;
  }

  @Override
  public List<ArrowBuf> getBuffers(int batchIndex, int numRecords) {
    FixedListVarcharVector flv = (FixedListVarcharVector) accumulators[batchIndex];
    return Arrays.asList(flv.getBuffers(numRecords));
  }

  @Override
  public UserBitShared.SerializedField getSerializedField(int batchIndex, int numRecords) {
    FixedListVarcharVector flv = (FixedListVarcharVector) accumulators[batchIndex];
    return flv.getSerializedField(numRecords);
  }

  @Override
  public void output(int startBatchIndex, int[] recordsInBatches) {
    outputToVarWidthVector(startBatchIndex, recordsInBatches);
  }

  protected void outputToVarWidthVector(int startBatchIndex, int[] recordsInBatches) {
    int numRecords = 0;
    int usedByteCapacity = 0;

    for (int i = 0; i < recordsInBatches.length; i++) {
      final FixedListVarcharVector flv = (FixedListVarcharVector) accumulators[startBatchIndex + i];
      flv.compact();
      usedByteCapacity += flv.getRequiredByteCapacity();
      numRecords += recordsInBatches[i];
    }

    /* trasferVector is always empty as after output, the buffers are transferred. */
    ((VariableWidthVector) transferVector).allocateNew(usedByteCapacity, numRecords);
    transferVector.reset();

    numRecords = 0;
    for (int i = 0; i < recordsInBatches.length; i++) {
      final FixedListVarcharVector flv = (FixedListVarcharVector) accumulators[startBatchIndex + i];
      flv.outputToVector((BaseVariableWidthVector)transferVector, numRecords, recordsInBatches[i]);
      numRecords += recordsInBatches[i];

      releaseBatch(startBatchIndex + i);
    }
  }

  protected void outputToListVector(int startBatchIndex, int[] recordsInBatches) {
    int numRecords = 0;
    int usedByteCapacity = 0;

    for (int i = 0; i < recordsInBatches.length; i++) {
      final FixedListVarcharVector flv = (FixedListVarcharVector) accumulators[startBatchIndex + i];
      flv.compact();
      usedByteCapacity += flv.getUsedByteCapacity();
      numRecords += recordsInBatches[i];
    }

    /* trasferVector is always empty as after output, the buffers are transferred. */
    ListVector listVector = FixedListVarcharVector.allocListVector(transferVector.getAllocator(), numRecords);
    Preconditions.checkState(((BaseVariableWidthVector)listVector.getDataVector()).getByteCapacity() >= usedByteCapacity);
    listVector.reset();

    numRecords = 0;
    for (int i = 0; i < recordsInBatches.length; i++) {
      final FixedListVarcharVector flv = (FixedListVarcharVector) accumulators[startBatchIndex + i];
      flv.outputToListVector(listVector, numRecords, recordsInBatches[i]);
      numRecords += recordsInBatches[i];

      releaseBatch(startBatchIndex + i);
    }

    /* Make sure that dataVector got populated in transferVector */
    ((ListVector) transferVector).addOrGetVector(FieldType.nullable(Types.MinorType.VARCHAR.getType()));

    TransferPair transferPair = listVector.makeTransferPair(transferVector);
    transferPair.transfer();
  }

  @Override
  public boolean hasSpace(final int space, final int numOfRecords, final int batchIndex, final int offsetInBatch) {
    final FixedListVarcharVector flv = (FixedListVarcharVector)accumulators[batchIndex];
    return flv.hasSpace(space, numOfRecords, offsetInBatch);
  }

  @Override
  public void moveValuesAndFreeSpace(int srcBatchIndex, int dstBatchIndex,
                                     int srcStartIndex, int dstStartIndex, int numRecords) {
    FixedListVarcharVector flv = (FixedListVarcharVector)accumulators[srcBatchIndex];
    flv.moveValuesAndFreeSpace(srcStartIndex, dstStartIndex, numRecords, accumulators[dstBatchIndex]);
  }

  @Override
  public void close() throws Exception {
    checkNotNull();
    final FieldVector[] accumulatorsToClose = new FieldVector[batches];
    for (int i = 0; i < batches; i++) {
      accumulatorsToClose[i] = accumulators[i];
    }
    AutoCloseables.close(ImmutableList.copyOf(accumulatorsToClose));
  }

  @Override
  public void accumulate(final long memoryAddr, final int count, final int bitsInChunk, final int chunkOffsetMask) {
    accumulateFromVarWidthVector(memoryAddr, count, bitsInChunk, chunkOffsetMask);
  }

  protected void accumulateFromVarWidthVector(final long memoryAddr, final int count, final int bitsInChunk, final int chunkOffsetMask) {
    final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
    final FieldVector inputVector = getInput();
    final long inputValidityBufAddr = inputVector.getValidityBufferAddress();
    final ArrowBuf inputOffsetBuf = inputVector.getOffsetBuffer();
    final ArrowBuf inputDataBuf = inputVector.getDataBuffer();

    for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
      final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);

      final int bitVal = (PlatformDependent.getByte(inputValidityBufAddr + (incomingIndex >>> 3)) >>> (incomingIndex & 7)) & 1;
      //incoming record is null, skip it
      if (bitVal == 0) {
        continue;
      }

      // get the hash table ordinal
      final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);

      //get the offset of incoming record
      final int startOffset = inputOffsetBuf.getInt(incomingIndex * BaseVariableWidthVector.OFFSET_WIDTH);
      final int endOffset = inputOffsetBuf.getInt((incomingIndex + 1) * BaseVariableWidthVector.OFFSET_WIDTH);

      // get the hash table batch index
      final int chunkIndex = tableIndex >>> bitsInChunk;
      final int chunkOffset = tableIndex & chunkOffsetMask;

      FixedListVarcharVector flv = (FixedListVarcharVector) this.accumulators[chunkIndex];
      flv.addValueToRowGroup(chunkOffset, startOffset, (endOffset - startOffset), inputDataBuf);
    }
  }

  protected void accumulateFromListVector(final long memoryAddr, final int count, final int bitsInChunk, final int chunkOffsetMask) {
    final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
    final ListVector listVector = (ListVector) getInput();
    final long inputValidityBufAddr = listVector.getValidityBufferAddress();

    for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
      final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);

      final int bitVal = (PlatformDependent.getByte(inputValidityBufAddr + (incomingIndex >>> 3)) >>> (incomingIndex & 7)) & 1;
      //incoming record is null, skip it
      if (bitVal == 0) {
        continue;
      }

      // get the hash table ordinal
      final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);

      // get the hash table batch index
      final int chunkIndex = tableIndex >>> bitsInChunk;
      final int chunkOffset = tableIndex & chunkOffsetMask;

      FixedListVarcharVector flv = (FixedListVarcharVector)this.accumulators[chunkIndex];
      flv.addListVectorToRowGroup(chunkOffset, listVector, incomingIndex);
    }
  }

  @Override
  public void compact(final int batchIndex, final int nextRecSize) {
    Preconditions.checkArgument(batchIndex >= 0, "Batch index must be non-negative");
    Preconditions.checkArgument(batchIndex < batches, String.format("Batch index must be less than %d", batches));

    final FixedListVarcharVector flv = (FixedListVarcharVector)accumulators[batchIndex];
    flv.compact(nextRecSize);
  }

  @Override
  public long getCompactionTime(TimeUnit unit) {
    return accumStats.getTotalCompactionTime(unit);
  }

  @Override
  public int getNumCompactions() {
    return accumStats.getNumCompactions();
  }

  @Override
  public int getMaxVarLenKeySize() {
    /* XXX: DX-50558 */
    return 0;
  }

  public int getMaxListAggSize() {
    return maxListAggSize;
  }

  public boolean getDistinct() {
    return distinct;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public boolean getOrderBy() {
    return orderby;
  }

  public boolean getAsc() {
    return asc;
  }

  public BaseValueVector getTempAccumulator() {
    return tempAccumulator;
  }

  public FieldVector getFirstAccumulator() {
    Preconditions.checkState(batches == 1);
    FieldVector firstAccum = accumulators[0];
    accumulators[0] = null;
    batches = 0;
    return firstAccum;
  }

  public AccumStats getAccumStats() {
    return accumStats;
  }
}

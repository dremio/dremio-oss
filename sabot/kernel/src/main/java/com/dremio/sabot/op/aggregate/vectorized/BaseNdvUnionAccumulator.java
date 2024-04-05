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

import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.SKETCH_ACCURACY;
import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.SKETCH_SIZE;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.WritableMemory;

/** A base accumulator for Union of HLL/NDV operator */
public abstract class BaseNdvUnionAccumulator implements Accumulator {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BaseNdvUnionAccumulator.class);

  private final int maxValuesPerBatch;
  private FieldVector input;
  private FieldVector transferVector;
  private int batches;
  protected HllAccumHolder[] accumBatches;
  private boolean resizeInProgress;
  private BaseVariableWidthVector tempAccumulatorHolder;
  private boolean isAccumulatorAtMinimum;

  public static class HllAccumHolder {
    private final int maxValuesPerBatch;
    private final ArrowBuf dataBuf;
    private final ByteBuffer accumAddress;

    public HllAccumHolder(final int maxValuesPerBatch, final ArrowBuf dataBuf) {
      this.maxValuesPerBatch = maxValuesPerBatch;

      this.dataBuf = dataBuf;
      Preconditions.checkArgument(dataBuf.capacity() >= (long) maxValuesPerBatch * SKETCH_SIZE);
      dataBuf.getReferenceManager().retain();

      accumAddress = MemoryUtil.directBuffer(dataBuf.memoryAddress(), (int) dataBuf.capacity());

      int sketchOffset = 0;
      for (int i = 0; i < maxValuesPerBatch; ++i, sketchOffset += SKETCH_SIZE) {
        accumAddress.limit(sketchOffset + SKETCH_SIZE);
        accumAddress.position(sketchOffset);
        ByteBuffer bb = accumAddress.slice();
        bb.order(ByteOrder.nativeOrder());

        /* Initialize backing memory for the sketch. HllSketch memset the first getMaxUpdatableSerializationBytes() bytes. */
        Union sketch = new Union(SKETCH_ACCURACY, WritableMemory.wrap(bb));
      }
    }

    private Union getAccumSketch(int accumIndex) {
      Preconditions.checkArgument(accumIndex < maxValuesPerBatch);

      final int sketchOffset = accumIndex * SKETCH_SIZE;
      accumAddress.limit(sketchOffset + SKETCH_SIZE);
      accumAddress.position(sketchOffset);
      ByteBuffer bb = accumAddress.slice();
      bb.order(ByteOrder.nativeOrder());
      return Union.writableWrap(WritableMemory.wrap(bb));
    }

    public void close() {
      dataBuf.getReferenceManager().release();
    }

    public void reset(final int startIndex, final int numRecords) {
      Preconditions.checkArgument(startIndex + numRecords <= maxValuesPerBatch);
      int sketchOffset = startIndex * SKETCH_SIZE;

      for (int i = 0; i < numRecords; ++i, sketchOffset += SKETCH_SIZE) {
        accumAddress.limit(sketchOffset + SKETCH_SIZE);
        accumAddress.position(sketchOffset);
        ByteBuffer bb = accumAddress.slice();
        bb.order(ByteOrder.nativeOrder());

        /* Reinitialize backing memory for the sketch. HllSketch memset the first getMaxUpdatableSerializationBytes() bytes. */
        Union sketch = new Union(SKETCH_ACCURACY, WritableMemory.wrap(bb));
      }
    }

    public void reset() {
      reset(0, maxValuesPerBatch);
    }
  }

  public void update(final int batchIndex, final int accumIndex, final HllSketch hllSketch) {
    Union unionSketch = accumBatches[batchIndex].getAccumSketch(accumIndex);
    unionSketch.update(hllSketch);
  }

  public BaseNdvUnionAccumulator(
      FieldVector input,
      FieldVector transferVector,
      int maxValuesPerBatch,
      BaseValueVector tempAccumulatorHolder,
      final ArrowBuf dataBuf) {
    this.input = input;
    this.transferVector = transferVector;
    this.tempAccumulatorHolder = (BaseVariableWidthVector) tempAccumulatorHolder;
    this.maxValuesPerBatch = maxValuesPerBatch;
    Preconditions.checkArgument(
        this.tempAccumulatorHolder.getByteCapacity() >= maxValuesPerBatch * SKETCH_SIZE);
    if (dataBuf == null) {
      initArrs(0);
      batches = 0;
    } else {
      initArrs(1);
      accumBatches[0] = new HllAccumHolder(maxValuesPerBatch, dataBuf);
      batches = 1;
    }
    resizeInProgress = false;
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

  @Override
  public void setInput(FieldVector input) {
    this.input = input;
  }

  private void initArrs(int size) {
    this.accumBatches = new HllAccumHolder[size];
  }

  /**
   * Get the target vector that stores the computed values for the accumulator.
   *
   * @return target vector
   */
  @Override
  public FieldVector getOutput() {
    return transferVector;
  }

  @Override
  public AccumulatorBuilder.AccumulatorType getType() {
    return AccumulatorBuilder.AccumulatorType.HLL_MERGE;
  }

  @Override
  public int getValidityBufferSize() {
    return 0;
  }

  @Override
  public int getDataBufferSize() {
    return maxValuesPerBatch * SKETCH_SIZE;
  }

  /**
   * HashTable and accumulator always run parallel -- when we add a block/batch to hashtable, we
   * also add new block/batch to accumulators. This function is used to verify state is consistent
   * across these data structures.
   *
   * @param batches number of blocks/batches in hashtable
   */
  @Override
  public void verifyBatchCount(final int batches) {
    Preconditions.checkArgument(
        batches == this.batches, "Error: Detected incorrect batch count in accumulator");
  }

  /**
   * Used to get the size of target accumulator vector that stores the computed values.
   *
   * <p>We use this method when computing the size of {@link VectorizedHashAggPartition} as part of
   * choosing a victim partition.
   *
   * @return size of vector (in bytes)
   */
  @Override
  public long getSizeInBytes() {
    if (this.isAccumulatorAtMinimum) {
      return 0;
    }
    return batches * ((long) SKETCH_SIZE * maxValuesPerBatch + getValidityBufferSize());
  }

  @Override
  public void addBatch(final ArrowBuf dataBuffer, final ArrowBuf validityBuffer) {
    try {
      if (batches == accumBatches.length) {
        /* save old references */
        final HllAccumHolder[] oldAccumBatches = accumBatches;
        /* provision more to avoid copy in the next call to addBatch */
        initArrs((batches == 0) ? 1 : batches * 2);
        System.arraycopy(oldAccumBatches, 0, this.accumBatches, 0, batches);
      }
      /* add a single batch */
      accumBatches[batches] = new HllAccumHolder(maxValuesPerBatch, dataBuffer);
      ++batches;
      resizeInProgress = true;
      this.isAccumulatorAtMinimum = false;
    } catch (Exception e) {
      /* this will be caught by LBlockHashTable and subsequently handled by VectorizedHashAggOperator */
      Throwables.throwIfUnchecked(e);
    }
  }

  @Override
  public int getBatchCount() {
    return batches;
  }

  private void prepareTransferVector(
      BaseVariableWidthVector transferVector,
      final int batchIndex,
      final int numRecords,
      final int targetIndex) {
    for (int i = 0; i < numRecords; ++i) {
      Union sketch = accumBatches[batchIndex].getAccumSketch(i);
      byte[] ba = sketch.toCompactByteArray();
      transferVector.set(targetIndex + i, ba);
    }
    transferVector.setValueCount(targetIndex + numRecords);
  }

  @Override
  public List<ArrowBuf> getBuffers(final int batchIndex, final int numRecordsInChunk) {
    tempAccumulatorHolder.reset();
    prepareTransferVector(tempAccumulatorHolder, batchIndex, numRecordsInChunk, 0);
    return tempAccumulatorHolder.getFieldBuffers();
  }

  @Override
  public void compact(final int batchIndex, final int nextRecSize) {}

  /**
   * Take the accumulator vector (the vector that stores computed values) for a particular batch
   * (identified by batchIndex) and output its contents. Output is done by copying the contents from
   * accumulator vector to its counterpart in outgoing container. Unlike fixed length accumulators,
   * we cannot transfer the contents as backing MutableVarcharVector does not have the data stored
   * in the index order. Even if requested to output multiple batches, the process is same.
   *
   * <p>We still want the memory associated with allocator of source vector because of post-spill
   * processing where this accumulator vector will still continue to store the computed values as we
   * start treating spilled batches as new input into the operator. However we do this for a singe
   * batch only as once we are done outputting a partition, we anyway get rid of all but 1 batch.
   */
  @Override
  public void output(int startBatchIndex, int[] recordsInBatches) {
    int size = 0;
    int numRecords = 0;
    for (int i = 0; i < recordsInBatches.length; ++i) {
      for (int k = 0; k < recordsInBatches[i]; ++k) {
        Union sketch = accumBatches[startBatchIndex + i].getAccumSketch(k);
        size += sketch.getCompactSerializationBytes();
      }
      numRecords += recordsInBatches[i];
    }

    ((BaseVariableWidthVector) transferVector).allocateNew(size, numRecords);
    transferVector.reset();

    numRecords = 0;
    for (int i = 0; i < recordsInBatches.length; ++i) {
      prepareTransferVector(
          (BaseVariableWidthVector) transferVector,
          startBatchIndex + i,
          recordsInBatches[i],
          numRecords);
      numRecords += recordsInBatches[i];
      releaseBatch(startBatchIndex + i);
    }
  }

  @Override
  public void resetToMinimumSize() throws Exception {
    assert accumBatches.length >= 1;

    accumBatches[0].reset();
    for (int i = 1; i < batches; ++i) {
      if (accumBatches[i] != null) {
        accumBatches[i].close();
        accumBatches[i] = null;
      }
    }
    batches = 1;
    this.isAccumulatorAtMinimum = true;
  }

  @Override
  public void commitResize() {
    resizeInProgress = false;
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

    accumBatches[batches - 1].close();
    accumBatches[batches - 1] = null;
    --batches;

    resizeInProgress = false;
  }

  @Override
  public void releaseBatch(int batchIndex) {
    if (batchIndex == 0) {
      accumBatches[batchIndex].reset();
    } else {
      accumBatches[batchIndex].close();
      accumBatches[batchIndex] = null;
    }
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
  public void close() throws Exception {
    for (int i = 0; i < batches; i++) {
      if (accumBatches[i] != null) {
        accumBatches[i].close();
        accumBatches[i] = null;
      }
    }
  }

  @Override
  public void moveValuesAndFreeSpace(
      int srcBatchIndex, int dstBatchIndex, int srcStartIndex, int dstStartIndex, int numRecords) {
    Preconditions.checkArgument(srcStartIndex + numRecords <= maxValuesPerBatch);
    /* setBytes take absolute byte address */
    accumBatches[dstBatchIndex].dataBuf.setBytes(
        dstStartIndex * SKETCH_SIZE,
        accumBatches[srcBatchIndex].dataBuf,
        srcStartIndex * SKETCH_SIZE,
        numRecords * SKETCH_SIZE);

    /* Reset the original sketches */
    accumBatches[srcBatchIndex].reset(srcStartIndex, numRecords);
  }
}

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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.dremio.exec.proto.UserBitShared;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;

/**
 * Interface for implementing a measure. Maintains an array of workspace and/or output vectors as
 * well as a refrence to the input vector.
 */
public interface Accumulator extends AutoCloseable {
  /**
   * Accumulate the data that is specified at the provided offset vector. The offset vector
   * describes which local mapping each of the <count> records should be addressed.
   *
   * @param offsetAddr starting address of buffer containing partition and hash table information
   *     along with record index
   * @param count number of records in the partition.
   *     <p>this function works on a per-partition basis.
   */
  void accumulate(long offsetAddr, int count, int bitsInChunk, int chunkOffsetMask);

  /**
   * Along with force accumulation, compact() allow to free up space in the accumulation vector, if
   * any.
   *
   * @param batchIndex Compaction to be applied on batchIndex batch.
   * @param nextRecSize Size of the next record, expected to join the accumulator. It is up to the
   *     accumulator to reserve the space or let the batch get spliced.
   */
  void compact(final int batchIndex, final int nextRecSize);

  /**
   * Output the data from multiple batches starting from startBatchIndex. for each batch, the record
   * count is provided in recordsInBatches array.
   *
   * @param startBatchIndex
   * @param recordsInBatches
   */
  void output(int startBatchIndex, int[] recordsInBatches);

  /**
   * return the size of accumulator by looking at vector in the accumulator and the size of
   * ArrowBufs inside the vectors.
   *
   * @return size(in bytes)
   */
  long getSizeInBytes();

  /**
   * Set the input vector that has source data to be accumulated.
   *
   * @param inputVector
   */
  void setInput(final FieldVector inputVector);

  /**
   * Get the input vector that has source data to be accumulated.
   *
   * @return input FieldVector.
   */
  FieldVector getInput();

  /**
   * Get the output vector that has the accumulated data.
   *
   * @return output FieldVector.
   */
  FieldVector getOutput();

  int getValidityBufferSize();

  int getDataBufferSize();

  void addBatch(final ArrowBuf dataBuffer, final ArrowBuf validityBuffer);

  void revertResize();

  void commitResize();

  void verifyBatchCount(int batches);

  void releaseBatch(final int batchIdx);

  void resetToMinimumSize() throws Exception;

  int getBatchCount();

  AccumulatorBuilder.AccumulatorType getType();

  /**
   * Check if the accumulator for the given batchIndex has given 'space' space available. This is
   * valid for variable length accumulators.
   *
   * @param space Total space to be reserved
   * @param numOfRecords Total number of records to be reserved
   * @param batchIndex Batch at which the reservation has to made
   * @param offsetInBatch Index in the batch for which the reservation is made
   * @return
   */
  default boolean hasSpace(
      final int space, final int numOfRecords, final int batchIndex, final int offsetInBatch) {
    return true;
  }

  /**
   * Get the backing buffers for the given batchIndex for numRecords which accumulated before. The
   * buffers will have the valueCount and readerIndex and writerIndex set.
   *
   * @param batchIndex
   * @param numRecords
   * @return
   */
  List<ArrowBuf> getBuffers(int batchIndex, int numRecords);

  /**
   * Generate a serializedField for given batchIndex with numRecords.
   *
   * @param batchIndex
   * @param numRecords
   * @return
   */
  UserBitShared.SerializedField getSerializedField(int batchIndex, final int numRecords);

  /**
   * Move numRecords entries starting from srcStartIndex from batch srcBatchIndex to dstStartIndex
   * in batch dstBatchIndex in the accumulator. This is called during splice() by HashTable.
   *
   * @param srcBatchIndex
   * @param dstBatchIndex
   * @param srcStartIndex
   * @param dstStartIndex
   * @param numRecords
   */
  default void moveValuesAndFreeSpace(
      final int srcBatchIndex,
      final int dstBatchIndex,
      final int srcStartIndex,
      final int dstStartIndex,
      int numRecords) {
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * Total time spend for compactions done in the accumulator. Only BaseVarBinaryAccumulator backed
   * MutableVarCharVector currently do compactions.
   *
   * @return Total time spend on compactions.
   */
  default long getCompactionTime(TimeUnit unit) {
    return 0;
  }

  /**
   * Number of compactions done in the accumulator. Only BaseVarBinaryAccumulator backed
   * MutableVarCharVector currently do compactions.
   *
   * @return number of compactions done in the accumulator
   */
  default int getNumCompactions() {
    return 0;
  }

  default int getMaxVarLenKeySize() {
    return 0;
  }

  class AccumStats {
    private int numCompactions = 0;
    private long totalCompactionTimeNS = 0;

    public void incNumCompactions() {
      numCompactions += 1;
    }

    public int getNumCompactions() {
      return numCompactions;
    }

    public void updateTotalCompactionTimeBy(long compactionTimeNS) {
      totalCompactionTimeNS += compactionTimeNS;
    }

    public long getTotalCompactionTime(TimeUnit unit) {
      return unit.convert(totalCompactionTimeNS, NANOSECONDS);
    }
  }
}

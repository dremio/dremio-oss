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

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;

/**
 * Interface for implementing a measure. Maintains an array of workspace and/or
 * output vectors as well as a refrence to the input vector.
 */
public interface Accumulator extends AutoCloseable {

  /**
   * Accumulate the data that is specified at the provided offset vector. The
   * offset vector describes which local mapping each of the <count> records
   * should be addressed.
   *
   * @param offsetAddr starting address of buffer containing partition and
   *                   hash table information along with record index
   * @param count number of records in the partition.
   *
   * this function works on a per-partition basis.
   */
  void accumulate(long offsetAddr, int count, int bitsInChunk, int chunkOffsetMask);

  /**
   * Output the data for the provided batch index to the output vectors.
   * @param batchIndex
   */
  void output(int batchIndex, int numRecords);

  /**
   * return the size of accumulator by looking at
   * interal vector in the accumulator and the size of
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

  void resetToMinimumSize() throws Exception;

  void revertResize();

  void commitResize();

  void verifyBatchCount(int batches);

  void releaseBatch(final int batchIdx);

  default boolean hasSpace(final int space, final int batchIndex) {
    return true;
  }

  default long getCompactionTime(TimeUnit unit) {
    return 0;
  }

  default int getNumCompactions() {
    return 0;
  }
}

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
package com.dremio.sabot.op.join.vhash;

import org.apache.arrow.memory.BufferAllocator;

import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;

/**
 * JoinBackPointer is used to maintain the ordinals of hash table for every record in a build batch.
 * The partition index will be included if the records in the build batch belong to multiple partitions.
 * We need the back pointer to get the keys of every record in the build batch during detangling
 * because the keys are not added to hyper container.
 * For VECTORIZED_BIGINT mode, we don't need back pointer because we keep the eight byte key in hyper container.
 */
public class JoinBackPointer implements AutoCloseable {
  /* The record size of back pointer is 5 when the records in build batch belong to multiple partitions.
   * In this case, the record of back pointer should have partition index to indicate which partition
   * the record in build batch belongs to.
   */
  private static final int BACKPOINTER_WITH_PARTITION_INDEX_RECORD_SIZE = 5;

  /* The record size of back pointer is 4 when all records in the batch batch belong to one partition.
   * In this case, the record of back pointer only need to maintain the ordinal in hash table for each records.
   */
  private static final int BACKPOINTER_WITHOUT_PARTITION_INDEX_RECORD_SIZE = 4;

  private ArrowBuf backPointerBuffer;

  // It's true if all the records in the build batch belong to one partition, otherwise it's false.
  private boolean singlePartition;
  /* The index of the partition which the records of back pointer belong to.
   * It's only valid when singlePartition is false.
   */
  private byte partitionIndex;

  /**
   * Creates a JoinBackPoint for a build batch
   * All the records in the build batch belong to one partition
   * @param allocator         Allocator used for the buffer allocation
   * @param partitionIndex    The index of the partition that all the records in the build batch belong to
   * @param recordCount       The record count in the build batch
   */
  public JoinBackPointer(BufferAllocator allocator, byte partitionIndex, int recordCount) {
    singlePartition = true;
    this.partitionIndex = partitionIndex;
    this.backPointerBuffer = allocator.buffer(recordCount * (BACKPOINTER_WITHOUT_PARTITION_INDEX_RECORD_SIZE));
  }

  /**
   * Creates a JoinBackPoint for a build batch
   * The records in the build batch belong to multiple partitions
   * @param allocator         Allocator used for the buffer allocation
   * @param recordCount       The record count in the build batch
   */
  public JoinBackPointer(BufferAllocator allocator, int recordCount) {
    singlePartition = false;
    this.backPointerBuffer = allocator.buffer(recordCount * (BACKPOINTER_WITH_PARTITION_INDEX_RECORD_SIZE));
  }

  public ArrowBuf getBackPointerBuffer() {
    return backPointerBuffer;
  }

  public boolean singlePartition() {
    return singlePartition;
  }

  public byte getPartitionIndex() {
    Preconditions.checkArgument(singlePartition, "partition index is invalid");
    return partitionIndex;
  }

  @Override
  public void close() throws Exception {
    backPointerBuffer.close();
  }
}

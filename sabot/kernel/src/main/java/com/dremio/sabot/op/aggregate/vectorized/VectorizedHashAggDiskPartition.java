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

import org.apache.hadoop.fs.FSDataOutputStream;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.dremio.sabot.op.sort.external.SpillManager.SpillFile;
import com.google.common.base.Preconditions;

/**
 * Disk based version of {@link VectorizedHashAggPartition}.
 * It does not hold any data structure but just
 * enough information to read the partition data from disk.
 *
 * There are two distinct concepts of partitions:
 *
 * CONCEPT 1:
 *
 * Partitions managed by the operator. In other words, these are
 * the partitions used to partition and distribute the incoming
 * data into multiple disjoint hash tables (and accumulators).
 * Over a period of time, one or more of these partitions may
 * get spilled.
 *
 * So they maintain an optional reference to a corresponding
 * {@link VectorizedHashAggDiskPartition} that stores the spill
 * related information for the partitions like number of batches,
 * spill file etc.
 *
 * CONCEPT 2:
 *
 * {@link VectorizedHashAggPartitionSpillHandler} maintains the set
 * of spilled partitions. These partitions maintain a temporary
 * backpointer to corresponding memory version {@link VectorizedHashAggPartition}
 * since once a partition has been spilled, subsequent incoming data
 * belonging to that partition will continue to be in-memory aggregated
 * or buffered (non-contraction).
 *
 * Once we have consumed the entire input, the operator flushes
 * all the spilled partitions that have non-empty portions in-memory.
 * We then go over all spilled partitions and get the backpointer
 * for the in-memory portion and spill it (if non-empty). After this
 * we transition the state of two sets of partitions:
 *
 * The partitions in CONCEPT 1 become "MEMORY ONLY" partitions. In other
 * words, they no longer hold references to their disk-based counterparts.
 * The partitions in CONCEPT 2 become "DISK ONLY" partitions. In other
 * words, they no longer hold references to their in-memory counterparts.
 *
 * Once this is done, we restart the aggregation algorithm in
 * {@link VectorizedHashAggOperator} by working on one {@link VectorizedHashAggDiskPartition}
 * at a time, reading spilled batches and feeding into the operator as new
 * incoming which then gets repartitioned into CONCEPT 1 partitions.
 */
public class VectorizedHashAggDiskPartition implements AutoCloseable {

  private long numberOfBatches;
  private final SpillFile spillFile;
  private final String identifier;
  private VectorizedHashAggPartition inmemoryPartitionBackPointer;
  private FSDataOutputStream outputStream;

  public void addNewSpilledBatches(final long newBatches) {
    this.numberOfBatches += newBatches;
  }

  public long getNumberOfBatches() {
    return numberOfBatches;
  }

  VectorizedHashAggDiskPartition(final long numberOfBatches, final SpillManager.SpillFile partitionSpillFile,
                                 final VectorizedHashAggPartition inmemoryPartitionBackPointer,
                                 final FSDataOutputStream outputStream) {
    Preconditions.checkArgument(partitionSpillFile != null && numberOfBatches > 0, "Error: must provide valid spill info for creating a disk partition.");
    Preconditions.checkArgument(outputStream != null, "Error: need a valid output stream for writing to spill file");
    this.numberOfBatches = numberOfBatches;
    this.spillFile = partitionSpillFile;
    this.identifier = inmemoryPartitionBackPointer.getIdentifier();
    this.inmemoryPartitionBackPointer = inmemoryPartitionBackPointer;
    this.outputStream = outputStream;
  }

  public SpillManager.SpillFile getSpillFile() {
    return spillFile;
  }

  public void transitionToDiskOnlyPartition() {
    Preconditions.checkArgument(inmemoryPartitionBackPointer != null, "Error: detected invalid state of disk partition");
    Preconditions.checkArgument(outputStream == null, "Error: spill stream should have been closed");
    inmemoryPartitionBackPointer = null;
  }

  public FSDataOutputStream getSpillStream() {
    return outputStream;
  }

  /**
   * Once {@link VectorizedHashAggOperator} has consumed all data, it flushes
   * the in-memory data (if any) for all spilled partitions and that's when we
   * finally close the output stream for partition's spill file since that is the
   * last moment we will spill anything for that partition as later on the partition
   * will transition to read-only (pure disk based) mode.
   * @throws Exception
   */
  public void closeSpillStream() throws Exception {
    outputStream.close();
    outputStream = null;
  }

  public VectorizedHashAggPartition getInmemoryPartitionBackPointer() {
    return inmemoryPartitionBackPointer;
  }

  public long getSize() {
    return (inmemoryPartitionBackPointer == null) ? 0 : inmemoryPartitionBackPointer.getSize();
  }

  public String getIdentifier() {
    return identifier;
  }

  @Override
  public void close() throws Exception {
    if (outputStream != null) {
      closeSpillStream();
    }
    AutoCloseables.close(spillFile);
  }
}

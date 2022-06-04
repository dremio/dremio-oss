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
package com.dremio.sabot.op.join.vhash.spill.partition;

import static com.dremio.sabot.op.common.ht2.LBlockHashTable.ORDINAL_SIZE;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.op.copier.CopierFactory;
import com.dremio.sabot.op.join.vhash.spill.JoinSetupParams;
import com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.join.vhash.spill.slicer.PageBatchSlicer;
import com.google.common.base.Stopwatch;

/**
 * Implementation of partition where the build table is entirely in memory.
 */
class MemoryPartition implements Partition {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryPartition.class);

  private final JoinSetupParams setupParams;
  private final CopierFactory copierFactory;
  private final BufferAllocator allocator;
  private final PagePool pool;
  private final long sv2Addr;
  private final long tableHashAddr4B;
  private final JoinTable table;
  private final Stopwatch slicerCopyWatch = Stopwatch.createUnstarted();
  private final Stopwatch linkWatch = Stopwatch.createUnstarted();
  private final ExpandableHyperContainer hyperContainer;
  private final List<RecordBatchData> slicedBatches = new ArrayList<>();
  private final PageListMultimap linkedList;
  private final PageBatchSlicer slicer;
  private final ArrowBuf hashTableOrdinals4B;
  private VectorizedProbe probe = null;
  private int buildBatchIndex = 0;

  MemoryPartition(JoinSetupParams setupParams, CopierFactory copierFactory, int partitionIdx, long sv2Addr, long tableHashAddr4B)  {
    this.setupParams = setupParams;
    this.copierFactory = copierFactory;
    this.allocator = setupParams.getBuildAllocator().newChildAllocator("partition-" + partitionIdx, 0, Long.MAX_VALUE);
    this.sv2Addr = sv2Addr;
    this.tableHashAddr4B = tableHashAddr4B;
    this.pool = new PagePool(allocator);

    /*
     * build side structures
     */

    // hash table
    this.table = new BlockJoinTable(setupParams.getBuildKeyPivot(), allocator, setupParams.getComparator(),
      (int)setupParams.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE), INITIAL_VAR_FIELD_AVERAGE_SIZE,
      setupParams.getSabotConfig(), setupParams.getOptions());
    // linked list to link duplicate records (not collisions)
    this.linkedList = new PageListMultimap(pool);
    // slicer to slice and copy incoming build batch into fixed size pages.
    this.slicer = new PageBatchSlicer(pool, sv2Addr, setupParams.getRight(), setupParams.getCarryAlongFieldsBitset());
    // container for all the sliced record batches.
    this.hyperContainer = new ExpandableHyperContainer(allocator, setupParams.getCarryAlongSchema());
    // temp buffer to hold hash table ordinals, post-insertion into the table
    this.hashTableOrdinals4B = allocator.buffer(setupParams.getMaxInputBatchSize() * ORDINAL_SIZE);
  }

  @Override
  public void hashPivoted(int records, long keyFixedVectorAddr, long keyVarVectorAddr, long seed, long hashoutAddr8B) {
    table.hashPivoted(records, keyFixedVectorAddr, keyVarVectorAddr, seed, hashoutAddr8B);
  }

  @Override
  public void buildPivoted(int records) throws Exception {
    // Add entries into the hash-table and get the hash table ordinals.
    table.insertPivoted(sv2Addr, records,
      tableHashAddr4B, setupParams.getPivotedFixedBlock(), setupParams.getPivotedVariableBlock(),
      hashTableOrdinals4B.memoryAddress() /*output*/);

    /* Slice and copy the carry-over columns into page-sized batches. The resulting set of batches may be more than
     * one batch.
     *
     * Having page-sized batches reduces fragmentation and simplifies the memory computations for reserve when spilling.
     */
    slicerCopyWatch.start();
    List<RecordBatchData> batchPages = slicer.addBatch(records);
    slicerCopyWatch.stop();
    if (batchPages == null) {
      throw new OutOfMemoryException("unable to add batch");
    }
    slicedBatches.addAll(batchPages);

    // For each batch, add to the hyper container and update the linked list.
    int numRecordsDone = 0;
    for (RecordBatchData batch : batchPages) {
      /* Transfer the batch* to the hyper vector container. Will be used when we want to retrieve
       * records that have matching keys on the probe side.
       */
      hyperContainer.addBatch(VectorContainer.getTransferClone(batch.getContainer(), allocator));

      // Update the links in the linked-list for the newly added batch.
      linkWatch.start();
      linkedList.insertCollection(hashTableOrdinals4B.memoryAddress() + numRecordsDone * ORDINAL_SIZE,
       table.size() - 1, buildBatchIndex, batch.getRecordCount());
      linkWatch.stop();

      // completed processing a batch, increment batch index
      numRecordsDone += batch.getRecordCount();
      buildBatchIndex++;
      if (buildBatchIndex < 0) {
        throw UserException.unsupportedError()
          .message("HashJoin doesn't support more than %d (Integer.MAX_VALUE) number of batches on build side in a " +
              "single partition",
            Integer.MAX_VALUE)
          .build(logger);
      }
    }
  }

  @Override
  public boolean isBuildSideEmpty() {
    return table.size() == 0;
  }

  private void checkAndCreateProbe() {
    if (probe == null) {
      probe = new VectorizedProbe(setupParams, copierFactory, sv2Addr, tableHashAddr4B, table, linkedList, hyperContainer);
    }
  }

  @Override
  public int probePivoted(int records, int startOutputIndex, int maxOutputIndex) throws Exception {
    checkAndCreateProbe();
    return probe.probeBatch(records, startOutputIndex, maxOutputIndex);
  }

  @Override
  public int projectBuildNonMatches(int startOutputIndex, int maxOutputIndex) throws Exception {
    checkAndCreateProbe();
    return probe.projectBuildNonMatches(startOutputIndex, maxOutputIndex);
  }

  @Override
  public Stats getStats() {
    return new Stats() {
      final TimeUnit ns = TimeUnit.NANOSECONDS;

      @Override
      public long getBuildNumEntries() {
        return table.size();
      }

      @Override
      public long getBuildNumBuckets() {
        return table.capacity();
      }

      @Override
      public long getBuildNumResizing() {
        return table.getRehashCount();
      }

      @Override
      public long getBuildResizingTimeNanos() {
        return table.getRehashTime(ns);
      }

      @Override
      public long getBuildPivotTimeNanos() {
        // pivoting is not done in this partition implementation
        return 0;
      }

      @Override
      public long getBuildHashComputationTimeNanos() {
        // hash computation is not done in this partition implementation
        return 0;
      }

      @Override
      public long getBuildInsertTimeNanos() {
        return table.getInsertTime(ns) - table.getRehashTime(ns);
      }

      @Override
      public long getBuildLinkTimeNanos() {
        return linkWatch.elapsed(ns);
      }

      @Override
      public long getBuildKeyCopyNanos() {
        return probe == null ? 0 : probe.getBuildCopyTime();
      }

      @Override
      public long getBuildCarryOverCopyNanos() {
        return slicerCopyWatch.elapsed(ns);
      }

      @Override
      public long getBuildUnmatchedKeyCount() {
        return probe == null ? 0 : probe.getUnmatchedBuildKeyCount();
      }

      @Override
      public long getBuildCopyNonMatchNanos() {
        return probe == null ? 0 : probe.getBuildNonMatchCopyTime();
      }

      @Override
      public long getProbePivotTimeNanos() {
        // pivoting is not done in this partition implementation
        return 0;
      }

      @Override
      public long getProbeHashComputationTime() {
        // hash computation is not done in this partition implementation
        return 0;
      }

      @Override
      public long getProbeFindTimeNanos() {
        return table.getProbeFindTime(ns);
      }

      @Override
      public long getProbeListTimeNanos() {
        return probe == null ? 0 : probe.getProbeListTime() - table.getProbeFindTime(ns);
      }

      @Override
      public long getProbeCopyNanos() {
        return probe == null ? 0 : probe.getProbeCopyTime();
      }

      @Override
      public long getProbeUnmatchedKeyCount() {
        return probe == null ? 0 : probe.getUnmatchedProbeCount();
      }
    };
  }

  @Override
  public void close() throws Exception {
    List<AutoCloseable> autoCloseables = new ArrayList<>();
    autoCloseables.add(hashTableOrdinals4B);
    autoCloseables.add(hyperContainer);
    autoCloseables.add(linkedList);
    autoCloseables.add(table);
    autoCloseables.addAll(slicedBatches);
    autoCloseables.add(probe);
    autoCloseables.add(pool);
    autoCloseables.add(allocator);
    AutoCloseables.close(autoCloseables);
  }
}

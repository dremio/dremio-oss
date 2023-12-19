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

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.exec.heap.HeapLowMemController;
import com.dremio.sabot.exec.heap.HeapLowMemParticipant;
import com.dremio.sabot.op.copier.CopierFactory;
import com.dremio.sabot.op.join.hash.HashJoinOperator;
import com.dremio.sabot.op.join.vhash.NonPartitionColFilters;
import com.dremio.sabot.op.join.vhash.PartitionColFilters;
import com.dremio.sabot.op.join.vhash.spill.JoinSetupParams;
import com.dremio.sabot.op.join.vhash.spill.MemoryReleaser;
import com.dremio.sabot.op.join.vhash.spill.OOBInfo;
import com.dremio.sabot.op.join.vhash.spill.io.SpillFileDescriptor;
import com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.join.vhash.spill.slicer.PageBatchSlicer;
import com.dremio.sabot.op.join.vhash.spill.slicer.RecordBatchPage;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

/**
 * Implementation of partition where the build table is entirely in memory.
 */
final class MemoryPartition implements Partition, CanSwitchToSpilling {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryPartition.class);

  private final JoinSetupParams setupParams;
  private final CopierFactory copierFactory;
  private final int partitionIdx;
  private final BufferAllocator allocator;
  private final PagePool pool;
  private final String partitionID;
  private final ArrowBuf sv2;
  private final ArrowBuf tableHash4B;

  private final JoinTable table;
  private final Stopwatch slicerCopyWatch = Stopwatch.createUnstarted();
  private final Stopwatch linkWatch = Stopwatch.createUnstarted();
  private final ExpandableHyperContainer hyperContainer;
  private final List<RecordBatchPage> slicedBatchPages = new ArrayList<>();
  private final PageListMultimap linkedList;
  private final PageBatchSlicer slicer;
  private final ArrowBuf hashTableOrdinals4B;
  private VectorizedProbe probe = null;
  private int buildBatchIndex = 0;
  private boolean switchedToSpilling = false;
  private HeapLowMemParticipant overheadParticipant = null;
  private HeapLowMemController memoryController = null;
  private final String participantId;
  private final int fieldCount;

  MemoryPartition(JoinSetupParams setupParams, CopierFactory copierFactory, int partitionIdx, ArrowBuf sv2, ArrowBuf tableHash4B) {
    this.setupParams = setupParams;
    this.copierFactory = copierFactory;
    this.partitionIdx = partitionIdx;
    this.sv2 = sv2;
    this.tableHash4B = tableHash4B;
    this.partitionID = String.format("p_gen_%08d_idx_%08d", setupParams.getGeneration(), partitionIdx);
    this.fieldCount = setupParams.getCarryAlongSchema().getTotalFieldCount();
    final ExecProtos.FragmentHandle fragmentHandle = setupParams.getContext().getFragmentHandle();
    participantId = String.format("joinspill-%s.%s.%s.%s.%s",
      QueryIdHelper.getQueryId(fragmentHandle.getQueryId()), fragmentHandle.getMajorFragmentId(),
      fragmentHandle.getMinorFragmentId(), this.setupParams.getOperatorId(), this.partitionID);
    this.memoryController = this.setupParams.getContext().getHeapLowMemController();
    if (memoryController != null) {
      this.overheadParticipant = this.memoryController.addParticipant(participantId, fieldCount);
    }
  /*
   * build side structures
   */
    try (AutoCloseables.RollbackCloseable rc = new AutoCloseables.RollbackCloseable(true)) {
      this.allocator = rc.add(setupParams.getOpAllocator().newChildAllocator(partitionID, 0, Long.MAX_VALUE));
      this.pool = rc.add(new PagePool(allocator, (int)setupParams.getOptions().getOption(HashJoinOperator.PAGE_SIZE)));
      // linked list to link duplicate records (not collisions)
      this.linkedList = rc.add(new PageListMultimap(pool));
      // slicer to slice and copy incoming build batch into fixed size pages.
      this.slicer = new PageBatchSlicer(pool, sv2, setupParams.getRight(), setupParams.getBuildNonKeyFieldsBitset());
      // container for all the sliced record batches.
      this.hyperContainer = rc.add(new ExpandableHyperContainer(allocator, setupParams.getCarryAlongSchema()));
      /*
       * Minimum hashtable size is set to 8K. This reduce the burden of unnecessarly allocating 1MB worth of control blocks
       * (for each partition) to 128K. This help batch workloads where total number of entries in the hashtable is very less.
       */
      this.table = rc.add(new BlockJoinTable(setupParams.getBuildKeyPivot(), allocator, setupParams.getComparator(), 8192,
        INITIAL_VAR_FIELD_AVERAGE_SIZE, setupParams.getSabotConfig(), setupParams.getOptions(), setupParams.isRuntimeFilterEnabled()));

      // temp buffer to hold hash table ordinals, post-insertion into the table
      this.hashTableOrdinals4B = rc.add(allocator.buffer(setupParams.getMaxInputBatchSize() * ORDINAL_SIZE));
      rc.commit();
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public int buildPivoted(int pivotShift, int records) throws Exception {
    Preconditions.checkState(!switchedToSpilling);
    if (overheadParticipant != null && overheadParticipant.isVictim()) {
      setupParams.getSpillStats().incrementHeapSpillCount();
      throw new HeapLowMemoryReachedException();
    }
    // Add entries into the hash-table and get the hash table ordinals.
    int recordsInserted = table.insertPivoted(sv2, pivotShift, records,
      tableHash4B, setupParams.getPivotedFixedBlock(), setupParams.getPivotedVariableBlock(),
      hashTableOrdinals4B /*output*/);
    Preconditions.checkState(recordsInserted <= records);
    if (recordsInserted == 0) {
      return 0;
    }

    /* Slice and copy the carry-over columns into page-sized batches. The resulting set of batches may be more than
     * one batch.
     *
     * Having page-sized batches reduces fragmentation and simplifies the memory computations for reserve when spilling.
     */
    slicerCopyWatch.start();
    List<RecordBatchPage> batchPages = new ArrayList<>();
    int recordsSliced = slicer.addBatch(recordsInserted, batchPages);
    slicerCopyWatch.stop();
    if (recordsSliced == 0) {
      return 0;
    }
    slicedBatchPages.addAll(batchPages);

    // For each batch, add to the hyper container and update the linked list.
    int numRecordsDone = 0;
    for (RecordBatchPage batch : batchPages) {
      // Update the links in the linked-list for the newly added batch.
      linkWatch.start();
      try {
        ArrowBuf hashOrdinals = hashTableOrdinals4B.slice(numRecordsDone * ORDINAL_SIZE, batch.getRecordCount() * ORDINAL_SIZE);
        linkedList.insertCollection(hashOrdinals, table.getMaxOrdinal(), buildBatchIndex, batch.getRecordCount());
      } finally {
        linkWatch.stop();
      }

      /* Transfer the batch to the hyper vector container. Will be used when we want to retrieve
       * records that have matching keys on the probe side.
       */
      hyperContainer.addBatch(VectorContainer.getTransferClone(batch.getContainer(), allocator));

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
    if (overheadParticipant != null) {
      overheadParticipant.addBatches(batchPages.size());
    }
    Preconditions.checkState(numRecordsDone == recordsSliced);
    logger.trace("partition {} processed {} build records", partitionID, numRecordsDone);
    return numRecordsDone;
  }

  @Override
  public boolean isBuildSideEmpty() {
    return table.size() == 0;
  }

  @Override
  public int hashTableSize() {
    return table.size();
  }

  private void checkAndCreateProbe() {
    if (probe == null) {
      probe = new VectorizedProbe(setupParams, copierFactory, sv2, tableHash4B, table, linkedList, hyperContainer);
    }
  }

  @Override
  public void probeBatchBegin(int pivotShift, int numRecords) {
    Preconditions.checkState(!switchedToSpilling);
    checkAndCreateProbe();
    probe.batchBegin(pivotShift, numRecords);
  }

  @Override
  public int probePivoted(int startOutputIndex, int maxOutputIndex) throws Exception {
    Preconditions.checkState(!switchedToSpilling);
    int ret = probe.probeBatch(startOutputIndex, maxOutputIndex);
    logger.trace("partition {} probe records output {}", partitionID, ret);
    return ret;
  }

  @Override
  public int projectBuildNonMatches(int startOutputIndex, int maxOutputIndex) throws Exception {
    Preconditions.checkState(!switchedToSpilling);
    checkAndCreateProbe();
    int ret = probe.projectBuildNonMatches(startOutputIndex, maxOutputIndex);
    logger.trace("partition {} projectBuildNonMatches output {}", partitionID, ret);
    return ret;
  }

  @Override
  public void prepareBloomFilters(PartitionColFilters partitionColFilters) {
    table.prepareBloomFilters(partitionColFilters);
  }

  @Override
  public void prepareValueListFilters(NonPartitionColFilters nonPartitionColFilters) {
    table.prepareValueListFilters(nonPartitionColFilters);
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

      @Override
      public long getEvaluationCount() {
        return probe == null ? 0 : probe.getEvaluationCount();
      }

      @Override
      public long getEvaluationMatchedCount() {
        return probe == null ? 0 : probe.getEvaluationMatchedCount();
      }

      @Override
      public long getSetupNanos() {
        return probe == null ? 0 : probe.getSetupNanos();
      }
    };
  }

  @Override
  public void close() throws Exception {
    if (memoryController != null) {
      memoryController.removeParticipant(participantId, fieldCount);
    }
    List<AutoCloseable> autoCloseables = new ArrayList<>();
    autoCloseables.add(hashTableOrdinals4B);
    autoCloseables.add(probe);
    autoCloseables.add(hyperContainer);
    if (!switchedToSpilling) {
      // these will be freed up by the MemoryReleaser
      autoCloseables.add(linkedList);
      autoCloseables.add(table);
      autoCloseables.addAll(slicedBatchPages);
      autoCloseables.add(pool);
      autoCloseables.add(allocator);
    }
    AutoCloseables.close(autoCloseables);
  }

  @Override
  public long estimateSpillableBytes() {
    return allocator.getAllocatedMemory();
  }

  @Override
  public SwitchResult switchToSpilling(boolean spillAll) {
    Preconditions.checkState(!switchedToSpilling);
    switchedToSpilling = true;
    setupParams.getSpillStats().incrementSpillCount();

    // create a releaser to free up the in-memory hash-table & the hyper-container.
    SpillFileDescriptor spillFile = new SpillFileDescriptor(setupParams.getSpillManager().getSpillFile(partitionID + "_preSpillBuild.arrow"));
    MemoryReleaser releaser = new BuildMemoryReleaser(setupParams, spillFile, linkedList,
      table, hyperContainer, slicedBatchPages, allocator, ImmutableList.of(pool));
    setupParams.getMultiMemoryReleaser().addReleaser(releaser);

    // Create a disk-partition to use in-place of this partition.
    Partition newPartition = new DiskPartition(setupParams, partitionIdx, sv2, ImmutableList.of(spillFile),
      new RecordedStats(getStats()), table.size());

    // clean-up this partition
    AutoCloseables.closeNoChecked(this);

    notifyOthersOfSpill();

    return new SwitchResult(true, newPartition);
  }

  /**
   * Notify other fragments of this spill by sending an Out-of-Band message
   */
  public void notifyOthersOfSpill() {
    OOBInfo oobInfo = setupParams.getOobInfo();
    if (!oobInfo.isOobSpillEnabled()) {
      return;
    }

    try {
      final OutOfBandMessage.Payload payload = new OutOfBandMessage.Payload(
        ExecProtos.HashJoinSpill.newBuilder().setMemoryUse(allocator.getAllocatedMemory()).setMessageType(oobInfo.getMessage_type()).build());
      for (CoordExecRPC.FragmentAssignment a : oobInfo.getAssignments()) {
        final OutOfBandMessage message = new OutOfBandMessage(
          oobInfo.getQueryId(),
          oobInfo.getMajorFragmentId(),
          a.getMinorFragmentIdList(),
          oobInfo.getOperatorId(),
          oobInfo.getMinorFragmentId(),
          payload, true);

        final CoordinationProtos.NodeEndpoint endpoint = oobInfo.getEndpointsIndex().getNodeEndpoint(a.getAssignmentIndex());
        oobInfo.getTunnelProvider().getExecTunnel(endpoint).sendOOBMessage(message);
      }
      setupParams.getSpillStats().incrementOOBSends();
    } catch(final Exception ex) {
      logger.warn("Failure while attempting to notify others of spilling.", ex);
    }
  }
}

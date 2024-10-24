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

import static com.dremio.sabot.op.join.vhash.spill.JoinSetupParams.TABLE_HASH_SIZE;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.VM;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.op.copier.CopierFactory;
import com.dremio.sabot.op.join.hash.HashJoinOperator;
import com.dremio.sabot.op.join.vhash.NonPartitionColFilters;
import com.dremio.sabot.op.join.vhash.PartitionColFilters;
import com.dremio.sabot.op.join.vhash.spill.JoinSetupParams;
import com.dremio.sabot.op.join.vhash.spill.MultiMemoryReleaser;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import io.netty.util.internal.PlatformDependent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;

/** Partition impl that acts as a bridge to multiple child partitions. */
public final class MultiPartition implements Partition, CanSwitchToSpilling {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MultiPartition.class);
  private static final int FULL_HASH_SIZE = 8;

  private final int numPartitions;
  private final int partitionMask;

  private final JoinSetupParams setupParams;
  private final CopierFactory copierFactory;
  private final Hasher hasher;
  private ArrowBuf fullHashValues8B;
  private ArrowBuf tableHashValues4B;
  private final PartitionWrapper[] childWrappers;

  private int probePivotShift;
  private int probePartitionCursor;
  private int probeNonMatchesPartitionCursor;

  // stats related
  private final Stopwatch buildHashComputationWatch = Stopwatch.createUnstarted();
  private final Stopwatch probeHashComputationWatch = Stopwatch.createUnstarted();
  private final Stopwatch spillWatch = Stopwatch.createUnstarted();
  Partition.Stats statsFromPrevGen;

  private static final boolean DEBUG = VM.areAssertsEnabled();

  public MultiPartition(JoinSetupParams setupParams) {
    this.setupParams = setupParams;
    this.copierFactory =
        CopierFactory.getInstance(setupParams.getSabotConfig(), setupParams.getOptions());

    numPartitions = (int) setupParams.getOptions().getOption(HashJoinOperator.NUM_PARTITIONS);
    partitionMask = numPartitions - 1;
    childWrappers = new PartitionWrapper[numPartitions];
    probeNonMatchesPartitionCursor = 0;

    try (RollbackCloseable rc = new RollbackCloseable(true)) {
      BufferAllocator allocator = setupParams.getOpAllocator();
      final int maxBatchSize = setupParams.getMaxInputBatchSize();
      fullHashValues8B = rc.add(allocator.buffer(maxBatchSize * FULL_HASH_SIZE));
      tableHashValues4B = rc.add(allocator.buffer(maxBatchSize * TABLE_HASH_SIZE));
      hasher = rc.add(new Hasher(setupParams));

      for (int idx = 0; idx < numPartitions; ++idx) {
        childWrappers[idx] = rc.add(new PartitionWrapper(idx, maxBatchSize));
      }

      rc.commit();
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void resetAllPartitionInputs() {
    for (int i = 0; i < numPartitions; ++i) {
      childWrappers[i].resetInput();
    }
  }

  @Override
  public int buildPivoted(int startIdx, int records) throws Exception {
    // Do hash computation on entire batch, and split into per partition inputs.
    buildHashComputationWatch.start();
    computeHashAndSplitToChildPartitions(startIdx, records);
    buildHashComputationWatch.stop();

    // pass along to the child partitions.
    for (int partitionIdx = 0; partitionIdx < numPartitions; ++partitionIdx) {
      PartitionWrapper child = childWrappers[partitionIdx];
      if (child.getNumRecords() > 0) {
        buildPivotedForChild(startIdx, child);
      }
    }
    return records;
  }

  // build the partition for the given child.
  private void buildPivotedForChild(int startIdx, PartitionWrapper child) throws Exception {
    int recordsDone = 0;
    int total = child.getNumRecords();
    while (recordsDone < total) {
      int recordsRequestedInIteration = total - recordsDone;
      int recordsInsertedInIteration = 0;
      try {
        recordsInsertedInIteration =
            child.getPartition().buildPivoted(startIdx, recordsRequestedInIteration);
      } catch (OutOfMemoryException ignore) {
      } catch (HeapLowMemoryReachedException e) {
        logger.debug(
            "major fragment id {} minor fragment id {} switching partition total usage {} limit {}",
            setupParams.getContext().getFragmentHandle().getMajorFragmentId(),
            setupParams.getContext().getFragmentHandle().getMinorFragmentId(),
            setupParams.getOpAllocator().getAllocatedMemory(),
            setupParams.getOpAllocator().getLimit());
        child.switchToSpilling();
        continue;
      }

      recordsDone += recordsInsertedInIteration;
      if (recordsInsertedInIteration < recordsRequestedInIteration) {
        spillWatch.start();
        boolean memReleased = tryAndReleaseMemory();
        if (memReleased || recordsInsertedInIteration > 0) {
          logger.debug("short insert during build, release some memory and retry");
          // try again
          child.removeInsertedRecordsInSV2(recordsInsertedInIteration);
        } else {
          logger.error(
              "zero records inserted during build, even after all partitions switched to spill");
          throw new OutOfMemoryException("unable to insert batch even after switching to spill");
        }
        spillWatch.stop();
      }
    }
  }

  private void computeHashAndSplitToChildPartitions(int startIdx, int records) {
    if (fullHashValues8B.capacity() < records * FULL_HASH_SIZE) {
      try (RollbackCloseable rc = new RollbackCloseable(true)) {
        BufferAllocator allocator = setupParams.getOpAllocator();
        fullHashValues8B.close();
        fullHashValues8B = null;
        fullHashValues8B = rc.add(allocator.buffer(records * FULL_HASH_SIZE));
        tableHashValues4B.close();
        tableHashValues4B = null;
        tableHashValues4B = rc.add(allocator.buffer(records * TABLE_HASH_SIZE));
        for (int partitionIdx = 0; partitionIdx < numPartitions; ++partitionIdx) {
          childWrappers[partitionIdx].createNewSv2(records);
          childWrappers[partitionIdx].updateTableHashBuffer(tableHashValues4B);
        }
        rc.commit();
      } catch (RuntimeException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    hasher.hashPivoted(records, fullHashValues8B);

    // split batch ordinals into per-partition buffers based on the highest 3-bits in the hash.
    resetAllPartitionInputs();
    fullHashValues8B.checkBytes(0, records * FULL_HASH_SIZE);
    tableHashValues4B.checkBytes(0, records * TABLE_HASH_SIZE);
    childWrappers[0].sv2.checkBytes(0, records * SelectionVector2.RECORD_SIZE);
    final long fullHashStartAddr8B = fullHashValues8B.memoryAddress();
    final long tableHashStartAddr4B = tableHashValues4B.memoryAddress();
    int currentFullHashOffset = 0;
    int currentTableHashOffset = 0;
    for (int ordinal = startIdx;
        ordinal < startIdx + records;
        ordinal++, currentFullHashOffset += FULL_HASH_SIZE,
            currentTableHashOffset += TABLE_HASH_SIZE) {
      /*
       * In the 64-bit hash,
       * - high 29 bits are unused.
       * - high 29-32 bits are used to determine the partition,
       * - low 32-bits are used in the hash table
       */
      final long fullHash = PlatformDependent.getLong(fullHashStartAddr8B + currentFullHashOffset);
      final int partitionIdx = (int) (fullHash >> 32) & partitionMask;
      final int tableHash = (int) fullHash;

      // add the tableHash to the common buffer
      PlatformDependent.putInt(tableHashStartAddr4B + currentTableHashOffset, tableHash);

      // add the ordinal in the common buffer to the per-partition sv2 input.
      childWrappers[partitionIdx].addToSV2(ordinal);
    }
  }

  @Override
  public boolean isBuildSideEmpty() {
    // empty only if all child partitions have empty build side.
    for (int i = 0; i < numPartitions; ++i) {
      if (!childWrappers[i].getPartition().isBuildSideEmpty()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void probeBatchBegin(int startIdx, int records) {
    probeHashComputationWatch.start();
    computeHashAndSplitToChildPartitions(startIdx, records);
    probeHashComputationWatch.stop();

    probePartitionCursor = 0;
    probePivotShift = startIdx;

    // begin for zero-th partition
    PartitionWrapper child = childWrappers[0];
    child.partition.probeBatchBegin(startIdx, child.getNumRecords());
  }

  @Override
  public int probePivoted(int startOutputIndex, int maxOutputIndex) throws Exception {
    int currentPartition = probePartitionCursor;
    int currentOutputIndex = startOutputIndex;
    while (currentOutputIndex <= maxOutputIndex && currentPartition < numPartitions) {
      PartitionWrapper child = childWrappers[currentPartition];
      int ret = 0;
      if (child.getNumRecords() > 0) {
        ret = child.getPartition().probePivoted(currentOutputIndex, maxOutputIndex);
      }
      if (ret > -1) {
        // this partition is done.
        ++currentPartition;

        // setup for the next partition
        if (currentPartition < numPartitions) {
          child = childWrappers[currentPartition];
          child.partition.probeBatchBegin(probePivotShift, child.getNumRecords());
        }
      }
      currentOutputIndex += Math.abs(ret);
    }

    // save partition cursor for next iteration.
    probePartitionCursor = currentPartition;
    int ret = currentOutputIndex - startOutputIndex;
    if (currentPartition < numPartitions) {
      // make the return value -ve to indicate that the probe for this batch is still incomplete.
      ret = -ret;
    }
    return ret;
  }

  @Override
  public int projectBuildNonMatches(int startOutputIndex, int maxOutputIndex) throws Exception {
    int currentPartition = probeNonMatchesPartitionCursor;
    int currentOutputIndex = startOutputIndex;
    while (currentOutputIndex <= maxOutputIndex && currentPartition < numPartitions) {
      Partition child = childWrappers[currentPartition].getPartition();
      int ret = child.projectBuildNonMatches(currentOutputIndex, maxOutputIndex);
      if (ret > -1) {
        // this partition is done.
        ++currentPartition;
      }
      currentOutputIndex += Math.abs(ret);
    }

    // save partition cursor for next iteration.
    probeNonMatchesPartitionCursor = currentPartition;
    int ret = currentOutputIndex - startOutputIndex;
    if (currentPartition < numPartitions) {
      // make the return value -ve to indicate that the probe for this batch is still incomplete.
      ret = -ret;
    }
    return ret;
  }

  @Override
  public void prepareBloomFilters(PartitionColFilters partitionColFilters) {
    for (int i = 0; i < numPartitions; i++) {
      Partition partition = childWrappers[i].getPartition();
      if (partition instanceof CanSwitchToSpilling) {
        partition.prepareBloomFilters(partitionColFilters);
      }
    }
  }

  @Override
  public void prepareValueListFilters(NonPartitionColFilters nonPartitionColFilters) {
    for (int i = 0; i < numPartitions; i++) {
      Partition partition = childWrappers[i].getPartition();
      if (partition instanceof CanSwitchToSpilling) {
        partition.prepareValueListFilters(nonPartitionColFilters);
      }
    }
  }

  @Override
  public void setFilters(
      NonPartitionColFilters nonPartitionColFilters, PartitionColFilters partitionColFilters) {
    for (int i = 0; i < numPartitions; i++) {
      childWrappers[i].getPartition().setFilters(nonPartitionColFilters, partitionColFilters);
    }
  }

  private long sumOverAllPartitions(
      Partition.Stats[] plist, Function<Partition.Stats, Long> statOne) {
    long sum = 0;
    for (int i = 0; i < numPartitions; ++i) {
      sum += statOne.apply(plist[i]);
    }
    if (statsFromPrevGen != null) {
      sum += statOne.apply(statsFromPrevGen);
    }
    return sum;
  }

  @Override
  public Partition.Stats getStats() {
    final TimeUnit ns = TimeUnit.NANOSECONDS;
    Partition.Stats[] allStats = new Partition.Stats[numPartitions];
    for (int i = 0; i < numPartitions; ++i) {
      allStats[i] = childWrappers[i].getPartition().getStats();
    }
    return new Partition.Stats() {
      @Override
      public long getBuildNumEntries() {
        return sumOverAllPartitions(allStats, Partition.Stats::getBuildNumEntries);
      }

      @Override
      public long getBuildNumBuckets() {
        return sumOverAllPartitions(allStats, Partition.Stats::getBuildNumBuckets);
      }

      @Override
      public long getBuildNumResizing() {
        return sumOverAllPartitions(allStats, Partition.Stats::getBuildNumResizing);
      }

      @Override
      public long getBuildResizingTimeNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getBuildResizingTimeNanos);
      }

      @Override
      public long getBuildPivotTimeNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getBuildPivotTimeNanos);
      }

      @Override
      public long getBuildHashComputationTimeNanos() {
        return buildHashComputationWatch.elapsed(ns);
      }

      @Override
      public long getBuildInsertTimeNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getBuildInsertTimeNanos);
      }

      @Override
      public long getBuildLinkTimeNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getBuildLinkTimeNanos);
      }

      @Override
      public long getBuildKeyCopyNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getBuildKeyCopyNanos);
      }

      @Override
      public long getBuildCarryOverCopyNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getBuildCarryOverCopyNanos);
      }

      @Override
      public long getBuildUnmatchedKeyCount() {
        return sumOverAllPartitions(allStats, Partition.Stats::getBuildUnmatchedKeyCount);
      }

      @Override
      public long getBuildCopyNonMatchNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getBuildCopyNonMatchNanos);
      }

      @Override
      public long getProbePivotTimeNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getProbePivotTimeNanos);
      }

      @Override
      public long getProbeHashComputationTime() {
        return probeHashComputationWatch.elapsed(ns);
      }

      @Override
      public long getProbeFindTimeNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getProbeFindTimeNanos);
      }

      @Override
      public long getProbeListTimeNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getProbeListTimeNanos);
      }

      @Override
      public long getProbeCopyNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getProbeCopyNanos);
      }

      @Override
      public long getProbeUnmatchedKeyCount() {
        return sumOverAllPartitions(allStats, Partition.Stats::getProbeUnmatchedKeyCount);
      }

      @Override
      public long getEvaluationCount() {
        return sumOverAllPartitions(allStats, Partition.Stats::getEvaluationCount);
      }

      @Override
      public long getEvaluationMatchedCount() {
        return sumOverAllPartitions(allStats, Partition.Stats::getEvaluationMatchedCount);
      }

      @Override
      public long getSetupNanos() {
        return sumOverAllPartitions(allStats, Partition.Stats::getSetupNanos);
      }
    };
  }

  private boolean tryAndReleaseMemory() throws Exception {
    MultiMemoryReleaser releaser = setupParams.getMultiMemoryReleaser();
    if (!releaser.isFinished()) {
      releaser.run();
      return true;
    }

    // If we cannot release memory, move a partition to spilling mode.
    return switchToSpilling(false).isSwitchDone();
  }

  @Override
  public long estimateSpillableBytes() {
    long spillableBytes = 0;
    for (int i = 0; i < numPartitions; ++i) {
      if (childWrappers[i].partition instanceof CanSwitchToSpilling) {
        CanSwitchToSpilling p = (CanSwitchToSpilling) childWrappers[i].partition;
        spillableBytes += p.estimateSpillableBytes();
      }
    }
    return spillableBytes;
  }

  // switch to spilling mode.
  @Override
  public CanSwitchToSpilling.SwitchResult switchToSpilling(boolean spillAll) {
    boolean switched = false;

    if (spillAll) {
      for (int i = 0; i < numPartitions; ++i) {
        if (childWrappers[i].partition instanceof CanSwitchToSpilling) {
          childWrappers[i].switchToSpilling();
          switched = true;
        }
      }
    } else {
      // pick a victim partition (pick one that uses max memory)
      int victimIdx = -1;
      long victimBytes = 0;
      long totalBytes = 0;
      for (int i = 0; i < numPartitions; ++i) {
        if (childWrappers[i].partition instanceof CanSwitchToSpilling) {
          CanSwitchToSpilling p = (CanSwitchToSpilling) childWrappers[i].partition;
          totalBytes += p.estimateSpillableBytes();
          if (victimBytes < p.estimateSpillableBytes()) {
            victimBytes = p.estimateSpillableBytes();
            victimIdx = i;
          }
        }
      }

      // if victim found, switch it to spilling.
      if (victimIdx != -1) {
        logger.info(
            "switching partition {} with memory usage {} of total {} to spilling, total usage {} limit {}",
            victimIdx,
            victimBytes,
            totalBytes,
            setupParams.getOpAllocator().getAllocatedMemory(),
            setupParams.getOpAllocator().getLimit());
        childWrappers[victimIdx].switchToSpilling();
        switched = true;
      }
    }
    return new CanSwitchToSpilling.SwitchResult(switched, switched ? this : null);
  }

  public boolean isSpilling() {
    for (int i = 0; i < numPartitions; ++i) {
      if (!(childWrappers[i].partition instanceof CanSwitchToSpilling)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isFiltersPreparedWithException() {
    for (int i = 0; i < numPartitions; ++i) {
      if (childWrappers[i].partition.isFiltersPreparedWithException()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void reset() {
    // record the current stats, they will be used for the next gen after reset.
    statsFromPrevGen = new RecordedStats(getStats());

    // This reseed is needed so that the data that was recorded for one partition in the previous
    // generation will
    // now get redistributed across all partitions during replay.
    hasher.reseed();
    // bump the generation, used for naming spill files
    setupParams.bumpGeneration();

    int numInMemoryPartitions = 0;
    int inMemTotalHashTableSize = 0;
    int preSpillBuildTotalHashTableSize = 0;

    for (PartitionWrapper p : childWrappers) {
      if (p.getPartition() instanceof CanSwitchToSpilling) {
        inMemTotalHashTableSize += p.getPartition().hashTableSize();
        numInMemoryPartitions++;
      } else {
        preSpillBuildTotalHashTableSize += p.getPartition().hashTableSize();
      }
    }
    /*
     * DX-55636 : If no in-memory hashtable entries exist in any of the in-memory partitions and preSpill hashtable
     * size is 1 (single entry) for DiskPartition, this will never converge.
     *
     * numInMemPartitions check here only exist to satisfy some unit tests...
     */
    Preconditions.checkState(
        numInMemoryPartitions == 0
            || inMemTotalHashTableSize != 0
            || preSpillBuildTotalHashTableSize != 1,
        "All in memory entries belong to same group. Replay never converge. Failing the query");

    // recreate the partitions in descending order of memory.
    List<PartitionWrapper> sortedWrappers = Arrays.asList(childWrappers);
    sortedWrappers.sort(Comparator.comparingLong(PartitionWrapper::estimateMemoryUsage).reversed());
    for (PartitionWrapper p : sortedWrappers) {
      // recreate all partitions.
      p.recreatePartition();
    }

    probeNonMatchesPartitionCursor = 0;
  }

  @Override
  public void close() throws Exception {
    List<AutoCloseable> autoCloseables = new ArrayList<>(Arrays.asList(childWrappers));
    autoCloseables.add(hasher);
    autoCloseables.add(tableHashValues4B);
    autoCloseables.add(fullHashValues8B);
    AutoCloseables.close(autoCloseables);
  }

  /*
   * Helper class to track input sv2 for a partition.
   */
  private final class PartitionWrapper implements AutoCloseable {
    private final int partitionIndex;
    private ArrowBuf sv2;
    private final long sv2Addr;
    private final int maxRecords;

    private Partition partition;
    private int numRecords;

    PartitionWrapper(int partitionIndex, int maxRecords) {
      try (RollbackCloseable rc = new RollbackCloseable(true)) {
        this.partitionIndex = partitionIndex;
        this.sv2 =
            rc.add(setupParams.getOpAllocator().buffer(maxRecords * SelectionVector2.RECORD_SIZE));
        this.sv2Addr = sv2.memoryAddress();
        this.maxRecords = maxRecords;
        rc.add(createPartition());
        rc.commit();
      } catch (RuntimeException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    public void createNewSv2(int numRecords) {
      if (this.sv2 != null) {
        this.sv2.close();
        this.sv2 = null;
      }
      this.sv2 = setupParams.getOpAllocator().buffer(numRecords * SelectionVector2.RECORD_SIZE);
      partition.updateSv2(sv2);
    }

    public void updateTableHashBuffer(ArrowBuf newBuffer) {
      if (partition instanceof CanSwitchToSpilling) {
        ((CanSwitchToSpilling) partition).updateTableHashBuffer(newBuffer);
      }
    }

    int getPartitionIndex() {
      return partitionIndex;
    }

    private Partition createPartition() {
      boolean useDiskPartition =
          setupParams.getOptions().getOption(HashJoinOperator.TEST_SPILL_MODE).equals("replay")
              && setupParams.getGeneration() == 1;
      partition =
          useDiskPartition
              ? new DiskPartition(setupParams, partitionIndex, sv2)
              : new MemoryPartition(
                  setupParams, copierFactory, partitionIndex, sv2, tableHashValues4B);
      return partition;
    }

    void resetInput() {
      numRecords = 0;
    }

    void addToSV2(int ordinal) {
      SV2UnsignedUtil.writeAtIndexUnsafe(sv2Addr, numRecords, ordinal);
      ++numRecords;
    }

    void removeInsertedRecordsInSV2(int numInserted) {
      Preconditions.checkArgument(numInserted < numRecords);
      // move the records to the start of the sv2
      for (int idx = 0; idx < numRecords - numInserted; ++idx) {
        int ordinal = SV2UnsignedUtil.readAtIndex(sv2, numInserted + idx);
        SV2UnsignedUtil.writeAtIndex(sv2, idx, ordinal);
      }
      numRecords -= numInserted;
    }

    int getNumRecords() {
      return numRecords;
    }

    Partition getPartition() {
      return partition;
    }

    void recreatePartition() {
      AutoCloseables.closeNoChecked(partition);
      createPartition();
    }

    void switchToSpilling() {
      Preconditions.checkState(partition instanceof CanSwitchToSpilling);
      CanSwitchToSpilling.SwitchResult result =
          ((CanSwitchToSpilling) partition).switchToSpilling(false);
      if (result.isSwitchDone()) {
        partition = result.getNewPartition();
      }
    }

    long estimateMemoryUsage() {
      return partition instanceof CanSwitchToSpilling
          ? ((CanSwitchToSpilling) partition).estimateSpillableBytes()
          : 0;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(partition, sv2);
    }
  }
}

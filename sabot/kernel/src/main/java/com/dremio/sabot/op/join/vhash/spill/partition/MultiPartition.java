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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.op.common.ht2.BlockChunk;
import com.dremio.sabot.op.common.ht2.HashComputation;
import com.dremio.sabot.op.join.hash.HashJoinOperator;
import com.dremio.sabot.op.join.vhash.spill.JoinSetupParams;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import io.netty.util.internal.PlatformDependent;

/**
 * Partition impl that acts as a bridge to multiple child partitions.
 */
public class MultiPartition implements Partition {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MultiPartition.class);
  private static final int FULL_HASH_SIZE = 8;

  private final int numPartitions;
  private final int partitionMask;

  private final JoinSetupParams setupParams;
  private final PagePool pool;
  private final long hashGenerationSeed = 0; //new Random().nextLong();
  private final List<Page> preAllocedPages = new ArrayList<>();
  private final ArrowBuf fullHashValues8B;
  private final ArrowBuf tableHashValues4B;
  private final PartitionWrapper[] childWrappers;

  private int probePartitionCursor;
  private int probeNonMatchesPartitionCursor;

  // stats related
  private final Stopwatch buildHashComputationWatch = Stopwatch.createUnstarted();
  private final Stopwatch probeHashComputationWatch = Stopwatch.createUnstarted();

  public MultiPartition(JoinSetupParams setupParams) {
    this.setupParams = setupParams;

    numPartitions = (int)setupParams.getOptions().getOption(HashJoinOperator.NUM_PARTITIONS);
    partitionMask = numPartitions - 1;
    childWrappers = new PartitionWrapper[numPartitions];
    probePartitionCursor = numPartitions;
    probeNonMatchesPartitionCursor = 0;

    pool = new PagePool(setupParams.getOpAllocator());
    final int maxBatchSize = setupParams.getMaxInputBatchSize();
    fullHashValues8B = allocBufFromPool(maxBatchSize * FULL_HASH_SIZE);
    tableHashValues4B = allocBufFromPool(maxBatchSize * TABLE_HASH_SIZE);
    for (int i = 0; i < numPartitions; ++i) {
      try (RollbackCloseable rc = new RollbackCloseable()) {
        ArrowBuf sv2Input = rc.add(allocBufFromPool(maxBatchSize * SelectionVector2.RECORD_SIZE));
        Partition partition = rc.add(new MemoryPartition(setupParams, i, sv2Input.memoryAddress(), tableHashValues4B.memoryAddress()));
        rc.commit();

        childWrappers[i] = new PartitionWrapper(partition, sv2Input, maxBatchSize);
      } catch (RuntimeException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  // Allocate sliced buffer from a page.
  private ArrowBuf allocBufFromPool(int bufSz) {
    Preconditions.checkArgument(bufSz <= pool.getPageSize());

    // use the last alloced page if it has enough remaining bytes. Else,
    // allocate a new one.
    Page currentPage = null;
    if (preAllocedPages.size() > 0) {
      currentPage = preAllocedPages.get(preAllocedPages.size() - 1);
      if (currentPage.getRemainingBytes() < bufSz) {
        currentPage = null;
      }
    }
    if (currentPage == null) {
      currentPage = pool.newPage();
      preAllocedPages.add(currentPage);
    }
    return currentPage.slice(bufSz);
  }

  private void resetAllPartitionInputs() {
    for (int i = 0; i < numPartitions; ++i) {
      childWrappers[i].resetInput();
    }
  }

  @Override
  public void buildPivoted(int records) throws Exception {
    // Do hash computation on entire batch, and split into per partition inputs.
    buildHashComputationWatch.start();
    final BlockChunk blockChunk = new BlockChunk(
      setupParams.getPivotedFixedBlock().getMemoryAddress(), setupParams.getPivotedVariableBlock().getMemoryAddress(),
      setupParams.getBuildKeyPivot().getVariableCount() == 0, setupParams.getBuildKeyPivot().getBlockWidth(),
      records, fullHashValues8B.memoryAddress(), hashGenerationSeed);

    computeHashAndSplitToChildPartitions(blockChunk, records);
    buildHashComputationWatch.stop();

    // pass along to the child partitions.
    for (int i = 0; i < numPartitions; ++i) {
      PartitionWrapper child = childWrappers[i];
      if (child.getNumRecords() > 0) {
        child.getPartition().buildPivoted(child.getNumRecords());
        logger.trace("partition {} : inserted {} records", i, child.getNumRecords());
      }
    }
  }

  private void computeHashAndSplitToChildPartitions(BlockChunk blockChunk, int records) {
    HashComputation.computeHash(blockChunk);

    // split batch ordinals into per-partition buffers based on the highest 3-bits in the hash.
    resetAllPartitionInputs();
    long currentFullHashAddr = fullHashValues8B.memoryAddress();
    long currentTableHashAddr = tableHashValues4B.memoryAddress();
    for (int ordinal = 0;
         ordinal < records;
         ordinal++, currentFullHashAddr += FULL_HASH_SIZE, currentTableHashAddr += TABLE_HASH_SIZE) {
      /*
       * In the 64-bit hash,
       * - high 29 bits are unused.
       * - high 29-32 bits are used to determine the partition,
       * - low 32-bits are used in the hash table
       */
      final long fullHash = PlatformDependent.getLong(currentFullHashAddr);
      final int partitionIdx = (int)(fullHash >> 32) & partitionMask;
      final int tableHash = (int) fullHash;

      // add the tableHash to the common buffer
      PlatformDependent.putInt(currentTableHashAddr, tableHash);

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
  public int probePivoted(int records, int startOutputIndex, int maxOutputIndex) throws Exception {
    int currentPartition = probePartitionCursor;
    if (currentPartition == numPartitions) {
      // First time we have seen this batch. Do hash computation on entire batch
      probeHashComputationWatch.start();
      final BlockChunk blockChunk = new BlockChunk(
        setupParams.getPivotedFixedBlock().getMemoryAddress(), setupParams.getPivotedVariableBlock().getMemoryAddress(),
        setupParams.getProbeKeyPivot().getVariableCount() == 0, setupParams.getProbeKeyPivot().getBlockWidth(),
        records, fullHashValues8B.memoryAddress(), hashGenerationSeed);
      computeHashAndSplitToChildPartitions(blockChunk, records);
      probeHashComputationWatch.stop();
      currentPartition = 0;
    }

    int currentOutputIndex = startOutputIndex;
    while (currentOutputIndex <= maxOutputIndex && currentPartition < numPartitions) {
      PartitionWrapper child = childWrappers[currentPartition];
      int ret = 0;
      if (child.getNumRecords() > 0) {
        ret = child.getPartition().probePivoted(child.getNumRecords(), currentOutputIndex, maxOutputIndex);
        logger.trace("partition {} : probe returned {} records", currentPartition, ret);
      }
      if (ret > -1) {
        // this partition is done.
        ++currentPartition;
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
      logger.trace("partition {} : probeBuildNonMatches returned {} records", currentPartition, ret);
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

  private long sumOverAllPartitions(Partition.Stats[] plist, Function<Partition.Stats, Long> statOne) {
    long sum = 0;
    for (int i = 0; i < numPartitions; ++i) {
      sum += statOne.apply(plist[i]);
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
    };
  }

  @Override
  public void close() throws Exception {
    List<AutoCloseable> autoCloseables = new ArrayList<>(Arrays.asList(childWrappers));
    autoCloseables.add(tableHashValues4B);
    autoCloseables.add(fullHashValues8B);
    autoCloseables.addAll(preAllocedPages);
    autoCloseables.add(pool);
    AutoCloseables.close(autoCloseables);
  }

  /*
   * Helper class to track input sv2 for a partition.
   */
  private static final class PartitionWrapper implements AutoCloseable {
    private final Partition partition;
    private final ArrowBuf sv2;
    private final int maxRecords;
    private int numRecords;

    PartitionWrapper(Partition partition, ArrowBuf sv2, int maxRecords) {
      this.partition = partition;
      this.sv2 = sv2;
      this.maxRecords = maxRecords;
    }

    void resetInput() {
      numRecords = 0;
    }

    void addToSV2(int ordinal) {
      assert ordinal >= 0 && ordinal < maxRecords;
      assert numRecords < maxRecords;

      SV2UnsignedUtil.write(sv2.memoryAddress(), numRecords, ordinal);
      ++numRecords;
    }

    int getNumRecords() {
      return numRecords;
    }

    Partition getPartition() { return partition; }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(partition, sv2);
    }
  }
}

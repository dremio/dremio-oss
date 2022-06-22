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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.dremio.sabot.op.sort.external.SpillManager.SpillFile;
import com.dremio.service.spill.SpillService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * {@link VectorizedHashAggOperator} uses an instance of this class to manage spilling
 * of partitions as and when decided by the operator.
 * Currently we have the basic infrastructure to spill/read partitions but this
 * needs to be augmented further in the next iteration of code when recursion
 * algorithm is complete.
 *
 * We maintain two different data structures for managing spilled partitions.
 * The main reason is to optimize algorithms like:
 *
 *  -- chooseVictimPartition()
 *  -- transitionPartitionState()
 *  -- spillAnyInMemoryDataForSpilledPartitions()
 *
 * Not only optimization, maintaining two data structures is somewhat
 * essential to ensure correctness of state machine in {@link VectorizedHashAggOperator}
 *
 * Here is how the two data structures are used for spilled partitions:
 *
 * (1) activeSpilledPartitions --
 *
 * An {@link ArrayList} of {@link VectorizedHashAggDiskPartition}. This contains the
 * partitions spilled in the current iteration of hash aggregation algorithm
 * being executed by {@link VectorizedHashAggOperator}. The number of
 * partitions in this list should never exceed the list of partitions
 * operator is working with. This list identifies spilled partitions from
 * the set of fixed number of partitions being used by the operator for hash aggregation.
 * The reason they are called as active spilled partitions is because they are being
 * actively used by the operator for incoming data distribution and aggregation but
 * during the processing they may get spilled.
 *
 * This list is not used for processing spilled partitions. Once an iteration
 * is over, we add all the partitions from this list to the FIFO queue of
 * spilled partitions and clear this list to prepare for next iteration.
 *
 * Maintaining a second list helps us to uniquely identify the set of partitions
 * that have been spilled in the current iteration (active and spilled) of hash
 * aggregation without being mixed with "full disk" based spilled partitions.
 *
 * This becomes necessary as recursion goes deeper and deeper. If we don't do this,
 * then the performance of above mentioned 3 functions will suffer as we needlessly
 * will have to go through several spilled partitions only to skip them.
 *
 * We sometimes refer to these partitions as "partitions spilled in current iteration".
 *
 * (2) spilledPartitions --
 *
 * A FIFO {@link Queue} of {@link VectorizedHashAggDiskPartition}. The sole
 * purpose is to process the spilled partitions. These are fully "disk-based"
 * partitions. In other words,
 */
public class VectorizedHashAggPartitionSpillHandler implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorizedHashAggPartitionSpillHandler.class);
  private final VectorizedHashAggPartition[] activePartitions;
  /* Global FIFO queue to manage spilled partitions */
  private Queue<VectorizedHashAggDiskPartition> spilledPartitions;
  /* partitions spilled in current iteration */
  private final List<VectorizedHashAggDiskPartition> activeSpilledPartitions;
  private final SpillManager spillManager;
  private final PartitionToLoadSpilledData loadingPartition;
  private SpilledPartitionIterator spilledPartitionIterator;
  private long maxBatchesSpilled;
  private long totalBatchesSpilled;
  private long maxRecordsSpilled;
  private long totalRecordsSpilled;
  private long maxSpilledDataSize;
  private long totalSpilledDataSize;
  private long numPartitionsSpilled; /* number of partitions or subpartitions spilled */
  private long spills;
  private final boolean minimizeSpilledPartitions;
  private static final int THRESHOLD_BLOCKS = 2;
  private VectorizedHashAggPartitionSerializable inProgressSpill;
  private final OperatorStats operatorStats;
  private final long warnMaxSpillTime;

  public VectorizedHashAggPartitionSpillHandler(
    final VectorizedHashAggPartition[] hashAggPartitions,
    final FragmentHandle fragmentHandle,
    final OptionManager optionManager,
    final SabotConfig sabotConfig,
    final int operatorId,
    final PartitionToLoadSpilledData loadingPartition,
    final SpillService spillService,
    final boolean minimizeSpilledPartitions,
    final OperatorStats stats) {

    this.activePartitions = hashAggPartitions;
    this.spilledPartitions = new LinkedList<>();
    this.activeSpilledPartitions = new ArrayList<>(activePartitions.length);

    final String id = String.format("aggspill-%s.%s.%s.%s", QueryIdHelper.getQueryId(fragmentHandle.getQueryId()),
      fragmentHandle.getMajorFragmentId(), fragmentHandle.getMinorFragmentId(), operatorId);

    this.spillManager = new SpillManager(sabotConfig, optionManager, id, null, spillService, "agg spilling", stats);

    Preconditions.checkArgument(loadingPartition != null, "Error: need a valid handle for loading partition");
    this.loadingPartition = loadingPartition;
    this.spilledPartitionIterator = null;
    this.maxBatchesSpilled = 0;
    this.totalBatchesSpilled = 0;
    this.maxRecordsSpilled = 0;
    this.totalRecordsSpilled = 0;
    this.maxSpilledDataSize = 0L;
    this.totalSpilledDataSize = 0L;
    this.numPartitionsSpilled = 0;
    this.spills = 0;
    this.minimizeSpilledPartitions = minimizeSpilledPartitions;
    this.inProgressSpill = null;
    this.operatorStats = stats;
    this.warnMaxSpillTime = optionManager.getOption(ExecConstants.SPILL_IO_WARN_MAX_RUNTIME_MS);
  }

  /**
   * Pick a victim partition to spill. We use the following strategy
   * to choose the victim partition:
   *
   * (1) Go over active spilled partitions: This effectively implies going
   * over the memory portion of these partitions and consider the
   * partitions larger than certain threshold. Select the partition
   * with maximum size.
   *
   * (2) The above step may fail to select a victim partition because
   * either there are no spilled partitions in the current
   * iteration or none of them is greater than threshold.
   * So now just go over all partitions and pick the
   * one with highest memory usage
   *
   * Let's take an example:
   *
   * Say we start with 4 partitions -- p0, p1, p2, p3
   *
   * State is:
   *
   * activePartitions: [p0, p1, p2, p3]
   * activeSpilledPartitions: []
   * spilledPartitions: []
   *
   * Some time later we decide to spill. Since there are no
   * active spilled partitions, step (1) returns null and we basically
   * look at memory consumption of each of these partitions and
   * spill the one with highest memory usage.
   *
   * Say p0 is the chosen one. State is:
   *
   * activePartitions: [p0, p1, p2, p3]
   * activeSpilledPartitions: [p0]
   * spilledPartitions: []
   *
   * aggregation proceeds further and we decide to spill again.
   * now we look at active spilled partitions [p0] and see what is
   * the memory usage of its memory portion. if it is larger than
   * threshold, we spill it.
   *
   * otherwise, we look at [p0, p1, p2, p3] and [p0] and pick
   * the partition with highest memory usage. it is evident that
   * looking at second list is redundant and we should just
   * look at [p0, p1, p2, p3]. however, it is important if we are
   * in the stage of processing spilled partitions.
   *
   * Say p1 is chosen.
   *
   * State is:
   *
   * activePartitions: [p0, p1, p2, p3]
   * activeSpilledPartitions: [p0, p1]
   * spilledPartitions:[]
   *
   * say all incoming data is consumed. we do the following
   *
   *  (1) output p2, p3.
   *  (2) flush in-memory (if any) portions of p0 and p1.
   *  (3) transition state for both sets of partitions which also
   *      involves adding the set of spilled partitions in current
   *      iteration to the global queue of spilled partitions and
   *      emptying the former.
   *
   * State is:
   *
   * activePartitions: [p0, p1, p2, p3] -- operator's set of in-memory partitions
   * downsized to minimum preallocation and absolutely no association
   * to spilled partitions
   * activeSpilledPartitions: []
   * spilledPartitions: [p0, p1]
   *
   * Start processing p0 from disk. This requires removing p0 from the queue
   * of spilled partitions and updating names of operator's partitions.
   *
   * State is:
   *
   * activePartitions: [p0.p0, p0.p1, p0.p2, p0.p3]
   * activeSpilledPartitions: []
   * spilledPartitions: [p1]
   *
   * The disk iterator for p0 will get to incoming batches and
   * repartition into p0.p0, p0.p1, p0.p2 and p0.p3.
   *
   * We decide to spill:
   *
   * step (1) scan activeSpilledPartitions which is empty since
   * in this iteration we haven't spilled any partition yet.
   *
   * step (2) scan activePartitions [p0.p0, p0.p1, p0.p2, p0.p3] and
   * activeSpilledPartitions [] and pick the one with highest memory usage.
   *
   * Say we choose p0.p3.
   *
   * State is:
   *
   * activePartitions: [p0.p0, p0.p1, p0.p2, p0.p3]
   * activeSpilledPartitions: [p0.p3]
   * spilledPartitions: [p1]
   *
   * say all incoming data is consumed from p0. we do the following
   *
   *  (1) output p0.p0, p0.p1 and p0.p2
   *  (2) flush in-memory (if any) portions of p0.p3.
   *  (3) transition state for both sets of partitions which also
   *      involves adding the set of spilled partitions in current
   *      iteration to the global queue of spilled partitions.
   *
   * State is:
   *
   * activePartitions: [p0, p1, p2, p3]
   * activeSpilledPartitions: []
   * spilledPartitions: [p1, p0.p3]
   *
   * The process continues
   *
   * @return chosen partition that will be spilled by the caller.
   */
  public VectorizedHashAggPartition chooseVictimPartition() {
    if (allPartitionsAtMinimum()) {
      /* no point in trying to choose the optimal victim partition
       * as every partition is at minimum preallocation.
       */
      logger.debug("All partitions are at minimum required memory usage. We will spill the current partition");
      return null;
    }
    /* choose the optimal partition to spill only if there
     * a possibility of releasing memory because there is at least
     * one partition consuming memory strictly greater than minimum
     * preallocation needed for all partitions.
     */
    VectorizedHashAggPartition victimPartition = considerAllSpilledPartitionsLargerThanThreshold();
    if (victimPartition == null) {
      /* if we couldn't choose a partition from the set of active spilled partitions
       * we just look at all active partitions and choose the one with highest
       * memory usage
       */
      victimPartition = getPartitionWithHighestMemoryUsage();
    }
    return victimPartition;
  }

  /**
   * We currently use this function when handling out of memory to check
   * if there is at least one partition with more than 1 batches/blocks
   * in hashtable (and accumulator).
   *
   * @return false if at least one partition's hash table has more than
   * one batch/block, true if all partitions are at minimum
   */
  private boolean allPartitionsAtMinimum() {
    for (VectorizedHashAggPartition partition : activePartitions) {
      if (partition.getNumberOfBlocks() > 1) {
        return false;
      }
    }
    return true;
  }

  /**
   * Go over the set of spilled partitions and get the partition
   * with highest memory usage and beyond minimum allocation.
   *
   * Our goal is to keep the number of unique spilled partitions
   * to as low as possible in order to maximize the number of partitions
   * that are always in memory. so first we target the set of active spilled
   * partitions (if any) to see if there is a candidate partition
   * with memory usage greater than minimum preallocation and largest
   * amongst all active spilled partitions. this also means that
   * as long as there is one spilled partition with memory usage beyond
   * minimum preallocation, we will choose it and this may come at the
   * expense of ignoring a non-spilled partition that has higher memory
   * usage and thus potential to release more memory.
   *
   * In general, this approach helps since it tends to prevent situations
   * where all partitions are spilled. But we may see a slightly higher
   * number of OOMs. For testing/debugging purposes, we have made the algorithm
   * configurable where we can disable the first part of selection algorithm
   * and just directly look at all partition (spilled or not-spilled) and
   * choose the one with highest memory usage.
   */
  private VectorizedHashAggPartition considerAllSpilledPartitionsLargerThanThreshold() {
    if (!minimizeSpilledPartitions) {
      return null;
    }
    /* grab local references for efficiency */
    final List<VectorizedHashAggDiskPartition> activeSpilledPartitions = this.activeSpilledPartitions;
    VectorizedHashAggPartition victimPartition = null;
    long maxPartitionSize = 0;
    for (VectorizedHashAggDiskPartition spilledPartition : activeSpilledPartitions) {
      final VectorizedHashAggPartition inmemoryPartition = spilledPartition.getInmemoryPartitionBackPointer();
      if (inmemoryPartition.getNumberOfBlocks() >= THRESHOLD_BLOCKS) {
        /* consider this partition iff it's memory usage is strictly above minimum preallocation */
        final long partitionSize = spilledPartition.getSize();
        if (partitionSize > maxPartitionSize) {
          /* choose based on actual size (in bytes) */
          maxPartitionSize = partitionSize;
          victimPartition = inmemoryPartition;
        }
      }
    }
    /* could be null if there were no spilled partitions or none of
     * the spilled partitions had size greater than threshold.
     */
    if (victimPartition != null) {
      logger.debug("Chose victim partition from spilled partitions, total number of partitions: {}, active spill partition count: {}, spill queue size: {}, victim partition size: {}",
        activePartitions.length, activeSpilledPartitions.size(), spilledPartitions.size(), maxPartitionSize);
    }
    return victimPartition;
  }

  /**
   * Go over the set of all active operator's partitions and get the
   * partition with highest memory usage. Since we are here only
   * after ascertaining that there is at least one partition with
   * memory usage beyond minimum preallocation, there is no need
   * to check the number of blocks in the partition.
   *
   * @return partition with highest memory usage
   */
  private VectorizedHashAggPartition getPartitionWithHighestMemoryUsage() {
    /* grab local references for efficiency */
    final List<VectorizedHashAggDiskPartition> activeSpilledPartitions = this.activeSpilledPartitions;
    final VectorizedHashAggPartition[] activePartitions = this.activePartitions;
    VectorizedHashAggPartition victimPartition = null;
    long maxPartitionSize = 0;
    for (VectorizedHashAggPartition partition : activePartitions) {
      final long partitionSize = partition.getSize();
      if (partitionSize > maxPartitionSize) {
        maxPartitionSize = partitionSize;
        victimPartition = partition;
      }
    }

    Preconditions.checkArgument(victimPartition != null, "Error: failed to select a victim partition");
    logger.debug("Chose victim partition, total number of partitions: {}, active spill partition count: {}, spill queue size: {}, victim partition size: {}",
      activePartitions.length, activeSpilledPartitions.size(), spilledPartitions.size(), maxPartitionSize);
    return victimPartition;
  }

  /**
   * Get number of times we have spilled
   * @return  number of times {@link VectorizedHashAggOperator} has spilled
   */
  long getNumberOfSpills() {
    return spills;
  }

  /**
   * This method is used by {@link VectorizedHashAggOperator} to spill a partition.
   * During its life cycle, operator may decide to spill at some points and invokes
   * this method.
   *
   * Here we choose a victim partition and spill it.
   *
   * @throws Exception
   */
  public void spillPartition(final VectorizedHashAggPartition victimPartition) throws Exception {
    final boolean isPartitionSpilled = victimPartition.isSpilled();
    final SpillFileHandle spillFileHandle = getSpillFileHandle(victimPartition);
    final SpillFile partitionSpillFile = spillFileHandle.partitionSpillFile;
    final FSDataOutputStream partitionSpillFileStream = spillFileHandle.partitionSpillFileStream;

    final VectorizedHashAggPartitionSerializable partitionSerializable = new VectorizedHashAggPartitionSerializable(victimPartition,
      this.operatorStats, this.warnMaxSpillTime);
    /* spill the partition -- done in 1 or more batches/chunks */
    partitionSerializable.writeToStream(partitionSpillFileStream);
    /* track number of spills */
    spills++;
    /* downsize the partition to minimum memory (ideally zeroed out for single batch) we would still like to keep allocated */
    victimPartition.resetToMinimumSize();

    final long batchesSpilled = partitionSerializable.getNumBatchesSpilled();
    final long recordsSpilled = partitionSerializable.getNumRecordsSpilled();
    final long spilledDataSize = partitionSerializable.getSpilledDataSize();
    updateLocalStats(batchesSpilled, recordsSpilled, spilledDataSize);


    updatePartitionSpillState(victimPartition, partitionSpillFile, partitionSpillFileStream, batchesSpilled);

    /* set the number of records inserted in the partition to 0 */
    victimPartition.resetRecords();

    if (!isPartitionSpilled) {
      logger.debug("Successfully spilled a partition: {} for the first time, batches spilled: {}, spill file path: {}, active spill partition count: {}, spill queue size: {}",
        victimPartition.getIdentifier(), batchesSpilled, partitionSpillFile.getPath(), activeSpilledPartitions.size(), spilledPartitions.size());
    } else {
      logger.debug("Successfully spilled a partition: {}, batches spilled: {}, spill file path: {}, pactive spill partition count: {}, spill queue size: {}",
        victimPartition.getIdentifier(), batchesSpilled, partitionSpillFile.getPath(), activeSpilledPartitions.size(), spilledPartitions.size());
    }
    partitionSpillFileStream.flush();
  }

  /**
   * Once we spill a partition (entire or single batch), we track the spill state
   * of the partition
   *
   * @param victimPartition
   * @param partitionSpillFile
   * @param partitionSpillFileStream
   * @param batchesSpilled
   */
  private void updatePartitionSpillState(final VectorizedHashAggPartition victimPartition,
                                         final SpillFile partitionSpillFile,
                                         final FSDataOutputStream partitionSpillFileStream,
                                         final long batchesSpilled) {
    final boolean isPartitionSpilled = victimPartition.isSpilled();
    VectorizedHashAggDiskPartition partitionSpillInfo = victimPartition.getSpillInfo();
    if (!isPartitionSpilled) {
      /* if this partition was never spilled before, marked the partition
       * as spilled and record all spill metadata -- handle for spill
       * file, spill file stream, number of batches spilled in this cycle.
       */
      partitionSpillInfo = new VectorizedHashAggDiskPartition(batchesSpilled, partitionSpillFile,
                                                              victimPartition, partitionSpillFileStream);
      victimPartition.setSpilled(partitionSpillInfo);
      activeSpilledPartitions.add(partitionSpillInfo);
      numPartitionsSpilled++;
    } else {
      /* if this partition was spilled before, we just need to bump the
       * total number of batches spilled for this partition.
       */
      partitionSpillInfo.addNewSpilledBatches(batchesSpilled);
    }
  }

  /**
   * Simple holder for spill file and stream
   */
  private static class SpillFileHandle {
    private final SpillFile partitionSpillFile;
    private final FSDataOutputStream partitionSpillFileStream;
    SpillFileHandle(final SpillFile partitionSpillFile, final FSDataOutputStream partitionSpillFileStream) {
      this.partitionSpillFile = partitionSpillFile;
      this.partitionSpillFileStream = partitionSpillFileStream;
    }
  }

  /**
   * Get spill file information for a victim partition
   * @param victimPartition victim partition to spill
   * @return SpillFileHandle containing reference for spill file and stream
   */
  private SpillFileHandle getSpillFileHandle(final VectorizedHashAggPartition victimPartition) {
    final boolean isPartitionSpilled = victimPartition.isSpilled();
    VectorizedHashAggDiskPartition partitionSpillInfo = victimPartition.getSpillInfo();
    SpillFile partitionSpillFile;
    FSDataOutputStream partitionSpillFileStream;

    Preconditions.checkState((isPartitionSpilled && partitionSpillInfo != null) ||
                               (!isPartitionSpilled && partitionSpillInfo == null), "Error: Detected invalid spill state of hash agg partition");

    if (!isPartitionSpilled) {
      /* get a brand new spill file as the partition is being spilled for the first time */
      partitionSpillFile = spillManager.getSpillFile(victimPartition.getIdentifier());
      try {
        partitionSpillFileStream = partitionSpillFile.create();
      } catch (Exception e) {
        final UserException.Builder builder = UserException.resourceError(e)
          .addContext("Failed to create output stream for spill file")
          .addContext("Spill file path:", partitionSpillFile.getPath());
        throw builder.build(logger);
      }
    } else {
      /* partition has already been spilled 1 or more times in the past.
       * get the existing spill file and stream handles.
       */
      partitionSpillFile = partitionSpillInfo.getSpillFile();
      partitionSpillFileStream = partitionSpillInfo.getSpillStream();
    }

    return new SpillFileHandle(partitionSpillFile, partitionSpillFileStream);
  }

  /**
   * Spill a single batch from the provided victim partition
   *
   * @param victimPartition partition to spill
   * @return true if there are no more batches to be spilled from this partition, false otherwise
   * @throws Exception exception while spilling the partition
   */
  boolean spillSingleBatchFromPartition(final VectorizedHashAggPartition victimPartition) throws Exception {
    final SpillFileHandle spillFileHandle = getSpillFileHandle(victimPartition);
    final SpillFile partitionSpillFile = spillFileHandle.partitionSpillFile;
    final FSDataOutputStream partitionSpillFileStream = spillFileHandle.partitionSpillFileStream;

    if (inProgressSpill == null) {
      inProgressSpill = new VectorizedHashAggPartitionSerializable(victimPartition, this.operatorStats,
        this.warnMaxSpillTime);
    }

    Preconditions.checkState(inProgressSpill.getPartition().getIdentifier().equals(victimPartition.getIdentifier()));
    /* spill a single batch from victim partition */
    final boolean done = inProgressSpill.writeBatchToStream(partitionSpillFileStream);

    if (done) {
      /* all batches from partition have been spilled */
      spills++;
      updateLocalStats(inProgressSpill.getNumBatchesSpilled(), inProgressSpill.getNumRecordsSpilled(), inProgressSpill.getSpilledDataSize());
      victimPartition.resetToMinimumSize();
      victimPartition.resetRecords();
      inProgressSpill = null;
      logger.debug("Finished spilling the last batch from partition: {}, spill file path: {}, active spilled partition count: {}, spill queue size; {}",
                   victimPartition.getIdentifier(), partitionSpillFile.getPath(), activeSpilledPartitions.size(), spilledPartitions.size());
    } else {
      updatePartitionSpillState(victimPartition, partitionSpillFile, partitionSpillFileStream, 1);
      logger.debug("Spilled a single batch from partition: {}, spill file path: {}, active spilled partition count: {}, spill queue size; {}",
                   victimPartition.getIdentifier(), partitionSpillFile.getPath(), activeSpilledPartitions.size(), spilledPartitions.size());
    }

    return done;
  }

  /**
   * Get number of partitions spilled
   * @return number of partitions spilled
   */
  long getNumPartitionsSpilled() {
    return numPartitionsSpilled;
  }

  /**
   * When a partition is spilled, it is spilled in 1 or more batches
   * and each batch has 1 or more records. We store this information
   * here to be used later by the operator for stats.
   *
   * @param batchesSpilled batches spilled
   * @param recordsSpilled records spilled
   * @param spilledDataSize total size (in bytes) of data spilled for a partition
   */
  private void updateLocalStats(final long batchesSpilled,
                                final long recordsSpilled,
                                final long spilledDataSize) {
    totalBatchesSpilled += batchesSpilled;
    totalRecordsSpilled += recordsSpilled;
    totalSpilledDataSize += spilledDataSize;
    if (batchesSpilled > maxBatchesSpilled) {
      maxBatchesSpilled = batchesSpilled;
    }
    if (recordsSpilled > maxRecordsSpilled) {
      maxRecordsSpilled = recordsSpilled;
    }
    if (spilledDataSize > maxSpilledDataSize) {
      maxSpilledDataSize = spilledDataSize;
    }
  }

  /**
   * Get the maximum number of batches spilled across all spills
   * done by the operator.
   * @return maximum batches spilled.
   *
   * Note: these batches are the hashtable (and accumulator)
   * blocks/batches and not operator's incoming batches.
   */
  long getMaxBatchesSpilled() {
    return maxBatchesSpilled;
  }

  /**
   * Get the total number of batches spilled across all spills
   * done by the operator.
   * @return total batches spilled.
   *
   * Note: these batches are the hashtable (and accumulator)
   * blocks/batches and not operator's incoming batches.
   */
  long getTotalBatchesSpilled() {
    return totalBatchesSpilled;
  }

  /**
   * Get the maximum number of records spilled across all spills
   * done by the operator.
   * @return maximum number of records spilled.
   */
  long getMaxRecordsSpilled() {
    return maxRecordsSpilled;
  }

  /**
   * Get the total number of records spilled across all spills
   * done by the operator.
   * @return total number of records spilled.
   */
  long getTotalRecordsSpilled() {
    return totalRecordsSpilled;
  }

  /**
   * Get the maximum size (in bytes) of data spilled by the operator
   * across all spill cycles. We compute the total length of data
   * spilled in a single spill cycle and a global maximum is tracked
   * across all cycles.
   *
   * @return maximum size (in bytes) spilled
   */
  long getMaxSpilledDataSize() {
    return maxSpilledDataSize;
  }

  /**
   * Get the total size (in bytes) of data spilled by the operator.
   * Every time operator spills a partition, we compute the total
   * length of buffers spilled. A global counter is used to
   * track cumulative length of data spilled across all spill cycles
   * @return total size (in bytes) spilled
   */
  long getTotalSpilledDataSize() {
    return totalSpilledDataSize;
  }

  /**
   * Get the number of spilled partitions in the current iteration
   * of aggregation algorithm -- active and spilled
   *
   * @return number active spilled partitions
   */
  public int getActiveSpilledPartitionCount() {
    return activeSpilledPartitions.size();
  }

  /**
   * Check if we have any "Disk Based" partitions.
   *
   * @return true if no "disk based" partitions, false otherwise
   */
  public boolean isSpillQueueEmpty() {
    return spilledPartitions.size() == 0;
  }

  /**
   *  Get the number of "disk based" partitions. In other
   *  words, get the size of FIFO queue used to process the
   *  spilled partitions.
   *
   *  @return number of spilled partitions in FIFO queue
   */
  public int getSpilledPartitionCount() {
    return spilledPartitions.size();
  }

  /**
   * When all the input data has been consumed, operator starts the recursion for
   * aggregation algorithm by starting to process one spilled partition at a time.
   * This involves reading spilled partition's data (batch by batch), using
   * it as new input to be fed into the operator and re-partitioned into same
   * number of partitions. Before we do this, transition the state of two sets
   * of partitions such that they no longer refer to each other.
   *
   * The partitions managed by operator become "MEMORY ONLY" partitions. In other
   * words, they no longer hold references to their disk-based counterparts.
   * The spilled partitions managed by this module become "DISK ONLY" partitions.
   * In other words, they no longer hold references to their in-memory counterparts.
   *
   * Doing this allows the recursion algorithm to treat the operator's partition
   * list as fresh set of in-memory partitions into which spilled batch's data can
   * be repartitioned.
   */
  public void transitionPartitionState() {
    /* operator state machine is responsible for deciding if there are spilled partitions to process
     * and accordingly invoke this method. if we are here when no partitions were ever spilled then
     * there is a bug in state machine in VectorizedHashAggOperator
     */
    Preconditions.checkArgument(activeSpilledPartitions.size() > 0 || spilledPartitions.size() > 0,
      "Error: expecting one or more active spilled partitions or spilled partitions in FIFO queue");

    /* grab local references for efficiency */
    final Queue<VectorizedHashAggDiskPartition> spilledPartitions = this.spilledPartitions;
    final List<VectorizedHashAggDiskPartition> activeSpilledPartitions = this.activeSpilledPartitions;

    final int size1 = activeSpilledPartitions.size();
    final int size2 = spilledPartitions.size();

    /* go over all active spilled partitions */
    Iterator<VectorizedHashAggDiskPartition> iterator = activeSpilledPartitions.iterator();
    while (iterator.hasNext()) {
      VectorizedHashAggDiskPartition spilledPartition = iterator.next();
      final VectorizedHashAggPartition inmemoryPartition = spilledPartition.getInmemoryPartitionBackPointer();
      Preconditions.checkArgument(inmemoryPartition != null, "Error: inconsistent state of spilled partition");
      inmemoryPartition.transitionToMemoryOnlyPartition();
      spilledPartition.transitionToDiskOnlyPartition();
      /* add the partition spilled in current iteration to global queue of spilled partitions */
      spilledPartitions.add(spilledPartition);
      /* remove the partition spilled in current iteration */
      iterator.remove();
    }

    /* sanity check:
     * (1) all the partitions from the list of active spilled partitions
     *     should have been added to spilled partition queue.
     * (2) the list of active spilled partitions (partitions spilled in current iteration)
      *    should be empty.
     */
    Preconditions.checkArgument(activeSpilledPartitions.size() == 0, "inconsistent state of active spilled partition list");
    Preconditions.checkArgument(spilledPartitions.size() == size1 + size2, "inconsistent state of spilled partition queue");
  }

  /**
   * This is used by the operator to get the next spilled partition to
   * process. When {@link VectorizedHashAggOperator} processes a spilled
   * partition from disk by reading spilled batches, it doesn't consume all
   * the spilled batches at one go. Instead, a single batch is consumed
   * by the operator from spill file and control is returned to Smart OP.
   * Upon the next entry into outputData, the state machine determines
   * if a spilled partition needs to be processed and if that is the case,
   * operator invokes this method to either get a brand new disk iterator for
   * a spilled partition or an existing open disk iterator for a spilled partition
   * whose one or more batches have already been processed.
   *
   * @return a disk iterator for a partition from the set of spilled partitions.
   */
  public SpilledPartitionIterator getNextSpilledPartitionToProcess() throws Exception {
    Preconditions.checkArgument(spilledPartitions.size() > 0 || spilledPartitionIterator != null,
      "Error: Nothing to process from disk. Not allowed to get disk iterator");
    if (spilledPartitionIterator == null) {
      logger.debug("Starting to process spilled partition from FIFO queue, spill queue size:{}", spilledPartitions.size());
      this.spilledPartitionIterator = new SpilledPartitionIterator(spilledPartitions.remove(), this.operatorStats);
    } else{
      Preconditions.checkArgument(spilledPartitionIterator.getCurrentBatchIndex() > 0,
        "Error: At least one batch should have already been read by an open disk iterator");
      logger.debug("Returning an existing iterator for spilled partition: {}, spill queue size:{}",
        spilledPartitionIterator.getIdentifier(), spilledPartitions.size());
    }
    return spilledPartitionIterator;
  }

  public void closeSpilledPartitionIterator() throws Exception {
    Preconditions.checkArgument(spilledPartitionIterator != null, "Error: expecting a valid disk iterator");
    spilledPartitionIterator.close();
    spilledPartitionIterator = null;
  }

  /**
   * Called by {@link VectorizedHashAggOperator} to ensure all spilled partitions are
   * entirely on disk. This is needed before the operator starts pumping out data from
   * in-memory partitions.
   *
   * The reason we do this is because once a partition has been spilled, it is very likely
   * that there would be subsequent incoming data belonging to the spilled partition. We will
   * continue to do the in-memory aggregation (contraction) or buffering (non-contraction).
   * The partition may or may not get spilled again. However, once all the input data has
   * been processed by the operator, spilled partition(s) could be holding data in-memory
   * and we need to spill/flush this data too.
   *
   * @throws Exception
   */
  public void spillAnyInMemoryDataForSpilledPartitions() throws Exception {
    /* get a local reference for efficiency */
    final List<VectorizedHashAggDiskPartition> activeSpilledPartitions = this.activeSpilledPartitions;

    /* spill the memory portion of each spilled partition */
    for (VectorizedHashAggDiskPartition partitionToSpill : activeSpilledPartitions) {
      /* the in-memory portion of partition could be empty if after the partition
       * was spilled, no incoming data ever mapped to that particular partition.
       * the writeToStream() function that spills partition's data structures
       * is aware of this fact and is a NOOP if partition is empty.
       */
      final VectorizedHashAggPartition inmemoryPartition = partitionToSpill.getInmemoryPartitionBackPointer();
      final SpillFile partitionSpillFile = partitionToSpill.getSpillFile();
      final VectorizedHashAggPartitionSerializable partitionSerializable = new VectorizedHashAggPartitionSerializable(inmemoryPartition,
        this.operatorStats, this.warnMaxSpillTime);
      FSDataOutputStream outputStream = partitionToSpill.getSpillStream();
      /* write the partition to disk */
      partitionSerializable.writeToStream(outputStream);
      /* track number of spills */
      spills++;
      /* downsize the partition to minimum memory (ideally zeroed out for single batch) we would still like to keep allocated */
      inmemoryPartition.resetToMinimumSize();
      final long batchesSpilled = partitionSerializable.getNumBatchesSpilled();
      final long recordsSpilled = partitionSerializable.getNumRecordsSpilled();
      final long spilledDataSize = partitionSerializable.getSpilledDataSize();
      updateLocalStats(batchesSpilled, recordsSpilled, spilledDataSize);
      partitionToSpill.addNewSpilledBatches(batchesSpilled);
      logger.debug("Flushed in-memory data for partition: {}, batches spilled: {}, spill file path: {}",
        inmemoryPartition.getIdentifier(), batchesSpilled, partitionSpillFile.getPath());
      outputStream.flush();
      /* no more to spill for this partition since operator will start a new iteration, so released the cached handle */
      partitionToSpill.closeSpillStream();
    }
  }

  /**
   * This iterator is created by {@link VectorizedHashAggOperator} to start
   * processing spilled data of a partition. This happens when the entire initial
   * input data into the operator has been consumed, the operator has pumped output
   * from MEMORY-ONLY partitions and starts recursion for spilled partitions.
   *
   * We iterate over the spilled batches and return one spilled batch at a time
   * which is then used by the operator to feed as new input into aggregation
   * algorithm.
   */
  public class SpilledPartitionIterator implements AutoCloseable {
    private final long batchCount;
    private final FSDataInputStream inputStream;
    private final VectorizedHashAggDiskPartition diskPartition;
    private int currentBatchIndex;
    private final OperatorStats operatorStats;

    SpilledPartitionIterator(final VectorizedHashAggDiskPartition spilledPartition, final OperatorStats stats) throws Exception {
      Preconditions.checkArgument(spilledPartition != null, "Error: need a valid partition handle to create a disk iterator");
      Preconditions.checkArgument(spilledPartition.getNumberOfBatches() > 0,
        "Error: Partition does not have any batches spilled to disk. Not allowed to create a disk iterator");
      this.batchCount = spilledPartition.getNumberOfBatches();
      final SpillFile partitionSpillFile = spilledPartition.getSpillFile();
      this.inputStream = partitionSpillFile.open();
      this.diskPartition = spilledPartition;
      this.currentBatchIndex = 0;
      this.operatorStats = stats;
      logger.debug("Created disk iterator for spilled partition: {}, batches to read: {}, spill file: {}", diskPartition.getIdentifier(), batchCount, partitionSpillFile.getPath());
    }

    /**
     * After creating the disk iterator, this method is used by {@link VectorizedHashAggOperator}
     * to get the next batch from spilled data. The batch will act as input into the operator.
     *
     * @return number of records read from deserialized spilled batch
     * @throws Exception
     */
    public int getNextBatch() throws Exception {
      if (currentBatchIndex == batchCount) {
        logger.debug("Finished reading all batches from spilled partition.");
        return 0;
      }
      logger.debug("Reading spilled batch:{} for partitions:{}", currentBatchIndex, diskPartition.getIdentifier());
      final VectorizedHashAggPartitionSerializable partitionSerializable = new VectorizedHashAggPartitionSerializable(loadingPartition, this.operatorStats, warnMaxSpillTime);
      partitionSerializable.readFromStream(inputStream);
      currentBatchIndex++;
      return loadingPartition.getRecordsInBatch();
    }

    public String getIdentifier() {
      return diskPartition.getIdentifier();
    }

    public int getCurrentBatchIndex() {
      return currentBatchIndex;
    }

    /* once the disk iterator for a spilled partition is closed, it
     * releases resources associated with input stream, spill file etc.
     */
    @Override
    public void close() throws Exception {
      AutoCloseables.close(inputStream, diskPartition);
    }
  }

  /**
   * Dump information about all spilled partitions.
   * Function can be useful for debugging.
   */
  public void dumpSpilledPartitionInfo() {
    logger.debug("Number of spilled partitions: {}", spilledPartitions.size());
    Iterator<VectorizedHashAggDiskPartition> iterator = spilledPartitions.iterator();
    while (iterator.hasNext()) {
      VectorizedHashAggDiskPartition spilledPartition = iterator.next();
      final long batches = spilledPartition.getNumberOfBatches();
      final String name = spilledPartition.getIdentifier();
      final SpillFile spillFile = spilledPartition.getSpillFile();
      logger.debug("Spilled partition info, ID:{}, batches:{}, file:{}", name, batches, spillFile.getPath());
    }
  }

  @Override
  public void close() throws Exception {
    if (spilledPartitionIterator != null) {
      closeSpilledPartitionIterator();
    }
    try {
      AutoCloseables.close(activeSpilledPartitions);
    }
    catch (IOException ignored) {
      /* Enter this catch block when disk is already full, and therefore cannot flush (write data) to disk
       * Making sure to catch this exception to allow this close() to complete
       * This ensures calling AutoCloseables.close(spillManager), hence clearing spill files
       */
    }
    try {
      AutoCloseables.close(spilledPartitions);
    }
    catch (IOException ignored) {
      /* Enter this catch block when disk is already full, and therefore cannot flush (write data) to disk
       * Making sure to catch this exception to allow this close() to complete
       * This ensures calling AutoCloseables.close(spillManager), hence clearing spill files
       */
    }
    AutoCloseables.close(spillManager);
  }

  /**
   * Get a particular active spilled partition
   * @param index index of the active spilled partition
   * @return reference to spilled partition
   */
  VectorizedHashAggDiskPartition getActiveSpilledPartition(final int index) {
    Preconditions.checkArgument(index >= 0 && index < activeSpilledPartitions.size(), "Error: invalid active spilled partition index");
    return activeSpilledPartitions.get(index);
  }

  /**
   * Close spill file streams for all active spilled partitions
   * @throws Exception
   */
  void closeSpillStreams() throws Exception {
    for (VectorizedHashAggDiskPartition activeSpilledPartition: activeSpilledPartitions) {
      activeSpilledPartition.closeSpillStream();
    }
  }

  /******************************************************
   *                                                    *
   * Helper functions to be used _only_ in unit tests   *
   *                                                    *
   ******************************************************/

  @VisibleForTesting
  public List<VectorizedHashAggDiskPartition> getActiveSpilledPartitions() {
    return activeSpilledPartitions;
  }

  @VisibleForTesting
  public Queue<VectorizedHashAggDiskPartition> getSpilledPartitions() {
    return spilledPartitions;
  }

  @VisibleForTesting
  public SpilledPartitionIterator getActiveSpilledPartition() throws Exception {
    Preconditions.checkArgument(activeSpilledPartitions.size() == 1,
      "Error: allowed to use this method only when there is one active spilled partition");
    return new SpilledPartitionIterator(activeSpilledPartitions.get(0), this.operatorStats);
  }
}

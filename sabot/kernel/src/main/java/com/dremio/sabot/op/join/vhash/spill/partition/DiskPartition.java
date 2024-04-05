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

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.join.vhash.NonPartitionColFilters;
import com.dremio.sabot.op.join.vhash.PartitionColFilters;
import com.dremio.sabot.op.join.vhash.spill.JoinSetupParams;
import com.dremio.sabot.op.join.vhash.spill.io.SpillFileDescriptor;
import com.dremio.sabot.op.join.vhash.spill.io.SpillWriter;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.join.vhash.spill.replay.JoinReplayEntry;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;

/**
 * Implementation of partition which simply records all batches (both the build-side & probe-side)
 * by spilling them to disk.
 */
public class DiskPartition implements Partition {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DiskPartition.class);

  private final JoinSetupParams setupParams;
  private final String partitionID;
  private final SpillWriter buildWriter;
  private final SpillWriter probeWriter;
  private final ImmutableList<SpillFileDescriptor> preSpillBuildFiles;
  private final int preSpillBuildHashTableSize;
  private final Stats recordedStats;
  private int probeBatchStartIdx;
  private int probeBatchNumRecords;
  private FixedBlockVector pivotedFixedBlockVector;
  private VariableBlockVector pivotedVariableBlockVector;
  private PartitionColFilters partitionColFilters;
  private NonPartitionColFilters nonPartitionColFilters;
  private final ArrowBuf sv2;
  private boolean filtersPreparedWithException = false;

  DiskPartition(JoinSetupParams setupParams, int partitionIdx, ArrowBuf sv2) {
    this(setupParams, partitionIdx, sv2, ImmutableList.of(), new RecordedStats(), 0);
  }

  DiskPartition(
      JoinSetupParams setupParams,
      int partitionIdx,
      ArrowBuf sv2,
      List<SpillFileDescriptor> preSpillBuildFiles,
      Stats recordedStats,
      int preSpillBuildHashTableSize) {
    this.setupParams = setupParams;
    this.partitionID =
        String.format("p_gen_%08d_idx_%08d", setupParams.getGeneration(), partitionIdx);
    this.preSpillBuildFiles = ImmutableList.copyOf(preSpillBuildFiles);
    this.preSpillBuildHashTableSize = preSpillBuildHashTableSize;
    this.recordedStats = recordedStats;
    this.pivotedFixedBlockVector = setupParams.getPivotedFixedBlock();
    this.pivotedVariableBlockVector = setupParams.getPivotedVariableBlock();
    this.sv2 = sv2;

    final PagePool pool = setupParams.getSpillPagePool();
    this.buildWriter =
        new SpillWriter(
            setupParams.getSpillManager(),
            setupParams.getSpillSerializable(true),
            getBuildFileName(),
            pool,
            sv2,
            setupParams.getRight(),
            setupParams.getBuildNonKeyFieldsBitset(),
            pivotedFixedBlockVector,
            pivotedVariableBlockVector);

    // For the probe side, we spill the key columns in both pivoted & unpivoted format.
    // - The pivoted format is useful for hash-table lookup
    // - The unpivoted format is useful for copying to outgoing vectors.
    this.probeWriter =
        new SpillWriter(
            setupParams.getSpillManager(),
            setupParams.getSpillSerializable(false),
            getProbeFileName(),
            pool,
            sv2,
            setupParams.getLeft(),
            null /*all columns are pivoted*/,
            pivotedFixedBlockVector,
            pivotedVariableBlockVector);
  }

  private String getBuildFileName() {
    return partitionID + "_build.arrow";
  }

  private String getProbeFileName() {
    return partitionID + "_probe.arrow";
  }

  @Override
  public int buildPivoted(int pivotShift, int records) throws Exception {
    // prepare bloom filters for partition columns before writing.
    try {
      if (partitionColFilters != null) {
        partitionColFilters.prepareBloomFilters(
            pivotedFixedBlockVector, pivotedVariableBlockVector, pivotShift, records, sv2);
      }
      // prepare value list filters for non-partition columns before writing.
      if (nonPartitionColFilters != null) {
        nonPartitionColFilters.prepareValueListFilters(
            pivotedFixedBlockVector, pivotedVariableBlockVector, pivotShift, records, sv2);
      }
    } catch (Exception e) {
      // This is just an optimisation. Hence, we don't throw the error further.
      logger.warn("Error while processing runtime join filter", e);
      filtersPreparedWithException = true;
    }
    buildWriter.writeBatch(pivotShift, records);
    logger.trace("partition {} spilled {} build records", partitionID, records);
    return records;
  }

  @Override
  public boolean isBuildSideEmpty() {
    return false;
  }

  @Override
  public int hashTableSize() {
    return preSpillBuildHashTableSize;
  }

  @Override
  public void probeBatchBegin(int startIdx, int records) {
    probeBatchStartIdx = startIdx;
    probeBatchNumRecords = records;
  }

  @Override
  public int probePivoted(int startOutputIndex, int maxOutputIndex) throws Exception {
    probeWriter.writeBatch(probeBatchStartIdx, probeBatchNumRecords);
    // return value of 0 indicates the batch has been completely processed.
    logger.trace("partition {} spilled {} probe records", partitionID, probeBatchNumRecords);
    return 0;
  }

  @Override
  public int projectBuildNonMatches(int startOutputIndex, int maxOutputIndex) throws Exception {
    // this partition type cannot project any output.
    return 0;
  }

  @Override
  public void prepareBloomFilters(PartitionColFilters partitionColFilters) {
    Preconditions.checkState(false, "DiskPartition shall never see prepareBloomFilters");
  }

  @Override
  public void prepareValueListFilters(NonPartitionColFilters nonPartitionColFilters) {
    Preconditions.checkState(false, "DiskPartition shall never see prepareValueListFilters");
  }

  @Override
  public void setFilters(
      NonPartitionColFilters nonPartitionColFilters, PartitionColFilters partitionColFilters) {
    this.nonPartitionColFilters = nonPartitionColFilters;
    this.partitionColFilters = partitionColFilters;
  }

  @Override
  public boolean isFiltersPreparedWithException() {
    return filtersPreparedWithException;
  }

  // TODO: add spill related stats.
  @Override
  public Stats getStats() {
    // the spill stats are recorded separately. For the remaining, use the stats as recorded while
    // this was a MemoryPartition.
    return recordedStats;
  }

  @Override
  public void close() throws Exception {
    setupParams
        .getReplayEntries()
        .add(
            JoinReplayEntry.of(
                preSpillBuildFiles,
                buildWriter.getSpillFileDescriptor(),
                probeWriter.getSpillFileDescriptor()));
    AutoCloseables.close(buildWriter, probeWriter);
  }
}

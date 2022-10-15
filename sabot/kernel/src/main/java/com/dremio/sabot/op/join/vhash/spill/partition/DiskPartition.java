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

import java.util.List;

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.join.vhash.NonPartitionColFilters;
import com.dremio.sabot.op.join.vhash.PartitionColFilters;
import com.dremio.sabot.op.join.vhash.spill.JoinSetupParams;
import com.dremio.sabot.op.join.vhash.spill.io.SpillFileDescriptor;
import com.dremio.sabot.op.join.vhash.spill.io.SpillWriter;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.join.vhash.spill.replay.JoinReplayEntry;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Implementation of partition which simply records all batches (both the build-side & probe-side) by spilling them to
 * disk.
 */
public class DiskPartition implements Partition {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DiskPartition.class);

  private final JoinSetupParams setupParams;
  private final String partitionID;
  private final SpillWriter buildWriter;
  private final SpillWriter probeWriter;
  private final ImmutableList<SpillFileDescriptor> preSpillBuildFiles;
  private final Stats recordedStats;
  private int probeBatchStartIdx;
  private int probeBatchNumRecords;

  DiskPartition(JoinSetupParams setupParams, int partitionIdx, ArrowBuf sv2) {
    this(setupParams, partitionIdx, sv2, ImmutableList.of(), new RecordedStats());
  }

  DiskPartition(JoinSetupParams setupParams, int partitionIdx, ArrowBuf sv2, List<SpillFileDescriptor> preSpillBuildFiles, Stats recordedStats) {
    this.setupParams = setupParams;
    this.partitionID = String.format("p_gen_%08d_idx_%08d", setupParams.getGeneration(), partitionIdx);
    this.preSpillBuildFiles = ImmutableList.copyOf(preSpillBuildFiles);
    this.recordedStats = recordedStats;

    final PagePool pool = setupParams.getSpillPagePool();
    this.buildWriter = new SpillWriter(setupParams.getSpillManager(), setupParams.getSpillSerializable(true), getBuildFileName(),
      pool, sv2, setupParams.getRight(), setupParams.getBuildNonKeyFieldsBitset(),
      setupParams.getPivotedFixedBlock(), setupParams.getPivotedVariableBlock());

    // For the probe side, we spill the key columns in both pivoted & unpivoted format.
    // - The pivoted format is useful for hash-table lookup
    // - The unpivoted format is useful for copying to outgoing vectors.
    this.probeWriter = new SpillWriter(setupParams.getSpillManager(), setupParams.getSpillSerializable(false), getProbeFileName(),
      pool, sv2, setupParams.getLeft(), null /*all columns are pivoted*/,
      setupParams.getPivotedFixedBlock(), setupParams.getPivotedVariableBlock());
  }

  private String getBuildFileName() {
    return partitionID +  "_build.arrow";
  }

  private String getProbeFileName() {
    return partitionID +  "_probe.arrow";
  }

  @Override
  public int buildPivoted(int pivotShift, int records) throws Exception {
    buildWriter.writeBatch(pivotShift, records);
    logger.trace("partition {} spilled {} build records", partitionID, records);
    return records;
  }

  @Override
  public boolean isBuildSideEmpty() {
    return false;
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

  // TODO: add spill related stats.
  @Override
  public Stats getStats() {
    // the spill stats are recorded separately. For the remaining, use the stats as recorded while this was a MemoryPartition.
    return recordedStats;
  }

  @Override
  public void close() throws Exception {
    // Add entry to replay list, so that the spilled data gets replayed.
    setupParams.getReplayEntries().add(JoinReplayEntry.of(preSpillBuildFiles, buildWriter.getSpillFileDescriptor(), probeWriter.getSpillFileDescriptor()));
    AutoCloseables.close(buildWriter, probeWriter);
  }
}

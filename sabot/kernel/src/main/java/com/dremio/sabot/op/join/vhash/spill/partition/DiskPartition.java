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
import java.util.concurrent.TimeUnit;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.join.vhash.spill.JoinSetupParams;
import com.dremio.sabot.op.join.vhash.spill.io.SpillWriter;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.join.vhash.spill.replay.JoinReplayEntry;
import com.dremio.sabot.op.sort.external.SpillManager.SpillFile;
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
  private final ImmutableList<SpillFile> preSpillBuildFiles;

  DiskPartition(JoinSetupParams setupParams, int partitionIdx, long sv2Addr) {
    this(setupParams, partitionIdx, sv2Addr, ImmutableList.of());
  }

  DiskPartition(JoinSetupParams setupParams, int partitionIdx, long sv2Addr, List<SpillFile> preSpillBuildFiles) {
    this.setupParams = setupParams;
    this.partitionID = String.format("p_gen_%08d_idx_%08d", setupParams.getGeneration(), partitionIdx);
    this.preSpillBuildFiles = ImmutableList.copyOf(preSpillBuildFiles);

    final PagePool pool = setupParams.getSpillPagePool();
    this.buildWriter = new SpillWriter(setupParams.getSpillManager(), setupParams.getSpillSerializable(), getBuildFileName(),
      pool, sv2Addr, setupParams.getRight(), setupParams.getBuildNonKeyFieldsBitset(),
      setupParams.getPivotedFixedBlock(), setupParams.getPivotedVariableBlock());

    // For the probe side, we spill the key columns in both pivoted & unpivoted format.
    // - The pivoted format is useful for hash-table lookup
    // - The unpivoted format is useful for copying to outgoing vectors.
    this.probeWriter = new SpillWriter(setupParams.getSpillManager(), setupParams.getSpillSerializable(), getProbeFileName(),
      pool, sv2Addr, setupParams.getLeft(), null /*all columns are pivoted*/,
      setupParams.getPivotedFixedBlock(), setupParams.getPivotedVariableBlock());
  }

  private String getBuildFileName() {
    return partitionID +  "_build.arrow";
  }

  private String getProbeFileName() {
    return partitionID +  "_probe.arrow";
  }

  @Override
  public int buildPivoted(int records) throws Exception {
    buildWriter.writeBatch(records);
    logger.trace("partition {} spilled {} build records", partitionID, records);
    return records;
  }

  @Override
  public boolean isBuildSideEmpty() {
    return false;
  }

  @Override
  public int probePivoted(int records, int startOutputIndex, int maxOutputIndex) throws Exception {
    probeWriter.writeBatch(records);
    // return value of 0 indicates the batch has been completely processed.
    logger.trace("partition {} spilled {} probe records", partitionID, records);
    return 0;
  }

  @Override
  public int projectBuildNonMatches(int startOutputIndex, int maxOutputIndex) throws Exception {
    // this partition type cannot project any output.
    return 0;
  }

  // TODO: add spill related stats.
  @Override
  public Stats getStats() {
    return new Stats() {
      final TimeUnit ns = TimeUnit.NANOSECONDS;

      @Override
      public long getBuildNumEntries() {
        return 0;
      }

      @Override
      public long getBuildNumBuckets() {
        return 0;
      }

      @Override
      public long getBuildNumResizing() {
        return 0;
      }

      @Override
      public long getBuildResizingTimeNanos() {
        return 0;
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
        return 0;
      }

      @Override
      public long getBuildLinkTimeNanos() {
        return 0;
      }

      @Override
      public long getBuildKeyCopyNanos() {
        return 0;
      }

      @Override
      public long getBuildCarryOverCopyNanos() {
        return 0;
      }

      @Override
      public long getBuildUnmatchedKeyCount() {
        return 0;
      }

      @Override
      public long getBuildCopyNonMatchNanos() {
        return 0;
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
        return 0;
      }

      @Override
      public long getProbeListTimeNanos() {
        return 0;
      }

      @Override
      public long getProbeCopyNanos() {
        return 0;
      }

      @Override
      public long getProbeUnmatchedKeyCount() {
        return 0;
      }
    };
  }

  @Override
  public void close() throws Exception {
    // Add entry to replay list, so that the spilled data gets replayed.
    setupParams.getReplayEntries().add(JoinReplayEntry.of(preSpillBuildFiles, buildWriter.getSpillFile(), probeWriter.getSpillFile()));
    AutoCloseables.close(buildWriter, probeWriter);
  }
}

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
package com.dremio.sabot.op.join.vhash.spill.replay;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.CloseableIterator;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.join.vhash.spill.JoinSetupParams;
import com.dremio.sabot.op.join.vhash.spill.YieldingRunnable;
import com.dremio.sabot.op.join.vhash.spill.io.BatchCombiningSpillReader;
import com.dremio.sabot.op.join.vhash.spill.io.SpillChunk;
import com.dremio.sabot.op.join.vhash.spill.io.SpillSerializable;
import com.dremio.sabot.op.join.vhash.spill.partition.Partition;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.sort.external.SpillManager.SpillFile;
import com.google.common.base.Preconditions;

/**
 * Replayer for one a of join spill files that correspond to one partition.
 *
 * - Processes build files
 * - Processes probe files
 * - project non-matches
 */
public class JoinReplayer implements YieldingRunnable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinReplayer.class);
  private final JoinSetupParams setupParams;
  private final Partition partition;
  private final VectorContainer outgoing;
  private final int targetOutputBatchSize;
  private final Map<String, VectorWrapper<?>> unpivotedBuildVectorsMap = new HashMap<>();
  private final Map<String, VectorWrapper<?>> unpivotedProbeVectorsMap = new HashMap<>();

  private ReplayState replayState;
  private long outputRecords = 0;
  private final SpillChunkIterator buildChunkIterator;
  private SpillChunk currentBuildChunk;
  private final SpillChunkIterator probeChunkIterator;
  private SpillChunk currentProbeChunk;

  enum ReplayState {
    PREPARE,
    BUILD_READ,
    BUILD_PROCESS,
    BUILD_NO_MORE,
    PROBE_READ,
    PROBE_PROCESS,
    PROBE_NO_MORE,
    PROJECT_NON_MATCHES,
    RESET,
    DONE;

    public void is(ReplayState expected) {
      assert expected == this : String.format("JoinReplayer should have been in state %s but was in state %s.",
        expected.name(), this.name());
    }
  }

  public JoinReplayer(JoinReplayEntry replayEntry, JoinSetupParams setupParams, Partition partition, VectorContainer outgoing, int targetOutputSize) {
    this.setupParams = setupParams;
    this.partition = partition;
    this.outgoing = outgoing;
    this.targetOutputBatchSize = targetOutputSize;
    this.replayState = ReplayState.PREPARE;

    // for build side, the spill has
    // - key columns in pivoted format
    // - non-key columns in unpivoted format
    int index = 0;
    List<Field> unpivotedBuildFields = new ArrayList<>();
    for (VectorWrapper<?> vector : setupParams.getRight()) {
      if (setupParams.getBuildNonKeyFieldsBitset().get(index)) {
        unpivotedBuildFields.add(vector.getField());
        unpivotedBuildVectorsMap.put(vector.getField().getName(), vector);
      }
      ++index;
    }
    this.buildChunkIterator = new SpillChunkIterator(setupParams.getSpillSerializable(true), setupParams.getSpillPagePool(),
      replayEntry.getBuildFiles(), setupParams.getBuildKeyPivot(), new BatchSchema(unpivotedBuildFields), setupParams.getMaxInputBatchSize());

    // for probe side, the spill has
    // - key columns in both pivoted and unpivoted format
    // - non-key columns in unpivoted format
    for (VectorWrapper<?> vector : setupParams.getLeft()) {
      unpivotedProbeVectorsMap.put(vector.getField().getName(), vector);
    }
    this.probeChunkIterator = new SpillChunkIterator(setupParams.getSpillSerializable(false), setupParams.getSpillPagePool(),
      replayEntry.getProbeFiles(), setupParams.getBuildKeyPivot(), setupParams.getLeft().getSchema(), setupParams.getMaxInputBatchSize());
  }

  @Override
  public int run() throws Exception {
    logger.trace("entry replayState {}", replayState);
    int ret = 0;
    switch (replayState) {
      case PREPARE:
        prepare();
        break;

      case BUILD_READ:
        buildRead();
        break;

      case BUILD_PROCESS:
        buildProcess();
        break;

      case BUILD_NO_MORE:
        buildNoMore();
        break;

      case PROBE_READ:
        probeRead();
        break;

      case PROBE_PROCESS:
        ret = probeProcess();
        break;

      case PROBE_NO_MORE:
        probeNoMore();
        break;

      case PROJECT_NON_MATCHES:
        ret = projectNonMatches();
        break;

      case RESET:
        reset();
        break;
    }
    logger.trace("exit replayState {}",  replayState);
    return ret;
  }

  @Override
  public boolean isFinished() {
    return replayState == ReplayState.DONE;
  }

  private void prepare() {
    replayState.is(ReplayState.PREPARE);
    replayState = ReplayState.BUILD_READ;
  }

  private void buildRead() {
    replayState.is(ReplayState.BUILD_READ);
    if (!buildChunkIterator.hasNext()) {
      replayState = ReplayState.BUILD_NO_MORE;
      return;
    }

    currentBuildChunk = buildChunkIterator.next();
    setupParams.getSpillStats().addReadBuildBatchesMerged(1);
    copyOrTransferFromChunk(currentBuildChunk, setupParams.getPivotedFixedBlock(),
      setupParams.getPivotedVariableBlock(), unpivotedBuildVectorsMap);
    replayState = ReplayState.BUILD_PROCESS;
  }

  private void buildProcess() throws Exception {
    replayState.is(ReplayState.BUILD_PROCESS);

    // apply build batch to partition
    int ret = partition.buildPivoted(0, currentBuildChunk.getNumRecords());
    Preconditions.checkState(ret == currentBuildChunk.getNumRecords());

    replayState = ReplayState.BUILD_READ;
  }

  private void buildNoMore() {
    replayState.is(ReplayState.BUILD_NO_MORE);

    if (partition.isBuildSideEmpty() &&
      !(setupParams.getJoinType() == JoinRelType.LEFT || setupParams.getJoinType() == JoinRelType.FULL)) {
      // nothing needs to be read on the left side as right side is empty
      replayState = ReplayState.RESET;
      return;
    }

    replayState = ReplayState.PROBE_READ;
  }

  private void probeRead() {
    replayState.is(ReplayState.PROBE_READ);

    if (!probeChunkIterator.hasNext()) {
      replayState = ReplayState.PROBE_NO_MORE;
      return;
    }

    currentProbeChunk = probeChunkIterator.next();
    setupParams.getSpillStats().addReadProbeBatchesMerged(1);
    copyOrTransferFromChunk(currentProbeChunk, setupParams.getPivotedFixedBlock(),
      setupParams.getPivotedVariableBlock(), unpivotedProbeVectorsMap);
    partition.probeBatchBegin(0, currentProbeChunk.getNumRecords());
    replayState = ReplayState.PROBE_PROCESS;
  }

  private int probeProcess() throws Exception {
    replayState.is(ReplayState.PROBE_PROCESS);

    final int probedRecords = partition.probePivoted(0, targetOutputBatchSize - 1);
    outputRecords += Math.abs(probedRecords);
    if (probedRecords > -1) {
      // this batch is fully processed, read the next batch.
      replayState = ReplayState.PROBE_READ;
      return outgoing.setAllCount(probedRecords);
    } else {
      // we didn't finish everything, will need to process again
      return outgoing.setAllCount(-probedRecords);
    }
  }

  private void probeNoMore() {
    replayState.is(ReplayState.PROBE_NO_MORE);

    if (setupParams.getJoinType() == JoinRelType.FULL || setupParams.getJoinType() == JoinRelType.RIGHT) {
      // if we need to project build records that didn't match, make sure we do so.
      replayState = ReplayState.PROJECT_NON_MATCHES;
    } else {
      replayState = ReplayState.RESET;
    }
  }

  private int projectNonMatches() throws Exception {
    replayState.is(ReplayState.PROJECT_NON_MATCHES);

    final int unmatched = partition.projectBuildNonMatches(0, targetOutputBatchSize - 1);
    outputRecords += Math.abs(unmatched);
    if (unmatched > -1) {
      replayState = ReplayState.RESET;
      return outgoing.setAllCount(unmatched);
    } else {
      // remainder, need to output again.
      return outgoing.setAllCount(-unmatched);
    }
  }

  private void reset() {
    setupParams.getSpillStats().incrementReplayCount();

    replayState.is(ReplayState.RESET);
    partition.reset();
    replayState = ReplayState.DONE;
  }

  private static void copyOrTransferFromChunk(SpillChunk chunk,
                                              FixedBlockVector fixed,
                                              VariableBlockVector variable,
                                              Map<String, VectorWrapper<?>> nameToVectorMap) {
    // The fixed/variable parts are copied from spill chunk to the vector expected by the partition
    fixed.getBuf().setBytes(0, chunk.getFixed(), 0, chunk.getFixed().capacity());
    if (variable != null) {
      variable.getBuf().setBytes(0, chunk.getVariable(), 0, chunk.getVariable().capacity());
    }

    // The rest of the data is transferred to either left/right containers.
    for (VectorWrapper<?> src : chunk.getContainer()) {
      VectorWrapper<?> dst = nameToVectorMap.get(src.getField().getName());
      Preconditions.checkNotNull(dst);

      src.transfer(dst);
    }
  }

  public long getOutputRecords() {
    return outputRecords;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(buildChunkIterator, probeChunkIterator);
  }

  // An iterator that can span multiple spill files (schema needs to be same).
  private static class SpillChunkIterator implements CloseableIterator<SpillChunk> {
    private final SpillSerializable serializable;
    private final PagePool pool;
    private final LinkedList<SpillFile> spillFiles;
    private final PivotDef pivotDef;
    private final BatchSchema schema;
    private final int maxInputBatchSize;
    private SpillFile currentFile;
    private BatchCombiningSpillReader currentReader;
    private SpillChunk currentChunk;
    private SpillChunk nextChunk;
    private boolean isFinished;

    SpillChunkIterator(SpillSerializable serializable, PagePool pool, List<SpillFile> spillFiles,
                       PivotDef pivotDef, BatchSchema schema, int maxInputBatchSize) {
      this.serializable = serializable;
      this.pool = pool;
      this.spillFiles = new LinkedList<>(spillFiles);
      this.pivotDef = pivotDef;
      this.schema = schema;
      this.maxInputBatchSize = maxInputBatchSize;
    }

    @Override
    public boolean hasNext() {
      do {
        if (nextChunk != null) {
          return true;
        }
        advance();
      } while (!isFinished);
      return false;
    }

    @Override
    public SpillChunk next() {
      AutoCloseables.closeNoChecked(currentChunk);
      Preconditions.checkNotNull(nextChunk);
      currentChunk = nextChunk;
      nextChunk = null;
      return currentChunk;
    }

    private void advance() {
      if (currentReader != null && !currentReader.hasNext()) {
        // If the current file is finished, close it.
        AutoCloseables.closeNoChecked(currentReader);

        SpillFile finishedFile = spillFiles.removeFirst();
        Preconditions.checkState(finishedFile == currentFile);
        AutoCloseables.closeNoChecked(currentFile); // this deletes the file on-disk
        currentFile = null;
        currentReader = null;
      }
      if (currentReader == null) {
        // switch to next file.
        if (spillFiles.isEmpty()) {
          isFinished = true;
          return;
        }
        currentFile = spillFiles.getFirst();
        currentReader = new BatchCombiningSpillReader(currentFile, serializable, pool, pivotDef, schema,
          maxInputBatchSize);
      }

      if (currentReader.hasNext()) {
        nextChunk = currentReader.next();
      } else {
        nextChunk = null;
      }
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(currentChunk, nextChunk, currentReader);
      currentReader = null;
      nextChunk = null;
      currentChunk = null;
    }
  }
}

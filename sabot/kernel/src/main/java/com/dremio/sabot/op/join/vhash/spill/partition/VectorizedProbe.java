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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Unpivots;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.copier.ConditionalFieldBufferCopier6;
import com.dremio.sabot.op.copier.FieldBufferCopier;
import com.dremio.sabot.op.copier.FieldBufferCopier6;
import com.dremio.sabot.op.join.vhash.spill.JoinSetupParams;
import com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap;
import com.dremio.sabot.op.join.vhash.spill.list.ProbeBuffers;
import com.dremio.sabot.op.join.vhash.spill.list.ProbeCursor;
import com.dremio.sabot.op.join.vhash.spill.list.UnmatchedCursor;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

public class VectorizedProbe implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorizedProbe.class);
  public static final int SKIP = -1;

  private final BufferAllocator allocator;

  private final JoinTable table;
  private final List<FieldBufferCopier> buildCopiers;
  private final List<FieldBufferCopier> probeCopiers;
  private final FieldBufferCopier.Cursor outputCursor = new FieldBufferCopier.Cursor();
  /* Used to copy the key vectors from probe side to build side in output.
   * For non matched records in probe side, the keys in build side will be indicated as SKIP and will be set to null.
   */
  private final List<FieldBufferCopier> keysCopiers;
  private final boolean projectUnmatchedProbe;
  private final boolean projectUnmatchedBuild;
  private final Stopwatch probeFind2Watch = Stopwatch.createUnstarted();
  private final Stopwatch buildCopyWatch = Stopwatch.createUnstarted();
  private final Stopwatch probeCopyWatch = Stopwatch.createUnstarted();
  private final Stopwatch projectBuildNonMatchesWatch = Stopwatch.createUnstarted();

  // Used to unpivot keys to output for non matched records in build side
  private final PivotDef buildKeyUnpivot;

  private final ProbeBuffers buffers;
  private final ProbeCursor cursor;
  private UnmatchedCursor unmatchedCursor;

  public VectorizedProbe(
    JoinSetupParams setupParams,
    long sv2Addr,
    long tableHashAddr4B,
    JoinTable table,
    PageListMultimap linkedList,
    ExpandableHyperContainer buildBatch) {

    this.buildKeyUnpivot = setupParams.getBuildKeyUnpivot();
    this.allocator = setupParams.getOpAllocator();
    this.buffers = new ProbeBuffers(sv2Addr, setupParams.getMaxInputBatchSize(), allocator);
    this.table = table;

    JoinRelType joinRelType = setupParams.getJoinType();
    this.projectUnmatchedBuild = joinRelType == JoinRelType.RIGHT || joinRelType == JoinRelType.FULL;
    this.projectUnmatchedProbe = joinRelType == JoinRelType.LEFT || joinRelType == JoinRelType.FULL;

    List<FieldVector> buildOutputCarryOvers = setupParams.getBuildOutputCarryOvers();
    if (table.size() > 0) {
      this.buildCopiers = projectUnmatchedProbe  ?
        ConditionalFieldBufferCopier6.getFourByteCopiers(VectorContainer.getHyperFieldVectors(buildBatch), buildOutputCarryOvers) :
        FieldBufferCopier6.getFourByteCopiers(VectorContainer.getHyperFieldVectors(buildBatch), buildOutputCarryOvers);
    } else {
      this.buildCopiers = ConditionalFieldBufferCopier6.getEmptySourceFourByteCopiers(buildOutputCarryOvers);
    }

    // create copier for copying keys from probe batch to build side output
    if (setupParams.getProbeIncomingKeys().size() > 0) {
      this.keysCopiers = FieldBufferCopier.getCopiers(setupParams.getProbeIncomingKeys(), setupParams.getBuildOutputKeys());
    } else {
      this.keysCopiers = Collections.emptyList();
    }

    this.probeCopiers = FieldBufferCopier.getCopiers(VectorContainer.getFieldVectors(setupParams.getLeft()),
      setupParams.getProbeOutputs());

    linkedList.moveToRead();
    this.cursor = ProbeCursor.startProbe(
      linkedList,
      buffers,
      projectUnmatchedBuild,
      projectUnmatchedProbe,
      records -> {
        logger.trace("lookup hash table for {} records", records);
        buffers.ensureInputCapacity(records);
        table.findPivoted(sv2Addr, records,
          tableHashAddr4B,
          setupParams.getPivotedFixedBlock(), setupParams.getPivotedVariableBlock(),
          buffers.getInTableMatchOrdinals4B().memoryAddress() /*output*/);
      });
    if (projectUnmatchedBuild) {
      this.unmatchedCursor = new UnmatchedCursor(linkedList, buffers, table::getVarKeyLength);
    }
  }

  /**
   * Probe with current batch. If we've run out of space, return a negative
   * record count. If we processed the entire incoming batch, return a positive
   * record count or zero.
   *
   * @return Negative if partial batch complete. Otherwise, all of probe batch
   *         is complete.
   */
  public int probeBatch(final int records, int startOutputIndex, int maxOutputIndex) {
    Preconditions.checkArgument(startOutputIndex <= maxOutputIndex);
    probeFind2Watch.start();
    ProbeCursor.Stats stats = cursor.next(records, startOutputIndex, maxOutputIndex);
    probeFind2Watch.stop();

    int outputRecords = stats.getRecordsFound();
    Preconditions.checkState(outputRecords <= maxOutputIndex - startOutputIndex + 1);
    projectProbe(buffers.getOutProbeProjectOffsets2B().memoryAddress(), startOutputIndex, outputRecords);
    projectBuild(buffers.getOutBuildProjectOffsets6B().memoryAddress(), buffers.getOutProbeProjectOffsets2B().memoryAddress(),
      startOutputIndex, outputRecords, buffers.getOutInvalidBuildKeyOffsets2B().memoryAddress(), stats.getNullKeyCount());
    return stats.isPartial() ? -outputRecords : outputRecords;
  }

  /**
   * Project any remaining build items that were not matched. Only used when doing a FULL or RIGHT join.
   * @return Negative output if records were output but batch wasn't completed. Positive output if batch was completed.
   */
  public int projectBuildNonMatches(int startOutputIndex, int maxOutputIndex) {
    Preconditions.checkArgument(startOutputIndex <= maxOutputIndex);
    assert projectUnmatchedBuild;
    projectBuildNonMatchesWatch.start();
    UnmatchedCursor.Stats stats = unmatchedCursor.next(startOutputIndex, maxOutputIndex);
    projectBuildNonMatchesWatch.stop();

    // Collect the keys for non matched records, and unpivot them to output
    int outputRecords = stats.getRecordsFound();
    allocateForUnpivot(startOutputIndex, maxOutputIndex);
    Preconditions.checkState(outputRecords <= maxOutputIndex - startOutputIndex + 1);
    try (FixedBlockVector fbv = new FixedBlockVector(allocator, buildKeyUnpivot.getBlockWidth());
         VariableBlockVector var = new VariableBlockVector(allocator, buildKeyUnpivot.getVariableCount());
    ) {
      fbv.ensureAvailableBlocks(outputRecords);
      var.ensureAvailableDataSpace(stats.getTotalVarSize());
      final long keyFixedVectorAddr = fbv.getMemoryAddress();
      final long keyVarVectorAddr = var.getMemoryAddress();
      // Collect all the pivoted keys for non matched records
      table.copyKeyToBuffer(buffers.getOutBuildProjectKeyOrdinals4B().memoryAddress(), outputRecords, keyFixedVectorAddr, keyVarVectorAddr);
      // Unpivot the keys for build side into output
      Unpivots.unpivotToAllocedOutput(buildKeyUnpivot, fbv, var, 0, outputRecords, startOutputIndex);
    }

    allocateOnlyProbe(startOutputIndex, maxOutputIndex);
    projectBuild(buffers.getOutBuildProjectOffsets6B().memoryAddress(), startOutputIndex, outputRecords);
    for (FieldVector v : buildKeyUnpivot.getOutputVectors()) {
      v.setValueCount(startOutputIndex + outputRecords);
    }
    return stats.isPartial() ? -stats.getRecordsFound() : stats.getRecordsFound();
  }

  private void allocateOnlyProbe(int startOutputIndex, int maxOutputIndex) {
    if (startOutputIndex == 0) {
      for (FieldBufferCopier c : probeCopiers) {
        c.allocate(maxOutputIndex + 1);
      }
    }
  }

  private void allocateForUnpivot(int startOutputIndex, int maxOutputIndex) {
    if (startOutputIndex == 0) {
      for (FieldVector v : buildKeyUnpivot.getOutputVectors()) {
        AllocationHelper.allocate(v, maxOutputIndex + 1, FieldBufferCopier.AVG_VAR_WIDTH);
      }
    }
  }

  /**
   * Project the build data (including keys from the probe)
   * @param offsetAddr
   * @param count
   */
  private void projectBuild(final long offsetAddr, final int startOutputIndex, final int count){
    buildCopyWatch.start();
    for (FieldBufferCopier c : buildCopiers) {
      outputCursor.setTargetIndex(startOutputIndex);
      c.copy(offsetAddr, count, outputCursor);
    }
    buildCopyWatch.stop();
  }

  /**
   * Project the build data (including keys from the probe)
   * @param offsetBuildAddr
   * @param count
   * @param nullKeyAddr
   * @param nullKeyCount
   */
  private void projectBuild(final long offsetBuildAddr, final long offsetProbeAddr, final int startOutputIdx,
                            final int count, final long nullKeyAddr, final int nullKeyCount){
    buildCopyWatch.start();
    // Copy the keys from probe batch to build side in output.
    if (projectUnmatchedProbe) {
      for (FieldBufferCopier c : keysCopiers) {
        outputCursor.setTargetIndex(startOutputIdx);
        c.copy(offsetProbeAddr, count, nullKeyAddr, nullKeyCount, outputCursor);
      }
    } else {
      for (FieldBufferCopier c : keysCopiers) {
        outputCursor.setTargetIndex(startOutputIdx);
        c.copy(offsetProbeAddr, count, outputCursor);
      }
    }
    for (FieldBufferCopier c : buildCopiers) {
      outputCursor.setTargetIndex(startOutputIdx);
      c.copy(offsetBuildAddr, count, outputCursor);
    }
    buildCopyWatch.stop();
  }

  /**
   * Project the probe data
   * @param sv4Addr
   * @param count
   */
  private void projectProbe(final long sv4Addr, final int startOutputIdx, final int count){
    probeCopyWatch.start();
    for(FieldBufferCopier c : probeCopiers){
      outputCursor.setTargetIndex(startOutputIdx);
      c.copy(sv4Addr, count, outputCursor);
    }
    probeCopyWatch.stop();
  }

  public long getProbeListTime(){
    return probeFind2Watch.elapsed(TimeUnit.NANOSECONDS);
  }

  public long getProbeCopyTime(){
    return probeCopyWatch.elapsed(TimeUnit.NANOSECONDS);
  }

  public long getBuildCopyTime(){
    return buildCopyWatch.elapsed(TimeUnit.NANOSECONDS);
  }

  public long getBuildNonMatchCopyTime(){
    return projectBuildNonMatchesWatch.elapsed(TimeUnit.NANOSECONDS);
  }

  public long getUnmatchedProbeCount() {
    return cursor == null ? 0 : cursor.getUnmatchedProbeCount();
  }

  public long getUnmatchedBuildKeyCount() {
    return unmatchedCursor == null ? 0 : unmatchedCursor.getUnmatchedBuildKeyCount();
  }

  @Override
  public void close() throws Exception {
    buffers.close();
  }

}

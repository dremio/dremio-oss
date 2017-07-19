/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.join.hash;

import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.common.hashtable.HashTable;

import java.util.BitSet;
import java.util.List;
import javax.inject.Named;

public abstract class HashJoinProbeTemplate implements HashJoinProbe {

  private static final int SHIFT_SIZE = 16;

  private VectorAccessible probeBatch;

  private int targetRecordsPerBatch;
  private HashTable hashTable;
  private boolean projectUnmatchedProbe;
  private boolean projectUnmatchedBuild;

  private int remainderBuildSetIndex = -1;
  private int remainderBuildElementIndex = -1;

  private int nextProbeIndex = 0;
  private int remainderBuildCompositeIndex = -1;

  private SelectionVector4[] links;
  private SelectionVector4[] starts;
  private BitSet[] matches;
  private int[] matchMaxes;

  @Override
  public void setupHashJoinProbe(
      FunctionContext functionContext,
      VectorAccessible buildBatch,
      VectorAccessible probeBatch,
      VectorAccessible outgoing,
      HashTable hashTable,
      JoinRelType joinRelType,
      List<BuildInfo> buildInfos,
      List<SelectionVector4> startIndices,
      int targetRecordsPerBatch) {

    links = new SelectionVector4[buildInfos.size()];
    matches = new BitSet[buildInfos.size()];
    matchMaxes = new int[buildInfos.size()];

    for (int i =0; i < links.length; i++) {
      links[i] = buildInfos.get(i).getLinks();
      matches[i] = buildInfos.get(i).getKeyMatchBitVector();
      matchMaxes[i] = buildInfos.get(i).getRecordCount();
    }

    starts = new SelectionVector4[startIndices.size()];
    for (int i = 0; i < starts.length; i++) {
      starts[i] = startIndices.get(i);
    }

    this.probeBatch = probeBatch;
    this.projectUnmatchedProbe = joinRelType == JoinRelType.LEFT || joinRelType == JoinRelType.FULL;
    this.projectUnmatchedBuild = joinRelType == JoinRelType.RIGHT || joinRelType == JoinRelType.FULL;
    this.hashTable = hashTable;
    this.targetRecordsPerBatch = targetRecordsPerBatch;

    doSetup(functionContext, buildBatch, probeBatch, outgoing);
  }

  /**
   * Project any remaining build items that were not matched. Only used when doing a FULL or RIGHT join.
   * @return Negative output if records were output but batch wasn't completed. Positive output if batch was completed.
   */
  @Override
  public int projectBuildNonMatches() {
    assert projectUnmatchedBuild;

    final int targetRecordsPerBatch = this.targetRecordsPerBatch;

    int outputRecords = 0;
    int remainderBuildSetIndex = this.remainderBuildSetIndex;

    BitSet currentBitset = remainderBuildSetIndex < 0 ? null : matches[remainderBuildSetIndex];

    int nextClearIndex = remainderBuildElementIndex;
    while(outputRecords < targetRecordsPerBatch) {
      if(nextClearIndex == -1){
        // we need to move to the next bit set since the current one has no more matches.
        remainderBuildSetIndex++;
        if (remainderBuildSetIndex < matches.length) {

          currentBitset = matches[remainderBuildSetIndex];
          nextClearIndex = 0;
        } else {
          // no bitsets left.
          this.remainderBuildSetIndex = matches.length;
          this.remainderBuildElementIndex = -1;
          return outputRecords;
        }
      }

      nextClearIndex = currentBitset.nextClearBit(nextClearIndex);
      if(nextClearIndex != -1){
        // the clear bit is only valid if it is within the batch it corresponds to.
        if(nextClearIndex >= matchMaxes[remainderBuildSetIndex]){
          nextClearIndex = -1;
        }else{
          int composite = (remainderBuildSetIndex << SHIFT_SIZE) | (nextClearIndex & HashTable.BATCH_MASK);
          projectBuildRecord(composite, outputRecords);
          outputRecords++;
          nextClearIndex++;
        }
      }
    }

    this.remainderBuildSetIndex = remainderBuildSetIndex;
    this.remainderBuildElementIndex = nextClearIndex;
    return -outputRecords;
  }

  /**
   * Probe with current batch. If we've run out of space, return a negative
   * record count. If we processed the entire incoming batch, return a positive
   * record count or zero.
   *
   * @return Negative if partial batch complete. Otherwise, all of probe batch
   *         is complete.
   */
  public int probeBatch() {
    final int targetRecordsPerBatch = this.targetRecordsPerBatch;
    final boolean projectUnmatchedProbe = this.projectUnmatchedProbe;
    final boolean projectUnmatchedBuild = this.projectUnmatchedBuild;
    final BitSet[] matches = this.matches;
    final SelectionVector4[] starts = this.starts;
    final SelectionVector4[] links = this.links;

    final HashTable hashTable = this.hashTable;

    // we have two incoming options: we're starting on a new batch or we're picking up an existing batch.
    final int probeMax = probeBatch.getRecordCount();
    int outputRecords = 0;
    int currentProbeIndex = this.nextProbeIndex;
    int currentCompositeBuildIdx = remainderBuildCompositeIndex;
    while (outputRecords < targetRecordsPerBatch && currentProbeIndex < probeMax) {

      // If we don't have a composite index, we're done with the current probe record and need to get another.
      if (currentCompositeBuildIdx == -1) {
        final int indexInBuild = hashTable.containsKey(currentProbeIndex, true);

        if (indexInBuild == -1) { // not a matching key.
          if (projectUnmatchedProbe) {
            projectProbeRecord(currentProbeIndex, outputRecords);
            outputRecords++;
          }
          currentProbeIndex++;
          continue;

        } else { // matching key
          /* The current probe record has a key that matches. Get the index
           * of the first row in the build side that matches the current key
           */
          currentCompositeBuildIdx = starts[indexInBuild / HashTable.BATCH_SIZE].get(indexInBuild % HashTable.BATCH_SIZE);
        }

      }

      /* Record in the build side at currentCompositeBuildIdx has a matching record in the probe
       * side. Set the bit corresponding to this index so if we are doing a FULL or RIGHT
       * join we keep track of which records we need to project at the end
       */
      if(projectUnmatchedBuild){
        matches[currentCompositeBuildIdx >>> SHIFT_SIZE].set(currentCompositeBuildIdx & HashTable.BATCH_MASK);
      }

      projectBuildRecord(currentCompositeBuildIdx, outputRecords);
      projectProbeRecord(currentProbeIndex, outputRecords);
      outputRecords++;

      /* Projected single row from the build side with matching key but there
       * may be more build rows with the same key. Check if that's the case
       */
      currentCompositeBuildIdx = links[currentCompositeBuildIdx >>> SHIFT_SIZE].get(currentCompositeBuildIdx & HashTable.BATCH_MASK);

      if (currentCompositeBuildIdx == -1) {
        /* We only had one row in the build side that matched the current key
         * from the probe side. Drain the next row in the probe side.
         */
        currentProbeIndex++;
      }
    }

    if(outputRecords == targetRecordsPerBatch){ // batch was full
      if(currentProbeIndex < probeMax){
        // we have remaining records to process, need to save our position for when we return.
        this.nextProbeIndex = currentProbeIndex;
        this.remainderBuildCompositeIndex = currentCompositeBuildIdx;
        return -outputRecords;
      }
    }

    // we need to clear the last saved position and tell the driver that we completed consuming the current batch.
    this.nextProbeIndex = 0;
    this.remainderBuildCompositeIndex = -1;
    return outputRecords;
  }

  public abstract void doSetup(
      @Named("context") FunctionContext context,
      @Named("buildBatch") VectorAccessible buildBatch,
      @Named("probeBatch") VectorAccessible probeBatch,
      @Named("outgoing") VectorAccessible outgoing);

  public abstract void projectBuildRecord(@Named("buildIndex") int buildIndex, @Named("outIndex") int outIndex);

  public abstract void projectProbeRecord(@Named("probeIndex") int probeIndex, @Named("outIndex") int outIndex);

}

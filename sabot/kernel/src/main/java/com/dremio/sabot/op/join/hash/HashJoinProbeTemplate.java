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
package com.dremio.sabot.op.join.hash;

import static com.dremio.sabot.op.common.hashtable.HashTable.BUILD_RECORD_LINK_SIZE;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.common.hashtable.HashTable;
import io.netty.util.internal.PlatformDependent;
import java.util.BitSet;
import java.util.List;
import javax.inject.Named;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.calcite.rel.core.JoinRelType;

public abstract class HashJoinProbeTemplate implements HashJoinProbe {

  private static final int SHIFT_SIZE = 16;

  private VectorAccessible probeBatch;

  private int targetRecordsPerBatch;
  private HashTable hashTable;
  private boolean projectUnmatchedProbe;
  private boolean projectUnmatchedBuild;

  /* The batch index of StartIndices for previous projectBuildNonMatches
   * It will be used to continue for next probe batch
   */
  private int remainderOrdinalBatchIndex = -1;
  /* The next index of the key in remainderOrdinalBatchIndex StartIndices for previous projectBuildNonMatches
   * It will be used to continue for next probe batch
   */
  private int remainderOrdinalOffset = -1;
  /* For each key in StartIndices, there are maybe many data records match its key.
   * Before all those records are processed, the number of output records reach the limit of output batch,
   * which means outputRecords >= targetRecordsPerBatch,
   * and then we need to indicate which data record is the first record that has not been processed.
   * remainderLinkBatch is the next link batch that should be processed,
   * and remainderLinkOffset is the next link record that should be processed.
   */
  private int remainderLinkBatch = -1;
  private int remainderLinkOffset = -1;

  private int nextProbeIndex = 0;
  private int remainderBuildLinkBatch = -1;
  private int remainderBuildLinkOffset = -1;

  private ArrowBuf[] links;
  private ArrowBuf[] starts;
  // Array of bitvectors. Keeps track of keys on the build side that matched any key on the probe
  // side
  private BitSet[] keyMatches;
  // The index of last key in last StartIndices batch in hash table
  private int maxOffsetForLastBatch;

  @Override
  public void setupHashJoinProbe(
      FunctionContext functionContext,
      VectorAccessible buildBatch,
      VectorAccessible probeBatch,
      VectorAccessible outgoing,
      HashTable hashTable,
      JoinRelType joinRelType,
      List<BuildInfo> buildInfos,
      List<ArrowBuf> startIndices,
      List<BitSet> keyMatchBitVectors,
      int maxHashTableIndex,
      int targetRecordsPerBatch) {

    links = new ArrowBuf[buildInfos.size()];

    for (int i = 0; i < links.length; i++) {
      links[i] = buildInfos.get(i).getLinks();
    }

    starts = new ArrowBuf[startIndices.size()];
    this.keyMatches = new BitSet[keyMatchBitVectors.size()];

    if (startIndices.size() > 0) {
      this.maxOffsetForLastBatch =
          maxHashTableIndex - (startIndices.size() - 1) * HashTable.BATCH_SIZE;
    } else {
      this.maxOffsetForLastBatch = -1;
    }
    for (int i = 0; i < starts.length; i++) {
      starts[i] = startIndices.get(i);
      keyMatches[i] = keyMatchBitVectors.get(i);
    }

    this.probeBatch = probeBatch;
    this.projectUnmatchedProbe = joinRelType == JoinRelType.LEFT || joinRelType == JoinRelType.FULL;
    this.projectUnmatchedBuild =
        joinRelType == JoinRelType.RIGHT || joinRelType == JoinRelType.FULL;
    this.hashTable = hashTable;
    this.targetRecordsPerBatch = targetRecordsPerBatch;

    doSetup(functionContext, buildBatch, probeBatch, outgoing);
  }

  /**
   * Project any remaining build items that were not matched. Only used when doing a FULL or RIGHT
   * join.
   *
   * @return Negative output if records were output but batch wasn't completed. Positive output if
   *     batch was completed.
   */
  @Override
  public int projectBuildNonMatches() {
    assert projectUnmatchedBuild;

    final int targetRecordsPerBatch = this.targetRecordsPerBatch;

    int outputRecords = 0;
    int remainderOrdinalBatchIndex = this.remainderOrdinalBatchIndex;
    int currentClearOrdinalOffset = remainderOrdinalOffset;
    int currentLinkBatch = remainderLinkBatch;
    int currentLinkOffset = remainderLinkOffset;

    BitSet currentBitset =
        remainderOrdinalBatchIndex < 0 ? null : keyMatches[remainderOrdinalBatchIndex];

    // determine the next set of unmatched bits.
    while (outputRecords < targetRecordsPerBatch) {
      if (currentClearOrdinalOffset == -1) {
        // we need to move to the next bit set since the current one has no more matches.
        remainderOrdinalBatchIndex++;
        if (remainderOrdinalBatchIndex < keyMatches.length) {

          currentBitset = keyMatches[remainderOrdinalBatchIndex];
          currentClearOrdinalOffset = 0;
          currentLinkBatch = -1;
          currentLinkOffset = -1;
        } else {
          // no bitsets left.
          remainderOrdinalBatchIndex = -1;
          currentLinkBatch = -1;
          currentLinkOffset = -1;
          break;
        }
      } else if ((remainderOrdinalBatchIndex == (keyMatches.length - 1))
          && (currentClearOrdinalOffset > maxOffsetForLastBatch)) {
        /* Current StartIndices is last batch and nextClearOrdinalOffset is greater than maxOffsetForLastBatch,
         * so no bitsets left.
         */
        remainderOrdinalBatchIndex = -1;
        currentClearOrdinalOffset = -1;
        currentLinkBatch = -1;
        currentLinkOffset = -1;
        break;
      }

      currentClearOrdinalOffset = currentBitset.nextClearBit(currentClearOrdinalOffset);
      if (currentClearOrdinalOffset != -1) {
        if (currentClearOrdinalOffset >= HashTable.BATCH_SIZE) {
          currentClearOrdinalOffset = -1;
        } else {
          if (currentLinkBatch == -1) {
            // Go through all the data records in BuildInfo from beginning of StartIndices to
            // collect the non matched data records
            ArrowBuf startIndex;
            startIndex = starts[remainderOrdinalBatchIndex];
            long linkMemAddr =
                startIndex.memoryAddress()
                    + currentClearOrdinalOffset * HashTable.BUILD_RECORD_LINK_SIZE;
            currentLinkBatch = PlatformDependent.getInt(linkMemAddr);
            currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(linkMemAddr + 4));
          }
          while ((currentLinkBatch != -1) && (outputRecords < targetRecordsPerBatch)) {
            int composite =
                (currentLinkBatch << SHIFT_SIZE) | (currentLinkOffset & HashTable.BATCH_MASK);
            projectBuildRecord(composite, outputRecords);
            outputRecords++;

            long linkMemAddr =
                links[currentLinkBatch].memoryAddress()
                    + currentLinkOffset * BUILD_RECORD_LINK_SIZE;
            currentLinkBatch = PlatformDependent.getInt(linkMemAddr);
            currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(linkMemAddr + 4));
          }
          if (currentLinkBatch != -1) {
            // More records for current key should be processed.
            break;
          }
          currentClearOrdinalOffset++;
        }
      }
    }

    this.remainderOrdinalBatchIndex = remainderOrdinalBatchIndex;
    this.remainderOrdinalOffset = currentClearOrdinalOffset;
    this.remainderLinkBatch = currentLinkBatch;
    this.remainderLinkOffset = currentLinkOffset;
    if (remainderOrdinalOffset == -1) {
      return outputRecords;
    } else {
      return -outputRecords;
    }
  }

  /**
   * Probe with current batch. If we've run out of space, return a negative record count. If we
   * processed the entire incoming batch, return a positive record count or zero.
   *
   * @return Negative if partial batch complete. Otherwise, all of probe batch is complete.
   */
  @Override
  public int probeBatch() {
    final int targetRecordsPerBatch = this.targetRecordsPerBatch;
    final boolean projectUnmatchedProbe = this.projectUnmatchedProbe;
    final boolean projectUnmatchedBuild = this.projectUnmatchedBuild;
    final BitSet[] keyMatches = this.keyMatches;
    final ArrowBuf[] starts = this.starts;
    final ArrowBuf[] links = this.links;

    final HashTable hashTable = this.hashTable;

    // we have two incoming options: we're starting on a new batch or we're picking up an existing
    // batch.
    final int probeMax = probeBatch.getRecordCount();
    int outputRecords = 0;
    int currentProbeIndex = this.nextProbeIndex;
    int currentLinkBatch = this.remainderBuildLinkBatch;
    int currentLinkOffset = this.remainderBuildLinkOffset;
    while (outputRecords < targetRecordsPerBatch && currentProbeIndex < probeMax) {
      final int indexInBuild = hashTable.containsKey(currentProbeIndex, true);
      if (indexInBuild == -1) { // not a matching key.
        if (projectUnmatchedProbe) {
          projectProbeRecord(currentProbeIndex, outputRecords);
          outputRecords++;
        }
        currentProbeIndex++;
      } else { // matching key
        if (currentLinkBatch == -1) {
          /* The current probe record has a key that matches. Get the index
           * of the first row in the build side that matches the current key
           */
          final long memStart =
              starts[indexInBuild >> SHIFT_SIZE].memoryAddress()
                  + ((indexInBuild) % HashTable.BATCH_SIZE) * BUILD_RECORD_LINK_SIZE;

          currentLinkBatch = PlatformDependent.getInt(memStart);
          currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(memStart + 4));

          /* The key in the build side at indexInBuild has a matching key in the probe side.
           * Set the bit corresponding to this index so if we are doing a FULL or RIGHT join
           * we keep track of which keys and records we need to project at the end
           */
          if (projectUnmatchedBuild) {
            keyMatches[indexInBuild >> SHIFT_SIZE].set(indexInBuild % HashTable.BATCH_SIZE);
          }
        }
        while ((currentLinkBatch != -1) && (outputRecords < targetRecordsPerBatch)) {
          projectBuildRecord(currentLinkBatch << SHIFT_SIZE | currentLinkOffset, outputRecords);
          projectProbeRecord(currentProbeIndex, outputRecords);
          outputRecords++;

          long linkMemAddr =
              links[currentLinkBatch].memoryAddress() + currentLinkOffset * BUILD_RECORD_LINK_SIZE;
          currentLinkBatch = PlatformDependent.getInt(linkMemAddr);
          currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(linkMemAddr + 4));
        }
        if (currentLinkBatch != -1) {
          // More records for current key should be processed.
          break;
        }
        currentProbeIndex++;
      }
    }

    if (outputRecords == targetRecordsPerBatch) { // batch was full
      if (currentProbeIndex < probeMax) {
        // we have remaining records to process, need to save our position for when we return.
        this.nextProbeIndex = currentProbeIndex;
        this.remainderBuildLinkBatch = currentLinkBatch;
        this.remainderBuildLinkOffset = currentLinkOffset;
        return -outputRecords;
      }
    }

    // we need to clear the last saved position and tell the driver that we completed consuming the
    // current batch.
    this.nextProbeIndex = 0;
    this.remainderBuildLinkBatch = -1;
    this.remainderBuildLinkOffset = -1;
    return outputRecords;
  }

  public abstract void doSetup(
      @Named("context") FunctionContext context,
      @Named("buildBatch") VectorAccessible buildBatch,
      @Named("probeBatch") VectorAccessible probeBatch,
      @Named("outgoing") VectorAccessible outgoing);

  public abstract void projectBuildRecord(
      @Named("buildIndex") long buildIndex, @Named("outIndex") int outIndex);

  public abstract void projectProbeRecord(
      @Named("probeIndex") int probeIndex, @Named("outIndex") int outIndex);
}

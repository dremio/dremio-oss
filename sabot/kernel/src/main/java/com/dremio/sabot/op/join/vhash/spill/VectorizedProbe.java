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
package com.dremio.sabot.op.join.vhash.spill;

import static com.dremio.sabot.op.common.hashtable.HashTable.BUILD_RECORD_LINK_SIZE;
import static com.dremio.sabot.op.common.ht2.LBlockHashTable.ORDINAL_SIZE;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.op.common.hashtable.HashTable;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Unpivots;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.copier.ConditionalFieldBufferCopier6;
import com.dremio.sabot.op.copier.FieldBufferCopier;
import com.dremio.sabot.op.copier.FieldBufferCopier6;
import com.dremio.sabot.op.join.hash.BuildInfo;
import com.google.common.base.Stopwatch;

import io.netty.util.internal.PlatformDependent;

public class VectorizedProbe implements AutoCloseable {

  private static final int SHIFT_SIZE = 16;
  private static final int BATCH_OFFSET_SIZE = 2;
  private static final int BATCH_INDEX_SIZE = 4;

  public static final int SKIP = -1;

  private BufferAllocator allocator;
  private ArrowBuf[] links;
  private ArrowBuf[] starts;
  // Array of bitvectors. Keeps track of keys on the build side that matched any key on the probe side
  private MatchBitSet[] keyMatches;
  // The index of last key in last StartIndices batch in hash table
  private int maxOffsetForLastBatch;
  private ArrowBuf projectProbeSv2;
  /* Maintain all the offsets of the non matched records in output
   * Used to set keys's validity of those records to 0 after copying keys from probe side to build side in output
   */
  private ArrowBuf projectNullKeyOffset;
  // The memory address of arrow buffer in projectNullKeyOffset
  private long projectNullKeyOffsetAddr;
  private ArrowBuf projectBuildOffsetBuf;
  private long projectBuildOffsetAddr;
  private ArrowBuf projectBuildKeyOffsetBuf;
  private long projectBuildKeyOffsetAddr;
  private long probeSv2Addr;

  private VectorizedSpillingHashJoinOperator.Mode mode = VectorizedSpillingHashJoinOperator.Mode.UNKNOWN;

  private JoinTable table;
  private List<FieldVector> buildOutputs;
  private List<FieldBufferCopier> buildCopiers;
  private List<FieldBufferCopier> probeCopiers;
  /* Used to copy the key vectors from probe side to build side in output.
   * For non matched records in probe side, the keys in build side will be indicated as SKIP and will be set to null.
   * It's only for VECTORIZED_GENERIC.
   * For VECTORIZED_BIGINT, we only have one eight byte key, and we keep it in the hyper container,
   * so it's not needed to copy the key vector from probe side to build side in output.
   */
  private List<FieldBufferCopier> keysCopiers;
  private int targetRecordsPerBatch;
  private boolean projectUnmatchedProbe;
  private boolean projectUnmatchedBuild;
  private final Stopwatch probeFind2Watch = Stopwatch.createUnstarted();
  private final Stopwatch buildCopyWatch = Stopwatch.createUnstarted();
  private final Stopwatch probeCopyWatch = Stopwatch.createUnstarted();
  private final Stopwatch projectBuildNonMatchesWatch = Stopwatch.createUnstarted();
  private NullComparator nullMask;

  private ArrowBuf probed;
  private PivotDef pivot;
  /* Used to unpivot keys to output for non matched records in build side
   * Only for VECTORIZED_GENERIC
   */
  private PivotDef buildUnpivot;
  /* The batch index of StartIndices in previous projectBuildNonMatches call
   * It will be used to continue for next probe batch
   */
  private int remainderOrdinalBatchIndex = -1;
  /* The offset of next key in StartIndices batch in previous projectBuildNonMatches call */
  private int remainderOrdinalOffset = -1;
  /* For each key in StartIndices, there are maybe many data records match its key.
   * Before all those records are processed, the number of output records reach the limit of output batch,
   * which means outputRecords >= targetRecordsPerBatch,
   * and then we need to indicate which record is the first record that has not been processed.
   * remainderLinkBatch is the next link batch that should be processed,
   * and remainderLinkOffset is the offset of next record that should be processed.
   * and then we need to indicate which data record is the first record that has not been processed.
   */
  private int remainderLinkBatch = -1;
  private int remainderLinkOffset = -1;
  private int nextProbeIndex = 0;
  private long unmatchedProbeCount = 0;
  private long maxHashTableIndex = 0;

  public VectorizedProbe() {
  }

  public void setup(
      BufferAllocator allocator,
      final ExpandableHyperContainer buildBatch,
      final VectorAccessible probeBatch,
      // Contains all vectors in probe side output
      final List<FieldVector> probeOutputs,
      /* Contains only carry over vectors in build side output for VECTORIZED_GENERIC
       * Contains all field vectors in build side output for VECTORIZED_BIGINT
       */
      final List<FieldVector> buildOutputs,
      /* Contains the key field vectors in incoming probe side batch for VECTORIZED_GENERIC
       * Only for VECTORIZED_GENERIC
       */
      final List<FieldVector> probeIncomingKeys,
      /* Contains the key field vectors in build side output for VECTORIZED_GENERIC
       * Only for VECTORIZED_GENERIC
       */
      final List<FieldVector> buildOutputKeys,
      VectorizedSpillingHashJoinOperator.Mode mode,
      JoinRelType joinRelType,
      List<BuildInfo> buildInfos,
      List<ArrowBuf> startIndices,
      List<MatchBitSet> keyMatchBitVectors,
      int maxHashTableIndex,
      JoinTable table,
      // Used to pivot the keys in incoming build batch into hash table
      PivotDef pivot,
      // Used to unpivot the keys in hash table to build side output
      PivotDef buildUnpivot,
      int targetRecordsPerBatch,
      final NullComparator nullMask){

    this.nullMask = nullMask;
    this.pivot = pivot;
    this.buildUnpivot = buildUnpivot;
    this.allocator = allocator;
    this.table = table;
    this.links = new ArrowBuf[buildInfos.size()];

    for (int i =0; i < links.length; i++) {
      links[i] = buildInfos.get(i).getLinks();
    }

    this.starts = new ArrowBuf[startIndices.size()];
    this.keyMatches = new MatchBitSet[keyMatchBitVectors.size()];

    if (startIndices.size() > 0) {
      this.maxOffsetForLastBatch = maxHashTableIndex - (startIndices.size() - 1) * HashTable.BATCH_SIZE;
    } else {
      this.maxOffsetForLastBatch = -1;
    }
    this.maxHashTableIndex = maxHashTableIndex;
    for (int i = 0; i < starts.length; i++) {
      starts[i] = startIndices.get(i);
      keyMatches[i] = keyMatchBitVectors.get(i);
    }

    this.projectUnmatchedBuild = joinRelType == JoinRelType.RIGHT || joinRelType == JoinRelType.FULL;
    this.projectUnmatchedProbe = joinRelType == JoinRelType.LEFT || joinRelType == JoinRelType.FULL;
    this.targetRecordsPerBatch = targetRecordsPerBatch;
    this.projectProbeSv2 = allocator.buffer(targetRecordsPerBatch * BATCH_OFFSET_SIZE);
    this.probeSv2Addr = projectProbeSv2.memoryAddress();
    // first 4 bytes (int) are for batch index and rest 2 bytes are offset within the batch
    this.projectBuildOffsetBuf = allocator.buffer(targetRecordsPerBatch * BUILD_RECORD_LINK_SIZE);
    this.projectBuildOffsetAddr = projectBuildOffsetBuf.memoryAddress();
    this.projectBuildKeyOffsetBuf = allocator.buffer(targetRecordsPerBatch * ORDINAL_SIZE);
    this.projectBuildKeyOffsetAddr = projectBuildKeyOffsetBuf.memoryAddress();

    this.mode = mode;

    this.buildOutputs = buildOutputs;
    if (table.size() > 0) {
      this.buildCopiers = projectUnmatchedProbe  ?
        ConditionalFieldBufferCopier6.getFourByteCopiers(VectorContainer.getHyperFieldVectors(buildBatch), buildOutputs) :
        FieldBufferCopier6.getFourByteCopiers(VectorContainer.getHyperFieldVectors(buildBatch), buildOutputs);
    } else {
      this.buildCopiers = Collections.emptyList();
    }

    /* For VECTORIZED_GENERIC, we don't keep the key vectors in hyper container,
     * and then we need to copy keys from probe batch to build side in output for matched and non matched records,
     * otherwise eight byte hash table is used, we keep the key vector in hyper container,
     * and then we don't need to copy keys from probe batch to build side in output for matched and non matched records.
     */
    if (this.mode == VectorizedSpillingHashJoinOperator.Mode.VECTORIZED_GENERIC) {
      // create copier for copying keys from probe batch to build side output
      if (probeIncomingKeys.size() > 0) {
        this.keysCopiers = FieldBufferCopier.getCopiers(probeIncomingKeys, buildOutputKeys);
      } else {
        this.keysCopiers = Collections.emptyList();
      }

      this.projectNullKeyOffset = allocator.buffer(targetRecordsPerBatch * BATCH_OFFSET_SIZE);
      this.projectNullKeyOffsetAddr = projectNullKeyOffset.memoryAddress();
    } else {
      this.projectNullKeyOffsetAddr = 0;
      this.keysCopiers = null;
    }

    this.probeCopiers = FieldBufferCopier.getCopiers(VectorContainer.getFieldVectors(probeBatch), probeOutputs);
  }

  /**
   * Find all the probe records that match the hash table.
   */
  private void findMatches(final int records){
    if(probed == null || probed.capacity() < records * ORDINAL_SIZE){
      if(probed != null){
        probed.release();
        probed = null;
      }

      probed = allocator.buffer(records * ORDINAL_SIZE);
    }
    long offsetAddr = probed.memoryAddress();

    this.table.find(offsetAddr, records);
  }

  /**
   * Probe with current batch. If we've run out of space, return a negative
   * record count. If we processed the entire incoming batch, return a positive
   * record count or zero.
   *
   * @return Negative if partial batch complete. Otherwise, all of probe batch
   *         is complete.
   */
  public int probeBatch(final int records) {
    final int targetRecordsPerBatch = this.targetRecordsPerBatch;
    final boolean projectUnmatchedProbe = this.projectUnmatchedProbe;
    final MatchBitSet[] keyMatches = this.keyMatches;
    final ArrowBuf[] starts = this.starts;
    final ArrowBuf[] links = this.links;
    long unmatchedProbeCount = this.unmatchedProbeCount;

    // we have two incoming options: we're starting on a new batch or we're picking up an existing batch.
    final int probeMax = records;
    int outputRecords = 0;
    int currentProbeIndex = this.nextProbeIndex;

    if(currentProbeIndex == 0){
      // when this is a new batch, we need to pivot the incoming data and then find all the matches.
      findMatches(records);
    }

    final long foundAddr = this.probed.memoryAddress();
    final long projectBuildOffsetAddr = this.projectBuildOffsetAddr;
    final long probeSv2Addr = this.probeSv2Addr;
    int currentLinkBatch = this.remainderLinkBatch;
    int currentLinkOffset = this.remainderLinkOffset;

    probeFind2Watch.start();

    if (mode == VectorizedSpillingHashJoinOperator.Mode.VECTORIZED_GENERIC) {
      final long projectNullKeyOffsetAddr = this.projectNullKeyOffsetAddr;
      short nullKeyCount = 0;

      while (outputRecords < targetRecordsPerBatch && currentProbeIndex < probeMax) {
        final int indexInBuild = PlatformDependent.getInt(foundAddr + currentProbeIndex * ORDINAL_SIZE);
        if (indexInBuild == -1) { // not a matching key.
          if (projectUnmatchedProbe) {
            PlatformDependent.putShort(probeSv2Addr + outputRecords * BATCH_OFFSET_SIZE, (short) currentProbeIndex);
            /* The build side keys of output should be null
             * Maintain all the index of the output records that have a null key
             * Used to set validity to 0 after copying keys from probe side to build side in output
             */
            PlatformDependent.putShort(projectNullKeyOffsetAddr + nullKeyCount * BATCH_OFFSET_SIZE, (short) outputRecords);
            PlatformDependent.putInt(projectBuildOffsetAddr + outputRecords * BUILD_RECORD_LINK_SIZE, SKIP);
            nullKeyCount ++;
            outputRecords++;
          }
          unmatchedProbeCount++;
          currentProbeIndex++;
        } else { // matching key
          if (currentLinkBatch == -1) {
            /* The current probe record has a key that matches. Get the index
             * of the first row in the build side that matches the current key
             */
            final long memStart = starts[indexInBuild >> SHIFT_SIZE].memoryAddress() +
              ((indexInBuild) % HashTable.BATCH_SIZE) * BUILD_RECORD_LINK_SIZE;

            currentLinkBatch = PlatformDependent.getInt(memStart);
            currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(memStart + BATCH_INDEX_SIZE));

            /* The key in the build side at indexInBuild has a matching key in the probe side.
             * Set the bit corresponding to this index so if we are doing a FULL or RIGHT join
             * we keep track of which keys and records we need to project at the end
             */
            keyMatches[indexInBuild >> SHIFT_SIZE].set(indexInBuild % HashTable.BATCH_SIZE);
          }
          while ((currentLinkBatch != -1) && (outputRecords < targetRecordsPerBatch)) {
            PlatformDependent.putShort(probeSv2Addr + outputRecords * BATCH_OFFSET_SIZE, (short) currentProbeIndex);
            final long projectBuildOffsetAddrStart = projectBuildOffsetAddr + outputRecords * BUILD_RECORD_LINK_SIZE;
            PlatformDependent.putInt(projectBuildOffsetAddrStart, currentLinkBatch);
            PlatformDependent.putShort(projectBuildOffsetAddrStart + BATCH_INDEX_SIZE, (short)currentLinkOffset);
            outputRecords++;

            long linkMemAddr = links[currentLinkBatch].memoryAddress() + currentLinkOffset * BUILD_RECORD_LINK_SIZE;
            currentLinkBatch = PlatformDependent.getInt(linkMemAddr);
            currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(linkMemAddr + BATCH_INDEX_SIZE));
          }
          if (currentLinkBatch != -1) {
            /* Output batch is full, we should exit now,
             * but more records for current key should be processed.
             */
            break;
          }
          currentProbeIndex++;
        }
      }
      probeFind2Watch.stop();

      projectProbe(probeSv2Addr, outputRecords);
      projectBuild(projectBuildOffsetAddr, probeSv2Addr, outputRecords, projectNullKeyOffsetAddr, nullKeyCount);
    } else {
      while (outputRecords < targetRecordsPerBatch && currentProbeIndex < probeMax) {
        final int indexInBuild = PlatformDependent.getInt(foundAddr + currentProbeIndex * ORDINAL_SIZE);
        if (indexInBuild == -1) { // not a matching key.
          if (projectUnmatchedProbe) {
            PlatformDependent.putShort(probeSv2Addr + outputRecords * BATCH_OFFSET_SIZE, (short) currentProbeIndex);
            PlatformDependent.putInt(projectBuildOffsetAddr + outputRecords * BUILD_RECORD_LINK_SIZE, SKIP);
            outputRecords++;
          }
          unmatchedProbeCount++;
          currentProbeIndex++;
        } else { // matching key
          if (currentLinkBatch == -1) {
            /* The current probe record has a key that matches. Get the index
             * of the first row in the build side that matches the current key
             */
            final long memStart = starts[indexInBuild >> SHIFT_SIZE].memoryAddress() +
              ((indexInBuild) % HashTable.BATCH_SIZE) * BUILD_RECORD_LINK_SIZE;

            currentLinkBatch = PlatformDependent.getInt(memStart);
            currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(memStart + BATCH_INDEX_SIZE));

            /* The key in the build side at indexInBuild has a matching key in the probe side.
             * Set the bit corresponding to this index so if we are doing a FULL or RIGHT join
             * we keep track of which keys and records we need to project at the end
             */
            keyMatches[indexInBuild >> SHIFT_SIZE].set(indexInBuild % HashTable.BATCH_SIZE);
          }
          while ((currentLinkBatch != -1) && (outputRecords < targetRecordsPerBatch)) {
            PlatformDependent.putShort(probeSv2Addr + outputRecords * BATCH_OFFSET_SIZE, (short) currentProbeIndex);
            final long projectBuildOffsetAddrStart = projectBuildOffsetAddr + outputRecords * BUILD_RECORD_LINK_SIZE;
            PlatformDependent.putInt(projectBuildOffsetAddrStart, currentLinkBatch);
            PlatformDependent.putShort(projectBuildOffsetAddrStart + BATCH_INDEX_SIZE, (short)currentLinkOffset);
            outputRecords++;

            long linkMemAddr = links[currentLinkBatch].memoryAddress() + currentLinkOffset * BUILD_RECORD_LINK_SIZE;
            currentLinkBatch = PlatformDependent.getInt(linkMemAddr);
            currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(linkMemAddr + BATCH_INDEX_SIZE));
          }
          if (currentLinkBatch != -1) {
            /* Output batch is full, we should exit now,
             * but more records for current key should be processed.
             */
            break;
          }
          currentProbeIndex++;
        }
      }
      probeFind2Watch.stop();

      projectProbe(probeSv2Addr, outputRecords);
      projectBuild(projectBuildOffsetAddr, outputRecords);
    }
    if(outputRecords == targetRecordsPerBatch){ // batch was full
      if(currentProbeIndex < probeMax){
        // we have remaining records to process, need to save our position for when we return.
        this.nextProbeIndex = currentProbeIndex;
        this.remainderLinkBatch = currentLinkBatch;
        this.remainderLinkOffset = currentLinkOffset;
        this.unmatchedProbeCount = unmatchedProbeCount;
        return -outputRecords;
      }
    }

    // we need to clear the last saved position and tell the driver that we completed consuming the current batch.
    this.nextProbeIndex = 0;
    this.remainderLinkBatch = -1;
    this.remainderLinkOffset = -1;
    this.unmatchedProbeCount = unmatchedProbeCount;
    return outputRecords;
  }

  /**
   * Project any remaining build items that were not matched. Only used when doing a FULL or RIGHT join.
   * @return Negative output if records were output but batch wasn't completed. Positive output if batch was completed.
   */
  public int projectBuildNonMatches() {
    assert projectUnmatchedBuild;
    projectBuildNonMatchesWatch.start();

    final int targetRecordsPerBatch = this.targetRecordsPerBatch;

    int outputRecords = 0;
    int remainderOrdinalBatchIndex = this.remainderOrdinalBatchIndex;
    int currentClearOrdinalOffset = remainderOrdinalOffset;
    int currentLinkBatch = remainderLinkBatch;
    int currentLinkOffset = remainderLinkOffset;
    int baseOrdinalBatchCount = remainderOrdinalBatchIndex * HashTable.BATCH_SIZE;

    MatchBitSet currentBitset = remainderOrdinalBatchIndex < 0 ? null : keyMatches[remainderOrdinalBatchIndex];

    final long projectBuildOffsetAddr = this.projectBuildOffsetAddr;

    if (this.mode == VectorizedSpillingHashJoinOperator.Mode.VECTORIZED_GENERIC) {

      // The total size of the variable keys that are non matched in build side
      int totalVarSize = 0;
      final long projectBuildKeyOffsetAddr = this.projectBuildKeyOffsetAddr;
      BlockJoinTable table = (BlockJoinTable)this.table;

      // determine the next set of unmatched bits.
      while(outputRecords < targetRecordsPerBatch) {
        if (currentClearOrdinalOffset == -1) {
          // we need to move to the next bit set since the current one has no more matches.
          remainderOrdinalBatchIndex++;
          baseOrdinalBatchCount += HashTable.BATCH_SIZE;
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
        } else if ((remainderOrdinalBatchIndex == (keyMatches.length - 1)) && (currentClearOrdinalOffset > maxOffsetForLastBatch)) {
          /* Current StartIndices is last batch and currentClearOrdinalOffset is greater than maxOffsetForLastBatch,
           * so no bitsets left.
           */
          remainderOrdinalBatchIndex = -1;
          currentClearOrdinalOffset = -1;
          currentLinkBatch = -1;
          currentLinkOffset = -1;
          break;
        }

        currentClearOrdinalOffset = currentBitset.nextUnSetBit(currentClearOrdinalOffset);
        if (currentClearOrdinalOffset != -1) {
          if (currentClearOrdinalOffset >= HashTable.BATCH_SIZE) {
            currentClearOrdinalOffset = -1;
          } else {
            if (currentLinkBatch == -1) {
              // Go through all the data records in BuildInfo from beginning of StartIndices to collect the non matched data records
              ArrowBuf startIndex = starts[remainderOrdinalBatchIndex];
              long linkMemAddr = startIndex.memoryAddress() + currentClearOrdinalOffset * HashTable.BUILD_RECORD_LINK_SIZE;
              currentLinkBatch = PlatformDependent.getInt(linkMemAddr);
              currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(linkMemAddr + BATCH_INDEX_SIZE));
            }
            while ((currentLinkBatch != -1) && (outputRecords < targetRecordsPerBatch)) {
              final long projectBuildOffsetAddrStart = projectBuildOffsetAddr + outputRecords * BUILD_RECORD_LINK_SIZE;
              PlatformDependent.putInt(projectBuildOffsetAddrStart, currentLinkBatch);
              PlatformDependent.putShort(projectBuildOffsetAddrStart + BATCH_INDEX_SIZE, (short)currentLinkOffset);
              // Get the length of variable key and added it to totalSize
              totalVarSize += table.getVarKeyLength(baseOrdinalBatchCount + currentClearOrdinalOffset);
              // Maintain the ordinal of the key for unpivot later
              PlatformDependent.putInt(projectBuildKeyOffsetAddr + outputRecords * ORDINAL_SIZE,baseOrdinalBatchCount + currentClearOrdinalOffset);
              outputRecords++;

              long linkMemAddr = links[currentLinkBatch].memoryAddress() + currentLinkOffset * BUILD_RECORD_LINK_SIZE;
              currentLinkBatch = PlatformDependent.getInt(linkMemAddr);
              currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(linkMemAddr + BATCH_INDEX_SIZE));
            }
            if (currentLinkBatch != -1) {
              /* Output batch is full, we should exit now,
               * but more records for current key should be processed.
               */
              break;
            }
            currentClearOrdinalOffset ++;
          }
        }
      }

      projectBuildNonMatchesWatch.stop();

      // Collect the keys for non matched records, and unpivot them to output
      try (FixedBlockVector fbv = new FixedBlockVector(allocator, buildUnpivot.getBlockWidth());
           VariableBlockVector var = new VariableBlockVector(allocator, buildUnpivot.getVariableCount());
      ) {
        fbv.ensureAvailableBlocks(outputRecords);
        var.ensureAvailableDataSpace(totalVarSize);
        final long keyFixedVectorAddr = fbv.getMemoryAddress();
        final long keyVarVectorAddr = var.getMemoryAddress();
        long offsetAddr = projectBuildKeyOffsetAddr;
        int varOffset = 0;
        int ordinal;
        // Collect all the pivoted keys for non matched records
        table.copyKeyToBuffer(projectBuildKeyOffsetAddr, outputRecords, keyFixedVectorAddr, keyVarVectorAddr);
        // Unpivot the keys for build side into output
        Unpivots.unpivot(buildUnpivot, fbv, var, 0, outputRecords);
      }
    } else {
      // For eight byte key, we keep key vector in hyper container, so we don't need to unpivot them

      // determine the next set of unmatched bits.
      while(outputRecords < targetRecordsPerBatch) {
        if (currentClearOrdinalOffset == -1) {
          // we need to move to the next bit set since the current one has no more matches.
          remainderOrdinalBatchIndex++;
          baseOrdinalBatchCount += HashTable.BATCH_SIZE;
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
        } else if ((remainderOrdinalBatchIndex == (keyMatches.length - 1)) && (currentClearOrdinalOffset > maxOffsetForLastBatch)) {
          /* Current StartIndices is last batch and currentClearOrdinalOffset is greater than maxOffsetForLastBatch,
           * so no bitsets left.
           */
          remainderOrdinalBatchIndex = -1;
          currentClearOrdinalOffset = -1;
          currentLinkBatch = -1;
          currentLinkOffset = -1;
          break;
        }

        currentClearOrdinalOffset = currentBitset.nextUnSetBit(currentClearOrdinalOffset);
        if (currentClearOrdinalOffset != -1) {
          if (currentClearOrdinalOffset >= HashTable.BATCH_SIZE) {
            currentClearOrdinalOffset = -1;
          } else {
            if (currentLinkBatch == -1) {
              // Go through all the data records in BuildInfo from beginning of StartIndices to collect the non matched data records
              ArrowBuf startIndex = starts[remainderOrdinalBatchIndex];
              long linkMemAddr = startIndex.memoryAddress() + currentClearOrdinalOffset * HashTable.BUILD_RECORD_LINK_SIZE;
              currentLinkBatch = PlatformDependent.getInt(linkMemAddr);
              currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(linkMemAddr + BATCH_INDEX_SIZE));
            }
            while ((currentLinkBatch != -1) && (outputRecords < targetRecordsPerBatch)) {
              final long projectBuildOffsetAddrStart = projectBuildOffsetAddr + outputRecords * BUILD_RECORD_LINK_SIZE;
              PlatformDependent.putInt(projectBuildOffsetAddrStart, currentLinkBatch);
              PlatformDependent.putShort(projectBuildOffsetAddrStart + BATCH_INDEX_SIZE, (short)currentLinkOffset);
              outputRecords++;

              long linkMemAddr = links[currentLinkBatch].memoryAddress() + currentLinkOffset * BUILD_RECORD_LINK_SIZE;
              currentLinkBatch = PlatformDependent.getInt(linkMemAddr);
              currentLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(linkMemAddr + BATCH_INDEX_SIZE));
            }
            if (currentLinkBatch != -1) {
              /* Output batch is full, we should exit now,
               * but more records for current key should be processed.
               */
              break;
            }
            currentClearOrdinalOffset++;
          }
        }
      }
      projectBuildNonMatchesWatch.stop();
    }

    allocateOnlyProbe(outputRecords);
    projectBuild(projectBuildOffsetAddr, outputRecords);

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

  public long getUnmatchedBuildKeyCount() {
    long matchedCount = 0;
    for (int i = 0; i < keyMatches.length; i++) {
      matchedCount += keyMatches[i].cardinality();
    }
    return this.maxHashTableIndex + 1 - matchedCount;
  }

  private void allocateOnlyProbe(int records){
    for(FieldBufferCopier c : probeCopiers){
      c.allocate(records);
    }
  }

  /**
   * Project the build data (including keys from the probe)
   * @param offsetAddr
   * @param count
   */
  private void projectBuild(final long offsetAddr, final int count){
    buildCopyWatch.start();
    if (buildCopiers.size() == 0) {
      // No data in build side
      final List<FieldVector> buildOutputs = this.buildOutputs;
      for (FieldVector fieldVector : buildOutputs) {
        fieldVector.allocateNew();
      }
    } else {
      for (FieldBufferCopier c : buildCopiers) {
        c.copy(offsetAddr, count);
      }
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
  private void projectBuild(final long offsetBuildAddr, final long offsetProbeAddr, final int count, final long nullKeyAddr, final int nullKeyCount){
    buildCopyWatch.start();
    // Copy the keys from probe batch to build side in output.
    if (projectUnmatchedProbe) {
      for (FieldBufferCopier c : keysCopiers) {
        c.copy(offsetProbeAddr, count, nullKeyAddr, nullKeyCount);
      }
    } else {
      for (FieldBufferCopier c : keysCopiers) {
        c.copy(offsetProbeAddr, count);
      }
    }
    if (buildCopiers.size() == 0) {
      // No data in build side except keys
      final List<FieldVector> buildOutputs = this.buildOutputs;
      for (FieldVector fieldVector : buildOutputs) {
        fieldVector.allocateNew();
      }
    } else {
      for (FieldBufferCopier c : buildCopiers) {
        c.copy(offsetBuildAddr, count);
      }
    }
    buildCopyWatch.stop();
  }

  /**
   * Project the probe data
   * @param sv4Addr
   * @param count
   */
  private void projectProbe(final long sv4Addr, final int count){
    probeCopyWatch.start();
    for(FieldBufferCopier c : probeCopiers){
      c.copy(sv4Addr, count);
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
    return unmatchedProbeCount;
  }

  @Override
  public void close() throws Exception {
    try{
      AutoCloseables.close(projectBuildOffsetBuf, projectProbeSv2, projectNullKeyOffset, projectBuildKeyOffsetBuf, probed);
    } finally {
      projectBuildOffsetBuf = null;
      projectProbeSv2 = null;
      projectNullKeyOffset = null;
      projectBuildKeyOffsetBuf = null;
      probed = null;
    }
  }

}

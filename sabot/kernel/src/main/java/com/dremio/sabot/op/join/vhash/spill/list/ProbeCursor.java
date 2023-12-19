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
package com.dremio.sabot.op.join.vhash.spill.list;

import static com.dremio.sabot.op.common.hashtable.HashTable.BUILD_RECORD_LINK_SIZE;
import static com.dremio.sabot.op.common.ht2.LBlockHashTable.ORDINAL_SIZE;
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.BATCH_INDEX_SIZE;
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.BATCH_OFFSET_SIZE;
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.ELEMENT_SIZE;
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.HEAD_SIZE;
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.NEXT_OFFSET;
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.TERMINAL;
import static com.dremio.sabot.op.join.vhash.spill.partition.VectorizedProbe.SKIP;

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.op.join.vhash.HashJoinExtraMatcher;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;

import io.netty.util.internal.PlatformDependent;

/**
 * Maintains a specific probe position as part of a join probe operation. This
 * is a somewhat destructive action since it will flip the sign bit of the head
 * list of the list we're interacting with for items that have been matched.
 */
public class ProbeCursor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProbeCursor.class);

  private final PageListMultimap list;
  private final ArrowBuf inProbeSV2Buf;
  private final boolean projectUnmatchedBuild;
  private final boolean projectUnmatchedProbe;

  private final ProbeBuffers buffers;
  private Location location;
  private long unmatchedProbeCount;

  public static ProbeCursor startProbe(PageListMultimap list, ArrowBuf inProbeSV2Buf, ProbeBuffers buffers,
                                       boolean projectUnmatchedBuild, boolean projectUnmatchedProbe) {
    return new ProbeCursor(list, inProbeSV2Buf, buffers, projectUnmatchedBuild, projectUnmatchedProbe);
  }

  private ProbeCursor(
      PageListMultimap list,
      ArrowBuf inProbeSV2Buf,
      ProbeBuffers buffers,
      boolean projectUnmatchedBuild,
      boolean projectUnmatchedProbe) {
    super();
    this.list = list;
    this.buffers = buffers;
    this.inProbeSV2Buf = inProbeSV2Buf;
    this.projectUnmatchedBuild = projectUnmatchedBuild;
    this.projectUnmatchedProbe = projectUnmatchedProbe;
    this.location = new Location(0, PageListMultimap.TERMINAL);
  }

  public Stats next(HashJoinExtraMatcher extraMatcher, int inRecords, int startOutputIdx, int maxOutputIdx) {
    final Location prev = this.location;
    final boolean projectUnmatchedProbe = this.projectUnmatchedProbe;
    final int headShift = list.getHeadShift();
    final int headMask = list.getHeadMask();
    final int elementShift = list.getElementShift();
    final int elementMask = list.getElementMask();
    final long[] headBufAddrs = list.getHeadBufAddrs();
    final long[] elementBufAddrs = list.getElementBufAddrs();

    int outputRecords = 0;
    int maxOutRecords = maxOutputIdx - startOutputIdx + 1;
    assert maxOutputIdx > 0;
    int currentProbeSV2Index = prev.nextProbeSV2Index;
    int currentElementIndex = prev.elementIndex;

    final ArrowBuf inTableMatchOrdinalBuf = buffers.getInTableMatchOrdinals4B();
    final ArrowBuf outProbeProjectBuf = buffers.getOutProbeProjectOffsets2B();
    final ArrowBuf outBuildProjectBuf = buffers.getOutBuildProjectOffsets6B();
    final ArrowBuf projectNullKeyOffsetBuf = buffers.getOutInvalidBuildKeyOffsets2B();

    outProbeProjectBuf.checkBytes(0, maxOutRecords * SelectionVector2.RECORD_SIZE);
    outBuildProjectBuf.checkBytes(0, maxOutRecords * BUILD_RECORD_LINK_SIZE);
    projectNullKeyOffsetBuf.checkBytes(0, maxOutRecords * SelectionVector2.RECORD_SIZE);

    final long inProbeSV2StartAddr = inProbeSV2Buf.memoryAddress();
    final long inTableMatchOrdinalStartAddr = inTableMatchOrdinalBuf.memoryAddress();
    final long outProbeProjectStartAddr = outProbeProjectBuf.memoryAddress();
    final long outBuildProjectStartAddr = outBuildProjectBuf.memoryAddress();
    final long projectNullKeyOffsetStartAddr = projectNullKeyOffsetBuf.memoryAddress();

    int nullKeyCount = 0;
    boolean someMismatched = false;
    boolean someMatched = false;
    while (outputRecords < maxOutRecords && currentProbeSV2Index < inRecords) {
      // we're starting a new probe match
      final int indexInBuild = PlatformDependent.getInt(inTableMatchOrdinalStartAddr + currentProbeSV2Index * ORDINAL_SIZE);
      int unMatchedProbeIndex = -1;
      if (indexInBuild != -1) { // a matching key.
        // we're starting a new probe match
        if (currentElementIndex == TERMINAL) {
          /*
           * The current probe record has a key that matches. Get the index
           * of the first row in the build side that matches the current key
           */
          assert ((indexInBuild & headMask) * HEAD_SIZE + 4 <= list.getBufSize());
          currentElementIndex =
            PlatformDependent.getInt(headBufAddrs[indexInBuild >> headShift] + (indexInBuild & headMask) * HEAD_SIZE);
        }

        while ((currentElementIndex != TERMINAL) && (outputRecords < maxOutRecords)) {
          final long elementBufStartAddr = elementBufAddrs[currentElementIndex >> elementShift];
          final int offsetInPage = ELEMENT_SIZE * (currentElementIndex & elementMask);
          assert (offsetInPage + ELEMENT_SIZE <= list.getBufSize());

          int idxInProbeBatch = SV2UnsignedUtil.readAtIndexUnsafe(inProbeSV2StartAddr, currentProbeSV2Index);

          final long buildItem = PlatformDependent.getLong(elementBufStartAddr + offsetInPage);
          final int batchId = PageListMultimap.getBatchIdFromLong(buildItem);
          final int recordIndexInBatch = PageListMultimap.getRecordIndexFromLong(buildItem);
          if (extraMatcher.checkCurrentMatch(idxInProbeBatch, batchId, recordIndexInBatch)) {
            // project probe side

            SV2UnsignedUtil.writeAtOffsetUnsafe(outProbeProjectStartAddr, outputRecords * BATCH_OFFSET_SIZE, idxInProbeBatch);

            // project matched build item
            final int projectBuildOffsetStart = outputRecords * BUILD_RECORD_LINK_SIZE;
            PlatformDependent.putInt(outBuildProjectStartAddr + projectBuildOffsetStart, batchId);
            SV2UnsignedUtil.writeAtOffsetUnsafe(outBuildProjectStartAddr, projectBuildOffsetStart + BATCH_INDEX_SIZE,
              recordIndexInBatch);
            outputRecords++;
            someMatched = true;
          } else {
            someMismatched = true;
          }
          currentElementIndex = PlatformDependent.getInt(elementBufStartAddr + offsetInPage + NEXT_OFFSET);

          if (projectUnmatchedBuild) {
            if (currentElementIndex > 0) {
              if (!someMismatched) {
                // first visit, mark as visited by flipping the sign.
                PlatformDependent.putInt(elementBufStartAddr + offsetInPage + NEXT_OFFSET, -currentElementIndex);
              }
            } else {
              // not the first visit, flip the sign of what we just read.
              currentElementIndex = -currentElementIndex;
            }
          }

          if (currentElementIndex == TERMINAL) {
            if (!someMatched) {
              unMatchedProbeIndex = currentProbeSV2Index;
              unmatchedProbeCount++;
            }
            someMatched = false;
            someMismatched = false;
          }
        }
        if (currentElementIndex != TERMINAL) {
          /*
           * Output batch is full, we should exit now,
           * but more records for current key should be processed.
           */
          break;
        }

        currentProbeSV2Index++;
      } else {
        unMatchedProbeIndex = currentProbeSV2Index;
        unmatchedProbeCount++;
        currentProbeSV2Index++;
      }

      if (unMatchedProbeIndex >= 0 && projectUnmatchedProbe) {

        // probe doesn't match but we need to project anyways.
        int idxInProbeBatch = SV2UnsignedUtil.readAtIndexUnsafe(inProbeSV2StartAddr, unMatchedProbeIndex);
        SV2UnsignedUtil.writeAtOffsetUnsafe(outProbeProjectStartAddr, outputRecords * BATCH_OFFSET_SIZE, idxInProbeBatch);

        // mark the build as not matching.
        PlatformDependent.putInt(outBuildProjectStartAddr + outputRecords * BUILD_RECORD_LINK_SIZE, SKIP);

        // add this build location to the list of build keys we'll invalidate after copying.
        SV2UnsignedUtil.writeAtOffsetUnsafe(projectNullKeyOffsetStartAddr, nullKeyCount * BATCH_OFFSET_SIZE, startOutputIdx + outputRecords);

        nullKeyCount++;
        outputRecords++;

      }
    }
    boolean isPartial;
    if (outputRecords == maxOutRecords && currentProbeSV2Index < inRecords) {
      // batch was full but we have remaining records to process, need to save our position for when we return.
      location = new Location(currentProbeSV2Index, currentElementIndex);
      isPartial = true;
    } else {
      // we need to clear the last saved position and tell the driver that we completed consuming the current batch.
      location = new Location(0, TERMINAL);
      isPartial = false;
    }
    return new Stats(outputRecords, nullKeyCount, isPartial);
  }

  public long getUnmatchedProbeCount() {
    return unmatchedProbeCount;
  }

  /**
   * An object to hold the current cursor state. We use an object here to make
   * sure we're replacing the entirety of the mutable cursor state after each
   * next() call.
   */
  static class Location {
    private final int nextProbeSV2Index;
    private final int elementIndex;

    public Location(int nextProbeSV2Index, int elementIndex) {
      this.nextProbeSV2Index = nextProbeSV2Index;
      this.elementIndex = elementIndex;
    }
  }

  public static class Stats {
    private final int recordsFound;
    private final int nullKeyCount;
    private final boolean partial;

    Stats(int recordsFound, int nullKeyCount, boolean partial) {
      this.recordsFound = recordsFound;
      this.nullKeyCount = nullKeyCount;
      this.partial = partial;
    }

    public int getRecordsFound() {
      return recordsFound;
    }

    public int getNullKeyCount() {
      return nullKeyCount;
    }

    public boolean isPartial() {
      return partial;
    }
  }
}

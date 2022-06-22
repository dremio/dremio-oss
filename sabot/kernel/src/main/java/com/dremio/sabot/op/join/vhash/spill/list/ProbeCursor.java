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

import java.util.function.Consumer;

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
  private final long inProbeSV2Addr;
  private final Consumer<Integer> refiller;
  private final boolean projectUnmatchedBuild;
  private final boolean projectUnmatchedProbe;

  private final ProbeBuffers buffers;
  private Location location;
  private long unmatchedProbeCount;

  public static ProbeCursor startProbe(PageListMultimap list, long inProbeSV2Addr, ProbeBuffers buffers,
                                       boolean projectUnmatchedBuild, boolean projectUnmatchedProbe,
                                       Consumer<Integer> refiller) {
    return new ProbeCursor(list, inProbeSV2Addr, buffers, projectUnmatchedBuild, projectUnmatchedProbe, refiller);
  }

  private ProbeCursor(
      PageListMultimap list,
      long inProbeSV2Addr,
      ProbeBuffers buffers,
      boolean projectUnmatchedBuild,
      boolean projectUnmatchedProbe,
      Consumer<Integer> refiller) {
    super();
    this.list = list;
    this.buffers = buffers;
    this.inProbeSV2Addr = inProbeSV2Addr;
    this.refiller = refiller;
    this.projectUnmatchedBuild = projectUnmatchedBuild;
    this.projectUnmatchedProbe = projectUnmatchedProbe;
    this.location = new Location(0, PageListMultimap.TERMINAL);
  }

  public Stats next(int inRecords, int startOutputIdx, int maxOutputIdx) {
    final Location prev = this.location;
    final boolean projectUnmatchedProbe = this.projectUnmatchedProbe;
    final int headShift = list.getHeadShift();
    final int headMask = list.getHeadMask();
    final int elementShift = list.getElementShift();
    final int elementMask = list.getElementMask();
    final long[] headAddresses = list.getHeadAddresses();
    final long[] elementAddresses = list.getElementAddresses();

    int outputRecords = 0;
    int maxOutRecords = maxOutputIdx - startOutputIdx + 1;
    assert maxOutputIdx > 0;
    int currentProbeSV2Index = prev.nextProbeSV2Index;
    int currentElementIndex = prev.elementIndex;

    // we have two incoming options: we're starting on a new batch or we're picking up an existing batch.
    if (currentProbeSV2Index == 0) {
      // when this is a new batch, we need to find all the matches.
      refiller.accept(inRecords);
    }

    final long inTableMatchOrdinalAddr = buffers.getInTableMatchOrdinals4B().memoryAddress();
    final long outProbeProjectAddr = buffers.getOutProbeProjectOffsets2B().memoryAddress();
    final long outBuildProjectAddr = buffers.getOutBuildProjectOffsets6B().memoryAddress();
    final long projectNullKeyOffsetAddr = buffers.getOutInvalidBuildKeyOffsets2B().memoryAddress();
    int nullKeyCount = 0;

    while (outputRecords < maxOutRecords && currentProbeSV2Index < inRecords) {
      // we're starting a new probe match
      final int indexInBuild = PlatformDependent.getInt(inTableMatchOrdinalAddr + currentProbeSV2Index * ORDINAL_SIZE);
      logger.trace("probe in table returned ordinal {}", indexInBuild);
      if (indexInBuild == -1) { // not a matching key.
        if (projectUnmatchedProbe) {

          // probe doesn't match but we need to project anyways.
          int idxInProbeBatch = SV2UnsignedUtil.read(inProbeSV2Addr, currentProbeSV2Index);
          SV2UnsignedUtil.write(outProbeProjectAddr + outputRecords * BATCH_OFFSET_SIZE, idxInProbeBatch);

          // mark the build as not matching.
          PlatformDependent.putInt(outBuildProjectAddr + outputRecords * BUILD_RECORD_LINK_SIZE, SKIP);

          // add this build location to the list of build keys we'll invalidate after copying.
          SV2UnsignedUtil.write(projectNullKeyOffsetAddr + nullKeyCount * BATCH_OFFSET_SIZE, startOutputIdx + outputRecords);

          nullKeyCount++;
          outputRecords++;
          unmatchedProbeCount++;
        }

        currentProbeSV2Index++;
        continue;
      }

      // we're starting a new probe match
      if (currentElementIndex == TERMINAL) {
        /*
         * The current probe record has a key that matches. Get the index
         * of the first row in the build side that matches the current key
         */
        final long memStart = headAddresses[indexInBuild >> headShift] + (indexInBuild & headMask) * HEAD_SIZE;
        currentElementIndex = PlatformDependent.getInt(memStart);
      }

      while ((currentElementIndex != TERMINAL) && (outputRecords < maxOutRecords)) {

        // project probe side
        int idxInProbeBatch = SV2UnsignedUtil.read(inProbeSV2Addr, currentProbeSV2Index);
        SV2UnsignedUtil.write(outProbeProjectAddr + outputRecords * BATCH_OFFSET_SIZE, idxInProbeBatch);

        final long elementAddress = elementAddresses[currentElementIndex >> elementShift] +
          ELEMENT_SIZE * (currentElementIndex & elementMask);
        final long buildItem = PlatformDependent.getLong(elementAddress);

        // project matched build item
        final long projectBuildOffsetAddrStart = outBuildProjectAddr + outputRecords * BUILD_RECORD_LINK_SIZE;
        PlatformDependent.putInt(projectBuildOffsetAddrStart, PageListMultimap.getBatchIdFromLong(buildItem));
        SV2UnsignedUtil.write(projectBuildOffsetAddrStart + BATCH_INDEX_SIZE,
          PageListMultimap.getRecordIndexFromLong(buildItem));

        currentElementIndex = PlatformDependent.getInt(elementAddress + NEXT_OFFSET);
        if (projectUnmatchedBuild) {
          if (currentElementIndex > 0) {
            // first visit, mark as visited by flipping the sign.
            PlatformDependent.putInt(elementAddress + NEXT_OFFSET, -currentElementIndex);
          } else {
            // not the first visit, flip the sign of what we just read.
            currentElementIndex = -currentElementIndex;
          }
        }
        outputRecords++;
      }

      if (currentElementIndex != TERMINAL) {
        /*
         * Output batch is full, we should exit now,
         * but more records for current key should be processed.
         */
        break;
      }

      currentProbeSV2Index++;
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

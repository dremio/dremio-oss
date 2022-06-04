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
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.BASE_ELEMENT_INDEX;
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.BATCH_INDEX_SIZE;
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.ELEMENT_SIZE;
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.KEY_OFFSET;
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.KEY_SIZE;
import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.NEXT_OFFSET;

import java.util.function.BiFunction;

import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;

import io.netty.util.internal.PlatformDependent;

public class UnmatchedCursor {

  private final PageListMultimap list;
  private final ProbeBuffers buffers;
  private final BiFunction<Long, Integer, Integer> varLengthFunction;
  private int currentElementIndex;
  private long unmatchedBuildKeyCount;

  public UnmatchedCursor(PageListMultimap list, ProbeBuffers buffers,
                         BiFunction<Long, Integer, Integer> varLengthFunction) {
    this.list = list;
    this.buffers = buffers;
    this.varLengthFunction = varLengthFunction;
    this.currentElementIndex = BASE_ELEMENT_INDEX;
  }

  public Stats next(int startOutputIndex, int maxOutputIndex) {
    final int maxOutputRecords = maxOutputIndex - startOutputIndex + 1;
    final long outBuildProjectAddr = buffers.getOutBuildProjectOffsets6B().memoryAddress();
    final long outBuildProjectKeyAddr = buffers.getOutBuildProjectKeyOrdinals4B().memoryAddress();
    final int elementShift = list.getElementShift();
    final int elementMask = list.getElementMask();
    final long[] elementAddresses = list.getElementAddresses();

    int outputRecords = 0;
    for (;
         outputRecords < maxOutputRecords && currentElementIndex < list.getTotalListSize();
         ++currentElementIndex) {
      final long address = elementAddresses[currentElementIndex >> elementShift] + ELEMENT_SIZE * (currentElementIndex & elementMask);
      int next = PlatformDependent.getInt(address + NEXT_OFFSET);
      if (next < 0) {
        // all visited entries will have their sign bit flipped to -ve value.
        continue;
      }
      final long carryAlongId = PlatformDependent.getLong(address);
      final int tableOrdinal = PlatformDependent.getInt(address + KEY_OFFSET);
      final long projectBuildOffsetAddrStart = outBuildProjectAddr + outputRecords * BUILD_RECORD_LINK_SIZE;
      PlatformDependent.putInt(projectBuildOffsetAddrStart, PageListMultimap.getBatchIdFromLong(carryAlongId));
      SV2UnsignedUtil.write(projectBuildOffsetAddrStart + BATCH_INDEX_SIZE,
        PageListMultimap.getRecordIndexFromLong(carryAlongId));

      // Maintain the ordinal of the key for unpivot later
      PlatformDependent.putInt(outBuildProjectKeyAddr + outputRecords * KEY_SIZE, tableOrdinal);
      unmatchedBuildKeyCount++;
      outputRecords++;
    }

    return new Stats(outputRecords,
      varLengthFunction.apply(outBuildProjectKeyAddr, outputRecords),
      currentElementIndex < list.getTotalListSize());
  }

  public long getUnmatchedBuildKeyCount() {
    return unmatchedBuildKeyCount;
  }

  public static class Stats {
    private final int recordsFound;
    private final int totalVarSize;
    private final boolean partial;

    Stats(int recordsFound, int totalVarSize, boolean partial) {
      this.recordsFound = recordsFound;
      this.totalVarSize = totalVarSize;
      this.partial = partial;
    }

    public int getRecordsFound() {
      return recordsFound;
    }

    public int getTotalVarSize() {
      return totalVarSize;
    }

    public boolean isPartial() {
      return partial;
    }
  }
}

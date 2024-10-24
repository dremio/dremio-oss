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
package com.dremio.sabot.op.join.vhash.spill.slicer;

import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

/** A sizer that adds an arbitrary constant overhead for each record. */
class ConstantExtraSizer implements Sizer {

  private final int sizeInBytes;

  public ConstantExtraSizer(int sizeInBytes) {
    this.sizeInBytes = sizeInBytes;
  }

  @Override
  public int getEstimatedRecordSizeInBits() {
    return sizeInBytes * BYTE_SIZE_BITS;
  }

  @Override
  public int getDataLengthFromIndex(int startIndex, int numberOfEntries) {
    return sizeInBytes * numberOfEntries;
  }

  @Override
  public void reset() {
    // no caching.
  }

  @Override
  public int computeBitsNeeded(ArrowBuf sv2, int startIdx, int count) {
    return (sizeInBytes * BYTE_SIZE_BITS) * count;
  }

  @Override
  public int getSizeInBitsStartingFromOrdinal(int ordinal, int len) {
    return (sizeInBytes * BYTE_SIZE_BITS) * len;
  }

  @Override
  public Copier getCopier(
      BufferAllocator allocator,
      ArrowBuf sv2,
      int startIdx,
      int count,
      List<FieldVector> vectorOutput) {
    return page -> page.deadSlice(count * sizeInBytes);
  }
}

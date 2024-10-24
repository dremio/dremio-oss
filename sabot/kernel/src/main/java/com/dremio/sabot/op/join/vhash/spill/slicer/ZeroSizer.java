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

import com.dremio.sabot.op.copier.FieldBufferPreAllocedCopier;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

public class ZeroSizer implements Sizer {
  private final ZeroVector incoming;

  public ZeroSizer(ZeroVector incoming) {
    this.incoming = incoming;
  }

  @Override
  public void reset() {}

  @Override
  public int computeBitsNeeded(ArrowBuf sv2, int startIdx, int count) {
    return 0;
  }

  @Override
  public int getEstimatedRecordSizeInBits() {
    return 0;
  }

  @Override
  public int getDataLengthFromIndex(int startIndex, int numberOfEntries) {
    return 0;
  }

  @Override
  public Copier getCopier(
      BufferAllocator allocator,
      ArrowBuf sv2,
      int startIdx,
      int count,
      List<FieldVector> vectorOutput) {
    final FieldVector outgoing =
        (FieldVector) incoming.getTransferPair(incoming.getField(), allocator).getTo();
    vectorOutput.add(outgoing);
    return page -> {
      outgoing.loadFieldBuffers(new ArrowFieldNode(count, -1), ImmutableList.of());

      // copy data.
      FieldBufferPreAllocedCopier.getCopiers(ImmutableList.of(incoming), ImmutableList.of(outgoing))
          .forEach(copier -> copier.copy(sv2.memoryAddress() + startIdx * SV2_SIZE_BYTES, count));
    };
  }

  @Override
  public int getSizeInBitsStartingFromOrdinal(int ordinal, int len) {
    return 0;
  }
}

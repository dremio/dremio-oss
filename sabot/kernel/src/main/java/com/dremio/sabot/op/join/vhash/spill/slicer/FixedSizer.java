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

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.util.RoundUtil;
import com.dremio.sabot.op.copier.FieldBufferPreAllocedCopier;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;

class FixedSizer implements Sizer {
  private final BaseFixedWidthVector incoming;
  private final int dataSizeInBits;

  public FixedSizer(BaseFixedWidthVector incoming) {
    super();
    this.incoming = incoming;
    if (incoming.getMinorType() == MinorType.BIT) {
      dataSizeInBits = 1;
    } else {
      dataSizeInBits = TypeHelper.getSize(incoming.getMinorType()) * BYTE_SIZE_BITS;
    }
  }

  @Override
  public int getEstimatedRecordSizeInBits() {
    return dataSizeInBits + VALID_SIZE_BITS;
  }

  @Override
  public int getDataLengthFromIndex(int startIndex, int numberOfEntries) {
    int dataLen = 0;
    int endIndex = startIndex + numberOfEntries;
    for (; startIndex < endIndex; startIndex++) {
      if (incoming.isNull(startIndex)) {
        continue;
      }
      dataLen += (dataSizeInBits / BYTE_SIZE_BITS);
    }

    return dataLen;
  }

  @Override
  public void reset() {
    // no caching
  }

  @Override
  public int computeBitsNeeded(ArrowBuf sv2, int startIdx, int len) {
    return dataSizeInBits == 1
        ? 2 * RoundUtil.round64up(len)
        : RoundUtil.round64up(dataSizeInBits * len) + RoundUtil.round64up(len);
  }

  @Override
  public int getSizeInBitsStartingFromOrdinal(final int ordinal, final int numberOfRecords) {
    if (dataSizeInBits == 1) {
      // numberOfRecords number of bits to store data buffer + numberOfRecords number of bits to
      // store validity bitmap buffer
      return 2 * RoundUtil.round64up(numberOfRecords);
    } else {
      final int dataBits = RoundUtil.round64up(dataSizeInBits * numberOfRecords); // data buffer
      final int validityBits =
          Sizer.getValidityBufferSizeInBits(numberOfRecords); // validity buffer

      return dataBits + validityBits;
    }
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
    sv2.checkBytes(startIdx * SV2_SIZE_BYTES, (startIdx + count) * SV2_SIZE_BYTES);
    return page -> {
      int totalSize = computeBitsNeeded(sv2, startIdx, count) / BYTE_SIZE_BITS;

      final int validityLen = RoundUtil.round64up(count) / BYTE_SIZE_BITS;
      final int dataLen = totalSize - validityLen;

      try (final ArrowBuf validityBuf = page.sliceAligned(validityLen);
          final ArrowBuf dataBuf = page.sliceAligned(dataLen)) {
        // The bit copiers do ORs to set the bits, and expect that the buffer is zero-filled to
        // begin with.
        validityBuf.setZero(0, validityLen);
        if (dataSizeInBits == 1) {
          dataBuf.setZero(0, dataLen);
        }

        outgoing.loadFieldBuffers(
            new ArrowFieldNode(count, -1), ImmutableList.of(validityBuf, dataBuf));

        // copy data.
        FieldBufferPreAllocedCopier.getCopiers(
                ImmutableList.of(incoming), ImmutableList.of(outgoing))
            .forEach(copier -> copier.copy(sv2.memoryAddress() + startIdx * SV2_SIZE_BYTES, count));

        assert outgoing.getValidityBufferAddress() == validityBuf.memoryAddress();
        assert outgoing.getDataBufferAddress() == dataBuf.memoryAddress();
      }
    };
  }
}

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

import com.dremio.exec.util.RoundUtil;
import com.dremio.sabot.op.copier.FieldBufferPreAllocedCopier;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

class VariableSizer implements Sizer {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(VariableSizer.class);
  private final BaseVariableWidthVector incoming;
  private long cachedSv2Addr;
  private int cachedStartIdx;
  private int cachedCount;
  private int cachedDataSize;

  public VariableSizer(BaseVariableWidthVector incoming) {
    this.incoming = incoming;
  }

  @Override
  public int getEstimatedRecordSizeInBits() {
    if (incoming.getValueCount() == 0) {
      return 0;
    } else {
      return (incoming.getBufferSize() / incoming.getValueCount()) * BYTE_SIZE_BITS;
    }
  }

  @Override
  public int getDataLengthFromIndex(int startIndex, int numberOfEntries) {

    final long startOffset =
        incoming.getOffsetBuffer().getInt((long) startIndex * OFFSET_SIZE_BYTES);
    final long endOffset =
        incoming
            .getOffsetBuffer()
            .getInt((long) (startIndex + numberOfEntries) * OFFSET_SIZE_BYTES);

    return (int) (endOffset - startOffset);
  }

  @Override
  public void reset() {
    cachedSv2Addr = 0;
  }

  @Override
  public int computeBitsNeeded(ArrowBuf sv2, int startIdx, int count) {
    int offsetSize = RoundUtil.round64up(OFFSET_SIZE_BITS * (count + 1));
    int validitySize = RoundUtil.round64up(VALID_SIZE_BITS * count);
    int dataSize;

    if (sv2.memoryAddress() == cachedSv2Addr
        && startIdx == cachedStartIdx
        && Math.abs(count - cachedCount) <= 8) {
      // do a delta with the cached value.
      dataSize = cachedDataSize;
      if (cachedCount < count) {
        dataSize += computeSimpleDataSize(sv2, startIdx + cachedCount, count - cachedCount);
      } else {
        dataSize -= computeSimpleDataSize(sv2, startIdx + count, cachedCount - count);
      }
      assert dataSize == computeSimpleDataSize(sv2, startIdx, count);
    } else {
      dataSize = computeSimpleDataSize(sv2, startIdx, count);
    }
    cachedSv2Addr = sv2.memoryAddress();
    cachedStartIdx = startIdx;
    cachedCount = count;
    cachedDataSize = dataSize;

    return RoundUtil.round64up(dataSize * BYTE_SIZE_BITS) + offsetSize + validitySize;
  }

  @Override
  public int getSizeInBitsStartingFromOrdinal(final int ordinal, final int numberOfRecords) {
    if (incoming.getValueCount() == 0) {
      return 0;
    }
    final int start = incoming.getOffsetBuffer().getInt((long) ordinal * OFFSET_SIZE_BYTES);
    final int end =
        incoming.getOffsetBuffer().getInt((long) (ordinal + numberOfRecords) * OFFSET_SIZE_BYTES);

    final int offsetBufferSize = Sizer.getOffsetBufferSizeInBits(numberOfRecords); // offset buffer
    final int dataBufferSize = RoundUtil.round64up((end - start) * BYTE_SIZE_BITS); // data buffer
    final int validityBufferSize =
        Sizer.getValidityBufferSizeInBits(numberOfRecords); // validity buffer

    return offsetBufferSize + dataBufferSize + validityBufferSize;
  }

  private int computeSimpleDataSize(final ArrowBuf sv2, final int startIdx, final int count) {
    int dataSize = 0;
    for (int i = 0; i < count; ++i) {
      final int ordinal = SV2UnsignedUtil.readAtIndex(sv2, startIdx + i);
      final long startAndEnd = incoming.getOffsetBuffer().getLong(ordinal * OFFSET_SIZE_BYTES);
      final int startOffset = (int) startAndEnd;
      final int endOffset = (int) (startAndEnd >> 32);
      dataSize += endOffset - startOffset;
    }
    return dataSize;
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
    return (page) -> {
      final int expectedSize = computeBitsNeeded(sv2, startIdx, count);

      final int offsetLen = Sizer.getOffsetBufferSizeInBits(count) / BYTE_SIZE_BITS;
      final int validityLen = Sizer.getValidityBufferSizeInBits(count) / BYTE_SIZE_BITS;
      final int dataLen = expectedSize / BYTE_SIZE_BITS - validityLen - offsetLen;

      try (final ArrowBuf validityBuf = page.sliceAligned(validityLen);
          final ArrowBuf offsetBuf = page.sliceAligned(offsetLen);
          final ArrowBuf dataBuf = page.sliceAligned(dataLen)) {
        outgoing.loadFieldBuffers(
            new ArrowFieldNode(count, -1), ImmutableList.of(validityBuf, offsetBuf, dataBuf));

        // The bit copiers do ORs to set the bits, and expect that the buffer is zero-filled to
        // begin with.
        validityBuf.setZero(0, validityLen);
        // copy data.
        offsetBuf.setInt(0, 0); // rest of the offsets will be filled in during the copy
        FieldBufferPreAllocedCopier.getCopiers(
                ImmutableList.of(incoming), ImmutableList.of(outgoing))
            .forEach(copier -> copier.copy(sv2.memoryAddress() + startIdx * SV2_SIZE_BYTES, count));

        assert outgoing.getValidityBufferAddress() == validityBuf.memoryAddress();
        assert outgoing.getOffsetBufferAddress() == offsetBuf.memoryAddress();
        assert outgoing.getDataBufferAddress() == dataBuf.memoryAddress();
      }
    };
  }
}

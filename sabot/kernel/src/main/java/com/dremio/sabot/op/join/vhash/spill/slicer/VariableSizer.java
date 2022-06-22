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
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

import com.dremio.sabot.op.copier.FieldBufferPreAllocedCopier;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

class VariableSizer implements Sizer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VariableSizer.class);
  private final BaseVariableWidthVector incoming;
  private long cachedSv2Addr;
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
      return (incoming.getBufferSize() / incoming.getValueCount()) * BYTE_SIZE_BITS + OFFSET_SIZE_BITS + VALID_SIZE_BITS;
    }
  }

  @Override
  public void reset() {
    cachedSv2Addr = 0;
  }

  @Override
  public int computeBitsNeeded(long sv2Addr, int count) {
    int offsetSize = Sizer.round64up(OFFSET_SIZE_BITS * (count + 1));
    int validitySize = Sizer.round64up(VALID_SIZE_BITS * count);
    int dataSize;

    if (sv2Addr == cachedSv2Addr && Math.abs(count - cachedCount) <= 8) {
      // do a delta with the cached value.
      dataSize = cachedDataSize;
      if (cachedCount < count) {
        dataSize += computeSimpleDataSize(sv2Addr + cachedCount * SV2_SIZE_BYTES, count - cachedCount);
      } else {
        dataSize -= computeSimpleDataSize(sv2Addr + count * SV2_SIZE_BYTES, cachedCount - count);
      }
      assert dataSize == computeSimpleDataSize(sv2Addr, count);
    } else {
      dataSize = computeSimpleDataSize(sv2Addr, count);
    }
    cachedSv2Addr = sv2Addr;
    cachedCount = count;
    cachedDataSize = dataSize;

    return Sizer.round64up(dataSize * BYTE_SIZE_BITS) + offsetSize + validitySize;
  }

  private int computeSimpleDataSize(long sv2Addr, int count) {
    int dataSize = 0;
    for (long curAddr = sv2Addr, maxAddr = sv2Addr + count * SV2_SIZE_BYTES;
         curAddr < maxAddr;
         curAddr += SV2_SIZE_BYTES) {
      int ordinal = SV2UnsignedUtil.read(curAddr);
      final long startAndEnd = PlatformDependent.getLong(incoming.getOffsetBuffer().memoryAddress() +
        ordinal * OFFSET_SIZE_BYTES);
      final int startOffset = (int) startAndEnd;
      final int endOffset = (int) (startAndEnd >> 32);
      dataSize += endOffset - startOffset;
    }
    return dataSize;
  }

  @Override
  public Copier getCopier(BufferAllocator allocator, long sv2Addr, int count, List<FieldVector> vectorOutput) {
    final FieldVector outgoing = (FieldVector) incoming.getTransferPair(allocator).getTo();
    vectorOutput.add(outgoing);
    return (page) -> {
      int expectedSize = computeBitsNeeded(sv2Addr, count);

      final int offsetLen = Sizer.round64up(OFFSET_SIZE_BITS * (count + 1)) / BYTE_SIZE_BITS;
      final int validityLen = Sizer.round64up(VALID_SIZE_BITS * count) / BYTE_SIZE_BITS;
      final int dataLen = expectedSize / BYTE_SIZE_BITS - validityLen - offsetLen;

      try (final ArrowBuf validityBuf = page.slice(validityLen);
           final ArrowBuf offsetBuf = page.slice(offsetLen);
           final ArrowBuf dataBuf = page.slice(dataLen)) {
        outgoing.loadFieldBuffers(new ArrowFieldNode(count, -1),
          ImmutableList.of(validityBuf, offsetBuf, dataBuf));

        // The bit copiers do ORs to set the bits, and expect that the buffer is zero-filled to begin with.
        validityBuf.setZero(0, validityLen);
        // copy data.
        PlatformDependent.putInt(offsetBuf.memoryAddress(), 0); // rest of the offsets will be filled in during the copy
        FieldBufferPreAllocedCopier.getCopiers(ImmutableList.of(incoming), ImmutableList.of(outgoing))
          .forEach(copier -> copier.copy(sv2Addr, count));
      }
    };
  }
}

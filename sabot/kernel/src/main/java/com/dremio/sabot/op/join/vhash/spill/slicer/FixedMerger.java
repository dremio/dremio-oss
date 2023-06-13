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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.util.RoundUtil;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.google.common.collect.ImmutableList;

/**
 * Merger for fixed-length vectors.
 */
public class FixedMerger implements Merger {
  private final int dataSizeInBits;
  private final int wrapperIdx;
  private final BufferAllocator allocator;

  FixedMerger(BaseFixedWidthVector vector, int wrapperIdx, BufferAllocator allocator) {
    if (vector.getMinorType() == Types.MinorType.BIT) {
      dataSizeInBits = 1;
    } else {
      dataSizeInBits = TypeHelper.getSize(vector.getMinorType()) * BYTE_SIZE_BITS;
    }
    this.wrapperIdx = wrapperIdx;
    this.allocator = allocator;
  }

  @Override
  public void merge(VectorContainerList srcContainers, Page dst, List<FieldVector> vectorOutput) {
    List<ValueVector> src = new ArrayList<>();
    for (VectorWrapper<?> wrapper : srcContainers.getWrappers(wrapperIdx)) {
      src.add(wrapper.getValueVector());
    }

    int recordCount = srcContainers.getRecordCount();
    if (dataSizeInBits == 1) {
      mergeBitType(src, dst, recordCount, vectorOutput);
      return;
    }
    final FieldVector outgoing = (FieldVector) src.get(0).getTransferPair(allocator).getTo();
    vectorOutput.add(outgoing);

    final int validityLen = RoundUtil.round64up(recordCount) / BYTE_SIZE_BITS;
    final int dataLen = RoundUtil.round64up(dataSizeInBits * recordCount) / BYTE_SIZE_BITS;
    try (final ArrowBuf validityBuf = dst.sliceAligned(validityLen);
         final ArrowBuf dataBuf = dst.sliceAligned(dataLen)) {
      outgoing.loadFieldBuffers(new ArrowFieldNode(recordCount, -1),
        ImmutableList.of(validityBuf, dataBuf));

      // The bit copiers do ORs to set the bits, and expect that the buffer is zero-filled to begin with.
      validityBuf.setZero(0, validityLen);

      // copy validity
      Merger.mergeValidityBuffers(src, validityBuf);

      // copy data.
      int offset = 0;
      for (ValueVector current : src) {
        int toCopy = current.getValueCount() * dataSizeInBits / BYTE_SIZE_BITS;
        dataBuf.setBytes(offset, current.getDataBuffer(), 0, toCopy);
        offset += toCopy;
      }

      assert outgoing.getValidityBufferAddress() == validityBuf.memoryAddress();
      assert outgoing.getDataBufferAddress() == dataBuf.memoryAddress();
    }
  }

  private void mergeBitType(List<ValueVector> src, Page dst, int recordCount, List<FieldVector> vectorOutput) {
    final FieldVector outgoing = (FieldVector) src.get(0).getTransferPair(allocator).getTo();
    vectorOutput.add(outgoing);

    final int validityLen = RoundUtil.round64up(recordCount) / BYTE_SIZE_BITS;
    final int dataLen = validityLen;

    try (final ArrowBuf validityBuf = dst.sliceAligned(validityLen);
         final ArrowBuf dataBuf = dst.sliceAligned(dataLen)) {
      validityBuf.setZero(0, validityLen);
      dataBuf.setZero(0, dataLen);

      outgoing.loadFieldBuffers(new ArrowFieldNode(recordCount, -1),
        ImmutableList.of(validityBuf, dataBuf));

      int dstBitPos = 0;
      for (ValueVector current : src) {
        // copy validity
        Merger.copyBits(current.getValidityBuffer(), validityBuf, dstBitPos, current.getValueCount());
        // copy data
        Merger.copyBits(current.getDataBuffer(), dataBuf, dstBitPos, current.getValueCount());
        dstBitPos += current.getValueCount();
      }

      assert outgoing.getValidityBufferAddress() == validityBuf.memoryAddress();
      assert outgoing.getDataBufferAddress() == dataBuf.memoryAddress();
    }
  }
}

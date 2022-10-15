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

import static org.apache.arrow.vector.BaseVariableWidthVector.OFFSET_WIDTH;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.util.RoundUtil;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.google.common.collect.ImmutableList;

public class VariableMerger implements Merger {
  private final int wrapperIdx;
  private final BufferAllocator allocator;

  VariableMerger(BaseVariableWidthVector vector, int wrapperIdx, BufferAllocator allocator) {
    this.wrapperIdx = wrapperIdx;
    this.allocator = allocator;
  }

  @Override
  public void merge(VectorContainerList srcContainers, Page dst, List<FieldVector> vectorOutput) {
    List<BaseVariableWidthVector> src = new ArrayList<>();
    for (VectorWrapper<?> wrapper : srcContainers.getWrappers(wrapperIdx)) {
      src.add((BaseVariableWidthVector) wrapper.getValueVector());
    }

    int recordCount = srcContainers.getRecordCount();
    final FieldVector outgoing = (FieldVector) src.get(0).getTransferPair(allocator).getTo();
    vectorOutput.add(outgoing);

    final int validityLen = RoundUtil.round64up(recordCount) / BYTE_SIZE_BITS;
    final int offsetLen = RoundUtil.round8up(OFFSET_WIDTH * (recordCount + 1));
    int dataLen = 0;
    for (BaseVariableWidthVector current : src) {
      dataLen += current.getDataBuffer().readableBytes();
    }
    dataLen = RoundUtil.round8up(dataLen);

    try (final ArrowBuf validityBuf = dst.sliceAligned(validityLen);
         final ArrowBuf offsetBuf = dst.sliceAligned(offsetLen);
         final ArrowBuf dataBuf = dst.sliceAligned(dataLen)) {
      outgoing.loadFieldBuffers(new ArrowFieldNode(recordCount, -1),
        ImmutableList.of(validityBuf, offsetBuf, dataBuf));

      // The bit copiers do ORs to set the bits, and expect that the buffer is zero-filled to begin with.
      validityBuf.setZero(0, validityLen);

      // copy validity
      Merger.mergeValidityBuffers(src, validityBuf);

      // copy offsets & data
      offsetBuf.setInt(0, 0);

      int dataCursor = 0;
      int offsetCursor = OFFSET_WIDTH; // first entry filled with 0.
      for (BaseVariableWidthVector current : src) {
        for (int i = 1; i <= current.getValueCount(); i++) {
          int srcOffset = current.getOffsetBuffer().getInt(i * OFFSET_WIDTH);
          offsetBuf.setInt(offsetCursor, srcOffset + dataCursor);
          offsetCursor += OFFSET_WIDTH;
        }

        int toCopyDataLen = (int) current.getDataBuffer().readableBytes();
        dataBuf.setBytes(dataCursor, current.getDataBuffer(), 0, toCopyDataLen);
        dataCursor += toCopyDataLen;
      }

      assert outgoing.getValidityBufferAddress() == validityBuf.memoryAddress();
      assert outgoing.getOffsetBufferAddress() == offsetBuf.memoryAddress();
      assert outgoing.getDataBufferAddress() == dataBuf.memoryAddress();
    }
  }
}

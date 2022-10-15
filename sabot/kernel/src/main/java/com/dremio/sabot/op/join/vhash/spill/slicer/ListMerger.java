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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.google.common.collect.ImmutableList;

public class ListMerger implements Merger {

  private final int wrapperIdx;
  private final BufferAllocator allocator;

  /**
   * A {@link Merger} implementation for list Arrow vectors {@link ListVector}
   * A ListVector consists of variable sized lists of primitive or complex types. It provides validity
   * bitmap to keep track of null values and offset buffer to that provide start and end offsets of lists in the vector.
   *
   * This merger provides functionality of merging together data of multiple ListVector vectors into one, it merges
   * validity buffers, offset buffers and underlying data vectors.
   *
   * Arrow vector layout types including fixed size list vector documented at -
   * https://arrow.apache.org/docs/format/Columnar.html
   *
   * JIRA ticket for this change -
   * https://dremio.atlassian.net/browse/DX-54668
   */
  ListMerger(ListVector vector, final int wrapperIdx, final BufferAllocator allocator) {
    this.wrapperIdx = wrapperIdx;
    this.allocator = allocator;
  }

  /**
   * Merge multiple ListVector instances into one.
   * @param srcContainers contains list of vectors to be merged
   * @param dst page from which buffers for merged vector will be allocated
   * @param vectorOutput merged vector
   */
  @Override
  public void merge(final VectorContainerList srcContainers, final Page dst, final List<FieldVector> vectorOutput) {

    //create a list of vectors to be merged into a single vector
    final List<ListVector> vectorListToBeMerged = new ArrayList<>();
    for (final VectorWrapper<?> wrapper : srcContainers.getWrappers(wrapperIdx)) {
      vectorListToBeMerged.add((ListVector) wrapper.getValueVector());
    }

    final int recordCount = srcContainers.getRecordCount();

    //outgoing vector into which all above vectors will be merged
    final ListVector outgoing = (ListVector) vectorListToBeMerged.get(0).getTransferPair(allocator).getTo();
    vectorOutput.add(outgoing);

    //validity buffer size and offset buffer size of the merged vector
    final int validityLen = Merger.getValidityBufferSizeInBits(recordCount) / BYTE_SIZE_BITS;
    final int offsetLen = Merger.getOffsetBufferSizeInBits(recordCount) / BYTE_SIZE_BITS;

    try (final ArrowBuf validityBuf = dst.sliceAligned(validityLen);
         final ArrowBuf offsetBuf = dst.sliceAligned(offsetLen)) {

      outgoing.loadFieldBuffers(new ArrowFieldNode(recordCount, -1),
        ImmutableList.of(validityBuf, offsetBuf));

      validityBuf.setZero(0, validityLen);

      // merge validity buffers
      Merger.mergeValidityBuffers(vectorListToBeMerged, validityBuf);

      //merge offset buffers
      offsetBuf.setInt(0, 0);
      int dataCursor = 0;
      int offsetCursor = OFFSET_WIDTH; // first entry filled with 0.
      int offsetValue = 0;
      for (final ListVector current : vectorListToBeMerged) {

        for (int i = 1; i <= current.getValueCount(); i++) {
          final int srcOffset = current.getOffsetBuffer().getInt((long) i * OFFSET_WIDTH);

          offsetValue = srcOffset + dataCursor;
          offsetBuf.setInt(offsetCursor, offsetValue);
          offsetCursor += OFFSET_WIDTH;
        }

        dataCursor = offsetValue;

      }

      //make a list of all data vectors, so they would be merged
      final List<ValueVector> dataVectors = new ArrayList<>();
      for (final ListVector vector : vectorListToBeMerged) {
        dataVectors.add(vector.getDataVector());
      }

      //get a merger instance for whatever underlying data vector's type is for this list vector
      final Merger childMerger = Merger.get(outgoing.getDataVector(), 0, allocator);

      final List<FieldVector> mergedDataVectors = new ArrayList<>();

      //create a VectorContainerList instance of all data vectors. It will be used as an input to child merger.
      final VectorContainerList containerList = new VectorContainerList(dataVectors, 0);

      //merge underlying data vectors
      childMerger.merge(containerList, dst, mergedDataVectors);

      //transfer contents of output vector (of all data vector) to out final output's data vector
      final TransferPair transferPair = mergedDataVectors.get(0).makeTransferPair(outgoing.getDataVector());
      transferPair.transfer();

    }

  }

}

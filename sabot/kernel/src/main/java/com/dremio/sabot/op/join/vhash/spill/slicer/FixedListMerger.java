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

import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.util.TransferPair;

/**
 * A {@link Merger} implementation for fixed size list Arrow vectors {@link FixedSizeListVector} A
 * FixedSizeListVector's children can be fixed sized lists of primitive or complex types.It provides
 * validity bitmap to keep track of null values.
 *
 * <p>This merger provides functionality of merging together data of multiple FixedSizeListVector
 * vectors into one, it merges validity buffers as well as data vectors.
 *
 * <p>Arrow vector layout types including fixed size list vector documented at -
 * https://arrow.apache.org/docs/format/Columnar.html
 *
 * <p>JIRA ticket for this change - DX-48490
 */
public class FixedListMerger implements Merger {

  private final int wrapperIdx;

  private final BufferAllocator allocator;

  public FixedListMerger(
      FixedSizeListVector vector, final int wrapperIdx, final BufferAllocator allocator) {
    this.wrapperIdx = wrapperIdx;
    this.allocator = allocator;
  }

  /**
   * Merge multiple FixedSizeListVector instances into one.
   *
   * @param srcContainers contains list of vectors to be merged
   * @param dst page from which buffers for merged vector will be allocated
   * @param vectorOutput merged vector
   */
  @Override
  public void merge(
      final VectorContainerList srcContainers,
      final Page dst,
      final List<FieldVector> vectorOutput) {

    // create a list of vectors to be merged into a single vector
    final List<FixedSizeListVector> vectorListToBeMerged = new ArrayList<>();

    for (final VectorWrapper<?> wrapper : srcContainers.getWrappers(wrapperIdx)) {
      vectorListToBeMerged.add((FixedSizeListVector) wrapper.getValueVector());
    }

    // outgoing vector into which all above vectors will be merged
    final FixedSizeListVector outgoing =
        (FixedSizeListVector)
            vectorListToBeMerged
                .get(0)
                .getTransferPair(vectorListToBeMerged.get(0).getField(), allocator)
                .getTo();
    vectorOutput.add(outgoing);

    // validity buffer size of the merged vector
    final int recordCount = srcContainers.getRecordCount();
    final int validityLen = Merger.getValidityBufferSizeInBits(recordCount) / BYTE_SIZE_BITS;

    try (final ArrowBuf validityBuf = dst.sliceAligned(validityLen)) {

      outgoing.loadFieldBuffers(new ArrowFieldNode(recordCount, -1), ImmutableList.of(validityBuf));

      validityBuf.setZero(0, validityLen);

      // merge validity buffers
      Merger.mergeValidityBuffers(vectorListToBeMerged, validityBuf);

      // make a list of all data vectors, so they would be merged
      final List<ValueVector> dataVectors = new ArrayList<>();
      for (final FixedSizeListVector vector : vectorListToBeMerged) {
        dataVectors.add(vector.getDataVector());
      }

      // get a merger instance for whatever underlying data vector's type is for this list vector
      final Merger childMerger = Merger.get(outgoing.getDataVector(), 0, allocator);

      final List<FieldVector> mergedDataVectors = new ArrayList<>();

      // create a VectorContainerList instance of all data vectors. It will be used as an input to
      // child merger.
      final VectorContainerList containerList = new VectorContainerList(dataVectors, 0);

      // merge underlying data vectors
      childMerger.merge(containerList, dst, mergedDataVectors);

      // transfer contents of output vector (of all data vector) to out final output's data vector
      final TransferPair transferPair =
          mergedDataVectors.get(0).makeTransferPair(outgoing.getDataVector());
      transferPair.transfer();
    }
  }
}

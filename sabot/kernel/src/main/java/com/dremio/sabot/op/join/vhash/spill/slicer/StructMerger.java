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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.google.common.collect.ImmutableList;

/**
 * A {@link Merger} implementation for Arrow vector type {@link StructVector}
 * A struct vector represents a struct (e.g. { "name" : "John", "id" : 1}) in columnar format.
 * For each field in the struct, there's a separate child data vector. Struct vector itself only has validity buffer.
 *
 * This merger provides functionality of merging together data of multiple StructVector instances into one, it merges
 * validity buffers and underlying fields' data vectors.
 *
 * Arrow vector layout types including struct vector documented at -
 * https://arrow.apache.org/docs/format/Columnar.html
 *
 * JIRA ticket for this change - DX-54668
 *
 */
public class StructMerger implements Merger {
  private final int wrapperIdx;
  private final BufferAllocator allocator;

  public StructMerger(StructVector vector, final int wrapperIdx, final BufferAllocator allocator) {
    this.wrapperIdx = wrapperIdx;
    this.allocator = allocator;
  }

  /**
   * Merges multiple StructVector instances into one.
   * @param srcContainers
   * @param dst
   * @param vectorOutput
   */
  @Override
  public void merge(final VectorContainerList srcContainers, final Page dst, final List<FieldVector> vectorOutput) {
    //create a list of vectors to be merged into a single vector
    final List<StructVector> vectorListToBeMerged = new ArrayList<>();

    for (final VectorWrapper<?> wrapper : srcContainers.getWrappers(wrapperIdx)) {
      vectorListToBeMerged.add((StructVector) wrapper.getValueVector());
    }

    //outgoing vector into which all above vectors will be merged
    final StructVector outgoing = (StructVector) vectorListToBeMerged.get(0).getTransferPair(allocator).getTo();
    vectorOutput.add(outgoing);

    //pre-calculated record count of all records at a particular wrapper index
    final int recordCount = srcContainers.getRecordCount();

    //validity buffer size of the merged vector
    final int validityLen = Merger.getValidityBufferSizeInBits(recordCount) / BYTE_SIZE_BITS;

    try (final ArrowBuf validityBuf = dst.sliceAligned(validityLen)) {
      outgoing.loadFieldBuffers(new ArrowFieldNode(recordCount, -1), ImmutableList.of(validityBuf));

      validityBuf.setZero(0, validityLen);

      // merge validity buffers
      Merger.mergeValidityBuffers(vectorListToBeMerged, validityBuf);

      /**
       * Struct vector has a separate data vector for each field in a struct. Each of this child vectors has an
       * ordinal associated with it. Below map has all the child vectors of a particular ordinal from all input vectors
       * against that ordinal. Since input list of vectors belong to same batch, they will have same names and ordinals
       * of struct fields.
       */
      final Map<Integer, List<ValueVector>> ordinalToVectorField = getFieldsByOrdinal(vectorListToBeMerged);

      //each child field vector will be merged separately
      for (final Map.Entry<Integer, List<ValueVector>> entry : ordinalToVectorField.entrySet()) {
        final int ordinal = entry.getKey();
        final List<ValueVector> fieldVectors = entry.getValue();

        //get merger for whatever type this child vector is
        final Merger childMerger = Merger.get(outgoing.getChildByOrdinal(ordinal), 0, allocator);

        final List<FieldVector> mergedDataVectors = new ArrayList<>();

        //create a VectorContainerList instance of all data vectors. It will be used as an input to child merger.
        final VectorContainerList containerList = new VectorContainerList(fieldVectors, 0);

        //merge underlying data vectors
        childMerger.merge(containerList, dst, mergedDataVectors);

        //transfer contents of output vector (of all data vector) to out final output's data vector
        final TransferPair transferPair = mergedDataVectors.get(0).makeTransferPair(outgoing.getChildByOrdinal(ordinal));
        transferPair.transfer();
      }
    }
  }

  Map<Integer, List<ValueVector>> getFieldsByOrdinal(final List<StructVector> vectorList) {
    final Map<Integer, List<ValueVector>> ordinalToFieldVectors = new HashMap<>();

    //all vectors in the list will have the same fields
    final int nFields = vectorList.get(0).getField().getChildren().size();

    for (final StructVector structVector : vectorList) {
      for (int i = 0; i < nFields; i++) {
        if (!ordinalToFieldVectors.containsKey(i)) {
          ordinalToFieldVectors.put(i, new ArrayList<>());
        }
        ordinalToFieldVectors.get(i).add(structVector.getChildByOrdinal(i));
      }
    }
    return ordinalToFieldVectors;
  }
}

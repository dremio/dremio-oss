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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.google.common.collect.ImmutableList;

/**
 * A {@link Merger} implementation for Arrow vector type {@link UnionVector}
 * In a sparse UnionVector, each record can be any one of the fixed set of types. TypeId of each record is defined
 * at the same index in a types buffer. For each of the types, there's a separate data vector.
 *
 * This merger provides functionality of merging together data of multiple sparse union vector instances into one, it merges
 * types buffers and underlying fields' data vectors.
 *
 * Arrow vector layout types including struct vector documented at -
 * https://arrow.apache.org/docs/format/Columnar.html
 *
 * JIRA ticket for this change - DX-54668
 *
 */
public class UnionMerger implements Merger {
  private final int wrapperIdx;
  private final BufferAllocator allocator;

  public UnionMerger(UnionVector vector, final int wrapperIdx, final BufferAllocator allocator) {
    this.wrapperIdx = wrapperIdx;
    this.allocator = allocator;
  }

  /**
   * Merges multiple UnionVector instances into one.
   * @param srcContainers
   * @param dst
   * @param vectorOutput
   */
  @Override
  public void merge(final VectorContainerList srcContainers, final Page dst, final List<FieldVector> vectorOutput) {
    //create a list of vectors to be merged into a single vector
    final List<UnionVector> vectorListToBeMerged = new ArrayList<>();

    for (final VectorWrapper<?> wrapper : srcContainers.getWrappers(wrapperIdx)) {
      vectorListToBeMerged.add((UnionVector) wrapper.getValueVector());
    }

    //outgoing vector into which all above vectors will be merged
    final UnionVector outgoing = (UnionVector) vectorListToBeMerged.get(0).getTransferPair(allocator).getTo();
    vectorOutput.add(outgoing);

    //pre-calculated record count of all records at a particular wrapper index
    final int recordCount = srcContainers.getRecordCount();

    final int typesBufferLen = Merger.getTypeBufferSizeInBits(recordCount) / BYTE_SIZE_BITS;

    try (final ArrowBuf typesBuffer = dst.sliceAligned(typesBufferLen)) {
      outgoing.loadFieldBuffers(new ArrowFieldNode(recordCount, -1), ImmutableList.of(typesBuffer));

      //Note all the type ids present in this vector while merging type buffers
      Set<Byte> allTypesIds = new HashSet<>();

      int globalIndex = 0;

      //merge types buffer
      for (final UnionVector current : vectorListToBeMerged) {
        for (int i = 0; i < current.getValueCount(); i++) {
          final byte srcType = (byte) current.getTypeValue(i);
          allTypesIds.add(srcType);
          typesBuffer.setByte(globalIndex, srcType);
          globalIndex++;
        }
      }

      final Map<Byte, List<ValueVector>> ordinalToVectorField = getVectorsByTypeId(vectorListToBeMerged, allTypesIds);

      //each data vector will be merged separately
      for (final Map.Entry<Byte, List<ValueVector>> entry : ordinalToVectorField.entrySet()) {
        final byte typeId = entry.getKey();
        final List<ValueVector> fieldVectors = entry.getValue();

        //get merger for whatever type this child vector is
        final Merger childMerger = Merger.get(outgoing.getVectorByType(typeId), 0, allocator);

        final List<FieldVector> mergedDataVectors = new ArrayList<>();

        //create a VectorContainerList instance of all data vectors. It will be used as an input to child merger.
        final VectorContainerList containerList = new VectorContainerList(fieldVectors, 0);

        //merge underlying data vectors
        childMerger.merge(containerList, dst, mergedDataVectors);

        //transfer contents of output vector (of all data vector) to out final output's data vector
        final TransferPair transferPair = mergedDataVectors.get(0).makeTransferPair(outgoing.getVectorByType(typeId));
        transferPair.transfer();
      }
    }
  }

  private Map<Byte, List<ValueVector>> getVectorsByTypeId(final List<UnionVector> vectorList, final Set<Byte> typeIds) {
    final Map<Byte, List<ValueVector>> typeIdToFieldVectors = new HashMap<>();

    for (final UnionVector unionVector : vectorList) {
      for (final Byte typeId : typeIds) {
        if (!typeIdToFieldVectors.containsKey(typeId)) {
          typeIdToFieldVectors.put(typeId, new ArrayList<>());
        }
        typeIdToFieldVectors.get(typeId).add(unionVector.getVectorByType(typeId));
      }
    }
    return typeIdToFieldVectors;
  }
}

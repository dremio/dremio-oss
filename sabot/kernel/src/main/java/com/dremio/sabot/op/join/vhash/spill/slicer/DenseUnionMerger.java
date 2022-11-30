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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.google.common.collect.ImmutableList;

public class DenseUnionMerger implements Merger {
  private final int wrapperIdx;
  private final BufferAllocator allocator;

  public DenseUnionMerger(DenseUnionVector vector, final int wrapperIdx, final BufferAllocator allocator) {
    this.wrapperIdx = wrapperIdx;
    this.allocator = allocator;
  }

  @Override
  public void merge(final VectorContainerList srcContainers, final Page dst, final List<FieldVector> vectorOutput) {
    //create a list of vectors to be merged into a single vector
    final List<DenseUnionVector> vectorListToBeMerged = new ArrayList<>();

    for (final VectorWrapper<?> wrapper : srcContainers.getWrappers(wrapperIdx)) {
      vectorListToBeMerged.add((DenseUnionVector) wrapper.getValueVector());
    }

    //outgoing vector into which all above vectors will be merged
    final DenseUnionVector outgoing = (DenseUnionVector) vectorListToBeMerged.get(0).getTransferPair(allocator).getTo();
    vectorOutput.add(outgoing);

    //pre-calculated record count of all records at a particular wrapper index
    final int recordCount = srcContainers.getRecordCount();

    final int typesBufferLen = Merger.getTypeBufferSizeInBits(recordCount) / BYTE_SIZE_BITS;
    final int offsetLen = Merger.getOffsetBufferSizeInBits(recordCount) / BYTE_SIZE_BITS;

    try (final ArrowBuf typesBuffer = dst.sliceAligned(typesBufferLen);
         final ArrowBuf offsetBuffer = dst.sliceAligned(offsetLen)) {
      outgoing.loadFieldBuffers(new ArrowFieldNode(recordCount, -1),
        ImmutableList.of(typesBuffer, offsetBuffer));

      final Set<Byte> allTypesIds = new HashSet<>();

      //merge types and offset buffer
      int typeGloablIndex = 0;
      int offsetGloablIndex = 0;

      /**
       * Union vector can accommodate 127 possible types. While merging offset buffers together, for each of those types, we need
       * max offset of this type in last vector merged, so we can continue merging from that offset. prevVectorLastPosition provides that
       * last vector offset. currentVectorLastPosition keeps track of last offset of current vector so it can be used for next vector.
       */
      int[] prevVectorLastPosition = new int[127];
      Arrays.fill(prevVectorLastPosition, 0);

      final int[] currentVectorLastPosition = new int[127];
      Arrays.fill(currentVectorLastPosition, 0);

      for (final DenseUnionVector current : vectorListToBeMerged) {
        for (int i = 0; i < current.getValueCount(); i++) {
          final byte type = current.getTypeId(i);
          allTypesIds.add(type);
          typesBuffer.setByte(typeGloablIndex, type);
          typeGloablIndex++;

          final int offset = current.getOffset(i) + prevVectorLastPosition[type];
          offsetBuffer.setInt(offsetGloablIndex, offset);
          currentVectorLastPosition[type] = offset + 1;
          offsetGloablIndex += OFFSET_WIDTH;
        }

        prevVectorLastPosition = Arrays.copyOf(currentVectorLastPosition, currentVectorLastPosition.length);
        Arrays.fill(currentVectorLastPosition, 0);
      }

      //all data vectors merged separately
      final Map<Byte, List<ValueVector>> ordinalToVectorField = getFieldsByTypeId(vectorListToBeMerged, allTypesIds);

      for (final Map.Entry<Byte, List<ValueVector>> entry : ordinalToVectorField.entrySet()) {
        final byte type = entry.getKey();
        final List<ValueVector> fieldVectors = entry.getValue();

        //get merger for whatever type this child vector is
        final Merger childMerger = Merger.get(outgoing.getVectorByType(type), 0, allocator);

        final List<FieldVector> mergedDataVectors = new ArrayList<>();

        //create a VectorContainerList instance of all data vectors. It will be used as an input to child merger.
        final VectorContainerList containerList = new VectorContainerList(fieldVectors, 0);

        //merge underlying data vectors
        childMerger.merge(containerList, dst, mergedDataVectors);

        //transfer contents of output vector (of all data vector) to out final output's data vector
        final TransferPair transferPair = mergedDataVectors.get(0).makeTransferPair(outgoing.getVectorByType(type));
        transferPair.transfer();
      }
    }
  }

  private Map<Byte, List<ValueVector>> getFieldsByTypeId(final List<DenseUnionVector> vectorList, final Set<Byte> typeIds) {
    final Map<Byte, List<ValueVector>> typeIdToFieldVectors = new HashMap<>();

    for (final DenseUnionVector unionVector : vectorList) {
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

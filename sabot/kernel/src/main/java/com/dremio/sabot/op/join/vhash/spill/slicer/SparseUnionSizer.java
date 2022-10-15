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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

import com.dremio.sabot.op.copier.FieldBufferPreAllocedCopier;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.google.common.collect.ImmutableList;
/**
 *  A {@link Sizer} implementation for sparse union Arrow vectors {@link UnionVector}
 *  A UnionVector is ordered list of records of various data types. A separate vector is assigned to each vector type in the union and
 *  all the children of same type are stored in those vectors.
 *
 *  As opposed to dense union vector, sparse union vector allocates space even for null values.
 *
 *  This Sizer provides support for copier related operations on UnionVector.
 *
 *  Arrow vector layout types including sparse union vector documented at -
 *  https://arrow.apache.org/docs/format/Columnar.html
 *
 *  JIRA ticket for this change -
 *  https://dremio.atlassian.net/browse/DX-48490
 */
public class SparseUnionSizer implements Sizer{


  private final UnionVector incoming;


  public SparseUnionSizer(final UnionVector incoming) {
    this.incoming = incoming;
  }

  @Override
  public void reset() {
    //no caching done for this vector type
  }

  @Override
  public int getEstimatedRecordSizeInBits() {
    if (incoming.getValueCount() == 0) {
      return 0;
    } else {
      return (incoming.getBufferSize() / incoming.getValueCount()) * BYTE_SIZE_BITS;
    }
  }

  /**
   * From given selection vector buffer, starting from given index, computer size needed to save data for given number of records
   * Computes space only for actual data buffers, excluding validity, offset etc. buffers
   * @param sv2 selection vector buffer - contains indices of records in the vector that we have to consider for size computation
   * @param startIdx pick records from this index in sv2 buffer
   * @param numberOfRecords
   * @return
   */
  private int computeDataSizeForGivenOrdinals(final ArrowBuf sv2, final int startIdx, final int numberOfRecords) {
    int dataSize = 0;
    for (int i = 0; i < numberOfRecords; ++i) {
      final int ordinal = SV2UnsignedUtil.readAtIndex(sv2, startIdx + i);
      dataSize += getDataSizeInBitsStartingFromOrdinal(ordinal, 1);
    }
    return dataSize;
  }

  @Override
  public int computeBitsNeeded(final ArrowBuf sv2Buffer, final int startIndex, final int numberOfRecords) {
    //space to save buffer of actual data records
    final int dataSize = computeDataSizeForGivenOrdinals(sv2Buffer, startIndex, numberOfRecords);
    //space to store type buffer of a union vector
    final int typeBufferSize = Sizer.getTypeBufferSizeInBits(numberOfRecords);
    return dataSize + typeBufferSize;
  }

  /**
   * Computes purely data size excludes size required for offsets/validity buffers
   * @param ordinal pick records starting from this ordinal
   * @param numberOfRecords
   * @return
   */
  private int getDataSizeInBitsStartingFromOrdinal(final int ordinal, final int numberOfRecords){

    int dataSize = 0;

    for(int index = ordinal; index < ordinal + numberOfRecords; index++){

      //Union vector's type buffer contains data type id of the vector at their ordinals
      final int vectorTypeId = incoming.getTypeValue(index);

      //get vector by type id
      final Sizer childVectorSizer = Sizer.get(incoming.getVectorByType((byte) vectorTypeId));

      dataSize += childVectorSizer.getSizeInBitsStartingFromOrdinal(index,1);

    }

    return dataSize;
  }

  @Override
  public int getSizeInBitsStartingFromOrdinal(final int ordinal, final int numberOfRecords) {

    final int typeBufferSize = Sizer.getTypeBufferSizeInBits(numberOfRecords);

    final int dataSize = getDataSizeInBitsStartingFromOrdinal(ordinal, numberOfRecords);

    return typeBufferSize + dataSize;
  }

  @Override
  public Copier getCopier(final BufferAllocator allocator, final ArrowBuf sv2, final int startIdx, final int numberOfRecords, final List<FieldVector> vectorOutput) {
    final FieldVector outgoing = (FieldVector) incoming.getTransferPair(allocator).getTo();
    vectorOutput.add(outgoing);
    sv2.checkBytes((long) startIdx * SV2_SIZE_BYTES, (long) (startIdx + numberOfRecords) * SV2_SIZE_BYTES);
    return (page) -> {

      //length of type array is 1 byte each record. i.e. numberOfRecords
      try (final ArrowBuf typeBuf = page.sliceAligned(numberOfRecords)) {

        outgoing.loadFieldBuffers(new ArrowFieldNode(numberOfRecords, -1),
          ImmutableList.of(typeBuf));

        FieldBufferPreAllocedCopier.getCopiers(ImmutableList.of(incoming), ImmutableList.of(outgoing))
          .forEach(copier -> copier.copy(sv2.memoryAddress() + (long) startIdx * SV2_SIZE_BYTES, numberOfRecords));
      }
    };
  }
}

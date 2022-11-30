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
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

import com.dremio.exec.util.RoundUtil;
import com.dremio.sabot.op.copier.FieldBufferPreAllocedCopier;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.google.common.collect.ImmutableList;

/**
 *
 * A {@link Sizer} implementation for fixed size list Arrow vectors {@link FixedSizeListVector}
 * A FixedSizeListVector's children can be fixed sized lists of primitive or complex types.It provides validity
 * bitmap to keep track of null values.
 * This Sizer provides support for copier related operations on FixedSizeListVector.
 *
 * Arrow vector layout types including fixed size list vector documented at -
 * https://arrow.apache.org/docs/format/Columnar.html
 *
 * JIRA ticket for this change - DX-48490
 *
 */
public class FixedListSizer implements Sizer{

  //variable-size list vector this class operates on
  private final FixedSizeListVector incoming;

  public FixedListSizer(final FixedSizeListVector incoming) {
    super();
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
   * Computes purely data size excludes size required for offsets/validity buffers
   * @param ordinal pick records starting from this ordinal
   * @param numberOfRecords
   * @return
   */
  private int getDataSizeInBitsStartingFromOrdinal(final int ordinal, final int numberOfRecords){

    final int start = ordinal * incoming.getListSize();
    final int length =  numberOfRecords * incoming.getListSize();

    final Sizer childVectorSizer = Sizer.get(incoming.getDataVector());
    return RoundUtil.round64up(childVectorSizer.getSizeInBitsStartingFromOrdinal(start, length));

  }

  @Override
  public int getSizeInBitsStartingFromOrdinal(final int ordinal, final int numberOfRecords) {

    final int validityBufferSize = Sizer.getValidityBufferSizeInBits(numberOfRecords);
    final int dataBufferSize = getDataSizeInBitsStartingFromOrdinal(ordinal, numberOfRecords);

    return  validityBufferSize + dataBufferSize;
  }

  @Override
  public int computeBitsNeeded(final ArrowBuf sv2Buffer, final int startIndex, final int numberOfRecords) {
    //space to save buffer of validity bits
    final int validitySize = Sizer.getValidityBufferSizeInBits(numberOfRecords);

    //space to save buffer of actual data records
    final int dataSize = computeDataSizeForGivenOrdinals(sv2Buffer, startIndex, numberOfRecords);

    return dataSize + validitySize;
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
  public Copier getCopier(final BufferAllocator allocator, final ArrowBuf sv2, final int startIdx, final int numberOfRecords, final List<FieldVector> vectorOutput) {
    final FieldVector outgoing = (FieldVector) incoming.getTransferPair(allocator).getTo();
    vectorOutput.add(outgoing);
    sv2.checkBytes(startIdx * SV2_SIZE_BYTES, (startIdx + numberOfRecords) * SV2_SIZE_BYTES);
    return (page) -> {

      final int validityLen = Sizer.getValidityBufferSizeInBits(numberOfRecords) / BYTE_SIZE_BITS;

      try (final ArrowBuf validityBuf = page.sliceAligned(validityLen)) {
        outgoing.loadFieldBuffers(new ArrowFieldNode(numberOfRecords, -1),
        ImmutableList.of(validityBuf));

        // The bit copiers do ORs to set the bits, and expect that the buffer is zero-filled to begin with.
        validityBuf.setZero(0, validityLen);
        // rest of the offsets will be filled in during the copy
        FieldBufferPreAllocedCopier.getCopiers(ImmutableList.of(incoming), ImmutableList.of(outgoing))
          .forEach(copier -> copier.copy(sv2.memoryAddress() + startIdx * SV2_SIZE_BYTES, numberOfRecords));
      }
    };
  }
}

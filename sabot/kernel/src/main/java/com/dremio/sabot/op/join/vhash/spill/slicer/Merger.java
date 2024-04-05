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

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.util.RoundUtil;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;

/** Merge input list of vectors into a single vector, allocated from the supplied page. */
interface Merger {

  int BYTE_SIZE_BITS = 8;
  int VALID_SIZE_BITS = 1;
  int OFFSET_SIZE_BITS = 32;

  void merge(VectorContainerList src, Page dst, List<FieldVector> vectorOutput);

  static Merger get(ValueVector vector, int wrapperIdx, BufferAllocator allocator) {
    if (vector instanceof BaseFixedWidthVector) {
      return new FixedMerger((BaseFixedWidthVector) vector, wrapperIdx, allocator);
    } else if (vector instanceof BaseVariableWidthVector) {
      return new VariableMerger((BaseVariableWidthVector) vector, wrapperIdx, allocator);
    } else if (vector instanceof FixedSizeListVector) {
      return new FixedListMerger((FixedSizeListVector) vector, wrapperIdx, allocator);
    } else if (vector instanceof ListVector) {
      return new ListMerger((ListVector) vector, wrapperIdx, allocator);
    } else if (vector instanceof StructVector) {
      return new StructMerger((StructVector) vector, wrapperIdx, allocator);
    } else if (vector instanceof UnionVector) {
      return new UnionMerger((UnionVector) vector, wrapperIdx, allocator);
    } else if (vector instanceof DenseUnionVector) {
      return new DenseUnionMerger((DenseUnionVector) vector, wrapperIdx, allocator);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Vectors for field %s of type %s not yet supported.",
              vector.getField().getName(), CompleteType.fromField(vector.getField()).toString()));
    }
  }

  // Copy bits from src to dst
  static void copyBits(ArrowBuf src, ArrowBuf dst, int seek, int count) {
    if (seek % BYTE_SIZE_BITS == 0) {
      int byteCount = (count + BYTE_SIZE_BITS - 1) / BYTE_SIZE_BITS;
      dst.setBytes(seek / BYTE_SIZE_BITS, src, 0, byteCount);
    } else {
      int targetIndex = seek;
      for (int srcIndex = 0; srcIndex < count; ++srcIndex, ++targetIndex) {
        final int byteValue = src.getByte(srcIndex >>> 3);
        final int bitVal = ((byteValue >>> (srcIndex & 7)) & 1) << (targetIndex & 7);

        final int oldValue = dst.getByte(targetIndex >>> 3);
        dst.setByte(targetIndex >>> 3, oldValue | bitVal);
      }
    }
  }

  /**
   * Bits required to store validity bits for given number of records
   *
   * @param numberOfRecords
   * @return
   */
  static int getValidityBufferSizeInBits(final int numberOfRecords) {
    return RoundUtil.round64up(VALID_SIZE_BITS * numberOfRecords);
  }

  /**
   * Merge validity buffers of given vectors into a given single buffer
   *
   * @param src
   * @param validityBuf all validity buffers are merged into this buffer
   */
  static void mergeValidityBuffers(
      final List<? extends ValueVector> src, final ArrowBuf validityBuf) {
    int bitPosition = 0;
    for (final ValueVector current : src) {
      Merger.copyBits(
          current.getValidityBuffer(), validityBuf, bitPosition, current.getValueCount());
      bitPosition += current.getValueCount();
    }
  }

  /**
   * Bits required to store offset values for given number of records
   *
   * @param numberOfRecords
   * @return
   */
  static int getOffsetBufferSizeInBits(final int numberOfRecords) {
    return RoundUtil.round64up(OFFSET_SIZE_BITS * (numberOfRecords + 1));
  }

  /**
   * Bits required to store type data (in union vectors) for given number of records
   *
   * @param numberOfRecords
   * @return
   */
  static int getTypeBufferSizeInBits(final int numberOfRecords) {
    return RoundUtil.round64up(numberOfRecords * BYTE_SIZE_BITS);
  }
}

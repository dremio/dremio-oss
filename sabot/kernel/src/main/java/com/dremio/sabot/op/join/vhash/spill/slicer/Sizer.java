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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.selection.SelectionVector2;

/**
 * A tool that determines size of an operation and then provides a copier to
 * later do that operation.
 */
interface Sizer {
  int BYTE_SIZE_BITS = 8;
  int VALID_SIZE_BITS = 1;
  int OFFSET_SIZE_BYTES = 4;
  int OFFSET_SIZE_BITS = 32;
  int SV2_SIZE_BYTES = SelectionVector2.RECORD_SIZE;

  /**
   * Reset any caching.
   */
  void reset();

  /**
   * Compute size in bits required to copy the specified sv2 entries from incoming
   *
   * @param sv2Addr sv2 address
   * @param count number of entries in the sv2
   * @return The amount of space consumed in bits.
   */
  int computeBitsNeeded(long sv2Addr, int count);

  /**
   * Estimate the number of BITS per record.
   * @return the size in bits.
   */
  int getEstimatedRecordSizeInBits();

  /**
   * Get a copier that will copy data to a new set of vectors. As part of the
   * creation of this copier, add the new vector to the provided vectorOuput list.
   *
   * @param allocator buffer allocator
   * @param sv2Addr Addr of the sv2 array that has the ordinals to copy.
   * @param count number of entries in the sv2
   * @param vectorOutput Output variable populated by copiers.
   * @return A copier that will copy the provided data (once it is allocated).
   */
  Copier getCopier(BufferAllocator allocator, long sv2Addr, int count, List<FieldVector> vectorOutput);

  static Sizer get(ValueVector vector) {
    if (vector instanceof BaseFixedWidthVector) {
      return new FixedSizer((BaseFixedWidthVector) vector);
    } else if (vector instanceof BaseVariableWidthVector) {
      return new VariableSizer((BaseVariableWidthVector) vector);
    } else {
      throw new UnsupportedOperationException(String.format("Vectors for field %s of type %s not yet supported.",
          vector.getField().getName(), CompleteType.fromField(vector.getField()).toString()));
    }
  }

  static int round8up(int val) {
    int rem = val % 8;
    if (rem == 0) {
      return val;
    } else {
      return val - rem + 8;
    }
  }

  // All buffers in a vector are expected to start at an eight-byte aligned buffer.
  static int round64up(int val) {
    int rem = val % 64;
    if (rem == 0) {
      return val;
    } else {
      return val - rem + 64;
    }
  }
}

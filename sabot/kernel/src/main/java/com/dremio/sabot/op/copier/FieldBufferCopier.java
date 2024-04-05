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
package com.dremio.sabot.op.copier;

import org.apache.arrow.vector.FixedWidthVector;

/** Abstraction of a vectorized copier */
public interface FieldBufferCopier {
  int AVG_VAR_WIDTH = 15;
  int BUILD_RECORD_LINK_SIZE = 6;

  void allocate(int records);

  // Cursor into the target vector.
  final class Cursor {
    // index in the target vector.
    private int targetIndex;

    public Cursor() {
      this.targetIndex = 0;
    }

    public Cursor(int targetIndex) {
      this.targetIndex = targetIndex;
    }

    public Cursor(Cursor cursor) {
      this.targetIndex = cursor.targetIndex;
    }

    public int getTargetIndex() {
      return targetIndex;
    }

    public void setTargetIndex(int targetIndex) {
      this.targetIndex = targetIndex;
    }
  }

  void copy(long offsetAddr, int count);

  /**
   * Copy starting from the previous cursor.
   *
   * @param offsetAddr offset addr of the selection vector
   * @param count number of entries to copy
   * @param cursor cursor (tracks state across invocations).
   */
  default void copy(long offsetAddr, int count, Cursor cursor) {
    throw new UnsupportedOperationException("copy with cursor not supported");
  }

  // Copy data and set validity to 0 for all records in nullAddr
  // nullAddr is all the offsets of the records that are null
  // The action to set validity is only needed for BitCopier in FieldBufferCopier,
  // because validity data is copied in BitCopier
  void copy(long offsetAddr, int count, long nullAddr, int nullCount);

  // same as above, but with a cursor.
  default void copy(long offsetAddr, int count, long nullAddr, int nullCount, Cursor cursor) {
    throw new UnsupportedOperationException("copy with cursor not supported");
  }

  /**
   * Copy data from source to target vector. This method is only called for the inner vectors of a
   * list vector In case of inner data vectors of list vectors the sv index won't correspond to the
   * actual indices, so we set the start and end offsets on a tmp buffer in the copy call of parent
   * list vector and update the offsets for the inner data vector in each copyInnerList call of
   * inner list vector. The leaf level copyInnerList call will make use of the offsets to copy the
   * values. (Another approach would have been to setting the selection vector for each inner list,
   * instead of setting the offsets, which would enable us to make use of existing copy methods
   * instead of introducing this new method. However the issue with that is the inner vector record
   * count can be large and so might not fit in the 2-byte limit of sv. Also it would need
   * allocating a large buffer for each level and setting the sv index for each inner vector member)
   *
   * @param listOffsetBufAddr
   * @param count
   * @param seekTo
   * @return
   */
  default void copyInnerList(long listOffsetBufAddr, int count, int seekTo) {
    throw new UnsupportedOperationException("copyInnerList() is not supported");
  }

  // Ensure that the vector is sized upto capacity 'size'.
  default boolean resizeIfNeeded(FixedWidthVector vector, int size) {
    boolean resized = false;
    while (vector.getValueCapacity() < size) {
      resized = true;
      vector.reAlloc();
    }
    return resized;
  }
}

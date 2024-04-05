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
package org.apache.arrow.vector;

import io.netty.util.internal.PlatformDependent;
import org.apache.arrow.vector.complex.ListVector;

public class VectorHelper {

  /**
   * For a ListVector record at a given index, return it's size in bytes and count of elements in
   * it.
   *
   * @param vector
   * @param listIndex
   * @return
   */
  public static ListVectorRecordInfo getListVectorEntrySizeAndCount(
      final ListVector vector, final int listIndex) {

    if (vector.isNull(listIndex)) {
      return new ListVectorRecordInfo(-1, -1);
    }

    // This list's start and end index in data vector
    final long startAndEndOffset = getLongOffsetValueAtAnIndex(vector, listIndex);

    // This list's start and end index in child data vector (values next to each other after given
    // index in offset buffer)
    final int start = (int) startAndEndOffset;
    final int end = (int) (startAndEndOffset >>> 32);

    final int recordCount = end - start;

    // This is an empty record.
    if (recordCount == 0) {
      return new ListVectorRecordInfo(0, 1);
    }

    // This list's start and end offsets in child data vector
    final int offsetEnd = getIntOffsetValueAtAnIndex(vector.getDataVector(), end);
    final int offsetStart = getIntOffsetValueAtAnIndex(vector.getDataVector(), start);

    final int recordSize = offsetEnd - offsetStart;

    return new ListVectorRecordInfo(recordSize, recordCount);
  }

  private static long getLongOffsetValueAtAnIndex(final FieldVector vector, final int index) {
    return PlatformDependent.getLong(vector.getOffsetBufferAddress() + index * 4L);
  }

  private static int getIntOffsetValueAtAnIndex(final FieldVector vector, final int index) {
    return PlatformDependent.getInt(vector.getOffsetBufferAddress() + index * 4L);
  }

  /**
   * Get length of the index'th record of a BaseVariableWidthVector
   *
   * @param vector
   * @param index
   * @return -1 if record is null else the record length
   */
  public static int getVariableWidthVectorValueLength(
      final BaseVariableWidthVector vector, final int index) {

    if (vector.isNull(index)) {
      return -1;
    }

    return vector.getValueLength(index);
  }
}

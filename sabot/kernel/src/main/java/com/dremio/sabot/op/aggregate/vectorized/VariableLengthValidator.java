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
package com.dremio.sabot.op.aggregate.vectorized;

import com.google.common.base.Preconditions;
import io.netty.util.internal.PlatformDependent;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BoundsChecking;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

public class VariableLengthValidator {

  private static final int SIGN_MASK = 0x80000000;

  /**
   * Validates whether the provided variable width vector has an internally consistent structure,
   * ensuring no invalid memory address references. This means that any correct code interacting
   * with these buffers should be guaranteed to be able to all internal memory accesses described by
   * this vector without checking if any memory is valid.
   *
   * <p>We should target ultimately moving this the variable length vectors themselves. Confirms
   * each of the following:
   *
   * <p>- The number of field buffers == 3 - The nullability buffer has enough bytes to support the
   * provided number of records. - The offset vectors has records + 1 total number of bytes of
   * addressable space. - The first offset was zero or positive. - The rest of the offsets in the
   * offset vector are greater than or equal to their previous values. - All references in the
   * offset vector are within the bytes available in the data vector.
   *
   * @param vector The variable length vector to validate.
   * @param records The number of valid records to validate.
   */
  public static void validateVariable(FieldVector vector, int records) {
    if (!BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      return;
    }

    assert vector instanceof VarCharVector || vector instanceof VarBinaryVector;

    final List<ArrowBuf> buffers = vector.getFieldBuffers();

    Preconditions.checkArgument(buffers.size() == 3, "Unexpected number of vectors.");

    if (records == 0) {
      return;
    }

    // first check that the bit buffer is long enough.
    ArrowBuf bitBuf = buffers.get(0);
    Preconditions.checkArgument(
        bitBuf.capacity() >= (records >>> 6),
        "BitVector not large enough for records. Expected at least %s bytes but actual bytes were %s.",
        records >>> 6,
        bitBuf.capacity());

    // next check that the increment is always non-negative in the offset vector.
    final ArrowBuf offsetBuf = buffers.get(1);
    Preconditions.checkArgument(
        offsetBuf.capacity() >= (records + 1) * 4,
        "Offset vector not large enough for records. Expected at least %s bytes but actual bytes were %s.",
        (records + 1) * 4,
        offsetBuf.capacity());

    int accum = 0;
    int prevVal = 0;
    final long max = offsetBuf.memoryAddress() + (records + 1) * 4;
    Preconditions.checkArgument(
        PlatformDependent.getInt(offsetBuf.memoryAddress()) >= 0,
        "The first offset of the offset vector was negative.");
    for (long memoryAddress = offsetBuf.memoryAddress(); memoryAddress < max; memoryAddress += 4) {
      int nextVal = PlatformDependent.getInt(memoryAddress);
      int diff = nextVal - prevVal;
      accum |= (diff & SIGN_MASK);
      prevVal = nextVal;
    }

    Preconditions.checkArgument(accum == 0, "One or more offsets were negative for vector.");

    // lastly, confirm that the final offset is within in the capacity of the data vector.
    final ArrowBuf dataBuf = buffers.get(2);
    Preconditions.checkArgument(
        dataBuf.capacity() >= prevVal,
        "Data vector undersized. Expected at least %s bytes but actual bytes were %s.",
        prevVal,
        dataBuf.capacity());
  }
}

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
package com.dremio.common.utils;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteOutput;

/**
 * {@link ByteOutput} implementation which optimistically which takes a copy of passed in array
 * references, from lazy methods only, when the complete array is available only.
 * <p>
 * Otherwise, any written bytes will be copied to a fallback ByteBuffer.
 * <p>
 * Errors are thrown if consuming of the internal array is attempted before all expected
 * bytes are available.
 */
public class OptimisticByteOutput extends ByteOutput {

  private static final byte[] EMPTY = new byte[0];

  private final int payloadSize;
  private byte[] arrayReference;
  private int arrayOffset;

  public OptimisticByteOutput(int payloadSize) {
    this.payloadSize = payloadSize;
  }

  @Override
  public void write(byte value) {
    checkArray();
    arrayReference[arrayOffset++] = value;
  }

  @Override
  public void write(byte[] value, int offset, int length) {
    if (length == 0) {
      // ignore empty array
      return;
    }

    checkArray();
    // Per ByteOutput#write(byte, int, int), value must be consumed/copied before this call is returned.
    System.arraycopy(value, offset, arrayReference, arrayOffset, length);
    this.arrayOffset += length;
  }

  @Override
  public void writeLazy(byte[] value, int offset, int length) {
    // If first write, check if value holds the complete payload, no more, no less
    if (arrayReference == null && payloadSize == length && offset == 0) {
      // Per ByteOutput#writeLazy(byte[], int, int), value is immutable and it is okay to keep a reference
      arrayReference = value;
      arrayOffset += payloadSize;
      return;
    }

    // fallback to copy
    write(value, offset, length);
  }


  @Override
  public void write(ByteBuffer value) {
    if (!value.hasRemaining()) {
      // ignore empty buffer
      return;
    }
    checkArray();
    int length = value.remaining();
    // Per ByteOutput#write(ByteBuffer), value must be consumed/copied before this call is returned.
    value.get(arrayReference, arrayOffset, length);
    // Per ByteOutput#write(ByteBuffer), position() should match limit() when this call returns
    arrayOffset += length;
  }

  @Override
  public void writeLazy(ByteBuffer value) {
    // If first write, check if value holds the complete payload, no more, no less
    if (arrayReference == null && value.hasArray() && value.arrayOffset() == 0 && value.array().length == payloadSize
        && value.position() == 0 && value.remaining() == payloadSize) {
      // Per ByteOutput#writeLazy(ByteBuffer), value is immutable and it is okay to keep a reference
      arrayReference = value.array();
      arrayOffset += payloadSize;

      // Per ByteOutput#writeLazy(ByteBuffer), position() should match limit() when this call returns
      value.position(payloadSize); // previous position was 0 per condition
      return;
    }

    // fallback to copy
    write(value);
  }

  public byte[] toByteArray() {
    if (payloadSize == 0) {
      return arrayReference != null ? arrayReference : EMPTY;
    }

    Preconditions.checkState(arrayOffset == payloadSize, "Byte payload not fully received.");
    return arrayReference;

  }

  private void checkArray() {
    if (arrayReference == null) {
      arrayReference = new byte[payloadSize];
    }
  }
}

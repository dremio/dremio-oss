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
package com.dremio.exec.expr.fn.impl;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Thin wrapper around byte array. This class is used by aggregate functions which consume decimal,
 * variable width vectors as inputs.
 */
public class ByteArrayWrapper implements Serializable {
  private byte[] bytes;
  private int length;

  public ByteArrayWrapper() {
    this.bytes = new byte[0];
    this.length = 0;
  }

  public ByteArrayWrapper(byte[] bytes, int length) {
    this.bytes = bytes;
    this.length = length;
  }

  public ByteArrayWrapper(byte[] bytes) {
    this(bytes, bytes.length);
  }

  public void setLength(int length) {
    this.length = length;
  }

  public byte[] getBytes() {
    return this.bytes;
  }

  public int getLength() {
    return this.length;
  }

  public void setBytes(byte[] bytes) {
    setBytes(bytes, bytes.length);
  }

  public void setBytes(byte[] bytes, int length) {
    this.bytes = bytes;
    this.length = length;
  }

  // Comparator for ByteArrayWrapper. Byte arrays must be compared by value, not reference.
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ByteArrayWrapper that = (ByteArrayWrapper) o;
    return Arrays.equals(bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public String toString() {
    return Arrays.toString(bytes);
  }
}

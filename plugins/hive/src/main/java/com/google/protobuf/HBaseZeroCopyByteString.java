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
package com.google.protobuf;

/**
 * This class shares the same FQCN as the HBaseZeroCopyByteString class in the hbase-protocol jar. The original
 * implementation uses {@code LiteralByteString} and {@code BoundedByteString} classes to avoid copying, but later
 * versions of protobuf make those classes private. This class is loaded before the original one, which avoids
 * {@code NoClassDefFoundError}. However, the class name is a misnomer since the implementation makes a copy.
 */
@SuppressWarnings("unused")
public final class HBaseZeroCopyByteString {

  /**
   * Private constructor so this class cannot be instantiated.
   */
  private HBaseZeroCopyByteString() {
    throw new UnsupportedOperationException("Should never be here.");
  }

  /**
   * Wraps a byte array in a {@code ByteString}.
   */
  public static ByteString wrap(final byte[] array) {
    return ByteString.copyFrom(array);
  }

  /**
   * Wraps a subset of a byte array in a {@code ByteString}.
   */
  public static ByteString wrap(final byte[] array, int offset, int length) {
    return ByteString.copyFrom(array, offset, length);
  }
}

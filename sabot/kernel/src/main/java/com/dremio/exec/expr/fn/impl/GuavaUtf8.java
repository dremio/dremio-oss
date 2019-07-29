/*
 * Copyright (C) 2013 The Guava Authors
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

import io.netty.buffer.ByteBuf;

/**
 * Utilities from Guava's Utf8, adapted to use ByteBufs instead of byte[]s
 */
public final class GuavaUtf8 {

  private GuavaUtf8() {}

  /**
   * Check if 'in' is a valid UTF-8 string in the range [start, end)
   * @param in  the buffer being checked
   * @param start start of the buffer
   * @param end  one past end of the buffer
   * @return true, if 'buf[start..end]' is a valid UTF-8 string; false if not
   */
  public static boolean isUtf8(ByteBuf in, int start, int end) {
    int index = start;
    while (true) {
      int byte1;

      // Optimize for interior runs of ASCII bytes.
      do {
        if (index >= end) {
          return true;
        }
      } while ((byte1 = in.getByte(index++)) >= 0);

      if (byte1 < (byte) 0xE0) {
        // Two-byte form.
        if (index == end) {
          return false;
        }
        // Simultaneously check for illegal trailing-byte in leading position
        // and overlong 2-byte form.
        if (byte1 < (byte) 0xC2 || in.getByte(index++) > (byte) 0xBF) {
          return false;
        }
      } else if (byte1 < (byte) 0xF0) {
        // Three-byte form.
        if (index + 1 >= end) {
          return false;
        }
        int byte2 = in.getByte(index++);
        if (byte2 > (byte) 0xBF
          // Overlong? 5 most significant bits must not all be zero.
          || (byte1 == (byte) 0xE0 && byte2 < (byte) 0xA0)
          // Check for illegal surrogate codepoints.
          || (byte1 == (byte) 0xED && (byte) 0xA0 <= byte2)
          // Third byte trailing-byte test.
          || in.getByte(index++) > (byte) 0xBF) {
          return false;
        }
      } else {
        // Four-byte form.
        if (index + 2 >= end) {
          return false;
        }
        int byte2 = in.getByte(index++);
        if (byte2 > (byte) 0xBF
          // Check that 1 <= plane <= 16. Tricky optimized form of:
          // if (byte1 > (byte) 0xF4
          //     || byte1 == (byte) 0xF0 && byte2 < (byte) 0x90
          //     || byte1 == (byte) 0xF4 && byte2 > (byte) 0x8F)
          || (((byte1 << 28) + (byte2 - (byte) 0x90)) >> 30) != 0
          // Third byte trailing-byte test
          || in.getByte(index++) > (byte) 0xBF
          // Fourth byte trailing-byte test
          || in.getByte(index++) > (byte) 0xBF) {
          return false;
        }
      }
    }
  }

}

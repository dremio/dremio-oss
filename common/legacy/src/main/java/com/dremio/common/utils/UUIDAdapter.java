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

import java.util.UUID;

/** UUID adapter to help with conversion */
public final class UUIDAdapter {

  private UUIDAdapter() {}

  /**
   * Converts UUID to byte array
   *
   * @param uuid Randomly generated UUID
   * @return uuid as byte array
   */
  public static byte[] getBytesFromUUID(UUID uuid) {
    byte[] result = new byte[16];
    long msb = uuid.getMostSignificantBits();
    long lsb = uuid.getLeastSignificantBits();
    for (int i = 15; i >= 8; i--) {
      result[i] = (byte) (lsb & 0xFF);
      lsb >>= 8;
    }
    for (int i = 7; i >= 0; i--) {
      result[i] = (byte) (msb & 0xFF);
      msb >>= 8;
    }
    return result;
  }

  /**
   * Converts byte array to UUID
   *
   * @param bytes uuid as byte array
   * @return UUID
   */
  public static UUID getUUIDFromBytes(byte[] bytes) {
    long msb = 0;
    long lsb = 0;
    assert bytes.length == 16 : "bytes must be 16 bytes in length";
    for (int i = 0; i < 8; i++) {
      msb = (msb << 8) | (bytes[i] & 0xff);
    }
    for (int i = 8; i < 16; i++) {
      lsb = (lsb << 8) | (bytes[i] & 0xff);
    }
    return new UUID(msb, lsb);
  }
}

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
/**
 *  Portions - Copyright 2015 Higher Frequency Trading http://www.higherfrequencytrading.com, licensed under the Apache 2.0 license.
 */
package com.dremio.sabot.op.common.ht2;

import io.netty.util.internal.PlatformDependent;

public class XXH64 {

  // Primes if treated as unsigned
  private static final long P1 = -7046029288634856825L;
  private static final long P2 = -4417276706812531889L;
  private static final long P3 = 1609587929392839161L;
  private static final long P4 = -8796714831421723037L;
  private static final long P5 = 2870177450012600261L;

  long toLittleEndian(long v) {
      return v;
  }

  int toLittleEndian(int v) {
      return v;
  }

  short toLittleEndian(short v) {
      return v;
  }

  private static int b(long addr){
    return PlatformDependent.getByte(addr) & 0xFF;
  }
  private static long l(long addr){
    return PlatformDependent.getLong(addr);
  }

  private static long i(long addr){
    return (long) (PlatformDependent.getInt(addr) & 0xFFFFFFFFL);
  }

  public static int xxHash6432(long addr, long length, long seed) {
    return (int) xxHash64(addr, length, seed);
  }

  public static long xxHash64(long addr, long length, long seed) {
      long hash;
      long remaining = length;

      if (remaining >= 32) {
          long v1 = seed + P1 + P2;
          long v2 = seed + P2;
          long v3 = seed;
          long v4 = seed - P1;

          do {
              v1 += l(addr) * P2;
              v1 = Long.rotateLeft(v1, 31);
              v1 *= P1;

              v2 += l(addr + 8) * P2;
              v2 = Long.rotateLeft(v2, 31);
              v2 *= P1;

              v3 += l(addr + 16) * P2;
              v3 = Long.rotateLeft(v3, 31);
              v3 *= P1;

              v4 += l(addr + 24) * P2;
              v4 = Long.rotateLeft(v4, 31);
              v4 *= P1;

              addr += 32;
              remaining -= 32;
          } while (remaining >= 32);

          hash = Long.rotateLeft(v1, 1)
              + Long.rotateLeft(v2, 7)
              + Long.rotateLeft(v3, 12)
              + Long.rotateLeft(v4, 18);

          v1 *= P2;
          v1 = Long.rotateLeft(v1, 31);
          v1 *= P1;
          hash ^= v1;
          hash = hash * P1 + P4;

          v2 *= P2;
          v2 = Long.rotateLeft(v2, 31);
          v2 *= P1;
          hash ^= v2;
          hash = hash * P1 + P4;

          v3 *= P2;
          v3 = Long.rotateLeft(v3, 31);
          v3 *= P1;
          hash ^= v3;
          hash = hash * P1 + P4;

          v4 *= P2;
          v4 = Long.rotateLeft(v4, 31);
          v4 *= P1;
          hash ^= v4;
          hash = hash * P1 + P4;
      } else {
          hash = seed + P5;
      }

      hash += length;

      while (remaining >= 8) {
          long k1 = l(addr);
          k1 *= P2;
          k1 = Long.rotateLeft(k1, 31);
          k1 *= P1;
          hash ^= k1;
          hash = Long.rotateLeft(hash, 27) * P1 + P4;
          addr += 8;
          remaining -= 8;
      }

      if (remaining >= 4) {
          hash ^= i(addr) * P1;
          hash = Long.rotateLeft(hash, 23) * P2 + P3;
          addr += 4;
          remaining -= 4;
      }

      while (remaining != 0) {
          hash ^= b(addr) * P5;
          hash = Long.rotateLeft(hash, 11) * P1;
          --remaining;
          ++addr;
      }

      return finalize(hash);
  }

  private static long finalize(long hash) {
      hash ^= hash >>> 33;
      hash *= P2;
      hash ^= hash >>> 29;
      hash *= P3;
      hash ^= hash >>> 32;
      return hash;
  }

}

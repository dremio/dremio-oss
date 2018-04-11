/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.op.common.ht2;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

public class XXHashByteBuf {

  public static int hash(ArrowBuf buf, int bufferOffset, int len, int seed) {
    buf.checkBytes(bufferOffset, bufferOffset + len);
    return hashAddr(buf.memoryAddress(), bufferOffset, len, seed);
  }

  public static int hashAddr(long memoryAddress, int bufferOffset, int len, int seed){
    long off = memoryAddress + bufferOffset;
    return hashAddr(off, len, seed);
  }

  public static int hashAddr(long off, int len, int seed){

    long end = off + len;
    int h32;

    if (len >= 16) {
      long limit = end - 16;
      int v1 = seed + -1640531535 + -2048144777;
      int v2 = seed + -2048144777;
      int v3 = seed + 0;
      int v4 = seed - -1640531535;
      do {
        v1 += PlatformDependent.getInt(off) * -2048144777;
        v1 = Integer.rotateLeft(v1, 13);
        v1 *= -1640531535;
        off += 4;

        v2 += PlatformDependent.getInt(off) * -2048144777;
        v2 = Integer.rotateLeft(v2, 13);
        v2 *= -1640531535;
        off += 4;

        v3 += PlatformDependent.getInt(off) * -2048144777;
        v3 = Integer.rotateLeft(v3, 13);
        v3 *= -1640531535;
        off += 4;

        v4 += PlatformDependent.getInt(off) * -2048144777;
        v4 = Integer.rotateLeft(v4, 13);
        v4 *= -1640531535;
        off += 4;
      } while (off <= limit);

      h32 = Integer.rotateLeft(v1, 1) + Integer.rotateLeft(v2, 7) + Integer.rotateLeft(v3, 12)
          + Integer.rotateLeft(v4, 18);
    } else {
      h32 = seed + 374761393;
    }

    h32 += len;

    while (off <= end - 4) {
      h32 += PlatformDependent.getInt(off) * -1028477379;
      h32 = Integer.rotateLeft(h32, 17) * 668265263;
      off += 4;
    }

    while (off < end) {
      h32 += (PlatformDependent.getInt(off) & 0xFF) * 374761393;
      h32 = Integer.rotateLeft(h32, 11) * -1640531535;
      ++off;
    }

    h32 ^= h32 >>> 15;
    h32 *= -2048144777;
    h32 ^= h32 >>> 13;
    h32 *= -1028477379;
    h32 ^= h32 >>> 16;

    return h32;
  }
}

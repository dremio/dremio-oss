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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.arrow.memory.ArrowBuf;

public class HashHelper {

  /** taken from mahout **/
  public static int hash(ByteBuffer buf, int seed) {
    // save byte order for later restoration

    int m = 0x5bd1e995;
    int r = 24;

    int h = seed ^ buf.remaining();

    while (buf.remaining() >= 4) {
      int k = buf.getInt();

      k *= m;
      k ^= k >>> r;
      k *= m;

      h *= m;
      h ^= k;
    }

    if (buf.remaining() > 0) {
      ByteBuffer finish = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      // for big-endian version, use this first:
      // finish.position(4-buf.remaining());
      finish.put(buf).rewind();
      h ^= finish.getInt();
      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }

  public static int hash32(int val, long seed) {
    double converted = val;
    return hash32(converted, seed);
  }
  public static int hash32(long val, long seed) {
    double converted = val;
    return hash32(converted, seed);
  }
  public static int hash32(float val, long seed){
    double converted = val;
    return hash32(converted, seed);
  }


  public static long hash64(float val, long seed){
    double converted = val;
    return hash64(converted, seed);
  }
  public static long hash64(long val, long seed){
    double converted = val;
    return hash64(converted, seed);
  }

  public static long hash64(double val, long seed){
    return MurmurHash3.hash64(val, (int)seed);
  }

  public static long hash64(long start, long end, ArrowBuf buffer, long seed){
    return MurmurHash3.hash64(start, end, buffer, (int)seed);
  }

  public static int hash32(double val, long seed) {
    //return com.google.common.hash.Hashing.murmur3_128().hashLong(Double.doubleToLongBits(val)).asInt();
    return MurmurHash3.hash32(val, (int)seed);
  }

  public static int hash32(int start, int end, ArrowBuf buffer, int seed){
    return MurmurHash3.hash32(start, end, buffer, seed);
  }

}


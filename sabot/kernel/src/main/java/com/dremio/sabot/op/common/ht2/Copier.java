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

import java.lang.reflect.Field;

import io.netty.util.internal.PlatformDependent;
import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class Copier {

  private static final Unsafe UNSAFE;

  static {
    Unsafe localUnsafe = null;
    try{
      localUnsafe = getUnsafe();
    }catch(Exception ex){
      System.err.println("Failure getting unsafe.");
      ex.printStackTrace(System.err);
    }

    UNSAFE = localUnsafe;
  }

  private static Unsafe getUnsafe() throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
    Field instanceField = Unsafe.class.getDeclaredField("theUnsafe");
    instanceField.setAccessible(true);
    return (Unsafe) instanceField.get(null);
  }

  public static void copy(long src, long dst, int len){
    handCopy2(src, dst, len);
  }

  public static void unsafeCopy(final long src, final long dst, int len) {
    UNSAFE.copyMemory(src, dst, len);
  }

  public static void unsafeCopy1MB(final long src, final long dst, int len) {
    PlatformDependent.copyMemory(src, dst, len);
  }
  public static final void handCopy(final long src, final long dst, int len) {
    int n = len;
    long lPos = src;
    long rPos = dst;

    while (n > 7) {
      PlatformDependent.putLong(rPos, PlatformDependent.getLong(lPos));
      lPos += 8;
      rPos += 8;
      n -= 8;
    }
    while (n > 3) {
      PlatformDependent.putInt(rPos, PlatformDependent.getInt(lPos));
      lPos += 4;
      rPos += 4;
      n -= 4;
    }
    while (n-- != 0) {
      PlatformDependent.putByte(rPos, PlatformDependent.getByte(lPos));
      lPos++;
      rPos++;
    }
  }


  public static final void handCopy2(final long src, final long dst, int len) {
    int n = len;
    long lPos = src;
    long rPos = dst;

    while (n > 7) {
      PlatformDependent.putLong(rPos, PlatformDependent.getLong(lPos));
      lPos += 8;
      rPos += 8;
      n -= 8;
    }
    switch(n){
    case 7:
      PlatformDependent.putInt(rPos, PlatformDependent.getInt(lPos));
      PlatformDependent.putByte(rPos + 4, PlatformDependent.getByte(lPos + 4));
      PlatformDependent.putByte(rPos + 5, PlatformDependent.getByte(lPos + 5));
      PlatformDependent.putByte(rPos + 6, PlatformDependent.getByte(lPos + 6));
      break;
    case 6:
      PlatformDependent.putInt(rPos, PlatformDependent.getInt(lPos));
      PlatformDependent.putByte(rPos + 4, PlatformDependent.getByte(lPos + 4));
      PlatformDependent.putByte(rPos + 5, PlatformDependent.getByte(lPos + 5));
      break;
    case 5:
      PlatformDependent.putInt(rPos, PlatformDependent.getInt(lPos));
      PlatformDependent.putByte(rPos + 4, PlatformDependent.getByte(lPos + 4));
      break;
    case 4:
      PlatformDependent.putInt(rPos, PlatformDependent.getInt(lPos));
      break;
    case 3:
      PlatformDependent.putByte(rPos, PlatformDependent.getByte(lPos));
      PlatformDependent.putByte(rPos + 1, PlatformDependent.getByte(lPos + 1));
      PlatformDependent.putByte(rPos + 2, PlatformDependent.getByte(lPos + 2));
      break;
    case 2:
      PlatformDependent.putByte(rPos, PlatformDependent.getByte(lPos));
      PlatformDependent.putByte(rPos + 1, PlatformDependent.getByte(lPos + 1));
      break;
    case 1:
      PlatformDependent.putByte(rPos, PlatformDependent.getByte(lPos));
      break;

    }
  }

}

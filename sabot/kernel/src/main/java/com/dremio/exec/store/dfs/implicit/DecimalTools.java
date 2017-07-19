/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.dfs.implicit;

import java.math.BigDecimal;

import com.google.common.base.Preconditions;

public class DecimalTools {

  private static final byte SIGN_MASK = -128;
  private static final byte NEG = -1;
  private static final byte POS = 0;

  private DecimalTools(){}

  public static void main(String[] args) {
    print("0");
    print("1");
    print("-1");
    print("-0.1");
    print("0.1");
  }

  private static void print(String str){
    System.out.println(toBinary(signExtend16(new BigDecimal(str).unscaledValue().toByteArray())));
  }

  public static byte[] signExtend16(byte[] bytes){
    Preconditions.checkArgument(bytes.length > 0, "Byte width needs to be 16 bytes or less.");
    Preconditions.checkArgument(bytes.length <= 16, "Byte width needs to be at least 1 byte.");
    if(bytes.length < 16){
      byte[] newBytes = new byte[16];
      System.arraycopy(bytes, 0, newBytes, 16 - bytes.length, bytes.length);
      System.out.println(bytes[0] & SIGN_MASK);
      byte replace = (bytes[0] & SIGN_MASK) == SIGN_MASK ? NEG : POS;
      for(int i = 0; i < newBytes.length - bytes.length; i++){
        newBytes[i] = replace;
      }

      return newBytes;

    }
    return bytes;
  }

  public static String toBinary(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE);
    for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
      sb.append((bytes[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
    }
    return sb.toString();
  }

  public byte[] fromBinary(String s) {
    int sLen = s.length();
    byte[] toReturn = new byte[(sLen + Byte.SIZE - 1) / Byte.SIZE];
    char c;
    for (int i = 0; i < sLen; i++) {
      if ((c = s.charAt(i)) == '1') {
        toReturn[i / Byte.SIZE] = (byte) (toReturn[i / Byte.SIZE] | (0x80 >>> (i % Byte.SIZE)));
      } else if (c != '0') {
        throw new IllegalArgumentException();
      }
    }
    return toReturn;
  }
}

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
package com.dremio.exec.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class containing functions that operate on bits
 */
public final class BitSetHelper {

  private static final List<List<Integer>> setBitPositions;

  public static int SIZE = 256;

  static {
    setBitPositions = new ArrayList<>(256);
    List<Integer> setBitsFor0 = new ArrayList<>();
    setBitPositions.add(setBitsFor0);
    List<Integer> setBitsFor1 = new ArrayList<>();
    setBitsFor1.add(0);
    setBitPositions.add(setBitsFor1);

    for (int powerOf2 = 2, highestSetBit = 1; powerOf2 <= SIZE / 2; powerOf2 *= 2, highestSetBit++) {
      for (int j = 0; j < powerOf2; j++) {
        List<Integer> setBitsArray = new ArrayList<>(setBitPositions.get(j));
        setBitsArray.add(highestSetBit);
        setBitPositions.add((powerOf2 + j), setBitsArray);
      }
    }
  }

  /**
   * @param byteVal byteVal
   * @return a list containing all the set bit positions in byteVal
   */
  public static List<Integer> getSetBitPositions(byte byteVal) {
    return setBitPositions.get(Byte.toUnsignedInt(byteVal));
  }

  /**
   * @param byteVal byteVal
   * @return a list containing all the unset bit positions in byteVal
   */
  public static List<Integer> getUnsetBitPositions(byte byteVal) {
    return setBitPositions.get(255 - Byte.toUnsignedInt(byteVal));
  }
}

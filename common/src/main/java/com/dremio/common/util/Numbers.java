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
package com.dremio.common.util;

/**
 * Utility class for number related things.
 */
public class Numbers {

  private Numbers(){}

  /**
   * Get the next power of two at or after the provided value.
   * @param val The minimum value to use.
   * @return The first power of two greater than or equal to the input.
   */
  public static int nextPowerOfTwo(int val) {
    int highestBit = Integer.highestOneBit(val);
    if (highestBit == val) {
      return val;
    } else {
      return highestBit << 1;
    }
  }

  /*
   * Get the next multiple of 8 at or after the provided value.
   * @param val The minimum value to use.
   * @return The first multiple of 8 greater than or equal to the input.
   */
  public static int nextMultipleOfEight(int val) {
    return ((val + 7) / 8) * 8;
  }
}

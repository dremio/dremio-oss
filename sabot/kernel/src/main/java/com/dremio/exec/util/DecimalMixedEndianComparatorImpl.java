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
package com.dremio.exec.util;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Mixed endian decimal bytes comparator
 */
public abstract class DecimalMixedEndianComparatorImpl implements DecimalMixedEndianComparator {

  private boolean isLeftValNegative;
  private boolean isRightValNegative;
  private long rightValLow;
  private long rightValHigh;
  protected byte padZero = 0, padNegative = (byte) 255;

  protected long getRightValLow() {
    return rightValLow;
  }

  protected long getRightValHigh() {
    return rightValHigh;
  }

  protected boolean isLeftValNegative() {
    return isLeftValNegative;
  }

  protected void setCommonValues(ArrowBuf left, int startIndexLeft, ArrowBuf right, int startIndexRight) {
    isLeftValNegative = left.getByte(startIndexLeft) < 0;
    isRightValNegative = right.getByte(startIndexRight + DecimalUtils.OFFSET_LE_MSB) < 0;
    //value in right is assumed to be 16 bytes (used for compare values in parquet filters)
    long startingAddressRight = right.memoryAddress() + (startIndexRight);
    rightValLow = PlatformDependent.getLong(startingAddressRight);
    rightValHigh = PlatformDependent.getLong(startingAddressRight + 8);
  }

  /**
   * Compare two signed decimals where left is in big endian and right in little endian
   * The left decimal is of arbitrary length.
   *
   * @param left            buffer containing LHS values for comparison
   * @param startIndexLeft  starting index in buffer to use for comparison
   * @param valueLength     length of the value to be compared
   * @param right           buffer containing RHS for comparison.
   * @param startIndexRight starting index in the buffer.
   * @return
   */
  public int compare(ArrowBuf left, int startIndexLeft,
                     int valueLength, ArrowBuf right, int startIndexRight) {

    setCommonValues(left, startIndexLeft, right, startIndexRight);
    // fast path, we are comparing one +ve and one -ve, return
    // appropriately.
    if (isLeftValNegative != isRightValNegative) {
      return isLeftValNegative ? -1 : 1;
    }

    return compareInner(left, startIndexLeft, valueLength, right, startIndexRight);
  }

  protected abstract int compareInner(ArrowBuf left, int startIndexLeft, int valueLength,
                             ArrowBuf right, int startIndexRight);

}



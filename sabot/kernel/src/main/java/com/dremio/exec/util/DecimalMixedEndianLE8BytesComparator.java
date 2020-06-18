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

import java.util.Arrays;

import org.apache.arrow.memory.ArrowBuf;

import com.google.common.primitives.Longs;

public class DecimalMixedEndianLE8BytesComparator extends DecimalMixedEndianComparatorImpl {

  private byte[] bytesLow = new byte[8];

  protected int compareInner(ArrowBuf left, int startIndexLeft, int valueLength,
                             ArrowBuf right, int startIndexRight) {
    long leftValLow;
    byte padValue = isLeftValNegative() ? padNegative : padZero;
    // sign extend
    Arrays.fill(bytesLow, 0, 8 - valueLength, padValue);
    left.getBytes(startIndexLeft, bytesLow, 8 - valueLength, valueLength);
    leftValLow = Longs.fromByteArray(bytesLow);
    long leftValHigh = leftValLow < 0 ? -1 : 0;
    return DecimalUtils.compareDecimalsAsTwoLongs(leftValHigh, leftValLow, getRightValHigh(),
      getRightValLow());
  }
}

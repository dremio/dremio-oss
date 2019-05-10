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

import java.util.Arrays;

import com.google.common.primitives.Longs;

import io.netty.buffer.ArrowBuf;

public class DecimalMixedEndianGT8BytesComparator extends DecimalMixedEndianComparatorImpl {

  private byte[] bytesLow = new byte[8], bytesHigh = new byte[8];

  protected int compareInner(ArrowBuf left, int startIndexLeft, int valueLength,
                             ArrowBuf right, int startIndexRight) {
    int highBytesLength = valueLength - 8;
    // sign extend
    byte padValue = isLeftValNegative() ? padNegative : padZero;
    Arrays.fill(bytesHigh, 0, 8 - highBytesLength, padValue);
    left.getBytes(startIndexLeft, bytesHigh, 8 - highBytesLength, highBytesLength);
    long leftValHigh = Longs.fromByteArray(bytesHigh);

    left.getBytes(startIndexLeft + highBytesLength, bytesLow, 0, 8);
    long leftValLow = Longs.fromByteArray(bytesLow);

    return DecimalUtils.compareDecimalsAsTwoLongs(leftValHigh, leftValLow, getRightValHigh(),
      getRightValLow());
  }
}

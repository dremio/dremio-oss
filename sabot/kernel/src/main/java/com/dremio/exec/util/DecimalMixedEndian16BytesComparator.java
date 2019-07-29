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

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

public class DecimalMixedEndian16BytesComparator extends DecimalMixedEndianComparatorImpl {

  protected int compareInner(ArrowBuf left, int startIndexLeft, int valueLength,
                             ArrowBuf right, int startIndexRight) {

    long startingAddress = left.memoryAddress() + (startIndexLeft);
    long leftValHigh = PlatformDependent.getLong(startingAddress);
    long leftValHighLE = Long.reverseBytes(leftValHigh);
    long leftValLow = PlatformDependent.getLong(startingAddress + 8);
    long leftValLowLE = Long.reverseBytes(leftValLow);

    return DecimalUtils.compareDecimalsAsTwoLongs(leftValHighLE, leftValLowLE, getRightValHigh(),
      getRightValLow());
  }
}

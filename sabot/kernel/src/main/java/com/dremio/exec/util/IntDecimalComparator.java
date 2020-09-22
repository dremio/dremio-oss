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

import org.apache.arrow.memory.ArrowBuf;

public class IntDecimalComparator extends DecimalMixedEndianComparatorImpl {

  public int compare(ArrowBuf left, int startIndexLeft,
                     int valueLength, ArrowBuf right, int startIndexRight) {
    setCommonValues(left, startIndexLeft, right, startIndexRight);
    return compareInner(left, startIndexLeft, valueLength, right, startIndexRight);
  }

  protected int compareInner(ArrowBuf left, int startIndexLeft, int valueLength,
                             ArrowBuf right, int startIndexRight) {
    long leftValLow = left.getInt(startIndexLeft);
    long leftValHigh = leftValLow < 0 ? -1 : 0;
    return DecimalUtils.compareDecimalsAsTwoLongs(leftValHigh, leftValLow, getRightValHigh(),
      getRightValLow());
  }
}

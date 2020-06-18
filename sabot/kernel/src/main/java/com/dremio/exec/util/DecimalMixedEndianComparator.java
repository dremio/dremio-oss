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

import io.netty.util.internal.PlatformDependent;

public interface DecimalMixedEndianComparator {

  int compare(ArrowBuf left, int startIndexLeft,
              int valueLength, ArrowBuf right, int startIndexRight);

  default long getLongValueHigh(ArrowBuf buf, int index) {
    long startingAddress = buf.memoryAddress() + index;
    return PlatformDependent.getLong(startingAddress + 8);
  }

  default long getLongValueLow(ArrowBuf buf, int index) {
    long startingAddress = buf.memoryAddress() + index;
    return PlatformDependent.getLong(startingAddress);
  }
}

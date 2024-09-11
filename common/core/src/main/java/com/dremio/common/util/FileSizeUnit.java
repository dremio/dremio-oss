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
package com.dremio.common.util;

public enum FileSizeUnit {
  BYTE(1L),
  KB(BYTE.getSizeInBytes() << 10),
  MB(KB.getSizeInBytes() << 10),
  GB(MB.getSizeInBytes() << 10),
  TB(GB.getSizeInBytes() << 10),
  PB(TB.getSizeInBytes() << 10);

  private final long sizeInBytes;

  FileSizeUnit(long valueInBytes) {
    this.sizeInBytes = valueInBytes;
  }

  public long getSizeInBytes() {
    return this.sizeInBytes;
  }

  public static long toUnit(long sizeInBytes, FileSizeUnit unit) {
    return toUnit(sizeInBytes, BYTE, unit);
  }

  /** Returns the given size in the desired unit (to the nearest long value) */
  public static long toUnit(long size, FileSizeUnit fromUnit, FileSizeUnit toUnit) {
    // Convert the size to bytes first
    long sizeInBytes = size * fromUnit.getSizeInBytes();
    return Math.round((double) sizeInBytes / toUnit.getSizeInBytes());
  }
}

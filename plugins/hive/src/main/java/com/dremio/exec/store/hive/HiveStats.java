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
package com.dremio.exec.store.hive;

/** Contains stats. Currently only numRows and totalSizeInBytes are used. */
public class HiveStats {
  private long numRows;
  private long sizeInBytes;

  public HiveStats(final long numRows, final long sizeInBytes) {
    this.numRows = numRows;
    this.sizeInBytes = sizeInBytes;
  }

  public long getNumRows() {
    return numRows;
  }

  public long getSizeInBytes() {
    return sizeInBytes;
  }

  /**
   * Both numRows and sizeInBytes are expected to be greater than 0 for stats to
   * be valid
   */
  public boolean isValid() {
    return numRows > 0 && sizeInBytes > 0;
  }

  public void add(HiveStats s) {
    numRows += s.numRows;
    sizeInBytes += s.sizeInBytes;
  }

  @Override
  public String toString() {
    return "numRows: " + numRows + ", sizeInBytes: " + sizeInBytes;
  }

}

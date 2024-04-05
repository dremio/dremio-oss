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
package com.dremio.connector.metadata;

/** Default implementation. */
final class DatasetStatsImpl implements DatasetStats {

  private final long recordCount;
  private final boolean isExactRecordCount;
  private final double scanFactor;

  /**
   * @param recordCount record count. A negative record count indicates getRecordCount() is
   *     unsupported.
   * @param isExactRecordCount if record count is exact.
   * @param scanFactor scan factor.
   */
  DatasetStatsImpl(long recordCount, boolean isExactRecordCount, double scanFactor) {
    this.recordCount = recordCount;
    this.isExactRecordCount = isExactRecordCount;
    this.scanFactor = scanFactor;
  }

  @Override
  public long getRecordCount() {
    return recordCount;
  }

  @Override
  public boolean isExactRecordCount() {
    return isExactRecordCount;
  }

  @Override
  public double getScanFactor() {
    return scanFactor;
  }
}

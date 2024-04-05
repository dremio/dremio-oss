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

/** Stats about dataset. */
public interface DatasetStats {

  /**
   * Get the number of records in the dataset.
   *
   * @return number of records in the dataset
   */
  long getRecordCount();

  /**
   * Whether the record count provided is exact.
   *
   * @return true iff the record count provided is exact
   */
  default boolean isExactRecordCount() {
    return false;
  }

  /**
   * Get the scan factor for the dataset.
   *
   * <p>Scan factor represents the relative cost of making the scan as compared to other scans.
   * TODO: what does this mean?
   *
   * @return scan factor
   */
  double getScanFactor();

  /**
   * Create {@code DatasetStats} that does not provide a row count.
   *
   * @param scanFactor scan factor
   * @return dataset stats
   * @throws IllegalArgumentException if the scan factor is less than or equal to zero.
   */
  static DatasetStats of(double scanFactor) {
    if (scanFactor <= 0.0d) {
      throw new IllegalArgumentException("Scan factor must be greater than zero.");
    }
    return of(-1L, false, scanFactor);
  }

  /**
   * Create {@code DatasetStats}.
   *
   * @param recordCount record count. A negative value indicates that the record count on the
   *     DatasetStats is not valid and should be calculated by other means.
   * @param scanFactor scan factor. Must be greater than zero.
   * @return dataset stats
   * @throws IllegalArgumentException if the record count is negative or the scan factor is less
   *     than or equal to zero.
   */
  static DatasetStats of(long recordCount, double scanFactor) {
    return of(recordCount, false, scanFactor);
  }

  /**
   * Create {@code DatasetStats}.
   *
   * @param recordCount record count. A negative value indicates that the record count on the
   *     DatasetStats is not valid and should be calculated by other means.
   * @param exact if record count is exact. Ignored if recordCount < 0.
   * @param scanFactor scan factor. Must be greater than zero.
   * @return datasets stats
   * @throws IllegalArgumentException if the record count is negative or the scan factor is less
   *     than or equal to zero.
   */
  static DatasetStats of(long recordCount, boolean exact, double scanFactor) {
    if (scanFactor <= 0.0d) {
      throw new IllegalArgumentException("Scan factor must be greater than zero.");
    }

    if (recordCount >= 0) {
      return new DatasetStatsImpl(recordCount, exact, scanFactor);
    }
    return new DatasetStatsImpl(recordCount, false, scanFactor);
  }
}

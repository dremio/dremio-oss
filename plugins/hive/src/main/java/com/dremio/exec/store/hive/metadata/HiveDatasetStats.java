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
package com.dremio.exec.store.hive.metadata;

import com.dremio.connector.metadata.DatasetStats;
import com.dremio.exec.planner.cost.ScanCostFactor;

public class HiveDatasetStats implements DatasetStats {
  private long recordCount;
  private long scanFactor;
  private boolean allowParquetNative;
  private boolean isExactRecordCount;

  public HiveDatasetStats() {
    this(0, 0);
  }

  public HiveDatasetStats(long recordCount, long scanFactor) {
    this.recordCount = recordCount;
    this.scanFactor = scanFactor;
    isExactRecordCount = false;
  }

  public HiveDatasetStats(long recordCount, long scanFactor, boolean isExactRecordCount) {
    this.recordCount = recordCount;
    this.scanFactor = scanFactor;
    this.isExactRecordCount = isExactRecordCount;
  }

  public void setAllowParquetNative(boolean splitFileTypeChanged) {
    this.allowParquetNative = splitFileTypeChanged;
  }

  public void addRecords(long recordCount) {
    this.recordCount += recordCount;
  }

  @Override
  public long getRecordCount() {
    return recordCount;
  }

  @Override
  public double getScanFactor() {
    if (allowParquetNative) {
      return ScanCostFactor.PARQUET.getFactor();
    }
    return ScanCostFactor.HIVE.getFactor();
  }

  @Override
  public boolean isExactRecordCount() {
    return isExactRecordCount;
  }

  public void setIsExactRecordCount(boolean isExactRecordCount) {
    this.isExactRecordCount = isExactRecordCount;
  }

  public boolean hasContent() {
    return recordCount > 0 && scanFactor > 0;
  }
}

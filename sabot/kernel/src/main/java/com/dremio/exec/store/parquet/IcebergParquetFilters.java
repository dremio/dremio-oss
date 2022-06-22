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
package com.dremio.exec.store.parquet;

import java.util.List;

import com.dremio.exec.store.iceberg.deletes.PositionalDeleteFilter;

/**
 * ParquetFilters implementation for Iceberg tables which only exposes filtering capabilities supported by Iceberg.
 */
public class IcebergParquetFilters extends ParquetFilters {
  public IcebergParquetFilters() {
    super();
  }

  public IcebergParquetFilters(List<ParquetFilterCondition> pushdownFilters) {
    super(pushdownFilters);
  }

  public IcebergParquetFilters(PositionalDeleteFilter positionalDeleteFilter) {
    super(positionalDeleteFilter);
  }

  public IcebergParquetFilters(List<ParquetFilterCondition> pushdownFilters,
      PositionalDeleteFilter positionalDeleteFilter) {
    super(pushdownFilters, positionalDeleteFilter);
  }

  public IcebergParquetFilters(ParquetFilters filters) {
    super(filters.getPushdownFilters(), filters.getPositionalDeleteFilter());
  }
}

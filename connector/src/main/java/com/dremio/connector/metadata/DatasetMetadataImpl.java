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

import java.util.List;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Default implementation.
 */
final class DatasetMetadataImpl implements DatasetMetadata {

  private final DatasetStats stats;
  private final Schema schema;
  private final List<String> partitionColumns;
  private final List<String> sortColumns;
  private final BytesOutput extraInfo;
  private final byte[] icebergMetadata;

  DatasetMetadataImpl(
      DatasetStats stats,
      Schema schema,
      List<String> partitionColumns,
      List<String> sortColumns,
      BytesOutput extraInfo,
      byte[] icebergMetadata
  ) {
    this.stats = stats;
    this.schema = schema;
    this.partitionColumns = partitionColumns;
    this.sortColumns = sortColumns;
    this.extraInfo = extraInfo;
    this.icebergMetadata = icebergMetadata;
  }

  @Override
  public DatasetStats getDatasetStats() {
    return stats;
  }

  @Override
  public Schema getRecordSchema() {
    return schema;
  }

  @Override
  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  @Override
  public List<String> getSortColumns() {
    return sortColumns;
  }

  @Override
  public BytesOutput getExtraInfo() {
    return extraInfo;
  }

  @Override
  public byte[] getIcebergMetadata() {
    return icebergMetadata;
  }
}

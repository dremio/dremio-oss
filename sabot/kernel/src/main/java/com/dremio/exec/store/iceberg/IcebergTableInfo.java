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
package com.dremio.exec.store.iceberg;

import java.util.List;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.record.BatchSchema;

/**
 * Metadata details about an iceberg table.
 */
public interface IcebergTableInfo {

  /**
   * Get the schema for the table.
   * @return schema.
   */
  BatchSchema getBatchSchema();

  /**
   * Get the record count for the table.
   * @return record count
   */
  long getRecordCount();

  /**
   * Get the names of the partition columns for the table.
   * @return list of partition column names.
   */
  List<String> getPartitionColumns();

  /**
   * Get the list of distinct partitions and corresponding splits.
   * @return list of partition chunks.
   */
  PartitionChunkListing getPartitionChunkListing();

  /**
   * Get extended info about the dataset (eg. column value counts).
   * @return stream of bytes.
   */
  BytesOutput getExtraInfo();

  /**
   * Provide the read signature.
   */
  BytesOutput provideSignature();
}

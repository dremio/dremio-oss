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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Interface for a connector to provide detailed metadata about a dataset.
 * <p>
 * For a particular dataset in the source, values returned by the getters may change from request to request.
 * However, implementations must be internally consistent. For example, a column name returned by
 * {@link #getPartitionColumns} should be present in the schema returned by {@link #getRecordSchema}.
 */
public interface DatasetMetadata extends Unwrappable {

  /**
   * Get stats about the dataset.
   *
   * @return dataset stats, not null
   */
  DatasetStats getDatasetStats();

  /**
   * Transactional table queries first scan manifest files, then the data files. Override this method to set stats
   * associated with manifest scan
   *
   * @return dataset stats, not null
   */
  default DatasetStats getManifestStats() {
    return null;
  }

  /**
   * Get the record schema for the dataset.
   *
   * @return record schema, not null
   */
  Schema getRecordSchema();

  /**
   * Get the list of partition column names.
   *
   * @return list of partition column names, not null
   */
  default List<String> getPartitionColumns() {
    return Collections.emptyList();
  }

  /**
   * Get the list of sort column names.
   *
   * @return list of sort column names, not null
   */
  default List<String> getSortColumns() {
    return Collections.emptyList();
  }

  /**
   * Get any additional information about the dataset. This will be provided by the catalog to other modules that
   * request the catalog about the dataset, so any custom state could be returned.
   *
   * @return extra information, not null
   */
  default BytesOutput getExtraInfo() {
    return BytesOutput.NONE;
  }

  /**
   * Different sources support Iceberg datasets. This method will provide required Iceberg metadata
   * that helps in triggering Iceberg execution for all such datasets.
   * @return iceberg metadata. can be empty.
   */
  default byte[] getIcebergMetadata() {
    return new byte[0];
  }

  /**
   * Create {@code DatasetMetadata}.
   *
   * @param stats dataset stats
   * @param schema schema
   * @return dataset metadata
   */
  static DatasetMetadata of(DatasetStats stats, Schema schema) {
    return of(stats, schema, BytesOutput.NONE);
  }

  /**
   * Create {@code DatasetMetadata}.
   *
   * @param stats dataset stats
   * @param schema schema
   * @param extraInfo extra info
   * @return dataset metadata
   */
  static DatasetMetadata of(DatasetStats stats, Schema schema, BytesOutput extraInfo) {
    return of(stats, schema, Collections.emptyList(), Collections.emptyList(), extraInfo, new byte[0]);
  }

  /**
   * Create {@code DatasetMetadata}.
   *
   * @param stats dataset stats
   * @param schema schema
   * @param partitionColumns partition columns
   * @param sortColumns sort columns
   * @param extraInfo extra info
   * @return dataset metadata
   */
  static DatasetMetadata of(
          DatasetStats stats,
          Schema schema,
          List<String> partitionColumns,
          List<String> sortColumns,
          BytesOutput extraInfo
  ) {
    return of(stats, schema, partitionColumns, sortColumns, extraInfo, new byte[0]);
  }

  /**
   * Create {@code DatasetMetadata}.
   *
   * @param stats dataset stats
   * @param schema schema
   * @param partitionColumns partition columns
   * @param sortColumns sort columns
   * @param extraInfo extra info
   * @param icebergMetadaa iceberg metadata
   * @return dataset metadata
   */
  static DatasetMetadata of(
      DatasetStats stats,
      Schema schema,
      List<String> partitionColumns,
      List<String> sortColumns,
      BytesOutput extraInfo,
      byte[] icebergMetadaa
  ) {
    Objects.requireNonNull(stats, "dataset stats is required");
    Objects.requireNonNull(schema, "schema is required");
    Objects.requireNonNull(partitionColumns, "partition columns is required");
    Objects.requireNonNull(sortColumns, "sort columns is required");
    Objects.requireNonNull(extraInfo, "extra info is required");
    Objects.requireNonNull(icebergMetadaa, "iceberg metadata is required");

    return new DatasetMetadataImpl(stats, schema, partitionColumns, sortColumns, extraInfo, icebergMetadaa);
  }
}

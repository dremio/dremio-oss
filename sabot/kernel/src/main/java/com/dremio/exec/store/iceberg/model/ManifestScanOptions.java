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
package com.dremio.exec.store.iceberg.model;

import org.immutables.value.Value;

import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.ManifestContentType;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(builder = ImmutableManifestScanOptions.Builder.class)
@Value.Immutable
public interface ManifestScanOptions {

  /**
   * ManifestContent describes type of files the manifest handles.
   *
   * @return
   */
  @Value.Default
  default ManifestContentType getManifestContentType() {
    return ManifestContentType.DATA;
  }
  /**
   * SplitGen generates splits for data scan. If turned OFF, the scan will generate abstract output such as path, size etc
   * for the entries in the manifest.
   *
   * @return
   */
  @Value.Default
  default boolean includesSplitGen() {
    return false;
  }

  /**
   * Includes the serialized DataFile object as IcebergMetadata in the output schema.
   * Applicable only when splitgen is turned OFF.
   *
   * @return
   */
  @Value.Default
  default boolean includesIcebergMetadata() {
    return false;
  }
  /**
   * Includes the Iceberg Partition Info as part of the PartitionProtobuf.PartitionValue Column
   * It provides more fine-grained partition information such as what transformation function is used
   * Applicable only when splitgen is turned OFF.
   *
   * @return true if we should include the IcebergPartitionInfo
   */
  @Value.Default
  default boolean includesIcebergPartitionInfo() {
    return false;
  }

  /**
   * Returns the Table Metadata to use
   * TableMetadata can be used to modify a scan to use a different snapshot
   * @return TableMetadata to use instead of the original table metadata in the scan
   */
  @Value.Default
  default TableMetadata getTableMetadata() {
    return null;
  }

  /**
   * Returns an instance with all defaults
   *
   * @return
   */
  static ManifestScanOptions withDefaults() {
    return new ImmutableManifestScanOptions.Builder().build();
  }
}

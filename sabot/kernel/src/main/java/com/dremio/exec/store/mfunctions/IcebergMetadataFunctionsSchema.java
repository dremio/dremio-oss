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
package com.dremio.exec.store.mfunctions;

import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.SchemaConverter;

/**
 * Captures metadata for iceberg metadata functions tables
 * Iceberg APi returns schema with map fields as well. Dremio currently doesn't support map so same is being casted as List<Struct>
 */
final class IcebergMetadataFunctionsSchema {

  private static final BatchSchema HISTORY;
  private static final BatchSchema SNAPSHOTS;
  private static final BatchSchema MANIFESTS;
  private static final BatchSchema TABLE_FILES;
  private static final SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();

  static {
    HISTORY = schemaConverter.fromIceberg(MetadataTableUtils.createMetadataTableInstance(null,
      null,null, MetadataTableType.HISTORY).schema());
    SNAPSHOTS = schemaConverter.fromIceberg(new Schema(
      Types.NestedField.required(1, "committed_at", Types.TimestampType.withZone()),
      Types.NestedField.required(2, "snapshot_id", Types.LongType.get()),
      Types.NestedField.optional(3, "parent_id", Types.LongType.get()),
      Types.NestedField.optional(4, "operation", Types.StringType.get()),
      Types.NestedField.optional(5, "manifest_list", Types.StringType.get()),
      Types.NestedField.optional(6, "summary", Types.ListType.ofRequired(7, Types.StructType.of(
        Types.NestedField.required(8, "key", Types.StringType.get()),
        Types.NestedField.required(9, "value", Types.StringType.get())
      )))
    ));
    MANIFESTS = schemaConverter.fromIceberg(MetadataTableUtils.createMetadataTableInstance(null,
      null,null, MetadataTableType.MANIFESTS).schema());
    TABLE_FILES = schemaConverter.fromIceberg(new Schema(
      Types.NestedField.optional(134, "content", Types.StringType.get(),
        "Contents of the file: 0=data, 1=position deletes, 2=equality deletes"),
      Types.NestedField.required(100, "file_path", Types.StringType.get(), "Location URI with FS scheme"),
      Types.NestedField.required(101, "file_format", Types.StringType.get(),
        "File format name: avro, orc, or parquet"),
      Types.NestedField.required(102, "partition", Types.StringType.get(),
        "Record for partition {}"),
      Types.NestedField.required(103, "record_count", Types.LongType.get(), "Number of records in the file"),
      Types.NestedField.required(104, "file_size_in_bytes", Types.LongType.get(), "Total file size in bytes"),
      Types.NestedField.optional(108, "column_sizes", Types.ListType.ofRequired(1008, Types.StructType.of(
        Types.NestedField.required(117, "key", Types.StringType.get()),
        Types.NestedField.required(118, "value", Types.StringType.get())
      )), "Map of column id to total size on disk"),
      Types.NestedField.optional(109, "value_counts", Types.ListType.ofRequired(1009, Types.StructType.of(
        Types.NestedField.required(119, "key", Types.StringType.get()),
        Types.NestedField.required(120, "value", Types.StringType.get())
      )), "Map of column id to total count, including null and NaN"),
      Types.NestedField.optional(110, "null_value_counts", Types.ListType.ofRequired(1010, Types.StructType.of(
        Types.NestedField.required(121, "key", Types.StringType.get()),
        Types.NestedField.required(122, "value", Types.StringType.get())
      )), "Map of column id to null value count"),
      Types.NestedField.optional(137, "nan_value_counts", Types.ListType.ofRequired(1037, Types.StructType.of(
        Types.NestedField.required(138, "key", Types.StringType.get()),
        Types.NestedField.required(139, "value", Types.StringType.get())
      )), "Map of column id to number of NaN values in the column"),
      Types.NestedField.optional(125, "lower_bounds", Types.ListType.ofRequired(1025, Types.StructType.of(
        Types.NestedField.required(126, "key", Types.StringType.get()),
        Types.NestedField.required(127, "value", Types.StringType.get())
      )), "Map of column id to lower bound"),
      Types.NestedField.optional(128, "upper_bounds", Types.ListType.ofRequired(1028, Types.StructType.of(
        Types.NestedField.required(129, "key", Types.StringType.get()),
        Types.NestedField.required(130, "value", Types.StringType.get())
      )), "Map of column id to upper bound"),
      Types.NestedField.optional(132, "split_offsets", Types.ListType.ofRequired(133, Types.LongType.get()),
        "Splittable offsets"),
      Types.NestedField.optional(135, "equality_ids", Types.ListType.ofRequired(136, Types.IntegerType.get()),
        "Equality comparison field IDs"),
      Types.NestedField.optional(140, "sort_order_id", Types.IntegerType.get(), "Sort order ID"),
      Types.NestedField.optional(141, "spec_id", Types.IntegerType.get(), "Partition spec ID")));
  }

  public static BatchSchema getHistoryRecordSchema() {
    return HISTORY;
  }

  public static BatchSchema getSnapshotRecordSchema() {
    return SNAPSHOTS;
  }

  public static BatchSchema getManifestFilesRecordSchema() {
    return MANIFESTS;
  }

  public static BatchSchema getTableFilesRecordSchema() {
    return TABLE_FILES;
  }

  private IcebergMetadataFunctionsSchema() {
  }
}

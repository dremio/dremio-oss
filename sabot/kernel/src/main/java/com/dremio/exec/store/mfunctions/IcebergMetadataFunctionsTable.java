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

import com.dremio.exec.record.BatchSchema;

/**
 * All schema related to iceberg metadata functions.
 */
public enum IcebergMetadataFunctionsTable {

  TABLE_HISTORY(IcebergMetadataFunctionsSchema.getHistoryRecordSchema(),MetadataTableType.HISTORY),
  TABLE_SNAPSHOT(IcebergMetadataFunctionsSchema.getSnapshotRecordSchema(),MetadataTableType.SNAPSHOTS),
  TABLE_MANIFESTS(IcebergMetadataFunctionsSchema.getManifestFilesRecordSchema(),MetadataTableType.MANIFESTS),
  TABLE_FILES(IcebergMetadataFunctionsSchema.getTableFilesRecordSchema(),MetadataTableType.FILES),
  TABLE_PARTITIONS(IcebergMetadataFunctionsSchema.getPartitionsRecordSchema(),MetadataTableType.PARTITIONS);

  private final BatchSchema recordSchema;
  private final MetadataTableType tableType;

  IcebergMetadataFunctionsTable(BatchSchema recordSchema, MetadataTableType tableType) {
    this.recordSchema = recordSchema;
    this.tableType = tableType;
  }

  public BatchSchema getRecordSchema() {
    return recordSchema;
  }

  public MetadataTableType getTableType() {
    return tableType;
  }

}

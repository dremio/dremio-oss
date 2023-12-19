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
package com.dremio.exec.store.dfs.copyinto;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import com.google.common.collect.ImmutableList;

/**
 * Provides a schema for the "copy_errors_history" view based on the specified schema version.
 */
public final class CopyErrorsHistoryViewSchemaProvider {

  private CopyErrorsHistoryViewSchemaProvider() {
  }

  /**
   * Get the schema for the "copy_errors_history" view based on the specified schema version.
   *
   * @param schemaVersion The schema version of the "copy_errors_history" view.
   * @return The schema for the "copy_errors_history" view.
   * @throws UnsupportedOperationException If the schema version is not supported.
   */
  public static Schema getSchema(long schemaVersion) {
    Schema jobHistorySchema = CopyJobHistoryTableSchemaProvider.getSchema(schemaVersion);
    Schema fileHistorySchema = CopyFileHistoryTableSchemaProvider.getSchema(schemaVersion);
    if (schemaVersion == 1) {
      return new Schema(
        ImmutableList.of(
          getFieldOf(1, jobHistorySchema.findField(CopyJobHistoryTableSchemaProvider.getExecutedAtColName(schemaVersion))),
          getFieldOf(2, jobHistorySchema.findField(CopyJobHistoryTableSchemaProvider.getJobIdColName(schemaVersion))),
          getFieldOf(3, jobHistorySchema.findField(CopyJobHistoryTableSchemaProvider.getTableNameColName(schemaVersion))),
          getFieldOf(4, jobHistorySchema.findField(CopyJobHistoryTableSchemaProvider.getUserNameColName(schemaVersion))),
          getFieldOf(5, jobHistorySchema.findField(CopyJobHistoryTableSchemaProvider.getBaseSnapshotIdColName(schemaVersion))),
          getFieldOf(6, jobHistorySchema.findField(CopyJobHistoryTableSchemaProvider.getStorageLocationColName(schemaVersion))),
          getFieldOf(7, fileHistorySchema.findField(CopyFileHistoryTableSchemaProvider.getFilePathColName(schemaVersion))),
          getFieldOf(8, fileHistorySchema.findField(CopyFileHistoryTableSchemaProvider.getFileStateColName(schemaVersion))),
          getFieldOf(9, fileHistorySchema.findField(CopyFileHistoryTableSchemaProvider.getRecordsLoadedColName(schemaVersion))),
          getFieldOf(10, fileHistorySchema.findField(CopyFileHistoryTableSchemaProvider.getRecordsRejectedColName(schemaVersion)))
        )
      );
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  private static Types.NestedField getFieldOf(int id, Types.NestedField oldField) {
    return Types.NestedField.required(id, oldField.name(), oldField.type());
  }

  private static UnsupportedOperationException newUnsupportedSchemaVersionException(long schemaVersion) {
    return new UnsupportedOperationException("Unsupported copy_errors_history view schema version: " + schemaVersion +
      ". Currently supported schema versions are: 1");
  }
}

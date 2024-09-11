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

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

/** Provides a schema for the "copy_errors_history" view based on the specified schema version. */
public final class CopyErrorsHistoryViewSchemaProvider {

  private CopyErrorsHistoryViewSchemaProvider() {}

  /**
   * Get the schema for the "copy_errors_history" view based on the specified schema version.
   *
   * @param schemaVersion The schema version of the "copy_errors_history" view.
   * @return The schema for the "copy_errors_history" view.
   * @throws UnsupportedOperationException If the schema version is not supported.
   */
  public static Schema getSchema(long schemaVersion) {
    if (schemaVersion == 1) {
      return V1SchemaDefinition.getSchema();
    }
    if (schemaVersion == 2) {
      return V2SchemaDefinition.getSchema();
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  private static UnsupportedOperationException newUnsupportedSchemaVersionException(
      long schemaVersion) {
    return new UnsupportedOperationException(
        "Unsupported copy_errors_history view schema version: "
            + schemaVersion
            + ". Currently supported schema versions are: 1");
  }

  /** Schema definition for schema version 1 of the copy_errors_history view */
  private static class V1SchemaDefinition {
    private static final long SCHEMA_VERSION = 1L;

    protected static Schema getSchema() {
      Schema jobHistorySchema = CopyJobHistoryTableSchemaProvider.getSchema(SCHEMA_VERSION);
      Schema fileHistorySchema = CopyFileHistoryTableSchemaProvider.getSchema(SCHEMA_VERSION);
      return new Schema(
          ImmutableList.of(
              getFieldOf(
                  1,
                  jobHistorySchema.findField(
                      CopyJobHistoryTableSchemaProvider.getExecutedAtColName())),
              getFieldOf(
                  2,
                  jobHistorySchema.findField(CopyJobHistoryTableSchemaProvider.getJobIdColName())),
              getFieldOf(
                  3,
                  jobHistorySchema.findField(
                      CopyJobHistoryTableSchemaProvider.getTableNameColName())),
              getFieldOf(
                  4,
                  jobHistorySchema.findField(
                      CopyJobHistoryTableSchemaProvider.getUserNameColName())),
              getFieldOf(
                  5,
                  jobHistorySchema.findField(
                      CopyJobHistoryTableSchemaProvider.getBaseSnapshotIdColName())),
              getFieldOf(
                  6,
                  jobHistorySchema.findField(
                      CopyJobHistoryTableSchemaProvider.getStorageLocationColName())),
              getFieldOf(
                  7,
                  fileHistorySchema.findField(
                      CopyFileHistoryTableSchemaProvider.getFilePathColName())),
              getFieldOf(
                  8,
                  fileHistorySchema.findField(
                      CopyFileHistoryTableSchemaProvider.getFileStateColName())),
              getFieldOf(
                  9,
                  fileHistorySchema.findField(
                      CopyFileHistoryTableSchemaProvider.getRecordsLoadedColName())),
              getFieldOf(
                  10,
                  fileHistorySchema.findField(
                      CopyFileHistoryTableSchemaProvider.getRecordsRejectedColName()))));
    }

    private static Types.NestedField getFieldOf(int id, Types.NestedField oldField) {
      return Types.NestedField.required(id, oldField.name(), oldField.type());
    }
  }

  private static class V2SchemaDefinition extends V1SchemaDefinition {}
}

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
 * This class provides the schema definition for the copy_jobs_history table used in copy-into operation.
 * It defines the schema for different versions of the table.
 */
public final class CopyJobHistoryTableSchemaProvider {

  private CopyJobHistoryTableSchemaProvider() {
  }

  private static final int EXECUTED_AT_COL_ID_V1_SCHEMA = 1;
  private static final int JOB_ID_COL_ID_V1_SCHEMA = 2;
  private static final int TABLE_NAME_COL_ID_V1_SCHEMA = 3;
  private static final int COPY_OPTIONS_COL_ID_V1_SCHEMA = 6;
  private static final int USER_NAME_COL_ID_V1_SCHEMA = 7;
  private static final int BASE_SNAPSHOT_ID_COL_ID_V1_SCHEMA = 8;
  private static final int STORAGE_LOCATION_COL_ID_V1_SCHEMA = 9;
  private static final int FILE_FORMAT_COL_ID_V1_SCHEMA = 10;

  // Schema definition for schema version 1 of the copy_jobs_history table
  private static final Schema ICEBERG_V1_TABLE_SCHEMA = new Schema(
    ImmutableList.of(
      Types.NestedField.required(EXECUTED_AT_COL_ID_V1_SCHEMA, "executed_at", Types.TimestampType.withZone()),
      Types.NestedField.required(JOB_ID_COL_ID_V1_SCHEMA, "job_id", new Types.StringType()),
      Types.NestedField.required(TABLE_NAME_COL_ID_V1_SCHEMA, "table_name", new Types.StringType()),
      Types.NestedField.required(4, "records_loaded_count", new Types.LongType()),
      Types.NestedField.required(5, "records_rejected_count", new Types.LongType()),
      Types.NestedField.required(COPY_OPTIONS_COL_ID_V1_SCHEMA, "copy_options", new Types.StringType()),
      Types.NestedField.required(USER_NAME_COL_ID_V1_SCHEMA, "user_name", new Types.StringType()),
      Types.NestedField.required(BASE_SNAPSHOT_ID_COL_ID_V1_SCHEMA, "base_snapshot_id", new Types.LongType()),
      Types.NestedField.required(STORAGE_LOCATION_COL_ID_V1_SCHEMA, "storage_location", new Types.StringType()),
      Types.NestedField.required(FILE_FORMAT_COL_ID_V1_SCHEMA, "file_format", new Types.StringType())
    )
  );

  /**
   * Get the schema definition for the specified schema version of the copy_jobs_history table.
   *
   * @param schemaVersion The version of the schema for which to get the schema definition.
   * @return The Schema object representing the schema for the given schema version.
   * @throws UnsupportedOperationException If the specified schema version is not supported.
   */
  public static Schema getSchema(long schemaVersion) {
    if (schemaVersion == 1) {
        return ICEBERG_V1_TABLE_SCHEMA;
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  private static UnsupportedOperationException newUnsupportedSchemaVersionException(long schemaVersion) {
    return new UnsupportedOperationException("Unsupported copy_job_history table schema version:" +
        schemaVersion + ". Currently supported schema versions are: 1");
  }

  /**
   * Get the name of the column that stores user names in the "copy_job_history" table schema based on the schema version.
   *
   * @param schemaVersion The schema version of the "copy into error" table.
   * @return The name of the column that stores user names.
   * @throws UnsupportedOperationException If the schema version is not supported.
   */
  public static String getUserNameColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(USER_NAME_COL_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getJobIdColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(JOB_ID_COL_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getTableNameColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(TABLE_NAME_COL_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getCopyOptionsColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(COPY_OPTIONS_COL_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getStorageLocationColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(STORAGE_LOCATION_COL_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getFileFormatColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(FILE_FORMAT_COL_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getBaseSnapshotIdColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(BASE_SNAPSHOT_ID_COL_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }
  /**
   * Get the name of the column that stores execution timestamps in the "copy_job_history" table schema based on the schema version.
   *
   * @param schemaVersion The schema version of the "copy into error" table.
   * @return The name of the column that stores execution timestamps.
   * @throws UnsupportedOperationException If the schema version is not supported.
   */
  public static String getExecutedAtColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(EXECUTED_AT_COL_ID_V1_SCHEMA);
    }
    throw new UnsupportedOperationException("Unsupported copy_job_history table schema version." +
      " Currently supported schema version are: 1");
  }
}

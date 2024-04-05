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

/**
 * This class provides the schema definition for the copy_file_history table used in copy-into
 * operation. It defines the schema for different versions of the table.
 */
public final class CopyFileHistoryTableSchemaProvider {

  private static final int EVENT_TIMESTAMP_COL_ID_V1_SCHEMA = 1;
  private static final int JOB_ID_COL_ID_V1_SCHEMA = 2;
  private static final int FILE_PATH_ID_V1_SCHEMA = 3;
  private static final int FILE_STATE_ID_V1_SCHEMA = 4;
  private static final int RECORDS_LOADED_COUNT_ID_V1_SCHEMA = 5;
  private static final int RECORDS_REJECTED_COUNT_ID_V1_SCHEMA = 6;
  private static final Schema ICEBERG_V1_TABLE_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(
                  EVENT_TIMESTAMP_COL_ID_V1_SCHEMA,
                  "event_timestamp",
                  Types.TimestampType.withZone()),
              Types.NestedField.required(JOB_ID_COL_ID_V1_SCHEMA, "job_id", new Types.StringType()),
              Types.NestedField.required(
                  FILE_PATH_ID_V1_SCHEMA, "file_path", new Types.StringType()),
              Types.NestedField.required(
                  FILE_STATE_ID_V1_SCHEMA, "file_state", new Types.StringType()),
              Types.NestedField.required(
                  RECORDS_LOADED_COUNT_ID_V1_SCHEMA, "records_loaded_count", new Types.LongType()),
              Types.NestedField.required(
                  RECORDS_REJECTED_COUNT_ID_V1_SCHEMA,
                  "records_rejected_count",
                  new Types.LongType())));

  private CopyFileHistoryTableSchemaProvider() {}

  public static Schema getSchema(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA;
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getEventTimestampColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(EVENT_TIMESTAMP_COL_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getJobIdColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(JOB_ID_COL_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getFilePathColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(FILE_PATH_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getFileStateColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(FILE_STATE_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getRecordsLoadedColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(RECORDS_LOADED_COUNT_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  public static String getRecordsRejectedColName(long schemaVersion) {
    if (schemaVersion == 1) {
      return ICEBERG_V1_TABLE_SCHEMA.findColumnName(RECORDS_REJECTED_COUNT_ID_V1_SCHEMA);
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  private static UnsupportedOperationException newUnsupportedSchemaVersionException(
      long schemaVersion) {
    return new UnsupportedOperationException(
        "Unsupported copy_file_history table schema version: "
            + schemaVersion
            + ". Currently supported schema versions are: 1");
  }
}

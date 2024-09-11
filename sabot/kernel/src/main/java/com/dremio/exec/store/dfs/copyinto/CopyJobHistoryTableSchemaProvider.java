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

import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.planner.sql.PartitionTransform.Type;
import com.dremio.exec.store.dfs.system.evolution.step.ImmutableSystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.ImmutableSystemIcebergTableSchemaUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.ImmutableSystemIcebergTableSchemaUpdateStep.Builder;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTableSchemaUpdateStep;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

/**
 * This class provides the schema definition for the copy_jobs_history table used in copy-into
 * operation. It defines the schema for different versions of the table.
 */
public final class CopyJobHistoryTableSchemaProvider {

  private CopyJobHistoryTableSchemaProvider() {}

  /**
   * Get the schema definition for the specified schema version of the copy_jobs_history table.
   *
   * @param schemaVersion The version of the schema for which to get the schema definition.
   * @return The Schema object representing the schema for the given schema version.
   * @throws UnsupportedOperationException If the specified schema version is not supported.
   */
  public static Schema getSchema(long schemaVersion) {
    if (schemaVersion == 1) {
      return V1SchemaDefinition.TABLE_SCHEMA;
    } else if (schemaVersion == 2) {
      return V2SchemaDefinition.TABLE_SCHEMA;
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  /**
   * Get the partition specification for the copy_file_history table based on the schema version.
   *
   * @param schemaVersion The schema version for which to get the partition specification.
   * @return The partition specification for the system Iceberg table.
   * @throws UnsupportedOperationException If the specified schema version is not supported.
   */
  public static PartitionSpec getPartitionSpec(long schemaVersion) {
    if (schemaVersion == 1) {
      return V1SchemaDefinition.PARTITION_SPEC;
    } else if (schemaVersion == 2) {
      return V2SchemaDefinition.PARTITION_SPEC;
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  /**
   * Get the partition evolution step for the system Iceberg table based on the schema version.
   *
   * @param schemaVersion The schema version for which to get the partition evolution step.
   * @return The partition evolution step for the system Iceberg table.
   * @throws UnsupportedOperationException If the specified schema version is not supported.
   */
  public static SystemIcebergTablePartitionUpdateStep getPartitionEvolutionStep(
      long schemaVersion) {
    if (schemaVersion == 1) {
      return V1SchemaDefinition.getPartitionEvolutionStep();
    } else if (schemaVersion == 2) {
      return V2SchemaDefinition.getPartitionEvolutionStep();
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  /**
   * Get the schema evolution step for the specified schema version of the copy_jobs_history table.
   *
   * @param schemaVersion The version of the schema for which to get the schema evolution step.
   * @return The schema evolution step for the given schema version.
   * @throws UnsupportedOperationException If the specified schema version is not supported.
   */
  public static SystemIcebergTableSchemaUpdateStep getSchemaEvolutionStep(long schemaVersion) {
    if (schemaVersion == 1) {
      return V1SchemaDefinition.getSchemaEvolutionStep();
    } else if (schemaVersion == 2) {
      return V2SchemaDefinition.getSchemaEvolutionStep();
    }
    throw newUnsupportedSchemaVersionException(schemaVersion);
  }

  private static UnsupportedOperationException newUnsupportedSchemaVersionException(
      long schemaVersion) {
    return new UnsupportedOperationException(
        "Unsupported copy_job_history table schema version:"
            + schemaVersion
            + ". Currently supported schema versions are: 1, 2");
  }

  /**
   * Get the name of the column that stores user names in the "copy_job_history" table schema based
   * on the schema version.
   *
   * @return The name of the column that stores user names.
   * @throws UnsupportedOperationException If the schema version is not supported.
   */
  public static String getUserNameColName() {
    return V1SchemaDefinition.USER_NAME_COL_NAME;
  }

  public static String getJobIdColName() {
    return V1SchemaDefinition.JOB_ID_COL_NAME;
  }

  public static String getTableNameColName() {
    return V1SchemaDefinition.TABLE_NAME_COL_NAME;
  }

  public static String getCopyOptionsColName() {
    return V1SchemaDefinition.COPY_OPTIONS_COL_NAME;
  }

  public static String getStorageLocationColName() {
    return V1SchemaDefinition.STORAGE_LOCATION_COL_NAME;
  }

  public static String getFileFormatColName() {
    return V1SchemaDefinition.FILE_FORMAT_COL_NAME;
  }

  public static String getBaseSnapshotIdColName() {
    return V1SchemaDefinition.BASE_SNAPSHOT_ID_COL_NAME;
  }

  /**
   * Get the name of the column that stores execution timestamps in the "copy_job_history" table
   * schema based on the schema version.
   *
   * @return The name of the column that stores execution timestamps.
   * @throws UnsupportedOperationException If the schema version is not supported.
   */
  public static String getExecutedAtColName() {
    return V1SchemaDefinition.EXECUTED_AT_COL_NAME;
  }

  /** Schema definition for schema version 1 of the copy_jobs_history table */
  private static class V1SchemaDefinition {
    private static final int EXECUTED_AT_COL_ID = 1;
    protected static final String EXECUTED_AT_COL_NAME = "executed_at";
    private static final int JOB_ID_COL_ID = 2;
    private static final String JOB_ID_COL_NAME = "job_id";
    private static final int TABLE_NAME_COL_ID = 3;
    private static final String TABLE_NAME_COL_NAME = "table_name";
    private static final int RECORDS_LOADED_COUNT_COL_ID = 4;
    private static final String RECORDS_LOADED_COUNT_COL_NAME = "records_loaded_count";
    private static final int RECORDS_REJECTED_COUNT_COL_ID = 5;
    private static final String RECORDS_REJECTED_COUNT_COL_NAME = "records_rejected_count";
    private static final int COPY_OPTIONS_COL_ID = 6;
    private static final String COPY_OPTIONS_COL_NAME = "copy_options";
    private static final int USER_NAME_COL_ID = 7;
    private static final String USER_NAME_COL_NAME = "user_name";
    private static final int BASE_SNAPSHOT_ID_COL_ID = 8;
    private static final String BASE_SNAPSHOT_ID_COL_NAME = "base_snapshot_id";
    private static final int STORAGE_LOCATION_COL_ID = 9;
    private static final String STORAGE_LOCATION_COL_NAME = "storage_location";
    private static final int FILE_FORMAT_COL_ID = 10;
    private static final String FILE_FORMAT_COL_NAME = "file_format";

    private static final List<NestedField> COL_LIST =
        ImmutableList.of(
            Types.NestedField.required(
                EXECUTED_AT_COL_ID, EXECUTED_AT_COL_NAME, Types.TimestampType.withZone()),
            Types.NestedField.required(JOB_ID_COL_ID, JOB_ID_COL_NAME, new Types.StringType()),
            Types.NestedField.required(
                TABLE_NAME_COL_ID, TABLE_NAME_COL_NAME, new Types.StringType()),
            Types.NestedField.required(
                RECORDS_LOADED_COUNT_COL_ID, RECORDS_LOADED_COUNT_COL_NAME, new Types.LongType()),
            Types.NestedField.required(
                RECORDS_REJECTED_COUNT_COL_ID,
                RECORDS_REJECTED_COUNT_COL_NAME,
                new Types.LongType()),
            Types.NestedField.required(
                COPY_OPTIONS_COL_ID, COPY_OPTIONS_COL_NAME, new Types.StringType()),
            Types.NestedField.required(
                USER_NAME_COL_ID, USER_NAME_COL_NAME, new Types.StringType()),
            Types.NestedField.required(
                BASE_SNAPSHOT_ID_COL_ID, BASE_SNAPSHOT_ID_COL_NAME, new Types.LongType()),
            Types.NestedField.required(
                STORAGE_LOCATION_COL_ID, STORAGE_LOCATION_COL_NAME, new Types.StringType()),
            Types.NestedField.required(
                FILE_FORMAT_COL_ID, FILE_FORMAT_COL_NAME, new Types.StringType()));
    private static final Schema TABLE_SCHEMA = new Schema(COL_LIST);
    private static final PartitionSpec PARTITION_SPEC = PartitionSpec.unpartitioned();

    private static SystemIcebergTableSchemaUpdateStep getSchemaEvolutionStep() {
      // this is the first version of the schema no need to update it
      return ImmutableSystemIcebergTableSchemaUpdateStep.of(1L);
    }

    private static SystemIcebergTablePartitionUpdateStep getPartitionEvolutionStep() {
      return ImmutableSystemIcebergTablePartitionUpdateStep.of(1L);
    }
  }

  /** Schema definition for schema version 2 of the copy_jobs_history table */
  private static class V2SchemaDefinition extends V1SchemaDefinition {
    private static final int BRANCH_COL_ID = 11;
    private static final String BRANCH_COL_NAME = "branch";
    private static final int PIPE_NAME_COL_ID = 12;
    private static final String PIPE_NAME_COL_NAME = "pipe_name";
    private static final int PIPE_ID_COL_ID = 13;
    private static final String PIPE_ID_COL_NAME = "pipe_id";
    private static final int TIME_ELAPSED_COL_ID = 14;
    private static final String TIME_ELAPSED_COL_NAME = "time_elapsed";
    private static final List<NestedField> ADDED_COL_LIST =
        ImmutableList.of(
            Types.NestedField.optional(BRANCH_COL_ID, BRANCH_COL_NAME, new Types.StringType()),
            Types.NestedField.optional(
                PIPE_NAME_COL_ID, PIPE_NAME_COL_NAME, new Types.StringType()),
            Types.NestedField.optional(PIPE_ID_COL_ID, PIPE_ID_COL_NAME, new Types.StringType()),
            Types.NestedField.optional(
                TIME_ELAPSED_COL_ID, TIME_ELAPSED_COL_NAME, new Types.LongType()));
    private static final List<NestedField> COL_LIST =
        Stream.of(V1SchemaDefinition.COL_LIST, ADDED_COL_LIST)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    protected static final Schema TABLE_SCHEMA = new Schema(COL_LIST);
    private static final PartitionSpec PARTITION_SPEC =
        PartitionSpec.builderFor(TABLE_SCHEMA).day(EXECUTED_AT_COL_NAME).build();

    private static SystemIcebergTableSchemaUpdateStep getSchemaEvolutionStep() {
      // establish the delta compared to the previous version of the schema
      Builder builder = new Builder().setSchemaVersion(2L);
      ADDED_COL_LIST.stream()
          .map(c -> Pair.of(c.name(), c.type()))
          .forEach(builder::addAddedColumns);
      return builder.build();
    }

    private static SystemIcebergTablePartitionUpdateStep getPartitionEvolutionStep() {
      return new ImmutableSystemIcebergTablePartitionUpdateStep.Builder()
          .setSchemaVersion(2L)
          .addAllPartitionTransforms(
              ImmutableList.of(new PartitionTransform(EXECUTED_AT_COL_NAME, Type.DAY)))
          .build();
    }
  }
}

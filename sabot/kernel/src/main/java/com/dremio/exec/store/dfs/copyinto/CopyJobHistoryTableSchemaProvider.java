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

import com.dremio.exec.store.dfs.copyinto.systemtable.schema.ColumnDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.SchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.SchemaDefinitionFactory;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.jobhistory.V1JobHistorySchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.jobhistory.V3JobHistorySchemaDefinition;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.SupportedSystemIcebergTable;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTableSchemaUpdateStep;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/**
 * This class provides the schema definition for the copy_jobs_history table used in copy-into
 * operation.
 */
public final class CopyJobHistoryTableSchemaProvider {

  private final SchemaDefinition schemaDefinition;

  public CopyJobHistoryTableSchemaProvider(long schemaVersion) {
    this.schemaDefinition =
        SchemaDefinitionFactory.getSchemaDefinition(
            schemaVersion, SupportedSystemIcebergTable.COPY_JOB_HISTORY);
  }

  /**
   * Get the schema definition of the copy_jobs_history table.
   *
   * @return The Schema object representing the schema for the given schema version.
   */
  public Schema getSchema() {
    return schemaDefinition.getTableSchema();
  }

  /**
   * Get the partition specification for the copy_file_history table.
   *
   * @return The partition specification for the system Iceberg table.
   */
  public PartitionSpec getPartitionSpec() {
    return schemaDefinition.getPartitionSpec();
  }

  /**
   * Get the partition evolution step for the system Iceberg table based on the schema version.
   *
   * @return The partition evolution step for the system Iceberg table.
   */
  public SystemIcebergTablePartitionUpdateStep getPartitionEvolutionStep() {
    return schemaDefinition.getPartitionEvolutionStep();
  }

  /**
   * Get the schema evolution step for the specified schema version of the copy_jobs_history table.
   *
   * @return The schema evolution step for the given schema version.
   */
  public SystemIcebergTableSchemaUpdateStep getSchemaEvolutionStep() {
    return schemaDefinition.getSchemaEvolutionStep();
  }

  public List<ColumnDefinition> getColumns() {
    return schemaDefinition.getColumns();
  }

  /**
   * Get the name of the column that stores usernames in the "copy_job_history" table.
   *
   * @return The name of the column that stores usernames.
   */
  public String getUserNameColName() {
    return V1JobHistorySchemaDefinition.USER_NAME_COL.getName();
  }

  public String getJobIdColName() {
    return V1JobHistorySchemaDefinition.JOB_ID_COL.getName();
  }

  public String getTableNameColName() {
    return V1JobHistorySchemaDefinition.TABLE_NAME_COL.getName();
  }

  public String getCopyOptionsColName() {
    return V1JobHistorySchemaDefinition.COPY_OPTIONS_COL.getName();
  }

  public String getStorageLocationColName() {
    return V1JobHistorySchemaDefinition.STORAGE_LOCATION_COL.getName();
  }

  public String getFileFormatColName() {
    return V1JobHistorySchemaDefinition.FILE_FORMAT_COL.getName();
  }

  public String getBaseSnapshotIdColName() {
    return V1JobHistorySchemaDefinition.BASE_SNAPSHOT_ID_COL.getName();
  }

  /**
   * Get the name of the column that stores execution timestamps in the "copy_job_history" table.
   *
   * @return The name of the column that stores execution timestamps.
   */
  public String getExecutedAtColName() {
    return V1JobHistorySchemaDefinition.EXECUTED_AT_COL.getName();
  }

  public String getTransformationProperties() {
    if (schemaDefinition.getSchemaVersion() > 2) {
      return V3JobHistorySchemaDefinition.TRANSFORMATION_PROPS_COL.getName();
    }
    throw newUnsupportedSchemaVersionException(
        schemaDefinition.getSchemaVersion(),
        V3JobHistorySchemaDefinition.TRANSFORMATION_PROPS_COL.getName());
  }

  private static UnsupportedOperationException newUnsupportedSchemaVersionException(
      long schemaVersion, String columnName) {
    return new UnsupportedOperationException(
        String.format(
            "Column %s in copy_job_history table is not found in current schema version %d",
            columnName, schemaVersion));
  }
}

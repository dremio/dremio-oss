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
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.filehistory.V1FileHistorySchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.filehistory.V2FileHistorySchemaDefinition;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.SupportedSystemIcebergTable;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTableSchemaUpdateStep;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/**
 * This class provides the schema definition for the copy_file_history table used in copy-into
 * operation. It defines the schema for different versions of the table.
 */
public final class CopyFileHistoryTableSchemaProvider {

  private final long schemaVersion;
  private final SchemaDefinition schemaDefinition;

  public CopyFileHistoryTableSchemaProvider(long schemaVersion) {
    this.schemaVersion = schemaVersion;
    this.schemaDefinition =
        SchemaDefinitionFactory.getSchemaDefinition(
            schemaVersion, SupportedSystemIcebergTable.COPY_FILE_HISTORY);
  }

  /**
   * Get the schema definition of the copy_file_history table.
   *
   * @return The Schema object representing the schema for the given schema version.
   */
  public Schema getSchema() {
    return schemaDefinition.getTableSchema();
  }

  /** Get the partition specification for the copy_file_history table. */
  public PartitionSpec getPartitionSpec() {
    return schemaDefinition.getPartitionSpec();
  }

  /**
   * Get the partition evolution step for the copy_file_history table.
   *
   * @return The partition evolution step for the system Iceberg table.
   * @throws UnsupportedOperationException If the specified schema version is not supported.
   */
  public SystemIcebergTablePartitionUpdateStep getPartitionEvolutionStep() {
    return schemaDefinition.getPartitionEvolutionStep();
  }

  /**
   * Get the schema evolution step for the copy_file_history table.
   *
   * @return The schema evolution step for the given schema version.
   */
  public SystemIcebergTableSchemaUpdateStep getSchemaEvolutionStep() {
    return schemaDefinition.getSchemaEvolutionStep();
  }

  public List<ColumnDefinition> getColumns() {
    return schemaDefinition.getColumns();
  }

  public String getEventTimestampColName() {
    return V1FileHistorySchemaDefinition.EVENT_TIMESTAMP_COL.getName();
  }

  public String getJobIdColName() {
    return V1FileHistorySchemaDefinition.JOB_ID_COL.getName();
  }

  public String getFilePathColName() {
    return V1FileHistorySchemaDefinition.FILE_PATH_COL.getName();
  }

  public String getFileStateColName() {
    return V1FileHistorySchemaDefinition.FILE_STATE_COL.getName();
  }

  public String getRecordsLoadedColName() {
    return V1FileHistorySchemaDefinition.RECORDS_LOADED_COUNT_COL.getName();
  }

  public String getRecordsRejectedColName() {
    return V1FileHistorySchemaDefinition.RECORDS_REJECTED_COUNT_COL.getName();
  }

  public String getPipeIdColName() {
    if (schemaVersion >= 2) {
      return V2FileHistorySchemaDefinition.PIPE_ID_COL.getName();
    }
    throw newUnsupportedSchemaVersionException(
        schemaVersion, V2FileHistorySchemaDefinition.PIPE_ID_COL.getName());
  }

  private static UnsupportedOperationException newUnsupportedSchemaVersionException(
      long schemaVersion, String columnName) {
    return new UnsupportedOperationException(
        String.format(
            "Column %s in copy_file_history table is not found in current schema version %d",
            columnName, schemaVersion));
  }
}

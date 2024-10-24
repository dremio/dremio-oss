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
package com.dremio.exec.store.dfs.copyinto.systemtable.schema.errorshistory;

import com.dremio.exec.store.dfs.copyinto.systemtable.schema.ColumnDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.SchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.filehistory.V1FileHistorySchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.jobhistory.V1JobHistorySchemaDefinition;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTableSchemaUpdateStep;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;

/** Schema definition for schema version 1 of the copy_errors_history view */
public class V1ErrorsHistorySchemaDefinition implements SchemaDefinition {

  private final long schemaVersion;
  private final List<ColumnDefinition> columns =
      ImmutableList.of(
          V1JobHistorySchemaDefinition.EXECUTED_AT_COL,
          V1JobHistorySchemaDefinition.JOB_ID_COL,
          V1JobHistorySchemaDefinition.TABLE_NAME_COL,
          V1JobHistorySchemaDefinition.USER_NAME_COL,
          V1JobHistorySchemaDefinition.BASE_SNAPSHOT_ID_COL,
          V1JobHistorySchemaDefinition.STORAGE_LOCATION_COL,
          V1FileHistorySchemaDefinition.FILE_PATH_COL,
          V1FileHistorySchemaDefinition.FILE_STATE_COL,
          V1FileHistorySchemaDefinition.RECORDS_LOADED_COUNT_COL,
          V1FileHistorySchemaDefinition.RECORDS_REJECTED_COUNT_COL);

  public V1ErrorsHistorySchemaDefinition(long schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  @Override
  public Schema getTableSchema() {
    return new Schema(
        IntStream.range(0, columns.size())
            .mapToObj(
                i ->
                    NestedField.required(i + 1, columns.get(i).getName(), columns.get(i).getType()))
            .collect(Collectors.toList()));
  }

  @Override
  public PartitionSpec getPartitionSpec() {
    throw new UnsupportedOperationException(
        "Partition specification is not supported for copy_errors_history table");
  }

  @Override
  public SystemIcebergTableSchemaUpdateStep getSchemaEvolutionStep() {
    throw new UnsupportedOperationException(
        "Schema evolution is not supported for copy_errors_history table");
  }

  @Override
  public SystemIcebergTablePartitionUpdateStep getPartitionEvolutionStep() {
    throw new UnsupportedOperationException(
        "Partition evolution is not supported for copy_errors_history table");
  }

  @Override
  public List<ColumnDefinition> getColumns() {
    return columns;
  }

  @Override
  public long getSchemaVersion() {
    return schemaVersion;
  }
}

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
package com.dremio.exec.store.dfs.copyinto.systemtable.schema.filehistory;

import com.dremio.exec.store.dfs.copyinto.systemtable.schema.ColumnDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.SchemaDefinition;
import com.dremio.exec.store.dfs.system.evolution.step.ImmutableSystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.ImmutableSystemIcebergTableSchemaUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTableSchemaUpdateStep;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

public class V1FileHistorySchemaDefinition implements SchemaDefinition {
  protected final long schemaVersion;

  public static final ColumnDefinition EVENT_TIMESTAMP_COL =
      new ColumnDefinition("event_timestamp", 1, Types.TimestampType.withZone(), false, false);
  public static final ColumnDefinition JOB_ID_COL =
      new ColumnDefinition("job_id", 2, Types.StringType.get(), false, false);
  public static final ColumnDefinition FILE_PATH_COL =
      new ColumnDefinition("file_path", 3, Types.StringType.get(), false, false);
  public static final ColumnDefinition FILE_STATE_COL =
      new ColumnDefinition("file_state", 4, Types.StringType.get(), false, false);
  public static final ColumnDefinition RECORDS_LOADED_COUNT_COL =
      new ColumnDefinition("records_loaded_count", 5, Types.LongType.get(), false, false);
  public static final ColumnDefinition RECORDS_REJECTED_COUNT_COL =
      new ColumnDefinition("records_rejected_count", 6, Types.LongType.get(), false, false);
  protected List<ColumnDefinition> columns =
      ImmutableList.of(
          EVENT_TIMESTAMP_COL,
          JOB_ID_COL,
          FILE_PATH_COL,
          FILE_STATE_COL,
          RECORDS_LOADED_COUNT_COL,
          RECORDS_REJECTED_COUNT_COL);

  public V1FileHistorySchemaDefinition(long schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  @Override
  public Schema getTableSchema() {
    return new Schema(
        columns.stream()
            .map(c -> Types.NestedField.of(c.getId(), c.isOptional(), c.getName(), c.getType()))
            .collect(Collectors.toList()));
  }

  @Override
  public PartitionSpec getPartitionSpec() {
    return PartitionSpec.unpartitioned();
  }

  @Override
  public SystemIcebergTableSchemaUpdateStep getSchemaEvolutionStep() {
    return ImmutableSystemIcebergTableSchemaUpdateStep.of(schemaVersion);
  }

  @Override
  public SystemIcebergTablePartitionUpdateStep getPartitionEvolutionStep() {
    return ImmutableSystemIcebergTablePartitionUpdateStep.of(schemaVersion);
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

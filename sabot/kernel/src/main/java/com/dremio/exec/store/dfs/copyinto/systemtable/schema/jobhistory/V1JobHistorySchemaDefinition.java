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
package com.dremio.exec.store.dfs.copyinto.systemtable.schema.jobhistory;

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

/** Schema definition for schema version 1 of the copy_jobs_history table */
public class V1JobHistorySchemaDefinition implements SchemaDefinition {

  protected final long schemaVersion;

  public static final ColumnDefinition EXECUTED_AT_COL =
      new ColumnDefinition("executed_at", 1, Types.TimestampType.withZone(), false, false);
  public static final ColumnDefinition JOB_ID_COL =
      new ColumnDefinition("job_id", 2, new Types.StringType(), false, false);
  public static final ColumnDefinition TABLE_NAME_COL =
      new ColumnDefinition("table_name", 3, new Types.StringType(), false, false);
  public static final ColumnDefinition RECORDS_LOADED_COUNT_COL =
      new ColumnDefinition("records_loaded_count", 4, new Types.LongType(), false, false);
  public static final ColumnDefinition RECORDS_REJECTED_COUNT_COL =
      new ColumnDefinition("records_rejected_count", 5, new Types.LongType(), false, false);
  public static final ColumnDefinition COPY_OPTIONS_COL =
      new ColumnDefinition("copy_options", 6, new Types.StringType(), false, false);
  public static final ColumnDefinition USER_NAME_COL =
      new ColumnDefinition("user_name", 7, new Types.StringType(), false, false);
  public static final ColumnDefinition BASE_SNAPSHOT_ID_COL =
      new ColumnDefinition("base_snapshot_id", 8, new Types.LongType(), false, false);
  public static final ColumnDefinition STORAGE_LOCATION_COL =
      new ColumnDefinition("storage_location", 9, new Types.StringType(), false, false);
  public static final ColumnDefinition FILE_FORMAT_COL =
      new ColumnDefinition("file_format", 10, new Types.StringType(), false, false);

  protected List<ColumnDefinition> columns =
      ImmutableList.of(
          EXECUTED_AT_COL,
          JOB_ID_COL,
          TABLE_NAME_COL,
          RECORDS_LOADED_COUNT_COL,
          RECORDS_REJECTED_COUNT_COL,
          COPY_OPTIONS_COL,
          USER_NAME_COL,
          BASE_SNAPSHOT_ID_COL,
          STORAGE_LOCATION_COL,
          FILE_FORMAT_COL);

  public V1JobHistorySchemaDefinition(long schemaVersion) {
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
    // this is the first version of the schema no need to update it
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

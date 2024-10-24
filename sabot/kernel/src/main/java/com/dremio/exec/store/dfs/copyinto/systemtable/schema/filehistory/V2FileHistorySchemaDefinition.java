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

import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.planner.sql.PartitionTransform.Type;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.ColumnDefinition;
import com.dremio.exec.store.dfs.system.evolution.step.ImmutableSystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.ImmutableSystemIcebergTableSchemaUpdateStep.Builder;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTableSchemaUpdateStep;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;

public class V2FileHistorySchemaDefinition extends V1FileHistorySchemaDefinition {
  public static final ColumnDefinition PIPE_ID_COL =
      new ColumnDefinition("pipe_id", 7, new Types.StringType(), true, false);
  public static final ColumnDefinition FILE_SIZE_COL =
      new ColumnDefinition("file_size", 8, new Types.LongType(), true, false);
  public static final ColumnDefinition FIRST_ERROR_MESSAGE_COL =
      new ColumnDefinition("first_error_message", 9, new Types.StringType(), true, false);
  public static final ColumnDefinition FILE_NOTIFICATION_TIMESTAMP_COL =
      new ColumnDefinition(
          "file_notification_timestamp", 10, Types.TimestampType.withZone(), true, false);
  public static final ColumnDefinition INGESTION_SOURCE_TYPE_COL =
      new ColumnDefinition("ingestion_source_type", 11, new Types.StringType(), true, false);
  public static final ColumnDefinition REQUEST_ID_COL =
      new ColumnDefinition("request_id", 12, new Types.StringType(), true, false);

  protected List<ColumnDefinition> addedColumns =
      ImmutableList.of(
          PIPE_ID_COL,
          FILE_SIZE_COL,
          FIRST_ERROR_MESSAGE_COL,
          FILE_NOTIFICATION_TIMESTAMP_COL,
          INGESTION_SOURCE_TYPE_COL,
          REQUEST_ID_COL);

  public V2FileHistorySchemaDefinition(long schemaVersion) {
    super(schemaVersion);
    columns =
        Stream.of(super.columns, addedColumns)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
  }

  @Override
  public PartitionSpec getPartitionSpec() {
    return PartitionSpec.builderFor(getTableSchema()).day(EVENT_TIMESTAMP_COL.getName()).build();
  }

  @Override
  public SystemIcebergTableSchemaUpdateStep getSchemaEvolutionStep() {
    // establish the delta compared to the previous version of the schema
    Builder builder = new Builder().setSchemaVersion(schemaVersion);
    addedColumns.forEach(
        c -> builder.addAddedColumns(Triple.of(c.getName(), c.getType(), c.isOptional())));
    return builder.build();
  }

  @Override
  public SystemIcebergTablePartitionUpdateStep getPartitionEvolutionStep() {
    return new ImmutableSystemIcebergTablePartitionUpdateStep.Builder()
        .setSchemaVersion(schemaVersion)
        .setPartitionTransforms(
            ImmutableList.of(new PartitionTransform(EVENT_TIMESTAMP_COL.getName(), Type.DAY)))
        .build();
  }
}

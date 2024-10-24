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
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;

/** Schema definition for schema version 2 of the copy_jobs_history table */
public class V2JobHistorySchemaDefinition extends V1JobHistorySchemaDefinition {

  public static final ColumnDefinition BRANCH_COL =
      new ColumnDefinition("branch", 11, new StringType(), true, false);
  public static final ColumnDefinition PIPE_NAME_COL =
      new ColumnDefinition("pipe_name", 12, new StringType(), true, false);
  public static final ColumnDefinition PIPE_ID_COL =
      new ColumnDefinition("pipe_id", 13, new StringType(), true, false);
  public static final ColumnDefinition TIME_ELAPSED_COL =
      new ColumnDefinition("time_elapsed", 14, new LongType(), true, false);

  protected List<ColumnDefinition> addedColumns =
      ImmutableList.of(BRANCH_COL, PIPE_NAME_COL, PIPE_ID_COL, TIME_ELAPSED_COL);

  public V2JobHistorySchemaDefinition(long schemaVersion) {
    super(schemaVersion);
    columns =
        Stream.of(super.columns, addedColumns)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
  }

  @Override
  public PartitionSpec getPartitionSpec() {
    return PartitionSpec.builderFor(getTableSchema()).day(EXECUTED_AT_COL.getName()).build();
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
        .addAllPartitionTransforms(
            ImmutableList.of(new PartitionTransform(EXECUTED_AT_COL.getName(), Type.DAY)))
        .build();
  }
}

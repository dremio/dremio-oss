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
import org.apache.iceberg.types.Types.StringType;

public class V3JobHistorySchemaDefinition extends V2JobHistorySchemaDefinition {
  public static final ColumnDefinition TRANSFORMATION_PROPS_COL =
      new ColumnDefinition("transformation_properties", 15, new StringType(), true, true);

  protected List<ColumnDefinition> addedColumns = ImmutableList.of(TRANSFORMATION_PROPS_COL);

  public V3JobHistorySchemaDefinition(long schemaVersion) {
    super(schemaVersion);
    columns =
        Stream.of(super.columns, addedColumns)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
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
    return ImmutableSystemIcebergTablePartitionUpdateStep.of(schemaVersion);
  }
}

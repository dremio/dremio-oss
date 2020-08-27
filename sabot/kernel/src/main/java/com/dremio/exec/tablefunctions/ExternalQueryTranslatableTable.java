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
package com.dremio.exec.tablefunctions;

import java.util.function.Function;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.record.BatchSchema;

/**
 * TranslatableTable implementation of External Query
 */
public final class ExternalQueryTranslatableTable implements TranslatableTable {
  private final StoragePluginId pluginId;
  private final String sql;
  private final BatchSchema batchSchema;
  private final RelDataType rowType;

  private ExternalQueryTranslatableTable(BatchSchema batchSchema,
                                         RelDataType rowType,
                                         StoragePluginId pluginId,
                                         String sql) {
    this.batchSchema = batchSchema;
    this.rowType = rowType;
    this.pluginId = pluginId;
    this.sql = sql;
  }

  public static ExternalQueryTranslatableTable create(Function<String, BatchSchema> schemaBuilder,
                                                      Function<BatchSchema, RelDataType> rowTypeBuilder,
                                                      StoragePluginId pluginId, String sql) {
    final BatchSchema schema = schemaBuilder.apply(sql);
    final RelDataType rowType = rowTypeBuilder.apply(schema);
    return new ExternalQueryTranslatableTable(schema, rowType, pluginId, sql);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
    return new ExternalQueryScanCrel(
      toRelContext.getCluster(),
      toRelContext.getCluster().traitSetOf(Convention.NONE),
      rowType,
      pluginId,
      sql,
      batchSchema);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return rowType;
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    // There isn't really a suitable choice for this.
    // TEMPORARY_VIEW might be better to indicate it's a view, but that loses
    // the fact that this is local in nature.
    // OTHER has the comment to indicate that the Calcite enum should be extended
    // to add a new table type.
    return Schema.TableType.LOCAL_TEMPORARY;
  }
}

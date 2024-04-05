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
package com.dremio.exec.planner.logical;

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.planner.sql.DremioToRelContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;

/**
 * ConvertedViewTable is used to decorate a ViewTable with the view's converted Calcite type. The
 * converted type is basically the validated and converted row type of the view's SQL. This
 * converted type is then returned as the validated type of this view by {@link
 * DremioCatalogReader}. This ensures that planning and reflection matching always see the view type
 * to be the same as its converted type regardless if the view is stored in Sonar Catalog or Nessie
 * sources (Iceberg Views).
 *
 * <p>Planning rules including reflection matching should be always be done with validated and
 * converted Calcite types. Using an Arrow or Iceberg type, translating to Calcite and then using
 * that type for planning will result in subtle bugs including varchar precision, nullability,
 * duplicate column name issues and complex types issues.
 */
public final class ConvertedViewTable implements DremioTable {

  private final ViewTable unconvertedViewTable;
  // Validated row type with view field names (if applicable)
  private final RelDataType validatedRowType;
  private final RelNode relNode;
  private final boolean isCorrelated;
  private boolean hasUsedCachedRel = false;

  private ConvertedViewTable(
      ViewTable unconvertedViewTable, RelDataType validatedRowType, RelNode relNode) {
    this.unconvertedViewTable = unconvertedViewTable;
    this.validatedRowType = validatedRowType;
    this.relNode = relNode;
    CorrelationCollector collector = new CorrelationCollector();
    relNode.accept(collector);
    isCorrelated = !collector.getCorrelationIds().isEmpty();
  }

  @Override
  public String getVersion() {
    return unconvertedViewTable.getVersion();
  }

  @Override
  public BatchSchema getSchema() {
    return unconvertedViewTable.getSchema();
  }

  @Override
  public DatasetConfig getDatasetConfig() {
    return unconvertedViewTable.getDatasetConfig();
  }

  @Override
  public TableVersionContext getVersionContext() {
    return unconvertedViewTable.getVersionContext();
  }

  @Override
  public NamespaceKey getPath() {
    return unconvertedViewTable.getPath();
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    if (isCorrelated && hasUsedCachedRel) {
      return ((DremioToRelContext.DremioQueryToRelContext) context)
          .expandView(unconvertedViewTable)
          .rel;
    }
    hasUsedCachedRel = true;
    return relNode;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return validatedRowType;
  }

  @Override
  public Statistic getStatistic() {
    return unconvertedViewTable.getStatistic();
  }

  @Override
  public TableType getJdbcTableType() {
    return unconvertedViewTable.getJdbcTableType();
  }

  public static ConvertedViewTable of(
      final ViewTable table, final RelDataType validatedRowType, final RelNode relNode) {
    return new ConvertedViewTable(table, validatedRowType, relNode);
  }

  /**
   * Used to find the presence of any correlationIds in a rel tree. Similar to {@link RelOptUtil}'s
   * CorrelationCollector except used variables are not unset by rels in parent scope.
   */
  private static class CorrelationCollector extends RelHomogeneousShuttle {
    private final RelOptUtil.VariableUsedVisitor vuv = new RelOptUtil.VariableUsedVisitor(this);

    @Override
    public RelNode visit(RelNode other) {
      other.collectVariablesUsed(vuv.variables);
      other.accept(vuv);
      RelNode result = super.visit(other);
      return result;
    }

    Set<CorrelationId> getCorrelationIds() {
      return ImmutableSet.copyOf(vuv.variables);
    }
  }
}

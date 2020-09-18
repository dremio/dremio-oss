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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

/**
 * Rel base for ExternalQuery RelNodes
 */
public abstract class ExternalQueryRelBase extends AbstractRelNode {
  final BatchSchema batchSchema;
  final String sql;
  final StoragePluginId pluginId;

  ExternalQueryRelBase(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType,
                       StoragePluginId pluginId, String sql, BatchSchema schema) {
    super(cluster, traitSet);
    this.batchSchema = schema;
    this.sql = sql;
    this.pluginId = pluginId;
    this.rowType = rowType;
  }

  public BatchSchema getBatchSchema() {
    return batchSchema;
  }

  public String getSql() {
    return sql;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(ImmutableList.of(pluginId.getName(), "external_query"));
  }

  /**
   * Subclasses must override copy to avoid problems where duplicate scan operators are
   * created due to the same (reference-equality) Prel being used multiple times in the plan.
   * The copy implementation in AbstractRelNode just returns a reference to "this".
   */
  @Override
  public abstract RelNode copy(RelTraitSet traitSet, List<RelNode> inputs);

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeCost(
      DremioCost.BIG_ROW_COUNT,
      DremioCost.BIG_ROW_COUNT,
      DremioCost.BIG_ROW_COUNT);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return DremioCost.BIG_ROW_COUNT;
  }

  @Override
  protected RelDataType deriveRowType() {
    return rowType;
  }

  @Override
  public final RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
      .item("source", pluginId.getName())
      .item("query", sql);
  }
}

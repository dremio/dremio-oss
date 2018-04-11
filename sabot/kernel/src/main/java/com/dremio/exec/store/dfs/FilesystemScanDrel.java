/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.dfs;

import java.util.List;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.LogicalPlanImplementor;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.RelOptNamespaceTable;
import com.dremio.exec.store.parquet.FilterCondition;
import com.dremio.exec.store.parquet.FilterConditions;
import com.google.common.base.Objects;

/**
 * ScanDrel for filesystem stores filter conditions.
 */
public class FilesystemScanDrel extends ScanRelBase implements Rel, FilterableScan, PruneableScan {
  private final List<FilterCondition> conditions;

  public FilesystemScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                            TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment) {
    this(cluster, traitSet, table, pluginId, dataset, projectedColumns, null, observedRowcountAdjustment);
  }


  private FilesystemScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                            TableMetadata dataset, List<SchemaPath> projectedColumns, List<FilterCondition> conditions, double observedRowcountAdjustment) {
    super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.conditions = conditions;
  }

  // Clone with new conditions
  private FilesystemScanDrel(FilesystemScanDrel that, List<FilterCondition> conditions) {
    super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(), that.getProjectedColumns(), that.getObservedRowcountAdjustment());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.conditions = conditions;
  }

  // Clone with new dataset pointer
  private FilesystemScanDrel(FilesystemScanDrel that, TableMetadata newDatasetPointer) {
    super(that.getCluster(), that.getTraitSet(), new RelOptNamespaceTable(newDatasetPointer, that.getCluster()), that.getPluginId(), newDatasetPointer, that.getProjectedColumns(), that.getObservedRowcountAdjustment());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.conditions = that.getConditions();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FilesystemScanDrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, projectedColumns, conditions, observedRowcountAdjustment);
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {
    throw new UnsupportedOperationException();
  }

  public FilesystemScanDrel applyConditions(List<FilterCondition> conditions) {
    return new FilesystemScanDrel(this, conditions);
  }

  public FilesystemScanDrel applyDatasetPointer(TableMetadata newDatasetPointer) {
    return new FilesystemScanDrel(this, newDatasetPointer);
  }

  public List<FilterCondition> getConditions() {
    return conditions;
  }

  @Override
  public double getCostAdjustmentFactor(){
    return FilterConditions.getCostAdjustment(conditions);
  }

  protected double getFilterReduction(){
    if(conditions != null && !conditions.isEmpty()){
      return 0.15d;
    }else {
      return 1d;
    }
  }

  @Override
  public FilesystemScanDrel cloneWithProject(List<SchemaPath> projection) {
    return new FilesystemScanDrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata, projection, observedRowcountAdjustment);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    pw = super.explainTerms(pw);
    if(conditions != null && !conditions.isEmpty()){
      return pw.item("filters",  conditions);
    }
    return pw;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof FilesystemScanDrel)) {
      return false;
    }
    FilesystemScanDrel castOther = (FilesystemScanDrel) other;
    return Objects.equal(conditions, castOther.conditions) && super.equals(other);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), conditions);
  }

}

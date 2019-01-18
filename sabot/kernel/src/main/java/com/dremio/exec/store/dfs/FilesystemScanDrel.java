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
import com.dremio.exec.store.RelOptNamespaceTable;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.parquet.ParquetScanFilter;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * ScanDrel for filesystem stores filter conditions.
 */
public class FilesystemScanDrel extends ScanRelBase implements Rel, FilterableScan, PruneableScan {
  private final ParquetScanFilter filter;

  public FilesystemScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                            TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment) {
    this(cluster, traitSet, table, pluginId, dataset, projectedColumns, null, observedRowcountAdjustment);
  }


  private FilesystemScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                            TableMetadata dataset, List<SchemaPath> projectedColumns, ParquetScanFilter filter, double observedRowcountAdjustment) {
    super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = filter;
  }

  // Clone with new conditions
  private FilesystemScanDrel(FilesystemScanDrel that, ParquetScanFilter filter) {
    super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(), that.getProjectedColumns(), that.getObservedRowcountAdjustment());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = filter;
  }

  // Clone with new dataset pointer
  private FilesystemScanDrel(FilesystemScanDrel that, TableMetadata newDatasetPointer) {
    super(that.getCluster(), that.getTraitSet(), new RelOptNamespaceTable(newDatasetPointer, that.getCluster()), that.getPluginId(), newDatasetPointer, that.getProjectedColumns(), that.getObservedRowcountAdjustment());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = that.getFilter();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FilesystemScanDrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, projectedColumns, filter, observedRowcountAdjustment);
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FilesystemScanDrel applyFilter(ScanFilter scanFilter) {
    Preconditions.checkArgument(scanFilter instanceof ParquetScanFilter);
    return new FilesystemScanDrel(this, (ParquetScanFilter) scanFilter);
  }

  public FilesystemScanDrel applyDatasetPointer(TableMetadata newDatasetPointer) {
    return new FilesystemScanDrel(this, newDatasetPointer);
  }

  public ParquetScanFilter getFilter() {
    return filter;
  }

  @Override
  public double getCostAdjustmentFactor(){
    return filter != null ? filter.getCostAdjustment() : super.getCostAdjustmentFactor();
  }

  protected double getFilterReduction(){
    if(filter != null){
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
    if(filter != null){
      return pw.item("filters", filter);
    }
    return pw;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof FilesystemScanDrel)) {
      return false;
    }
    FilesystemScanDrel castOther = (FilesystemScanDrel) other;
    return Objects.equal(filter, castOther.filter) && super.equals(other);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), filter);
  }

}

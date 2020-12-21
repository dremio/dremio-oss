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
package com.dremio.exec.store.dfs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.RelOptNamespaceTable;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetScanFilter;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * ScanDrel for filesystem stores filter conditions.
 */
public class FilesystemScanDrel extends ScanRelBase implements Rel, FilterableScan, PruneableScan {
  private final ParquetScanFilter filter;
  private final boolean arrowCachingEnabled;

  public FilesystemScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                            TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment, boolean arrowCachingEnabled) {
    this(cluster, traitSet, table, pluginId, dataset, projectedColumns, null, observedRowcountAdjustment, arrowCachingEnabled);
  }

  private FilesystemScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                             TableMetadata dataset, List<SchemaPath> projectedColumns, ParquetScanFilter filter, double observedRowcountAdjustment, boolean arrowCachingEnabled) {
    super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = filter;
    this.arrowCachingEnabled = arrowCachingEnabled;
  }

  // Clone with new conditions
  private FilesystemScanDrel(FilesystemScanDrel that, ParquetScanFilter filter) {
    super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(), that.getProjectedColumns(), that.getObservedRowcountAdjustment());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = filter;
    this.arrowCachingEnabled = that.isArrowCachingEnabled();
  }

  // Clone with new dataset pointer
  private FilesystemScanDrel(FilesystemScanDrel that, TableMetadata newDatasetPointer) {
    super(that.getCluster(), that.getTraitSet(), new RelOptNamespaceTable(newDatasetPointer, that.getCluster()), that.getPluginId(), newDatasetPointer, that.getProjectedColumns(), that.getObservedRowcountAdjustment());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = that.getFilter();
    this.arrowCachingEnabled = that.isArrowCachingEnabled();
  }

  // Clone with new arrowCachingEnabled
  private FilesystemScanDrel(FilesystemScanDrel that, boolean arrowCachingEnabled) {
    super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(), that.getProjectedColumns(), that.getObservedRowcountAdjustment());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = that.getFilter();
    this.arrowCachingEnabled = arrowCachingEnabled;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FilesystemScanDrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, getProjectedColumns(), filter, observedRowcountAdjustment, false);
  }

  @Override
  public FilesystemScanDrel applyFilter(ScanFilter scanFilter) {
    Preconditions.checkArgument(scanFilter instanceof ParquetScanFilter);
    return new FilesystemScanDrel(this, (ParquetScanFilter) scanFilter);
  }

  @Override
  public FilesystemScanDrel applyDatasetPointer(TableMetadata newDatasetPointer) {
    return new FilesystemScanDrel(this, newDatasetPointer);
  }

  public FilesystemScanDrel applyArrowCachingEnabled(boolean arrowCachingEnabled) {
    return new FilesystemScanDrel(this, arrowCachingEnabled);
  }

  public ParquetScanFilter getFilter() {
    return filter;
  }

  @Override
  public double getCostAdjustmentFactor(){
    return filter != null ? filter.getCostAdjustment() : super.getCostAdjustmentFactor();
  }

  @Override
  protected double getFilterReduction(){
    if(filter != null){
      return 0.15d;
    }else {
      return 1d;
    }
  }

  public FilesystemScanDrel removeRowCountAdjustment() {
    return new FilesystemScanDrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata, getProjectedColumns(), filter, 1.0, arrowCachingEnabled);
  }

  @Override
  public FilesystemScanDrel cloneWithProject(List<SchemaPath> projection, boolean preserveFilterColumns) {
    if (filter != null && preserveFilterColumns) {
      final List<SchemaPath> newProjection = new ArrayList<>(projection);
      final Set<SchemaPath> projectionSet = new HashSet<>(projection);
      if (filter.getConditions() != null) {
        for (ParquetFilterCondition f : filter.getConditions()) {
          final SchemaPath col = f.getPath();
          if (!projectionSet.contains(col)) {
            newProjection.add(col);
          }
        }
        return cloneWithProject(newProjection);
      }
    }
    return cloneWithProject(projection);
  }

  @Override
  public FilesystemScanDrel cloneWithProject(List<SchemaPath> projection) {
    return new FilesystemScanDrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata, projection, filter, observedRowcountAdjustment, arrowCachingEnabled);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    pw = super.explainTerms(pw);
    if(filter != null){
      return pw.item("filters", filter);
    }
    if (arrowCachingEnabled) {
      pw.item("arrowCachingEnabled", true);
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

  public boolean isArrowCachingEnabled() {
    return arrowCachingEnabled;
  }
}

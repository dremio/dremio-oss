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

import static com.dremio.exec.store.dfs.FileSystemRulesFactory.IcebergMetadataFilesystemScanPrule.getInternalIcebergTableMetadata;
import static com.dremio.exec.store.dfs.FileSystemRulesFactory.IcebergMetadataFilesystemScanPrule.supportsConvertedIcebergDataset;
import static com.dremio.service.namespace.DatasetHelper.isConvertedIcebergDataset;
import static com.dremio.service.namespace.DatasetHelper.supportsPruneFilter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.store.RelOptNamespaceTable;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetScanFilter;
import com.dremio.exec.store.parquet.ParquetScanRowGroupFilter;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.google.common.base.Preconditions;

/**
 * ScanDrel for filesystem stores filter conditions.
 */
public class FilesystemScanDrel extends ScanRelBase implements Rel, FilterableScan, PruneableScan {
  private final ParquetScanFilter filter;
  private final ParquetScanRowGroupFilter rowGroupFilter;
  private final PruneFilterCondition partitionFilter;
  private final boolean arrowCachingEnabled;
  private final Long survivingRowCount;
  private final Long survivingFileCount;
  private final boolean partitionValuesEnabled;

  public FilesystemScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                            TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment,
                            List<RelHint> hints, boolean arrowCachingEnabled, SnapshotDiffContext snapshotDiffContext) {
    this(cluster, traitSet, table, pluginId, dataset, projectedColumns, null, null, observedRowcountAdjustment, hints,
         arrowCachingEnabled, null, null, null, snapshotDiffContext, false);
  }

  private FilesystemScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                             TableMetadata dataset, List<SchemaPath> projectedColumns, ParquetScanFilter filter,
                             ParquetScanRowGroupFilter rowGroupFilter, double observedRowcountAdjustment, List<RelHint> hints, boolean arrowCachingEnabled,
                             PruneFilterCondition partitionFilter, Long survivingRowCount, Long survivingFileCount,
                             SnapshotDiffContext snapshotDiffContext, boolean partitionValuesEnabled) {
    super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment, hints, snapshotDiffContext);
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = filter;
    this.rowGroupFilter = rowGroupFilter;
    this.arrowCachingEnabled = arrowCachingEnabled;
    this.partitionFilter = partitionFilter;
    this.survivingRowCount = survivingRowCount;
    this.survivingFileCount = survivingFileCount;
    this.partitionValuesEnabled = partitionValuesEnabled;
  }

  // Clone with new conditions
  private FilesystemScanDrel(FilesystemScanDrel that, ParquetScanFilter filter) {
    super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(),
          that.getProjectedColumns(), that.getObservedRowcountAdjustment(), that.getHints(), that.getSnapshotDiffContext());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = filter;
    this.rowGroupFilter = that.getRowGroupFilter();
    this.arrowCachingEnabled = that.isArrowCachingEnabled();
    this.partitionFilter = that.getPartitionFilter();
    this.survivingRowCount = that.getSurvivingRowCount();
    this.survivingFileCount = that.getSurvivingFileCount();
    this.partitionValuesEnabled = that.isPartitionValuesEnabled();
  }

  // Clone with partition filter
  private FilesystemScanDrel(FilesystemScanDrel that, PruneFilterCondition partitionFilter, Long survivingRowCount, Long survivingFileCount) {
    super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(),
          that.getProjectedColumns(), that.getObservedRowcountAdjustment(), that.getHints(), that.getSnapshotDiffContext());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = that.getFilter();
    this.rowGroupFilter = that.getRowGroupFilter();
    this.arrowCachingEnabled = that.isArrowCachingEnabled();
    this.partitionFilter = partitionFilter;
    this.survivingRowCount = survivingRowCount;
    this.survivingFileCount = survivingFileCount;
    this.partitionValuesEnabled = that.isPartitionValuesEnabled();
  }

  // Clone with new dataset pointer
  private FilesystemScanDrel(FilesystemScanDrel that, TableMetadata newDatasetPointer) {
    super(that.getCluster(), that.getTraitSet(), new RelOptNamespaceTable(newDatasetPointer, that.getCluster()),
          that.getPluginId(), newDatasetPointer, that.getProjectedColumns(), that.getObservedRowcountAdjustment(),
          that.getHints(), that.getSnapshotDiffContext());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = that.getFilter();
    this.rowGroupFilter = that.getRowGroupFilter();
    this.arrowCachingEnabled = that.isArrowCachingEnabled();
    this.partitionFilter = that.getPartitionFilter();
    this.survivingRowCount = that.getSurvivingRowCount();
    this.survivingFileCount = that.getSurvivingFileCount();
    this.partitionValuesEnabled = that.isPartitionValuesEnabled();
  }

  // Clone with new arrowCachingEnabled
  private FilesystemScanDrel(FilesystemScanDrel that, boolean arrowCachingEnabled) {
    super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(),
          that.getProjectedColumns(), that.getObservedRowcountAdjustment(), that.getHints(), that.getSnapshotDiffContext());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = that.getFilter();
    this.rowGroupFilter = that.getRowGroupFilter();
    this.arrowCachingEnabled = arrowCachingEnabled;
    this.partitionFilter = that.getPartitionFilter();
    this.survivingRowCount = that.getSurvivingRowCount();
    this.survivingFileCount = that.getSurvivingFileCount();
    this.partitionValuesEnabled = that.isPartitionValuesEnabled();
  }

  // Clone with new rowGroupFilter
  private FilesystemScanDrel(FilesystemScanDrel that, ParquetScanRowGroupFilter rowGroupFilter) {
    super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(),
      that.getProjectedColumns(), that.getObservedRowcountAdjustment(), that.getHints(), that.getSnapshotDiffContext());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = that.getFilter();
    this.rowGroupFilter = rowGroupFilter;
    this.arrowCachingEnabled = that.isArrowCachingEnabled();
    this.partitionFilter = that.getPartitionFilter();
    this.survivingRowCount = that.getSurvivingRowCount();
    this.survivingFileCount = that.getSurvivingFileCount();
    this.partitionValuesEnabled = that.isPartitionValuesEnabled();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FilesystemScanDrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, getProjectedColumns(),
      filter, rowGroupFilter, observedRowcountAdjustment, getHints(), arrowCachingEnabled, partitionFilter,
      survivingRowCount, survivingFileCount, snapshotDiffContext, partitionValuesEnabled);
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

  @Override
  public FilesystemScanDrel applyPartitionFilter(PruneFilterCondition partitionFilter, Long survivingRowCount, Long survivingFileCount) {
    Preconditions.checkArgument(supportsPruneFilter(getTableMetadata().getDatasetConfig()));
    return new FilesystemScanDrel(this, partitionFilter, survivingRowCount, survivingFileCount);
  }

  @Override
  public FilterableScan applyRowGroupFilter(ParquetScanRowGroupFilter rowGroupFilter) {
    return new FilesystemScanDrel(this, rowGroupFilter);
  }

  @Override
  public Long getSurvivingRowCount() {
    return survivingRowCount;
  }

  @Override
  public Long getSurvivingFileCount() {
    return survivingFileCount;
  }

  public FilesystemScanDrel applyArrowCachingEnabled(boolean arrowCachingEnabled) {
    return new FilesystemScanDrel(this, arrowCachingEnabled);
  }

  @Override
  public ParquetScanFilter getFilter() {
    return filter;
  }

  @Override
  public PruneFilterCondition getPartitionFilter() {
    return partitionFilter;
  }

  @Override
  public ParquetScanRowGroupFilter getRowGroupFilter() {
    return rowGroupFilter;
  }

  @Override
  public double getCostAdjustmentFactor(){
    return filter != null ? filter.getCostAdjustment() : super.getCostAdjustmentFactor();
  }

  @Override
  public double getFilterReduction(){
    if (filter != null) {
      return 0.15d;
    } else {
      return 1d;
    }
  }

  public FilesystemScanDrel removeRowCountAdjustment() {
    return new FilesystemScanDrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata,
      getProjectedColumns(), filter, rowGroupFilter, 1.0, getHints(), arrowCachingEnabled,
      partitionFilter, survivingRowCount, survivingFileCount, snapshotDiffContext, partitionValuesEnabled);
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
    return new FilesystemScanDrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata, projection,
      filter == null ? filter : filter.applyProjection(projection, rowType, getCluster(), getBatchSchema()),
      rowGroupFilter == null ? null : rowGroupFilter.applyProjection(projection, rowType, getCluster(), getBatchSchema()),
      observedRowcountAdjustment, getHints(), arrowCachingEnabled,
      partitionFilter == null ? partitionFilter : partitionFilter.applyProjection(projection, rowType,
        getCluster(), getBatchSchema()),
      getSurvivingRowCount(), getSurvivingFileCount(), snapshotDiffContext, partitionValuesEnabled);
  }

  @Override
  public StoragePluginId getIcebergStatisticsPluginId(OptimizerRulesContext context) {
    if (FileSystemRulesFactory.isIcebergDataset(getTableMetadata())) {
      return getPluginId();
    } else if (supportsConvertedIcebergDataset(context, getTableMetadata())) { // internal
      return getInternalIcebergTableMetadata(getTableMetadata(), context).getIcebergTableStoragePlugin();
    }
    return null;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    pw = super.explainTerms(pw);
    return pw
      .itemIf("filters", filter, filter != null)
      .itemIf("arrowCachingEnabled", arrowCachingEnabled, arrowCachingEnabled)
      .itemIf("partitionFilters", partitionFilter, partitionFilter != null)
      .itemIf("rowGroupFilter", rowGroupFilter, rowGroupFilter != null)
      .itemIf("partitionValuesEnabled", partitionValuesEnabled, partitionValuesEnabled);
  }

  public boolean isArrowCachingEnabled() {
    return arrowCachingEnabled;
  }

  @Override
  public boolean canUsePartitionStats(){
    return isConvertedIcebergDataset(getTableMetadata().getDatasetConfig()) || pluginId.getCapabilities().getCapability(SourceCapabilities.getCanUsePartitionStats());
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
    return partitionValuesEnabled
      ? planner.getCostFactory().makeTinyCost()
      : super.computeSelfCost(planner, mq);
  }

  public FilesystemScanDrel applyPartitionValuesEnabled(final boolean partitionValuesEnabled) {
    return new FilesystemScanDrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata,
      getProjectedColumns(), filter, rowGroupFilter, observedRowcountAdjustment, getHints(), arrowCachingEnabled,
      partitionFilter, survivingRowCount, survivingFileCount, snapshotDiffContext, partitionValuesEnabled);
  }

  public boolean isPartitionValuesEnabled() {
    return partitionValuesEnabled;
  }
}

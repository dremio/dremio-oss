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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.store.RelOptNamespaceTable;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetScanFilter;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.google.common.base.Preconditions;

/**
 * ScanDrel for filesystem stores filter conditions.
 */
public class FilesystemScanDrel extends ScanRelBase implements Rel, FilterableScan, PruneableScan {
  private final ParquetScanFilter filter;
  private final PruneFilterCondition partitionFilter;
  private final boolean arrowCachingEnabled;
  private final Long survivingRowCount;
  private final Long survivingFileCount;

  public FilesystemScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                            TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment, boolean arrowCachingEnabled) {
    this(cluster, traitSet, table, pluginId, dataset, projectedColumns, null, observedRowcountAdjustment, arrowCachingEnabled, null, null, null);
  }

  private FilesystemScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                             TableMetadata dataset, List<SchemaPath> projectedColumns, ParquetScanFilter filter, double observedRowcountAdjustment, boolean arrowCachingEnabled, PruneFilterCondition partitionFilter, Long survivingRowCount,
                             Long survivingFileCount) {
    super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = filter;
    this.arrowCachingEnabled = arrowCachingEnabled;
    this.partitionFilter = partitionFilter;
    this.survivingRowCount = survivingRowCount;
    this.survivingFileCount = survivingFileCount;
  }

  // Clone with new conditions
  private FilesystemScanDrel(FilesystemScanDrel that, ParquetScanFilter filter) {
    super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(), that.getProjectedColumns(), that.getObservedRowcountAdjustment());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = filter;
    this.arrowCachingEnabled = that.isArrowCachingEnabled();
    this.partitionFilter = that.getPartitionFilter();
    this.survivingRowCount = that.getSurvivingRowCount();
    this.survivingFileCount = that.getSurvivingFileCount();
  }

  // Clone with partition filter
  private FilesystemScanDrel(FilesystemScanDrel that, PruneFilterCondition partitionFilter, Long survivingRowCount, Long survivingFileCount) {
    super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(), that.getProjectedColumns(), that.getObservedRowcountAdjustment());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = that.getFilter();
    this.arrowCachingEnabled = that.isArrowCachingEnabled();
    this.partitionFilter = partitionFilter;
    this.survivingRowCount = survivingRowCount;
    this.survivingFileCount = survivingFileCount;
  }

  // Clone with new dataset pointer
  private FilesystemScanDrel(FilesystemScanDrel that, TableMetadata newDatasetPointer) {
    super(that.getCluster(), that.getTraitSet(), new RelOptNamespaceTable(newDatasetPointer, that.getCluster()), that.getPluginId(), newDatasetPointer, that.getProjectedColumns(), that.getObservedRowcountAdjustment());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = that.getFilter();
    this.arrowCachingEnabled = that.isArrowCachingEnabled();
    this.partitionFilter = that.getPartitionFilter();
    this.survivingRowCount = that.getSurvivingRowCount();
    this.survivingFileCount = that.getSurvivingFileCount();
  }

  // Clone with new arrowCachingEnabled
  private FilesystemScanDrel(FilesystemScanDrel that, boolean arrowCachingEnabled) {
    super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(), that.getProjectedColumns(), that.getObservedRowcountAdjustment());
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
    this.filter = that.getFilter();
    this.arrowCachingEnabled = arrowCachingEnabled;
    this.partitionFilter = that.getPartitionFilter();
    this.survivingRowCount = that.getSurvivingRowCount();
    this.survivingFileCount = that.getSurvivingFileCount();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FilesystemScanDrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, getProjectedColumns(), filter, observedRowcountAdjustment, arrowCachingEnabled, partitionFilter, survivingRowCount, survivingFileCount);
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

  public ParquetScanFilter getFilter() {
    return filter;
  }

  @Override
  public PruneFilterCondition getPartitionFilter() {
    return partitionFilter;
  }

  @Override
  public double getCostAdjustmentFactor(){
    return filter != null ? filter.getCostAdjustment() : super.getCostAdjustmentFactor();
  }

  @Override
  public double getFilterReduction(){
    if(filter != null){
      return 0.15d;
    }else {
      return 1d;
    }
  }

  public FilesystemScanDrel removeRowCountAdjustment() {
    return new FilesystemScanDrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata, getProjectedColumns(), filter, 1.0, arrowCachingEnabled, partitionFilter, survivingRowCount, survivingFileCount);
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
    return new FilesystemScanDrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata, projection, filter == null ? filter : filter.applyProjection(projection, rowType, getCluster(), getBatchSchema()),
      observedRowcountAdjustment, arrowCachingEnabled, partitionFilter == null ? partitionFilter : partitionFilter.applyProjection(projection, rowType, getCluster(), getBatchSchema()), getSurvivingRowCount(), getSurvivingFileCount());
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
      .itemIf("partitionFilters", partitionFilter, partitionFilter != null);
  }

  public boolean isArrowCachingEnabled() {
    return arrowCachingEnabled;
  }

  @Override
  public boolean canUsePartitionStats(){
    return isConvertedIcebergDataset(getTableMetadata().getDatasetConfig()) || pluginId.getCapabilities().getCapability(SourceCapabilities.getCanUsePartitionStats());
  }
}

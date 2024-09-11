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

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.parquet.ParquetScanRowGroupFilter;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;

/** Methods required for filter pushdown */
public abstract class FilterableScan extends ScanRelBase {

  private PartitionStatsStatus partitionStats;

  public FilterableScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      StoragePluginId pluginId,
      TableMetadata tableMetadata,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      List<RelHint> hints,
      SnapshotDiffContext snapshotDiffContext,
      PartitionStatsStatus partitionStats) {
    super(
        cluster,
        traitSet,
        table,
        pluginId,
        tableMetadata,
        projectedColumns,
        observedRowcountAdjustment,
        hints,
        snapshotDiffContext);
    this.partitionStats = partitionStats;
  }

  public enum PartitionStatsStatus {
    NONE, // Partition Stats were unavailable and not used
    USED, // Partition Stats were available and used
    SKIPPED, // Partition Stats were available but were not used because of long planning time
    ERROR, // Error occurred
  }

  public void setPartitionStatsStatus(PartitionStatsStatus partitionStats) {
    this.partitionStats = partitionStats;
  }

  public PartitionStatsStatus getPartitionStatsStatus() {
    return partitionStats;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("partitionStats", partitionStats, partitionStats != PartitionStatsStatus.NONE)
        .itemIf("survivingRowCount", getSurvivingRowCount(), getSurvivingRowCount() != null)
        .itemIf("survivingFileCount", getSurvivingFileCount(), getSurvivingFileCount() != null);
  }

  public abstract ScanFilter getFilter();

  public abstract PruneFilterCondition getPartitionFilter();

  public abstract ParquetScanRowGroupFilter getRowGroupFilter();

  public abstract FilterableScan applyRowGroupFilter(ParquetScanRowGroupFilter rowGroupFilter);

  public abstract FilterableScan applyFilter(ScanFilter scanFilter);

  public abstract FilterableScan applyPartitionFilter(
      PruneFilterCondition partitionFilter, Long survivingRowCount, Long survivingFileCount);

  public abstract FilterableScan cloneWithProject(
      List<SchemaPath> projection, boolean preserveFilterColumns);

  public abstract Long getSurvivingRowCount();

  public abstract Long getSurvivingFileCount();

  public abstract boolean canUsePartitionStats();
}

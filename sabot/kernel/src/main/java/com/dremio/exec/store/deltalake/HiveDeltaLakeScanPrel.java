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
package com.dremio.exec.store.deltalake;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.parquet.ParquetScanFilter;
import com.dremio.exec.store.parquet.ParquetScanRowGroupFilter;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

/** Hive version of DeltaLake dataset prel */
public class HiveDeltaLakeScanPrel extends DeltaLakeScanPrel {
  public HiveDeltaLakeScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      StoragePluginId pluginId,
      TableMetadata tableMetadata,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      List<RelHint> hints,
      ParquetScanFilter filter,
      ParquetScanRowGroupFilter rowGroupFilter,
      boolean arrowCachingEnabled,
      PruneFilterCondition pruneCondition) {
    super(
        cluster,
        traitSet,
        table,
        pluginId,
        tableMetadata,
        projectedColumns,
        observedRowcountAdjustment,
        hints,
        filter,
        rowGroupFilter,
        arrowCachingEnabled,
        pruneCondition);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HiveDeltaLakeScanPrel(
        getCluster(),
        getTraitSet(),
        getTable(),
        getPluginId(),
        getTableMetadata(),
        getProjectedColumns(),
        getObservedRowcountAdjustment(),
        hints,
        getFilter(),
        getRowGroupFilter(),
        isArrowCachingEnabled(),
        getPruneCondition());
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
    return new HiveDeltaLakeScanPrel(
        getCluster(),
        getTraitSet(),
        getTable(),
        getPluginId(),
        getTableMetadata(),
        projection,
        getObservedRowcountAdjustment(),
        hints,
        getFilter() == null
            ? null
            : getFilter().applyProjection(projection, rowType, getCluster(), getBatchSchema()),
        getRowGroupFilter() == null
            ? null
            : getRowGroupFilter()
                .applyProjection(projection, rowType, getCluster(), getBatchSchema()),
        isArrowCachingEnabled(),
        getPruneCondition() == null
            ? null
            : getPruneCondition()
                .applyProjection(projection, rowType, getCluster(), getBatchSchema()));
  }

  @Override
  protected RelNode createDeltaLakeCommitLogScan(boolean scanForAddedPaths) {
    return new HiveDeltaLakeCommitLogScanPrel(
        getCluster(),
        getTraitSet().plus(DistributionTrait.ANY), /*
                                                  * The broadcast condition depends on the probe side being non-SINGLETON. Since
                                                  * we will only be broadcasting commit log files, it should primarily depend on
                                                  * the number of added and removed files. Making the distribution trait of type
                                                  * ANY here so that the broadcast check logic falls back to row count estimate.
                                                  */
        getTableMetadata(),
        isArrowCachingEnabled(),
        scanForAddedPaths);
  }
}

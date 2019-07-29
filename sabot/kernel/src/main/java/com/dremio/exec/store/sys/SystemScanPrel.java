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
package com.dremio.exec.store.sys;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Physical scan operator.
 */
public class SystemScanPrel extends ScanPrelBase {

  private final SystemTable systemTable;
  private final int executorCount;
  private final StoragePluginId pluginId;

  public SystemScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      TableMetadata dataset,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment
      ) {
    super(cluster, traitSet, table, dataset.getStoragePluginId(), dataset, projectedColumns, observedRowcountAdjustment);
    this.systemTable = Preconditions.checkNotNull(SystemStoragePlugin.TABLE_MAP.get(dataset.getName().getName().toLowerCase()), "Unexpected system table.");
    this.executorCount = PrelUtil.getPlannerSettings(cluster).getExecutorCount();
    this.pluginId = dataset.getStoragePluginId();
  }

  @VisibleForTesting
  public SystemScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      TableMetadata dataset,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      RelDataType rowType
      ) {
    super(cluster, traitSet, table, dataset.getStoragePluginId(), dataset, projectedColumns, observedRowcountAdjustment);
    this.systemTable = Preconditions.checkNotNull(SystemStoragePlugin.TABLE_MAP.get(dataset.getName().getName().toLowerCase()), "Unexpected system table.");
    this.executorCount = PrelUtil.getPlannerSettings(cluster).getExecutorCount();
    this.rowType = rowType;
    this.pluginId = dataset.getStoragePluginId();
  }

  @Override
  public int getMaxParallelizationWidth() {
    return systemTable.isDistributed() ? executorCount : 1;
  }

  @Override
  public int getMinParallelizationWidth() {
    return systemTable.isDistributed() ? executorCount : 1;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return systemTable.isDistributed() ? DistributionAffinity.HARD : DistributionAffinity.SOFT;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new SystemGroupScan(
        creator.props(this, getTableMetadata().getUser(), getTableMetadata().getSchema().maskAndReorder(getProjectedColumns()), null, null),
        systemTable,
        getProjectedColumns(),
        pluginId,
        executorCount);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    Preconditions.checkArgument(inputs == null || inputs.size() == 0);
    return new SystemScanPrel(getCluster(), traitSet, getTable(), getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor());
  }

  @Override
  public SystemScanPrel cloneWithProject(List<SchemaPath> projection) {
    return new SystemScanPrel(getCluster(), getTraitSet(), getTable(), getTableMetadata(), projection, getCostAdjustmentFactor());
  }

}

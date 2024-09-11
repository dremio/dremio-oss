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
package com.dremio.plugins.sysflight;

import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.proto.SearchProtos.SearchQuery;
import com.dremio.exec.store.TableMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

/** Physical scan operator. */
public class SysFlightScanPrel extends ScanPrelBase {

  private final SearchQuery query;
  private final StoragePluginId pluginId;
  private final int executorCount;
  private final boolean isDistributed;
  private final SysFlightStoragePlugin plugin;

  public SysFlightScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      TableMetadata dataset,
      SearchQuery query,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      List<RelHint> hints,
      SysFlightStoragePlugin plugin,
      List<Info> runtimeFilters) {
    super(
        cluster,
        traitSet,
        table,
        dataset.getStoragePluginId(),
        dataset,
        projectedColumns,
        observedRowcountAdjustment,
        hints,
        runtimeFilters);

    this.pluginId = dataset.getStoragePluginId();
    this.executorCount = PrelUtil.getPlannerSettings(cluster).getExecutorCount();
    this.isDistributed =
        plugin.isDistributed(new EntityPath(dataset.getName().getPathComponents()));
    this.query = query;
    this.plugin = plugin;
  }

  @VisibleForTesting
  public SysFlightScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      TableMetadata dataset,
      SearchQuery query,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      List<RelHint> hints,
      RelDataType rowType,
      SysFlightStoragePlugin plugin,
      List<Info> runtimeFilters) {
    super(
        cluster,
        traitSet,
        table,
        dataset.getStoragePluginId(),
        dataset,
        projectedColumns,
        observedRowcountAdjustment,
        hints,
        runtimeFilters);
    this.rowType = rowType;
    this.pluginId = dataset.getStoragePluginId();
    this.executorCount = PrelUtil.getPlannerSettings(cluster).getExecutorCount();
    this.isDistributed =
        plugin.isDistributed(new EntityPath(dataset.getName().getPathComponents()));
    this.query = query;
    this.plugin = plugin;
  }

  public SysFlightStoragePlugin getPlugin() {
    return plugin;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return isDistributed ? executorCount : 1;
  }

  @Override
  public int getMinParallelizationWidth() {
    return isDistributed ? executorCount : 1;
  }

  @Override
  public boolean hasFilter() {
    return query != null;
  }

  @Override
  public double getFilterReduction() {
    return query == null ? super.getFilterReduction() : 0.15d;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    String str = query == null ? null : query.toString().replace('\n', ' ');
    return super.explainTerms(pw).itemIf("query", str, query != null);
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return isDistributed ? DistributionAffinity.HARD : DistributionAffinity.SOFT;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new SysFlightGroupScan(
        creator.props(
            this,
            getTableMetadata().getUser(),
            getTableMetadata().getSchema().maskAndReorder(getProjectedColumns()),
            null,
            null),
        getTableMetadata().getName().getPathComponents(),
        getTableMetadata().getSchema(),
        getProjectedColumns(),
        query,
        pluginId,
        isDistributed,
        executorCount,
        null);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    Preconditions.checkArgument(inputs == null || inputs.size() == 0);
    return new SysFlightScanPrel(
        getCluster(),
        traitSet,
        getTable(),
        getTableMetadata(),
        query,
        getProjectedColumns(),
        getCostAdjustmentFactor(),
        getHints(),
        plugin,
        getRuntimeFilters());
  }

  @Override
  public SysFlightScanPrel cloneWithProject(List<SchemaPath> projection) {
    return new SysFlightScanPrel(
        getCluster(),
        getTraitSet(),
        getTable(),
        getTableMetadata(),
        query,
        projection,
        getCostAdjustmentFactor(),
        getHints(),
        plugin,
        getRuntimeFilters());
  }
}

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
package com.dremio.exec.store.ischema;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.catalog.SearchQuery;
import com.google.common.base.Preconditions;

/**
 * Physical scan operator.
 */
public class InfoSchemaScanPrel extends ScanPrelBase {

  private final InformationSchemaTable table;
  private final SearchQuery query;
  private final StoragePluginId pluginId;

  public InfoSchemaScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      TableMetadata dataset,
      SearchQuery query,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      List<Info> runtimeFilters
      ) {

    super(cluster, traitSet, table, dataset.getStoragePluginId(), dataset, projectedColumns, observedRowcountAdjustment, runtimeFilters);
    this.pluginId = dataset.getStoragePluginId();
    this.table = Preconditions.checkNotNull(
      InfoSchemaStoragePlugin.TABLE_MAP.get(dataset.getName().getName().toLowerCase()), "Unexpected system table.");
    this.query = query;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public int getMinParallelizationWidth() {
    return 1;
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
    return DistributionAffinity.SOFT;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new InfoSchemaGroupScan(
        creator.props(this, getTableMetadata().getUser(), getTableMetadata().getSchema().maskAndReorder(getProjectedColumns())),
        table,
        getProjectedColumns(),
        query,
        pluginId);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    Preconditions.checkArgument(inputs == null || inputs.size() == 0);
    return new InfoSchemaScanPrel(getCluster(), traitSet, getTable(), getTableMetadata(), query, getProjectedColumns(), getCostAdjustmentFactor(), getRuntimeFilters());
  }

  @Override
  public InfoSchemaScanPrel cloneWithProject(List<SchemaPath> projection) {
    return new InfoSchemaScanPrel(getCluster(), getTraitSet(), getTable(), getTableMetadata(), query, projection, getCostAdjustmentFactor(), getRuntimeFilters());
  }

}

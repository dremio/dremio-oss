/*
 * Copyright (C) 2017 Dremio Corporation
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
import org.apache.curator.shaded.com.google.common.base.Preconditions;

import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.ischema.tables.InfoSchemaTable;
import com.google.common.base.Objects;

/**
 * Physical scan operator.
 */
public class InfoSchemaScanPrel extends ScanPrelBase {

  private final InfoSchemaTable table;
  private final SearchQuery query;

  public InfoSchemaScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      TableMetadata dataset,
      SearchQuery query,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment
      ) {

    super(cluster, traitSet, table, dataset.getStoragePluginId(), dataset, projectedColumns, observedRowcountAdjustment);
    this.table = Preconditions.checkNotNull(InfoSchemaStoragePlugin.TABLE_MAP.get(dataset.getName().getName().toLowerCase()), "Unexpected system table.");
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

  public boolean hasFilter() {
    return query != null;
  }

  public InfoSchemaTable getInfoSchemaTable() {
    return table;
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
  public boolean equals(Object other) {
    if(!other.getClass().equals(this.getClass())){
      return false;
    }

    InfoSchemaScanPrel castOther = (InfoSchemaScanPrel) other;

    return Objects.equal(query, castOther.query) && super.equals(other);
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.SOFT;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return creator.addMetadata(this, new InfoSchemaGroupScan(table, query, getProjectedColumns()));
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    Preconditions.checkArgument(inputs == null || inputs.size() == 0);
    return new InfoSchemaScanPrel(getCluster(), traitSet, getTable(), getTableMetadata(), query, getProjectedColumns(), getCostAdjustmentFactor());
  }

  @Override
  public InfoSchemaScanPrel cloneWithProject(List<SchemaPath> projection) {
    return new InfoSchemaScanPrel(getCluster(), getTraitSet(), getTable(), getTableMetadata(), query, projection, getCostAdjustmentFactor());
  }

}

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
package com.dremio.exec.store.ischema;


import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.LogicalPlanImplementor;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;

public class InfoSchemaScanDrel extends ScanRelBase implements Rel {

  public InfoSchemaScanDrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      StoragePluginId pluginId,
      TableMetadata tableMetadata,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment) {
    super(cluster, traitSet, table, pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment);
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new InfoSchemaScanDrel(getCluster(), traitSet, table, pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
    return new InfoSchemaScanDrel(getCluster(), traitSet, table, pluginId, tableMetadata, projection, observedRowcountAdjustment);
  }

}


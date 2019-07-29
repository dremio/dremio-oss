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
package com.dremio.exec.store.dfs.easy;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

/**
 * Convert scan prel to easy group scan.
 */
@Options
public class EasyScanPrel extends ScanPrelBase {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.scan.easy.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.scan.easy.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  public EasyScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId, TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment) {
    super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    final BatchSchema schema = tableMetadata.getSchema().maskAndReorder(projectedColumns);
    return new EasyGroupScan(
        creator.props(this, tableMetadata.getUser(), schema, RESERVE, LIMIT),
        tableMetadata,
        projectedColumns);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EasyScanPrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
    return new EasyScanPrel(getCluster(), getTraitSet(), table, pluginId, tableMetadata, projection, observedRowcountAdjustment);
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof EasyScanPrel)) {
      return false;
    }
    return super.equals(other);
  }


}

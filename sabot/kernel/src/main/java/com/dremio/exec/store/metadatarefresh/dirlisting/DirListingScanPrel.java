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
package com.dremio.exec.store.metadatarefresh.dirlisting;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
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
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

@Options
public class DirListingScanPrel extends ScanPrelBase {
  public static final TypeValidators.LongValidator RESERVE = new TypeValidators.PositiveLongValidator("planner.op.scan.dir_listing.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final TypeValidators.LongValidator LIMIT = new TypeValidators.PositiveLongValidator("planner.op.scan.dir_listing.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  private static final List<SchemaPath> PROJECTED_COLS = MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.BATCH_SCHEMA.getFields()
    .stream()
    .map(Field::getName)
    .map(SchemaPath::getSimplePath)
    .collect(Collectors.toList());

  public DirListingScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                            TableMetadata tableMetadata, double observedRowcountAdjustment) {
    super(cluster, traitSet, table, pluginId, tableMetadata, PROJECTED_COLS, observedRowcountAdjustment);
  }

  @Override
  public DirListingScanPrel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DirListingScanPrel(getCluster(), traitSet, table, pluginId, tableMetadata, observedRowcountAdjustment);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
    return new DirListingScanPrel(getCluster(), traitSet, table, pluginId, tableMetadata, observedRowcountAdjustment);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new DirListingGroupScan(
      creator.props(this, tableMetadata.getUser(), tableMetadata.getSchema(), RESERVE, LIMIT),
      tableMetadata, tableMetadata.getSchema(), projectedColumns, pluginId);
  }

  public double getObservedRowcountAdjustment() {
    return 1.0;
  }
}

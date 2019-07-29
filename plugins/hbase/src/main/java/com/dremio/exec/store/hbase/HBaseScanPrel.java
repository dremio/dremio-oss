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
package com.dremio.exec.store.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
/**
 * Physical scan operator.
 */
@Options
public class HBaseScanPrel extends ScanPrelBase {

  private final byte[] filter;
  private final byte[] startRow;
  private final byte[] stopRow;

  public static final TypeValidators.LongValidator RESERVE = new TypeValidators.PositiveLongValidator("planner.op.scan.hbase.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final TypeValidators.LongValidator LIMIT = new TypeValidators.PositiveLongValidator("planner.op.scan.hbase.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);


  public HBaseScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      TableMetadata dataset,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      byte[] startRow,
      byte[] stopRow,
      byte[] filter) {
    super(cluster, traitSet, table, dataset.getStoragePluginId(), dataset, projectedColumns, observedRowcountAdjustment);
    this.filter = filter;
    this.startRow = startRow;
    this.stopRow = stopRow;
  }

  public byte[] getFilter() {
    return filter;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public byte[] getStopRow() {
    return stopRow;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("startRow", HBaseUtils.keyToStr(startRow), startRow != null && startRow.length > 0)
        .itemIf("stopRow", HBaseUtils.keyToStr(stopRow), stopRow != null && stopRow.length > 0)
        .itemIf("filter", HBaseUtils.filterToStr(filter), filter != null);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  @Override
  public boolean hasFilter() {
    try {
      return filter != null || getTableMetadata().getSplitRatio() < 1.0d;
    } catch(Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    final HBaseScanSpec spec = new HBaseScanSpec(getTableMetadata().getName(), startRow, stopRow, filter);
    final BatchSchema schema = tableMetadata.getSchema().maskAndReorder(getProjectedColumns());
    return new HBaseGroupScan(
      creator.props(this, tableMetadata.getUser(), schema, RESERVE, LIMIT),
      spec, getTableMetadata(), getProjectedColumns(), getTableMetadata().getApproximateRecordCount());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    Preconditions.checkArgument(inputs == null || inputs.size() == 0);
    return new HBaseScanPrel(getCluster(), traitSet, getTable(), getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(), getStartRow(), getStopRow(), getFilter());
  }

  @Override
  public HBaseScanPrel cloneWithProject(List<SchemaPath> projection) {
    return new HBaseScanPrel(getCluster(), getTraitSet(), getTable(), getTableMetadata(), projection, getCostAdjustmentFactor(), getStartRow(), getStopRow(), getFilter());
  }

}

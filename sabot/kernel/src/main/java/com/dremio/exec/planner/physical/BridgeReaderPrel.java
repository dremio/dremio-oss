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
package com.dremio.exec.planner.physical;

import static com.dremio.exec.planner.cost.DremioCost.BYTE_DISK_READ_COST;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.BridgeFileReader;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;

public class BridgeReaderPrel extends AbstractRelNode implements Prel {
  private final BatchSchema schema;
  private final String bridgeSetId;
  private final double estimatedRowCount;

  public BridgeReaderPrel(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType, double estimatedRowCount,
                          BatchSchema schema, String bridgeSetId) {
    super(cluster, traitSet);
    this.rowType = rowType;
    this.estimatedRowCount = estimatedRowCount;
    this.schema = schema;
    this.bridgeSetId = bridgeSetId;
  }

  public BatchSchema getBatchSchema() {
    return schema;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BridgeReaderPrel(getCluster(), traitSet, rowType, estimatedRowCount, schema, bridgeSetId);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return pw
      .item("bridgeSetId", bridgeSetId)
      .item("schema", schema.toString());
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    final double rowCount = mq.getRowCount(this);
    final int fieldCount = getRowType().getFieldCount();
    final DremioCost.Factory costFactory = (DremioCost.Factory)planner.getCostFactory();

    double adjustmentFactor = PrelUtil.getPlannerSettings(getCluster()).getCseCostAdjustmentFactor();
    if (adjustmentFactor == Double.MAX_VALUE) {
      return costFactory.makeHugeCost();
    }
    double workCost = (rowCount * fieldCount * ScanCostFactor.ARROW.getFactor());

    return costFactory.makeCost(estimatedRowCount, workCost * DremioCost.SCAN_CPU_COST_MULTIPLIER, workCost * BYTE_DISK_READ_COST, 0).multiplyBy(adjustmentFactor);
  }

  @Override
  public double getCostForParallelization() {
    return Math.max(estimatedRowCount, 1);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return estimatedRowCount;
  }

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new BridgeFileReader(creator.props(this, null, schema,
      BroadcastExchangePrel.RECEIVER_RESERVE, BroadcastExchangePrel.RECEIVER_LIMIT), schema, bridgeSetId);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }
}

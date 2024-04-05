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
package com.dremio.exec.planner.logical;

import static com.dremio.exec.planner.cost.DremioCost.BYTE_DISK_READ_COST;

import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.physical.PrelUtil;
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

public class BridgeReaderRel extends AbstractRelNode implements Rel {
  private final String bridgeSetId;
  private final double estimatedRowCount;

  public BridgeReaderRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType rowType,
      double estimatedRowCount,
      String bridgeSetId) {
    super(cluster, traitSet);
    this.rowType = rowType;
    this.estimatedRowCount = estimatedRowCount;
    this.bridgeSetId = bridgeSetId;
  }

  public String getBridgeSetId() {
    return bridgeSetId;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BridgeReaderRel(getCluster(), traitSet, rowType, estimatedRowCount, bridgeSetId);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("bridgeSetId", bridgeSetId);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    final double rowCount = mq.getRowCount(this);
    final int fieldCount = getRowType().getFieldCount();
    final DremioCost.Factory costFactory = (DremioCost.Factory) planner.getCostFactory();

    double adjustmentFactor =
        PrelUtil.getPlannerSettings(getCluster()).getCseCostAdjustmentFactor();
    if (adjustmentFactor == Double.MAX_VALUE) {
      return costFactory.makeHugeCost();
    }
    double workCost = (rowCount * fieldCount * ScanCostFactor.ARROW.getFactor());

    return costFactory
        .makeCost(
            estimatedRowCount,
            workCost * DremioCost.SCAN_CPU_COST_MULTIPLIER,
            workCost * BYTE_DISK_READ_COST,
            0)
        .multiplyBy(adjustmentFactor);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return estimatedRowCount;
  }
}

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

import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PrelUtil;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class BridgeExchangeRel extends SingleRel implements Rel {
  private final String bridgeSetId;

  public BridgeExchangeRel(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, String bridgeSetId) {
    super(cluster, traitSet, input);
    this.bridgeSetId = bridgeSetId;
  }

  public String getBridgeSetId() {
    return bridgeSetId;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BridgeExchangeRel(getCluster(), traitSet, inputs.get(0), bridgeSetId);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("bridgeSetId", bridgeSetId);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelNode child = this.getInput();
    final double rowCount = mq.getRowCount(this);
    final DremioCost.Factory costFactory = (DremioCost.Factory) planner.getCostFactory();

    double adjustmentFactor =
        PrelUtil.getPlannerSettings(getCluster()).getCseCostAdjustmentFactor();
    if (adjustmentFactor == Double.MAX_VALUE) {
      return costFactory.makeHugeCost();
    }

    final int rowWidth = child.getRowType().getFieldCount() * DremioCost.AVG_FIELD_WIDTH;
    final double cpuCost = DremioCost.PROJECT_SIMPLE_CPU_COST * rowCount * rowWidth;
    final double ioCost = DremioCost.BYTE_DISK_WRITE_COST * rowCount * rowWidth;

    return costFactory.makeCost(rowCount, cpuCost, ioCost, 0).multiplyBy(adjustmentFactor);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return mq.getRowCount(input);
  }
}

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
package com.dremio.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.UnionExchange;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;

public class UnionExchangePrel extends ExchangePrel {

  public UnionExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
    assert input.getConvention() == Prel.PHYSICAL;
  }

  /**
   * A UnionExchange processes a total of M rows coming from N senders and
   * combines them into a single output stream.  Note that there is
   * no sort or merge operation going on. For costing purposes, we can
   * assume each sender is sending M/N rows to a single receiver.
   * (See DremioCost for symbol notations)
   * C =  CPU cost of SV remover for M/N rows
   *      + Network cost of sending M/N rows to 1 destination.
   * So, C = (s * M/N) + (w * M/N)
   * Total cost = N * C
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    RelNode child = this.getInput();
    double inputRows = child.estimateRowCount(mq);
    int  rowWidth = child.getRowType().getFieldCount() * DremioCost.AVG_FIELD_WIDTH;
    double svrCpuCost = DremioCost.SVR_CPU_COST * inputRows;
    double networkCost = DremioCost.BYTE_NETWORK_COST * inputRows * rowWidth;
    Factory costFactory = (Factory)planner.getCostFactory();
    return costFactory.makeCost(inputRows, svrCpuCost, 0, networkCost);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new UnionExchangePrel(getCluster(), traitSet, sole(inputs));
  }

  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    UnionExchange g = new UnionExchange(childPOP);
    return creator.addMetadata(this, g);
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}

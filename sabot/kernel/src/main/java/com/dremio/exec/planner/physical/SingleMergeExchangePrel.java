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


import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.SingleMergeExchange;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;

public class SingleMergeExchangePrel extends ExchangePrel {

  private final RelCollation collation ;

  public SingleMergeExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation) {
    super(cluster, traitSet, input);
    this.collation = collation;
    assert input.getConvention() == Prel.PHYSICAL;
  }

  /**
   * A SingleMergeExchange processes a total of M rows coming from N
   * sorted input streams (from N senders) and merges them into a single
   * output sorted stream. For costing purposes we can assume each sender
   * is sending M/N rows to a single receiver.
   * (See DremioCost for symbol notations)
   * C =  CPU cost of SV remover for M/N rows
   *     + Network cost of sending M/N rows to 1 destination.
   * So, C = (s * M/N) + (w * M/N)
   * Cost of merging M rows coming from N senders = (M log2 N) * c
   * Total cost = N * C + (M log2 N) * c
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    RelNode child = this.getInput();
    double inputRows = mq.getRowCount(child);
    int  rowWidth = child.getRowType().getFieldCount() * DremioCost.AVG_FIELD_WIDTH;
    double svrCpuCost = DremioCost.SVR_CPU_COST * inputRows;
    double networkCost = DremioCost.BYTE_NETWORK_COST * inputRows * rowWidth;
    int numEndPoints = PrelUtil.getSettings(getCluster()).numEndPoints();
    double mergeCpuCost = DremioCost.COMPARE_CPU_COST * inputRows * (Math.log(numEndPoints)/Math.log(2));
    Factory costFactory = (Factory)planner.getCostFactory();
    return costFactory.makeCost(inputRows, svrCpuCost + mergeCpuCost, 0, networkCost);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SingleMergeExchangePrel(getCluster(), traitSet, sole(inputs), collation);
  }

  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    SingleMergeExchange g = new SingleMergeExchange(childPOP, PrelUtil.getOrdering(this.collation, getInput().getRowType()));
    return creator.addMetadata(this, g);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (pw.nest()) {
      pw.item("collation", collation);
    } else {
      for (Ord<RelFieldCollation> ord : Ord.zip(collation.getFieldCollations())) {
        pw.item("sort" + ord.i, ord.e);
      }
    }
    return pw;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}

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
package com.dremio.exec.planner.physical;

import java.io.IOException;
import java.util.List;


import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.HashToRandomExchange;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;


public class HashToRandomExchangePrel extends ExchangePrel {


  private final List<DistributionField> fields;

  public HashToRandomExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<DistributionField> fields) {
    super(cluster, traitSet, input);
    this.fields = fields;
    assert input.getConvention() == Prel.PHYSICAL;
  }

  /**
   * HashToRandomExchange processes M input rows and hash partitions them
   * based on computing a hash value on the distribution fields.
   * If there are N nodes (endpoints), we can assume for costing purposes
   * on average each sender will send M/N rows to 1 destination endpoint.
   * (See DremioCost for symbol notations)
   * Include impact of skewness of distribution : the more keys used, the less likely the distribution will be skewed.
   * The hash cpu cost will be proportional to 1 / #_keys.
   * C =  CPU cost of hashing k fields of M/N rows
   *      + CPU cost of SV remover for M/N rows
   *      + Network cost of sending M/N rows to 1 destination.
   * So, C = (h * 1/k * M/N) + (s * M/N) + (w * M/N)
   * Total cost = N * C
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    RelNode child = this.getInput();
    double inputRows = mq.getRowCount(child);

    int  rowWidth = child.getRowType().getFieldCount() * DremioCost.AVG_FIELD_WIDTH;

    double hashCpuCost = DremioCost.HASH_CPU_COST * inputRows / fields.size();
    double svrCpuCost = DremioCost.SVR_CPU_COST * inputRows;
    double networkCost = DremioCost.BYTE_NETWORK_COST * inputRows * rowWidth;
    Factory costFactory = (Factory)planner.getCostFactory();
    return costFactory.makeCost(inputRows, hashCpuCost + svrCpuCost, 0, networkCost);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HashToRandomExchangePrel(getCluster(), traitSet, sole(inputs), fields);
  }

  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    // TODO - refactor to different exchange name
    HashToRandomExchange g = new HashToRandomExchange(childPOP, HashPrelUtil.getHashExpression(this.fields, getInput().getRowType()));
    return creator.addMetadata(this, g);
  }

  public List<DistributionField> getFields() {
    return this.fields;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
      for (Ord<DistributionField> ord : Ord.zip(fields)) {
        pw.item("dist" + ord.i, ord.e);
      }
    return pw;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}

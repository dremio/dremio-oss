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

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.UnionAll;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.google.common.collect.Lists;

@Options
public class UnionAllPrel extends UnionPrel {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionAllPrel.class);

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.unionall.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.unionall.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  public UnionAllPrel(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs,
      boolean checkCompatibility) throws InvalidRelException {
    super(cluster, traits, inputs, true /* all */, checkCompatibility);
  }

  @Override
  public Union copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    try {
      return new UnionAllPrel(this.getCluster(), traitSet, inputs,
          false /* don't check compatibility during copy */);
    }catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    double totalInputRowCount = 0;
    for (int i = 0; i < this.getInputs().size(); i++) {
      totalInputRowCount += mq.getRowCount(this.getInputs().get(i));
    }

    double cpuCost = totalInputRowCount * DremioCost.BASE_CPU_COST;
    Factory costFactory = (Factory)planner.getCostFactory();
    return costFactory.makeCost(totalInputRowCount, cpuCost, 0, 0);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    List<PhysicalOperator> children = Lists.newArrayList();

    for (int i = 0; i < this.getInputs().size(); i++) {
      children.add( ((Prel)this.getInputs().get(i)).getPhysicalOperator(creator));
    }

    BatchSchema left = children.get(0).getProps().getSchema();
    BatchSchema right = children.get(1).getProps().getSchema();
    if(!right.equalsTypesAndPositions(left)){
      throw UserException.dataReadError()
      .message("Unable to complete query, attempting to union two datasets that have different underlying schemas. Left: %s, Right: %s", left, right)
      .build(logger);
    }

    return new UnionAll(
        creator.props(this, null, left, RESERVE, LIMIT),
        children
        );
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}

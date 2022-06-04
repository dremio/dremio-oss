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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.UnionRel;
import com.google.common.collect.Lists;

public class UnionAllPrule extends Prule {
  public static final RelOptRule INSTANCE = new UnionAllPrule();
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private UnionAllPrule() {
    super(
        RelOptHelper.any(UnionRel.class), "Prel.UnionAllPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    UnionRel union = (UnionRel) call.rel(0);
    final RelMetadataQuery mq = union.getCluster().getMetadataQuery();
    return (! Boolean.TRUE.equals(mq.areRowsUnique(union)));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final UnionRel union = (UnionRel) call.rel(0);
    final List<RelNode> inputs = union.getInputs();
    List<RelNode> convertedInputList = Lists.newArrayList();

    for (RelNode input : inputs) {
      RelTraitSet traitsChild = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL);
      convertedInputList.add(convert(input, PrelUtil.fixTraits(call, traitsChild)));
    }

    RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.ANY);
    try {
    call.transformTo(new UnionAllPrel(union.getCluster(), traits, convertedInputList, false /* compatibility already checked during logical phase */));
    } catch (InvalidRelException e) {
      tracer.warn(e.toString());
    }
  }
}

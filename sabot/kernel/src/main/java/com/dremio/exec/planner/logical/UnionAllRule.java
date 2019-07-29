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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

/**
 * Rule that converts a {@link LogicalUnion} to a {@link UnionRelBase}, implemented by a "union" operation.
 */
public class UnionAllRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new UnionAllRule();
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private UnionAllRule() {
    super(RelOptHelper.any(LogicalUnion.class, Convention.NONE), "UnionAllRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalUnion union = (LogicalUnion) call.rel(0);

    // This rule applies to Union-All only
    if(!union.all) {
      return;
    }

    final RelTraitSet traits = union.getTraitSet().plus(Rel.LOGICAL);
    final List<RelNode> convertedInputs = new ArrayList<>();
    for (RelNode input : union.getInputs()) {
      final RelNode convertedInput = convert(input, input.getTraitSet().plus(Rel.LOGICAL).simplify());
      convertedInputs.add(convertedInput);
    }
    try {
      call.transformTo(new UnionRel(union.getCluster(), traits, convertedInputs, union.all,
          true /* check compatibility */));
    } catch (InvalidRelException e) {
      tracer.warn(e.toString()) ;
    }
  }
}

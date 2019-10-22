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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;


public class FilterNLJMergeRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new FilterNLJMergeRule();

  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private FilterNLJMergeRule() {
    super(operand(FilterPrel.class, operand(NestedLoopJoinPrel.class, any())), "NLJMergeRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    NestedLoopJoinPrel join = call.rel(1);
    return join.getJoinType() == JoinRelType.INNER &&
        PrelUtil.getPlannerSettings(call.getPlanner()).getOptions().getOption(NestedLoopJoinPrel.VECTORIZED);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FilterPrel filter = call.rel(0);
    NestedLoopJoinPrel join = call.rel(1);
    call.transformTo(NestedLoopJoinPrel.create(join.getCluster(), join.getTraitSet(), join.getLeft(), join.getRight(), join.getJoinType(), RelOptUtil.andJoinFilters(join.getCluster().getRexBuilder(), join.getCondition(), filter.getCondition())));
  }
}

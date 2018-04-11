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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.JoinUtils.JoinCategory;
import com.google.common.collect.Lists;


public class NestedLoopJoinPrule extends JoinPruleBase {
  public static final RelOptRule INSTANCE = new NestedLoopJoinPrule("Prel.NestedLoopJoinPrule", RelOptHelper.any(JoinRel.class));

  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private NestedLoopJoinPrule(String name, RelOptRuleOperand operand) {
    super(operand, name);
  }

  @Override
  protected boolean checkPreconditions(JoinRel join, RelNode left, RelNode right,
      PlannerSettings settings) {
    JoinRelType type = join.getJoinType();

    if (! (type == JoinRelType.INNER || type == JoinRelType.LEFT)) {
      return false;
    }

    JoinCategory category = JoinUtils.getJoinCategory(left, right, join.getCondition(),
        Lists.<Integer>newArrayList(), Lists.<Integer>newArrayList(), Lists.<Boolean>newArrayList());
    if (category == JoinCategory.EQUALITY
        && (settings.isHashJoinEnabled() || settings.isMergeJoinEnabled())) {
      return false;
    }

    if (settings.isNlJoinForScalarOnly()) {
      if (JoinUtils.isScalarSubquery(left) || JoinUtils.isScalarSubquery(right)) {
        return true;
      } else {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return PrelUtil.getPlannerSettings(call.getPlanner()).isNestedLoopJoinEnabled();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    if (!settings.isNestedLoopJoinEnabled()) {
      return;
    }

    final JoinRel join = (JoinRel) call.rel(0);
    final RelNode left = join.getLeft();
    final RelNode right = join.getRight();

    if (!checkPreconditions(join, left, right, settings)) {
      return;
    }

    try {

      if (checkBroadcastConditions(call.getPlanner(), join, left, right)) {
        createBroadcastPlan(call, join, join.getCondition(), PhysicalJoinType.NESTEDLOOP_JOIN,
            left, right, null /* left collation */, null /* right collation */);
      }

    } catch (InvalidRelException e) {
      tracer.warn(e.toString());
    }
  }

}

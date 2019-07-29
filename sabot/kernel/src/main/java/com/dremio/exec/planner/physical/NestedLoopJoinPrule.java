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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.work.foreman.UnsupportedRelOperatorException;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.JoinUtils.JoinCategory;


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

    if (! (type == JoinRelType.INNER || type == JoinRelType.LEFT || type == JoinRelType.RIGHT)) {
      return false;
    }

    JoinCategory category = join.getJoinCategory();
    if (category == JoinCategory.EQUALITY
        && (settings.isHashJoinEnabled() || settings.isMergeJoinEnabled())) {
      return false;
    }

    if (settings.isNlJoinForScalarOnly()) {
      return JoinUtils.isScalarSubquery(left) || JoinUtils.isScalarSubquery(right);
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

      if (checkBroadcastConditions(call.getPlanner(), join, left, right, PhysicalJoinType.NESTEDLOOP_JOIN)) {
        createBroadcastPlan(call, join, join.getCondition(), left, right, null, null);
      }

    } catch (InvalidRelException e) {
      tracer.warn(e.toString());
    }
  }

  @Override
  protected void createBroadcastPlan(final RelOptRuleCall call,final JoinRel join,
                                     final RexNode joinCondition,
                                     final RelNode left, final RelNode right,
                                     final RelCollation collationLeft, final RelCollation collationRight) throws InvalidRelException {
    RelTraitSet traitsLeft = left.getTraitSet().plus(Prel.PHYSICAL);
    RelTraitSet traitsRight = right.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.BROADCAST);
    RelNode convertedLeft = convert(left, traitsLeft);
    RelNode convertedRight = convert(right, traitsRight);
    JoinRelType joinType = join.getJoinType();
    NestedLoopJoinPrel newJoin = NestedLoopJoinPrel.create(join.getCluster(), convertedLeft.getTraitSet(), convertedLeft, convertedRight, joinType);
    if (joinCondition.isAlwaysTrue()) {
      call.transformTo(newJoin);
    } else if (joinType == JoinRelType.INNER) {
      call.transformTo(new FilterPrel(join.getCluster(), convertedLeft.getTraitSet(), newJoin, joinCondition));
    }
  }

  @Override
  protected void createDistBothPlan(RelOptRuleCall call, JoinRel join,
                                    PhysicalJoinType physicalJoinType,
                                    RelNode left, RelNode right,
                                    RelCollation collationLeft, RelCollation collationRight,
                                    DistributionTrait hashLeftPartition, DistributionTrait hashRightPartition) throws UnsupportedRelOperatorException {
    throw new UnsupportedRelOperatorException("Nested loop join does not support creating join plan with both left and right children hash distributed");
  }
}

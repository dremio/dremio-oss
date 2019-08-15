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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import com.dremio.exec.planner.common.JoinRelBase;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.work.foreman.UnsupportedRelOperatorException;

public class HashJoinPrule extends JoinPruleBase {
  public static final RelOptRule DIST_INSTANCE = new HashJoinPrule("Prel.HashJoinDistPrule", RelOptHelper.any(JoinRel.class), true);
  public static final RelOptRule BROADCAST_INSTANCE = new HashJoinPrule("Prel.HashJoinBroadcastPrule", RelOptHelper.any(JoinRel.class), false);

  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private final boolean isDist;
  private HashJoinPrule(String name, RelOptRuleOperand operand, boolean isDist) {
    super(operand, name);
    this.isDist = isDist;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    return settings.isHashJoinEnabled();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    if (!settings.isHashJoinEnabled()) {
      return;
    }

    final JoinRel join = (JoinRel) call.rel(0);
    final RelNode left = join.getLeft();
    final RelNode right = join.getRight();

    if (!checkPreconditions(join, left, right, settings)) {
      return;
    }

    boolean hashSingleKey = PrelUtil.getPlannerSettings(call.getPlanner()).isHashSingleKey();

    try {

      if (isDist) {
        createDistBothPlan(call, join,
            left, right, null /* left collation */, null /* right collation */, hashSingleKey);
      } else {
        if (checkBroadcastConditions(call.getPlanner(), join, left, right)) {
          createBroadcastPlan(call, join, join.getCondition(), left, right, null, null);
        }
      }


    } catch (InvalidRelException | UnsupportedRelOperatorException e) {
      tracer.warn(e.toString());
    }
  }

  @Override
  protected void createBroadcastPlan(final RelOptRuleCall call, final JoinRel join,
                                     final RexNode joinCondition,
                                     final RelNode left, final RelNode right,
                                     final RelCollation collationLeft, final RelCollation collationRight) throws InvalidRelException {

    final RelNode convertedLeft = convert(left, left.getTraitSet().plus(Prel.PHYSICAL));
    final RelNode convertedRight = convert(right, right.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.BROADCAST));
    call.transformTo(HashJoinPrel.create(join.getCluster(), convertedLeft.getTraitSet(), convertedLeft, convertedRight, joinCondition,
        join.getJoinType()));
  }

  @Override
  protected void createDistBothPlan(RelOptRuleCall call, JoinRel join,
                                  RelNode left, RelNode right,
                                  RelCollation collationLeft, RelCollation collationRight,
                                  DistributionTrait hashLeftPartition, DistributionTrait hashRightPartition) throws InvalidRelException {
    RelTraitSet traitsLeft = left.getTraitSet().plus(Prel.PHYSICAL).plus(hashLeftPartition);
    RelTraitSet traitsRight = right.getTraitSet().plus(Prel.PHYSICAL).plus(hashRightPartition);

    final RelNode convertedLeft = convert(left, traitsLeft);
    final RelNode convertedRight = convert(right, traitsRight);

    JoinRelBase newJoin = HashJoinPrel.create(join.getCluster(), traitsLeft,
        convertedLeft, convertedRight, join.getCondition(),
        join.getJoinType());

    call.transformTo(newJoin);
  }
}

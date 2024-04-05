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
package com.dremio.exec.planner.physical.rule;

import com.dremio.exec.planner.common.MoreRelOptUtil.RexNodeCountVisitor;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.NestedLoopJoinPrel;
import com.dremio.exec.planner.physical.PrelUtil;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

/**
 * An enhanced version of FilterJoinRule
 *
 * @see org.apache.calcite.rel.rules.FilterJoinRule
 */
public class FilterNestedLoopJoinPRule extends RelRule<FilterNestedLoopJoinPRule.Config> {
  public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

  private FilterNestedLoopJoinPRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    NestedLoopJoinPrel join = call.rel(1);
    return join.getJoinType() == JoinRelType.INNER
        && PrelUtil.getPlannerSettings(call.getPlanner())
            .getOptions()
            .getOption(NestedLoopJoinPrel.VECTORIZED);
  }

  private static RexNode trimCondition(
      long max, RexNode condition, RexBuilder rexBuilder, List<RexNode> residue) {
    List<RexNode> newConditionsList = new ArrayList<>(); // these conditions will be pushed down
    int currentNodeCnt = 0;
    LinkedList<RexNode> conjunctions =
        new LinkedList<>(); // list to hold the remaining conditions after finding geo conditions
    // first find any GEO conditions, and make sure they are included in the pushdown
    for (RexNode node : RelOptUtil.conjunctions(condition)) {
      if (node instanceof RexCall) {
        String operatorName = ((RexCall) node).getOperator().getName();
        if ("GEO_BEYOND".equals(operatorName) || "GEO_NEARBY".equals(operatorName)) {
          newConditionsList.add(node);
          currentNodeCnt += RexNodeCountVisitor.count(node);
          continue;
        }
      }
      conjunctions.add(node);
    }
    // now iterate through remaining conditions until we reach the max node count and add them to
    // pushdown
    while (!conjunctions.isEmpty()) {
      RexNode node = conjunctions.pop();
      int cnt = RexNodeCountVisitor.count(node);
      if (cnt + currentNodeCnt <= max) {
        newConditionsList.add(node);
        currentNodeCnt += cnt;
      } else {
        residue.add(node);
        break;
      }
    }
    residue.addAll(conjunctions);
    return RexUtil.composeConjunction(rexBuilder, newConditionsList, false);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FilterPrel filter = call.rel(0);
    NestedLoopJoinPrel join = call.rel(1);
    long maxNodes = PrelUtil.getPlannerSettings(call.getPlanner()).getMaxNLJConditionNodesPerPlan();
    List<RexNode> residue = new ArrayList<>();
    RexNode condition =
        trimCondition(
            maxNodes, filter.getCondition(), filter.getCluster().getRexBuilder(), residue);
    RelNode result =
        NestedLoopJoinPrel.create(
            join.getCluster(),
            join.getTraitSet(),
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            RelOptUtil.andJoinFilters(
                join.getCluster().getRexBuilder(), join.getCondition(), condition));
    if (!residue.isEmpty()) {
      result =
          filter.copy(
              filter.getTraitSet(),
              result,
              RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), residue, false));
    }
    call.transformTo(result);
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        EMPTY
            .withDescription("FilterProjectTransposePRule")
            .withOperandSupplier(
                os1 ->
                    os1.operand(FilterPrel.class)
                        .oneInput(os2 -> os2.operand(NestedLoopJoinPrel.class).anyInputs()))
            .as(Config.class);

    @Override
    default FilterNestedLoopJoinPRule toRule() {
      return new FilterNestedLoopJoinPRule(this);
    }
  }
}

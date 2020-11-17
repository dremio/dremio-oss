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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import com.dremio.exec.planner.common.MoreRelOptUtil.RexNodeCountVisitor;


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

  private static RexNode trimCondition(long max, RexNode condition, RexBuilder rexBuilder, List<RexNode> residue){
    List<RexNode> newConditionsList = new ArrayList<>(); // these conditions will be pushed down
    int currentNodeCnt = 0;
    LinkedList<RexNode> conjunctions = new LinkedList<>(); // list to hold the remaining conditions after finding geo conditions
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
    // now iterate through remaining conditions until we reach the max node count and add them to pushdown
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
    RexNode condition = trimCondition(maxNodes, filter.getCondition(), filter.getCluster().getRexBuilder(), residue);
    if ((join.getProjectedFields() == null) || join.getProjectedFields().cardinality() == join.getInputRowType().getFieldCount()) {
      RelNode result = NestedLoopJoinPrel.create(join.getCluster(), join.getTraitSet(), join.getLeft(), join.getRight(), join.getJoinType(), RelOptUtil.andJoinFilters(join.getCluster().getRexBuilder(), join.getCondition(), condition), join.getProjectedFields());
      if (!residue.isEmpty()) {
        result = filter.copy(filter.getTraitSet(), result, RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), residue, false));
      }
      call.transformTo(result);
    } else {
      // Current filter condition is written based on projected fields on join. In order to push this filter down we need to rewrite filter condition
      final ImmutableBitSet topProjectedColumns = RelOptUtil.InputFinder.bits(condition);
      final ImmutableBitSet bottomProjectedColumns = join.getProjectedFields();

      Mapping mapping = Mappings.create(MappingType.SURJECTION, join.getRowType().getFieldCount(), join.getInputRowType().getFieldCount());
      for (Ord<Integer> ord : Ord.zip(bottomProjectedColumns)) {
        if (topProjectedColumns.get(ord.i)) {
          mapping.set(ord.i, ord.e);
        }
      }

      RexShuttle shuttle = new RexPermuteInputsShuttle(mapping);
      RexNode updatedCondition = shuttle.apply(filter.getCondition());

      RelNode result = NestedLoopJoinPrel.create(join.getCluster(), join.getTraitSet(), join.getLeft(), join.getRight(), join.getJoinType(), RelOptUtil.andJoinFilters(join.getCluster().getRexBuilder(), join.getCondition(), updatedCondition), join.getProjectedFields());
      if (!residue.isEmpty()) {
        result = filter.copy(filter.getTraitSet(), result, RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), residue, false));
      }
      call.transformTo(result);
    }
  }
}

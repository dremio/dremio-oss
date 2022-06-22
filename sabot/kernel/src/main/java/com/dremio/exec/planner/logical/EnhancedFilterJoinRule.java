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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import com.dremio.exec.planner.common.MoreRelOptUtil;

/**
 * Rule that pushes predicates from a Filter and a Join into the Join below, and pushes the join
 * condition into the Join.
 */
public abstract class EnhancedFilterJoinRule extends RelOptRule {
  public static final EnhancedFilterJoinRule WITH_FILTER = new WithFilter();
  public static final EnhancedFilterJoinRule NO_FILTER = new NoFilter();

  private EnhancedFilterJoinRule(RelOptRuleOperand operand, String desc) {
    super(operand, DremioRelFactories.CALCITE_LOGICAL_BUILDER, "FilterJoinRule:" + desc);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode rewrite = doMatch(call);
    if (rewrite != null) {
      call.transformTo(rewrite.accept(new RelShuttleImpl() {
        @Override
        public RelNode visit(LogicalJoin join) {
          DremioJoinPushTransitivePredicatesRule instance = new DremioJoinPushTransitivePredicatesRule();
          MoreRelOptUtil.TransformCollectingCall c = new MoreRelOptUtil.TransformCollectingCall(call.getPlanner(), instance.getOperand(), new RelNode[]{join}, null);
          instance.onMatch(c);
          if (!c.isTransformed()) {
            return join;
          } else {
            return c.getTransformedRel();
          }
        }
      }));
    }
  }

  protected abstract RelNode doMatch(RelOptRuleCall call);

  protected RelNode doMatch(Filter filterRel, Join joinRel, RelBuilder relBuilder) {
    // Extract the join condition and pushdown predicates, also simplify the remaining filter
    EnhancedFilterJoinExtraction extraction = new EnhancedFilterJoinExtractor(filterRel, joinRel,
      FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM).extract();
    RexNode inputFilterConditionPruned = extraction.getInputFilterConditionPruned();
    RexNode inputJoinConditionPruned = extraction.getInputJoinConditionPruned();
    RexNode newJoinCondition = extraction.getJoinCondition();
    RexNode leftPushdownPredicate = extraction.getLeftPushdownPredicate();
    RexNode rightPushdownPredicate = extraction.getRightPushdownPredicate();
    RexNode remainingFilterCondition = extraction.getRemainingFilterCondition();
    JoinRelType simplifiedJoinType = extraction.getSimplifiedJoinType();

    // Shift filters
    List<RelDataTypeField> joinFields = joinRel.getRowType().getFieldList();
    List<RelDataTypeField> leftFields = joinRel.getInputs().get(0).getRowType().getFieldList();
    List<RelDataTypeField> rightFields = joinRel.getInputs().get(1).getRowType().getFieldList();
    RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
    RexNode leftPushdownPredicateShifted = MoreRelOptUtil.shiftFilter(
      0,
      leftFields.size(),
      0,
      rexBuilder,
      joinFields,
      joinFields.size(),
      leftFields,
      leftPushdownPredicate);
    RexNode rightPushdownPredicateShifted = MoreRelOptUtil.shiftFilter(
      leftFields.size(),
      joinFields.size(),
      -leftFields.size(),
      rexBuilder,
      joinFields,
      joinFields.size(),
      rightFields,
      rightPushdownPredicate);

    // Prune left and right predicates that have already been pushed down
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();
    RexNode leftPushdownPredicatePruned = EnhancedFilterJoinPruner.prunePushdown(
      leftPushdownPredicateShifted, mq, joinRel.getLeft(), rexBuilder);
    RexNode rightPushdownPredicatePruned = EnhancedFilterJoinPruner.prunePushdown(
      rightPushdownPredicateShifted, mq, joinRel.getRight(), rexBuilder);

    // If nothing is changed, then no pushdown happens
    if (leftPushdownPredicatePruned.isAlwaysTrue()
      && rightPushdownPredicatePruned.isAlwaysTrue()
      && (newJoinCondition.isAlwaysTrue() ||
        (newJoinCondition.equals(inputJoinConditionPruned)
        && remainingFilterCondition.equals(inputFilterConditionPruned)))) {
      return null;
    }

    // Construct the rewritten result
    return relBuilder
      .push(joinRel.getLeft())
      .filter(leftPushdownPredicatePruned)   // left child of join
      .push(joinRel.getRight())
      .filter(rightPushdownPredicatePruned)    // right child of join
      .join(simplifiedJoinType, newJoinCondition)    // join
      .convert(joinRel.getRowType(), false)   // project if needed
      .filter(remainingFilterCondition)   // remaining filter
      .build();
  }

  private static class WithFilter extends EnhancedFilterJoinRule {
    private WithFilter() {
      super(RelOptRule.operand(LogicalFilter.class,
        RelOptRule.operand(LogicalJoin.class, RelOptRule.any())), "EnhancedFilterJoinRule:filter");
    }

    @Override
    protected RelNode doMatch(RelOptRuleCall call) {
      Filter filter = call.rel(0);
      Join join = call.rel(1);
      return super.doMatch(filter, join, call.builder());
    }
  }

  private static class NoFilter extends EnhancedFilterJoinRule {
    private NoFilter() {
      super(RelOptRule.operand(LogicalJoin.class, RelOptRule.any()),
        "EnhancedFilterJoinRule:no-filter");
    }

    @Override
    protected RelNode doMatch(RelOptRuleCall call) {
      Join join = call.rel(0);
      return super.doMatch(null, join, call.builder());
    }
  }
}

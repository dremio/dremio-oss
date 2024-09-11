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

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.sql.SqlOperatorBuilder;
import java.util.List;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.Util;

/**
 * Rule that pushes predicates from a Filter and a Join into the Join below, and pushes the join
 * condition into the Join.
 */
public final class EnhancedFilterJoinRule extends RelRule<EnhancedFilterJoinRule.Config>
    implements TransformationRule {
  // This is a special operator we introduce to let this rule know that we shouldn't fire anymore to
  // avoid infinite loops
  private static final SqlOperator DO_NOT_PUSH_DOWN_OPERATOR =
      SqlOperatorBuilder.name("DO_NOT_PUSH_DOWN")
          .returnType(ReturnTypes.ARG0)
          .anyOperands()
          .build();

  private EnhancedFilterJoinRule(EnhancedFilterJoinRule.Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter;
    Join join;

    if (call.rel(0) instanceof Filter) {
      // With Filter Configuration
      filter = call.rel(0);
      join = call.rel(1);
    } else {
      filter = null;
      join = call.rel(0);
    }

    RelNode rewrite = doMatch(filter, join, call.builder(), config.useGuardRail());
    if (rewrite != null) {
      call.transformTo(
          rewrite.accept(
              new RelShuttleImpl() {
                @Override
                public RelNode visit(LogicalJoin join) {
                  DremioJoinPushTransitivePredicatesRule instance =
                      new DremioJoinPushTransitivePredicatesRule();
                  MoreRelOptUtil.TransformCollectingCall c =
                      new MoreRelOptUtil.TransformCollectingCall(
                          call.getPlanner(), instance.getOperand(), new RelNode[] {join}, null);
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

  protected RelNode doMatch(
      Filter filterRel, Join joinRel, RelBuilder relBuilder, boolean useGuardRail) {
    if (useGuardRail
        && (filterRel) != null
        && containsDoNotPushdownOperator(filterRel.getCondition())) {
      return null;
    }

    // Extract the join condition and pushdown predicates, also simplify the remaining filter
    EnhancedFilterJoinExtraction extraction =
        new EnhancedFilterJoinExtractor(
                filterRel, joinRel, FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM)
            .extract();
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
    RexNode leftPushdownPredicateShifted =
        MoreRelOptUtil.shiftFilter(
            0,
            leftFields.size(),
            0,
            rexBuilder,
            joinFields,
            joinFields.size(),
            leftFields,
            leftPushdownPredicate);
    RexNode rightPushdownPredicateShifted =
        MoreRelOptUtil.shiftFilter(
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
    RelOptPredicateList leftPulledUpPredicates = mq.getPulledUpPredicates(joinRel.getLeft());
    RelOptPredicateList rightPulledUpPredicates = mq.getPulledUpPredicates(joinRel.getRight());
    final RelOptPredicateList predicates =
        leftPulledUpPredicates.union(
            rexBuilder, rightPulledUpPredicates.shift(rexBuilder, leftFields.size()));

    final RexExecutor executor =
        Util.first(joinRel.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR);
    final RexSimplify simplify = new RexSimplify(rexBuilder, predicates, executor);

    RexNode leftPushdownPredicatePruned =
        MoreRelOptUtil.prunePushdown(
            simplify.simplifyPreservingType(
                leftPushdownPredicateShifted, RexUnknownAs.falseIf(true), true),
            leftPulledUpPredicates,
            rexBuilder);
    RexNode rightPushdownPredicatePruned =
        MoreRelOptUtil.prunePushdown(
            simplify.simplifyPreservingType(
                rightPushdownPredicateShifted, RexUnknownAs.falseIf(true), true),
            rightPulledUpPredicates,
            rexBuilder);

    // If nothing is changed, then no pushdown happens
    if (leftPushdownPredicatePruned.isAlwaysTrue()
        && rightPushdownPredicatePruned.isAlwaysTrue()
        && (newJoinCondition.isAlwaysTrue()
            || (newJoinCondition.equals(inputJoinConditionPruned)
                && remainingFilterCondition.equals(inputFilterConditionPruned)))) {
      return null;
    }

    // Wrap the top filter in a special node that tells the rule not to push it down further
    if (useGuardRail && !remainingFilterCondition.isAlwaysTrue()) {
      remainingFilterCondition =
          rexBuilder.makeCall(DO_NOT_PUSH_DOWN_OPERATOR, remainingFilterCondition);
    }

    // Construct the rewritten result
    return relBuilder
        .push(joinRel.getLeft())
        .filter(leftPushdownPredicatePruned) // left child of join
        .push(joinRel.getRight())
        .filter(rightPushdownPredicatePruned) // right child of join
        .join(simplifiedJoinType, newJoinCondition) // join
        .convert(joinRel.getRowType(), false) // project if needed
        .filter(remainingFilterCondition) // remaining filter
        .build();
  }

  public interface Config extends RelRule.Config {
    Config WITH_FILTER =
        RelRule.Config.EMPTY
            .withDescription("EnhancedFilterJoinRule:withFilter")
            .withOperandSupplier(
                op ->
                    op.operand(Filter.class).oneInput(join -> join.operand(Join.class).anyInputs()))
            .as(Config.class);

    Config WITHOUT_FILTER =
        RelRule.Config.EMPTY
            .withDescription("EnhancedFilterJoinRule:withoutFilter")
            .withOperandSupplier(op -> op.operand(Join.class).anyInputs())
            .as(Config.class);

    @Override
    default EnhancedFilterJoinRule toRule() {
      return new EnhancedFilterJoinRule(this);
    }

    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean useGuardRail();

    /** Sets {@link #useGuardRail()}. */
    Config withUseGuardRail(boolean withUseGuardRail);
  }

  public static RelNode removeArtifacts(RelNode relNode) {
    return relNode.accept(
        new RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            if (!(other instanceof Filter)) {
              return super.visit(other);
            }

            Filter filter = (Filter) other;
            RelNode input = filter.getInput().accept(this);
            RexNode condition =
                filter
                    .getCondition()
                    .accept(
                        new RexShuttle() {
                          @Override
                          public RexNode visitCall(final RexCall call) {
                            if (!call.getOperator().equals(DO_NOT_PUSH_DOWN_OPERATOR)) {
                              return super.visitCall(call);
                            }

                            return call.getOperands().get(0);
                          }
                        });
            return filter.copy(filter.getTraitSet(), input, condition);
          }
        });
  }

  private static boolean containsDoNotPushdownOperator(RexNode node) {
    Boolean containsOperator =
        node.accept(
            new RexVisitorImpl<Boolean>(true) {
              @Override
              public Boolean visitCall(RexCall call) {
                if (call.getOperator().equals(DO_NOT_PUSH_DOWN_OPERATOR)) {
                  return true;
                }

                // Recursively visit all the operands (children) of the current call
                for (RexNode operand : call.getOperands()) {
                  Boolean contains = operand.accept(this);
                  if (contains != null && contains) {
                    return true;
                  }
                }

                // If none of the children or the current node match the criteria, return false
                return false;
              }
            });
    return containsOperator != null && containsOperator;
  }
}

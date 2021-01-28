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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Litmus;
import org.apache.commons.collections.CollectionUtils;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.partition.FindSimpleFilters;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.service.Pointer;

/**
 * Dremio version of JoinPushTransitivePredicatesRule extended from Calcite
 * to avoid getting into infinite loop of transitive filter push down.
 */
public class DremioJoinPushTransitivePredicatesRule extends RelOptRule {

  public DremioJoinPushTransitivePredicatesRule() {
    super(RelOptRule.operand(LogicalJoin.class, RelOptRule.any()),
      DremioRelFactories.CALCITE_LOGICAL_BUILDER,
      "DremioJoinPushTransitivePredicatesRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalJoin join = call.rel(0);
    LogicalJoin newJoin = findAndApplyTransitivePredicates(join);
    if (newJoin != null) {
      call.getPlanner().onCopy(join, newJoin);
      call.transformTo(newJoin);
    }
  }

  public static LogicalJoin findAndApplyTransitivePredicates(LogicalJoin join) {
    final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    RelOptPredicateList preds = mq.getPulledUpPredicates(join);

    List<RexNode> leftInferredPredicates = getCanonicalizedPredicates(join, rexBuilder, preds.leftInferredPredicates);
    List<RexNode> rightInferredPredicates = getCanonicalizedPredicates(join, rexBuilder, preds.rightInferredPredicates);

    List<RexNode> leftPredicates = new ArrayList<>();
    List<RexNode> rightPredicates = new ArrayList<>();

    match(join, leftInferredPredicates, rightInferredPredicates, leftPredicates, rightPredicates);

    if (leftPredicates.isEmpty() && rightPredicates.isEmpty()) {
      return null;
    }

    RelNode left = getNewChild(join.getLeft(), leftPredicates, rexBuilder);
    RelNode right = getNewChild(join.getRight(), rightPredicates, rexBuilder);
    return join.copy(join.getTraitSet(), join.getCondition(), left, right, join.getJoinType(), join.isSemiJoinDone());
  }

  private static RelNode getNewChild(RelNode node,
                              List<RexNode> predicates,
                                     RexBuilder builder) {
    if (!predicates.isEmpty()) {
      node = LogicalFilter.create(node, RexUtil.composeConjunction(builder, predicates, false));
    }
    return node;
  }

  private static void match(Join join,
                     List<RexNode> leftInferredPredicates,
                     List<RexNode> rightInferredPredicates,
                     List<RexNode> leftPredicates,
                     List<RexNode> rightPredicates) {
    RelNode left = join.getLeft();
    RelNode right = join.getRight();

    boolean leftPredMatch = predicatesMatchWithNullability(leftInferredPredicates, left.getRowType());
    boolean rightPredMatch = predicatesMatchWithNullability(rightInferredPredicates, right.getRowType());

    if (!leftPredMatch && !rightPredMatch) {
      // If the inferred predicates cannot be applied on both sides
      return;
    }

    if (leftPredMatch && isValidFilter(leftInferredPredicates)) {
      leftPredicates.addAll(leftInferredPredicates);
    }
    if (rightPredMatch && isValidFilter(rightInferredPredicates)) {
      rightPredicates.addAll(rightInferredPredicates);
    }
  }

  public static List<RexNode> getCanonicalizedPredicates(RelNode join, RexBuilder builder, List<RexNode> inferredPredicates) {
    List<RexNode> predicates = new ArrayList<>();
    // To simplify predicates of the type "Input IS NOT DISTINCT FROM Constant" so that we don't end up
    // with filter conditions like: condition=[AND(=($0, 3), IS NOT DISTINCT FROM($0, CAST(3):INTEGER))]
    for (RexNode predicate : inferredPredicates) {
      if (!isTransitiveFilterNotNullExprPushdownEnabled(join, predicate)) {
        // If the filter condition contains "IS NOT NULL($*)", it generates
        // illegal SQL during JDBC RelToSql, and fails to run the query.
        // Disable pushing IS NOT NULL until DX-26452 is fixed.
        continue;
      }
      predicate = predicate.accept(new MoreRelOptUtil.RexLiteralCanonicalizer(builder));
      final FindSimpleFilters.StateHolder holder = predicate.accept(new FindSimpleFilters(builder, false));
      if (holder.hasConditions()) {
        predicates.addAll(holder.getConditions());
      } else {
        predicates.add(predicate);
      }
    }
    return predicates;
  }

  public static boolean isTransitiveFilterNotNullExprPushdownEnabled(RelNode join, RexNode predicate) {
    // TODO: Remove this when DX-26452 is fixed
    Pointer<Boolean> found = new Pointer<>(false);
    predicate.accept(new RexVisitorImpl<Void>(true) {
      @Override
      public Void visitCall(RexCall call) {
        if (call.getKind() == SqlKind.IS_NOT_NULL) {
          found.value = true;
          return null;
        }
        return super.visitCall(call);
      }
    });
    if (found.value) {
      return PrelUtil.getPlannerSettings(join.getCluster()).isTransitiveFilterNotNullExprPushdownEnabled();
    }
    return true;
  }

  private static boolean isValidFilter(List<RexNode> predicates) {
    // A valid filter should not contain a flatten in it
    if (CollectionUtils.isEmpty(predicates)) {
      return false;
    }
    return FlattenVisitors.count(predicates) == 0;
  }

  public static boolean predicatesMatchWithNullability(List<RexNode> predicates, RelDataType rowType) {
    // Make sure the filter we are pushing matches the row type
    if (CollectionUtils.isEmpty(predicates)) {
      return false;
    }
    boolean match = true;
    for (RexNode predicate : predicates) {
      RexChecker checker = new RexChecker(rowType, null, Litmus.IGNORE);
      predicate.accept(checker);
      match &= checker.getFailureCount() == 0;
    }
    return match;
  }
}

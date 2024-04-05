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
import com.dremio.exec.planner.logical.partition.FindSimpleFilters;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.collections4.CollectionUtils;

/**
 * Dremio version of JoinPushTransitivePredicatesRule extended from Calcite to avoid getting into
 * infinite loop of transitive filter push down.
 */
public class DremioJoinPushTransitivePredicatesRule extends RelOptRule {

  public DremioJoinPushTransitivePredicatesRule() {
    super(
        RelOptRule.operand(LogicalJoin.class, RelOptRule.any()),
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

    List<RexNode> leftInferredPredicates =
        getCanonicalizedPredicates(rexBuilder, preds.leftInferredPredicates);
    List<RexNode> rightInferredPredicates =
        getCanonicalizedPredicates(rexBuilder, preds.rightInferredPredicates);

    List<RexNode> leftPredicates = new ArrayList<>();
    List<RexNode> rightPredicates = new ArrayList<>();

    match(join, leftInferredPredicates, rightInferredPredicates, leftPredicates, rightPredicates);

    if (leftPredicates.isEmpty() && rightPredicates.isEmpty()) {
      return null;
    }

    RelNode left = getNewChild(join.getLeft(), leftPredicates, rexBuilder);
    RelNode right = getNewChild(join.getRight(), rightPredicates, rexBuilder);
    return join.copy(
        join.getTraitSet(),
        join.getCondition(),
        left,
        right,
        join.getJoinType(),
        join.isSemiJoinDone());
  }

  private static RelNode getNewChild(RelNode node, List<RexNode> predicates, RexBuilder builder) {
    if (!predicates.isEmpty()) {
      node = LogicalFilter.create(node, RexUtil.composeConjunction(builder, predicates, false));
    }
    return node;
  }

  private static void match(
      Join join,
      List<RexNode> leftInferredPredicates,
      List<RexNode> rightInferredPredicates,
      List<RexNode> leftPredicates,
      List<RexNode> rightPredicates) {
    RelNode left = join.getLeft();
    RelNode right = join.getRight();
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    boolean leftPredMatch =
        predicatesMatchWithNullability(leftInferredPredicates, left.getRowType(), rexBuilder);

    boolean rightPredMatch =
        predicatesMatchWithNullability(rightInferredPredicates, right.getRowType(), rexBuilder);

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

  public static List<RexNode> getCanonicalizedPredicates(
      RexBuilder builder, List<RexNode> inferredPredicates) {
    List<RexNode> predicates = new ArrayList<>();
    // To simplify predicates of the type "Input IS NOT DISTINCT FROM Constant" so that we don't end
    // up
    // with filter conditions like: condition=[AND(=($0, 3), IS NOT DISTINCT FROM($0,
    // CAST(3):INTEGER))]
    for (RexNode predicate : inferredPredicates) {
      predicate = predicate.accept(new MoreRelOptUtil.RexLiteralCanonicalizer(builder));
      final FindSimpleFilters.StateHolder holder =
          predicate.accept(new FindSimpleFilters(builder, false));
      if (holder.hasConditions()) {
        predicates.addAll(holder.getConditions());
      } else {
        predicates.add(predicate);
      }
    }
    return predicates;
  }

  private static boolean isValidFilter(List<RexNode> predicates) {
    // A valid filter should not contain a flatten in it
    if (CollectionUtils.isEmpty(predicates)) {
      return false;
    }
    return FlattenVisitors.count(predicates) == 0;
  }

  public static boolean predicatesMatchWithNullability(
      List<RexNode> predicates, RelDataType rowType, RexBuilder rexBuilder) {
    // Make sure the filter we are pushing matches the row type
    // If the decimal precision and scale do not match, we update the data type of operand in order
    // to match the row
    // type.
    if (CollectionUtils.isEmpty(predicates)) {
      return false;
    }
    for (int i = 0; i < predicates.size(); i++) {
      MoreRelOptUtil.RexTypeMatcher typeMatcher =
          new MoreRelOptUtil.RexTypeMatcher(rowType, rexBuilder);
      RexNode predicate = predicates.get(i).accept(typeMatcher);
      if (!typeMatcher.isValid()) {
        return false;
      }
      predicates.set(i, predicate);
    }
    return true;
  }
}

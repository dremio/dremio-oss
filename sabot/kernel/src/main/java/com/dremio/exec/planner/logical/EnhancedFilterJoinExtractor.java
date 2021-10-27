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
import java.util.function.Predicate;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Predicate extractor used in {@link EnhancedFilterJoinRule} to find out the join condition that
 * can be pushed into the join, and filter predicates that can be pushed below the join.
 */
public final class EnhancedFilterJoinExtractor {
  private final RexBuilder rexBuilder;
  private final Join joinRel;
  private final FilterJoinRule.Predicate predicate;
  private final RexNode inputFilterConditionPruned;
  private final RexNode inputJoinConditionPruned;
  private final ImmutableBitSet leftBitSet;
  private final ImmutableBitSet rightBitSet;

  public EnhancedFilterJoinExtractor(Filter filterRel, Join joinRel, FilterJoinRule.Predicate predicate) {
    this.rexBuilder = joinRel.getCluster().getRexBuilder();
    this.joinRel = joinRel;
    this.predicate = predicate;

    // Get input conditions
    RexNode inputFilterCondition = (filterRel != null) ?
      filterRel.getCondition() : rexBuilder.makeLiteral(true);
    RexNode inputJoinCondition = joinRel.getCondition();

    // Convert to Nnf
    RexNode inputFilterConditionNnf = SimpleExpressionCanonicalizer.toNnf(inputFilterCondition,
      rexBuilder);
    RexNode inputJoinConditionNnf = SimpleExpressionCanonicalizer.toNnf(inputJoinCondition,
      rexBuilder);

    // Prune if some child nodes are the superset of some others
    this.inputFilterConditionPruned = EnhancedFilterJoinPruner.pruneSuperset(
      inputFilterConditionNnf, true, rexBuilder);
    this.inputJoinConditionPruned = EnhancedFilterJoinPruner.pruneSuperset(
      inputJoinConditionNnf, true, rexBuilder);

    // Construct left and right bitSet
    List<RelDataTypeField> leftFields = joinRel.getInputs().get(0).getRowType().getFieldList();
    List<RelDataTypeField> rightFields = joinRel.getInputs().get(1).getRowType().getFieldList();
    int nFieldsLeft = leftFields.size();
    int nFieldsRight = rightFields.size();
    this.leftBitSet = ImmutableBitSet.range(0, nFieldsLeft);
    this.rightBitSet = ImmutableBitSet.range(nFieldsLeft, nFieldsLeft + nFieldsRight);
  }

  /**
   * Extract join condition and pushdown predicates, also simplify the remaining filter
   * @return An {@link EnhancedFilterJoinExtraction}, including join condition, pushdown predicates,
   * simplified remaining filter and simplified join type.
   */
  public EnhancedFilterJoinExtraction extract() {
    // Simplify the outer join
    ImmutableList<RexNode> aboveFilters = ImmutableList.copyOf(
      RelOptUtil.conjunctions(inputFilterConditionPruned));
    JoinRelType joinType = joinRel.getJoinType();
    if (!aboveFilters.isEmpty() && joinType != JoinRelType.INNER) {
      joinType = RelOptUtil.simplifyJoin(joinRel, aboveFilters, joinType);
    }

    // Perform extraction
    switch (joinType) {
      case INNER: {
        return extractInnerJoin();
      }
      case LEFT: {
        return extractLeftOuterJoin();
      }
      case RIGHT: {
        return extractRightOuterJoin();
      }
      case FULL: {
        return extractFullOuterJoin();
      }
      default:
        throw new UnsupportedOperationException();
    }
  }

  /**
   * Perform extraction for INNER join
   */
  private EnhancedFilterJoinExtraction extractInnerJoin() {
    // Pull inputJoinCondition up to inputFilterCondition and make them together, to handle the case
    // that one is a superset of the other.
    RexNode inputCondition = RexUtil.composeConjunction(rexBuilder,
      Lists.newArrayList(inputFilterConditionPruned, inputJoinConditionPruned), false);

    // Prune if some child nodes are the superset of some others, single level is enough because
    // we did pruning for the to child nodes before
    RexNode inputConditionPruned = EnhancedFilterJoinPruner.pruneSuperset(
      inputCondition, false, rexBuilder);

    // Extract the join condition
    Pair<RexNode, RexNode> joinConditionExtraction =
      extractJoinCondition(inputConditionPruned, JoinRelType.INNER);
    RexNode joinCondition = joinConditionExtraction.getKey();
    RexNode remainingFilter = joinConditionExtraction.getValue();

    // Extract left pushdown predicate
    Pair<RexNode, RexNode> leftPushdownPredicateExtraction =
      extractPushdownPredicates(remainingFilter, JoinRelType.INNER, leftBitSet);
    RexNode leftPushdownPredicate = leftPushdownPredicateExtraction.getKey();
    remainingFilter = leftPushdownPredicateExtraction.getValue();

    // Extract right pushdown predicate
    Pair<RexNode, RexNode> rightPushdownPredicateExtraction =
      extractPushdownPredicates(remainingFilter, JoinRelType.INNER, rightBitSet);
    RexNode rightPushdownPredicate = rightPushdownPredicateExtraction.getKey();
    remainingFilter = rightPushdownPredicateExtraction.getValue();

    // Construct EnhancedFilterJoinExtraction, make remainingJoinCondition as true because
    // we pulled up inputJoinConditions before.
    return new EnhancedFilterJoinExtraction(inputFilterConditionPruned, inputJoinConditionPruned,
      joinCondition, leftPushdownPredicate, rightPushdownPredicate, remainingFilter,
      JoinRelType.INNER);
  }

  /**
   * Perform extraction for LEFT outer join
   * As the LEFT join is the type that already simplified, so there are nulls on the right:
   *  join condition [filter -> join]:          NO
   *  left filter [filter -> left of join]:     YES
   *  right filter [filter -> right of join]:   NO
   *  left filter [join -> left of join]:       NO
   *  right filter [join -> right of join]:     YES
   */
  private EnhancedFilterJoinExtraction extractLeftOuterJoin() {
    // Extract left pushdown predicates from inputFilterCondition
    Pair<RexNode, RexNode> leftPushdownPredicateExtraction =
      extractPushdownPredicates(inputFilterConditionPruned, JoinRelType.LEFT, leftBitSet);
    RexNode leftPushdownPredicate = leftPushdownPredicateExtraction.getKey();
    RexNode remainingFilterCondition = leftPushdownPredicateExtraction.getValue();

    // Extract right pushdown predicates from inputJoinCondition
    Pair<RexNode, RexNode> rightPushdownPredicateExtraction =
      extractPushdownPredicates(inputJoinConditionPruned, JoinRelType.LEFT, rightBitSet);
    RexNode rightPushdownPredicate = rightPushdownPredicateExtraction.getKey();
    RexNode remainingJoinCondition = rightPushdownPredicateExtraction.getValue();

    // Make EnhancedFilterJoinExtraction
    return new EnhancedFilterJoinExtraction(inputFilterConditionPruned, inputJoinConditionPruned,
      remainingJoinCondition, leftPushdownPredicate, rightPushdownPredicate,
      remainingFilterCondition, JoinRelType.LEFT);
  }

  /**
   * Perform extraction for RIGHT outer join
   * As the RIGHT join is the type that already simplified, so there are nulls on the left:
   *  join condition [filter -> join]:          NO
   *  left filter [filter -> left of join]:     NO
   *  right filter [filter -> right of join]:   YES
   *  left filter [join -> left of join]:       YES
   *  right filter [join -> right of join]:     NO
   */
  private EnhancedFilterJoinExtraction extractRightOuterJoin() {
    // Extract left pushdown predicates from inputJoinCondition
    Pair<RexNode, RexNode> leftPushdownPredicateExtraction =
      extractPushdownPredicates(inputJoinConditionPruned, JoinRelType.RIGHT, leftBitSet);
    RexNode leftPushdownPredicate = leftPushdownPredicateExtraction.getKey();
    RexNode remainingJoinCondition = leftPushdownPredicateExtraction.getValue();

    // Extract right pushdown predicates from inputFilterCondition
    Pair<RexNode, RexNode> rightPushdownPredicateExtraction =
      extractPushdownPredicates(inputFilterConditionPruned, JoinRelType.RIGHT, rightBitSet);
    RexNode rightPushdownPredicate = rightPushdownPredicateExtraction.getKey();
    RexNode remainingFilterCondition = rightPushdownPredicateExtraction.getValue();

    // Make EnhancedFilterJoinExtraction
    return new EnhancedFilterJoinExtraction(inputFilterConditionPruned, inputJoinConditionPruned,
      remainingJoinCondition, leftPushdownPredicate, rightPushdownPredicate,
      remainingFilterCondition, JoinRelType.RIGHT);
  }

  /**
   * Perform extraction for FULL outer join
   * As the FULL join is the type that already simplified, so there are nulls on both sides:
   *  join condition [filter -> join]:          NO
   *  left filter [filter -> left of join]:     NO
   *  right filter [filter -> right of join]:   NO
   *  left filter [join -> left of join]:       NO
   *  right filter [join -> right of join]:     NO
   */
  private EnhancedFilterJoinExtraction extractFullOuterJoin() {
    // Make EnhancedFilterJoinExtraction
    return new EnhancedFilterJoinExtraction(inputFilterConditionPruned, inputJoinConditionPruned,
      inputJoinConditionPruned, rexBuilder.makeLiteral(true), rexBuilder.makeLiteral(true),
      inputFilterConditionPruned, JoinRelType.FULL);
  }

  /**
   * Extract the join condition from a RexNode, and simplify the remaining filter
   * @param inputFilter The input RexNode to extract
   * @param joinType Type of join
   * @return A pair of extracted filter and the simplified remaining filter
   */
  private Pair<RexNode, RexNode> extractJoinCondition(RexNode inputFilter, JoinRelType joinType) {
    return extract(inputFilter, true, joinType, true,
      (leafFilter) -> {
        RelOptUtil.InputFinder inputFinder = RelOptUtil.InputFinder.analyze(leafFilter);
        ImmutableBitSet filterBitSet = inputFinder.build();
        return !leftBitSet.contains(filterBitSet) && !rightBitSet.contains(filterBitSet);
      });
  }

  /**
   * Extract the pushdown predicates from a RexNode for the left/right side, and simplify the
   * remaining filter
   * @param inputFilter The input RexNode to extract
   * @param joinType Type of join
   * @param targetBitSet Field bitSets of the left/right side
   * @return A pair of extracted filter and the simplified remaining filter
   */
  private Pair<RexNode, RexNode> extractPushdownPredicates(RexNode inputFilter,
    JoinRelType joinType, ImmutableBitSet targetBitSet) {
    return extract(inputFilter, true, joinType, false,
      (leafFilter) -> {
        RelOptUtil.InputFinder inputFinder = RelOptUtil.InputFinder.analyze(leafFilter);
        ImmutableBitSet filterBitSet = inputFinder.build();
        return targetBitSet.contains(filterBitSet);
      });
  }

  /**
   * Extract required join condition / pushdown predicates, and simplify the remaining filter
   * @param filter Filter to extract
   * @param isRoot Whether the current filter is the root, to handle the case that a filter is entirely
   *               pushed down
   * @param joinType Type of join
   * @param pushToJoin Whether the extracted condition is pushed into join or below
   * @param leafValidator Function to check if a leaf node can be extracted
   * @return A pair of extracted predicates and the simplified remaining filter
   */
  private Pair<RexNode, RexNode> extract(RexNode filter, boolean isRoot, JoinRelType joinType,
    boolean pushToJoin, Predicate<RexNode> leafValidator) {
    SqlKind sqlKind = filter.getKind();

    switch (sqlKind) {
      case AND:
      case OR: {
        List<RexNode> childNodes = MoreRelOptUtil.conDisjunctions(filter);
        List<RexNode> extractedChildNodes = Lists.newArrayList();
        List<Pair<RexNode, RexNode>> extractions = Lists.newArrayList();

        // Extract for each child node
        for (RexNode childNode : childNodes) {
          Pair<RexNode, RexNode> extraction =
            extract(childNode, false, joinType, pushToJoin, leafValidator);
          extractions.add(extraction);
          RexNode extractedChildNode = extraction.getKey();
          if (!extractedChildNode.isAlwaysTrue() &&
            !(pushToJoin && !predicate.apply(joinRel, joinType, extractedChildNode))) {
            extractedChildNodes.add(extractedChildNode);
          } else if (sqlKind == SqlKind.OR) {
            // Cannot extract pushdown predicates
            return Pair.of(rexBuilder.makeLiteral(true), filter);
          }
        }

        // Make the resulting con/disjunction
        RexNode extractedFilter = MoreRelOptUtil.composeConDisjunction(
          rexBuilder, extractedChildNodes, false, sqlKind);
        RexNode simplifiedRemainingFilter = EnhancedFilterJoinSimplifier.
          simplifyConDisjunction(childNodes, extractions, sqlKind, isRoot, rexBuilder);

        // Prune superset
        RexNode extractedFilterPruned = EnhancedFilterJoinPruner.pruneSuperset(
          extractedFilter, false, rexBuilder);
        RexNode simplifiedRemainingFilterPruned = EnhancedFilterJoinPruner.pruneSuperset(
          simplifiedRemainingFilter, false, rexBuilder);

        return Pair.of(extractedFilterPruned, simplifiedRemainingFilterPruned);
      }

      default:
        if (leafValidator.test(filter) && !(pushToJoin && !predicate.apply(joinRel, joinType, filter))) {
          if (isRoot) {
            return Pair.of(filter, rexBuilder.makeLiteral(true));
          } else {
            return Pair.of(filter, filter);
          }
        } else {
          return Pair.of(rexBuilder.makeLiteral(true), filter);
        }
    }
  }
}


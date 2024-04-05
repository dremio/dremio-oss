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

import com.dremio.exec.planner.common.FlattenRelBase;
import com.dremio.extra.exec.store.dfs.parquet.pushdownfilter.FilterExtractor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;

public class PushFilterPastFlattenrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new PushFilterPastFlattenrule();

  private PushFilterPastFlattenrule() {
    super(RelOptHelper.any(Filter.class, FlattenRelBase.class), "PushFilterPastFlattenRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final FlattenRelBase flatten = call.rel(1);
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

    final ImmutableBitSet flattenIndices = ImmutableBitSet.of(flatten.getFlattenedIndices());
    final RexNode filterCondition = filter.getCondition();

    // Extract the parts of filter which can be pushed down the flatten node.
    RexNode belowFilter =
        FilterExtractor.extractFilter(
            rexBuilder,
            filterCondition,
            rexNode -> {
              InputFinder inputFinder = InputFinder.analyze(rexNode);
              return !flattenIndices.intersects(inputFinder.build());
            });

    // We have nothing to push down.
    if (belowFilter.isAlwaysTrue()) {
      return;
    }

    List<RexNode> belowFilters = RelOptUtil.conjunctions(belowFilter);

    // Remove the conditions that we already pushed down from the original filter to simplify it.
    final RexNode aboveFilter =
        simplifyFilterCondition(
            rexBuilder, filterCondition, belowFilters.stream().collect(Collectors.toSet()));

    final RelMetadataQuery mq = flatten.getCluster().getMetadataQuery();
    // Retrieve predicates which are already present below the flatten
    final RelOptPredicateList preds = mq.getPulledUpPredicates(flatten.getInput());

    // filter out the predicates which are already present below the flatten, so that we don't push
    // it down again.
    belowFilter =
        RexUtil.composeConjunction(
            rexBuilder,
            belowFilters.stream()
                .filter(rexNode -> !preds.pulledUpPredicates.contains(rexNode))
                .collect(Collectors.toList()),
            false);

    // create a filter from the pushed down predicates.
    RelNode filterBelowFlatten =
        belowFilter.isAlwaysTrue()
            ? flatten.getInput()
            : filter.copy(filter.getTraitSet(), flatten.getInput(), belowFilter);
    // change the input of flatten to the above filter.
    RelNode flattenWithNewInput =
        flatten.copy(flatten.getTraitSet(), Arrays.asList(filterBelowFlatten));
    // change the original filter with this new simplified filter after push down.
    RelNode filterAboveFlatten =
        aboveFilter.isAlwaysTrue()
            ? flattenWithNewInput
            : filter.copy(filter.getTraitSet(), flattenWithNewInput, aboveFilter);
    call.transformTo(filterAboveFlatten);
  }

  /**
   * Simplifies the filter condition to exclude the pushed down predicates.
   *
   * @param rexBuilder
   * @param filterCondition condition to simplify.
   * @param belowPredicates predicates that needs to be excluded.
   * @return simplified filter condition.
   */
  private RexNode simplifyFilterCondition(
      RexBuilder rexBuilder, RexNode filterCondition, Set<RexNode> belowPredicates) {
    if (belowPredicates.contains(filterCondition)) {
      return rexBuilder.makeLiteral(true);
    }
    if (filterCondition instanceof RexCall) {
      RexCall rexCall = (RexCall) filterCondition;
      switch (rexCall.getOperator().getKind()) {
        case AND:
          {
            List<RexNode> nodeList = new ArrayList<>();
            for (RexNode rexNode : rexCall.getOperands()) {
              nodeList.add(simplifyFilterCondition(rexBuilder, rexNode, belowPredicates));
            }
            return RexUtil.composeConjunction(rexBuilder, nodeList, false);
          }
        case OR:
          {
            List<RexNode> nodeList = new ArrayList<>();
            for (RexNode rexNode : rexCall.getOperands()) {
              RexNode sub = simplifyFilterCondition(rexBuilder, rexNode, belowPredicates);
              if (sub.isAlwaysTrue()) {
                return rexBuilder.makeLiteral(true);
              }
              nodeList.add(sub);
            }
            return RexUtil.composeDisjunction(rexBuilder, nodeList, false);
          }
      }
    }
    return filterCondition;
  }
}

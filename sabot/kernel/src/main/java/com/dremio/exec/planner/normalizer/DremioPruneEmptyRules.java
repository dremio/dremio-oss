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
package com.dremio.exec.planner.normalizer;

import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.operandJ;
import static org.apache.calcite.plan.RelOptRule.unordered;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.tools.RelBuilder;

public final class DremioPruneEmptyRules {

  public static final RelOptRule INTERSECT_INSTANCE = PruneEmptyRules.INTERSECT_INSTANCE;
  public static final RelOptRule PROJECT_INSTANCE = PruneEmptyRules.PROJECT_INSTANCE;
  public static final RelOptRule FILTER_INSTANCE = PruneEmptyRules.FILTER_INSTANCE;
  public static final RelOptRule SORT_INSTANCE = PruneEmptyRules.SORT_INSTANCE;
  public static final RelOptRule SORT_FETCH_ZERO_INSTANCE =
      PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE;
  public static final RelOptRule AGGREGATE_INSTANCE = PruneEmptyRules.AGGREGATE_INSTANCE;
  public static final RelOptRule JOIN_LEFT_INSTANCE = CalcitePruneEmptyRules.JOIN_LEFT_INSTANCE;
  public static final RelOptRule JOIN_RIGHT_INSTANCE = CalcitePruneEmptyRules.JOIN_RIGHT_INSTANCE;
  public static final RelOptRule CORRELATE_RIGHT_INSTANCE =
      CalcitePruneEmptyRules.CORRELATE_RIGHT_INSTANCE;

  // Calcite has a bug where UNIONS with one non-empty child doesn't preserve the all flag.
  public static final RelOptRule UNION_INSTANCE =
      new RelOptRule(
          operand(Union.class, unordered(operandJ(Values.class, null, Values::isEmpty, none()))),
          "Union") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          final Union union = call.rel(0);
          final List<RelNode> inputs = union.getInputs();
          assert inputs != null;
          final RelBuilder builder = call.builder();
          int nonEmptyInputs = 0;
          for (RelNode input : inputs) {
            if (!isEmpty(input)) {
              builder.push(input);
              nonEmptyInputs++;
            }
          }
          assert nonEmptyInputs < inputs.size()
              : "planner promised us at least one Empty child: " + RelOptUtil.toString(union);
          if (nonEmptyInputs == 0) {
            builder.push(union).empty();
          } else if (nonEmptyInputs == 1) {
            if (!union.all) {
              builder.distinct();
            }
          } else {
            builder.union(union.all, nonEmptyInputs);
          }

          builder.convert(union.getRowType(), true);
          call.transformTo(builder.build());
        }
      };

  // Calcite has a bug where MINUS with one non-empty child doesn't preserve the all flag.
  public static final RelOptRule MINUS_INSTANCE =
      new RelOptRule(
          operand(Minus.class, unordered(operandJ(Values.class, null, Values::isEmpty, none()))),
          "Minus") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          final Minus minus = call.rel(0);
          final List<RelNode> inputs = minus.getInputs();
          assert inputs != null;
          int nonEmptyInputs = 0;
          final RelBuilder builder = call.builder();
          for (RelNode input : inputs) {
            if (!isEmpty(input)) {
              builder.push(input);
              nonEmptyInputs++;
            } else if (nonEmptyInputs == 0) {
              // If the first input of Minus is empty, the whole thing is
              // empty.
              break;
            }
          }
          assert nonEmptyInputs < inputs.size()
              : "planner promised us at least one Empty child: " + RelOptUtil.toString(minus);
          if (nonEmptyInputs == 0) {
            builder.push(minus).empty();
          } else if (nonEmptyInputs == 1) {
            if (!minus.all) {
              builder.distinct();
            }
          } else {
            builder.minus(minus.all, nonEmptyInputs);
          }
          builder.convert(minus.getRowType(), true);
          call.transformTo(builder.build());
        }
      };

  public static final List<RelOptRule> ALL_RULES =
      ImmutableList.of(
          DremioAggregateRemoveRule.Config.DEFAULT.toRule(),
          CoreRules.AGGREGATE_UNION_AGGREGATE_FIRST,
          CoreRules.AGGREGATE_UNION_AGGREGATE_SECOND,
          CoreRules.UNION_TO_DISTINCT,
          UNION_INSTANCE,
          INTERSECT_INSTANCE,
          MINUS_INSTANCE,
          PROJECT_INSTANCE,
          FILTER_INSTANCE,
          SORT_INSTANCE,
          SORT_FETCH_ZERO_INSTANCE,
          AGGREGATE_INSTANCE,
          JOIN_LEFT_INSTANCE,
          JOIN_RIGHT_INSTANCE,
          CORRELATE_RIGHT_INSTANCE);

  private DremioPruneEmptyRules() {}

  // Copied from Calcite
  private static boolean isEmpty(RelNode node) {
    if (node instanceof Values) {
      return ((Values) node).getTuples().isEmpty();
    }
    if (node instanceof HepRelVertex) {
      return isEmpty(((HepRelVertex) node).getCurrentRel());
    }
    // Note: relation input might be a RelSubset, so we just iterate over the relations
    // in order to check if the subset is equivalent to an empty relation.
    if (!(node instanceof RelSubset)) {
      return false;
    }
    RelSubset subset = (RelSubset) node;
    for (RelNode rel : subset.getRels()) {
      if (isEmpty(rel)) {
        return true;
      }
    }
    return false;
  }
}

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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.logical.LogicalFilter}
 * past a {@link org.apache.calcite.rel.logical.LogicalWindow}
 * if the filter is in the PARTITION BY expression of the OVER clause.
 */
public class FilterWindowTransposeRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new FilterWindowTransposeRule();

  private FilterWindowTransposeRule() {
    super(operand(LogicalFilter.class,
        operand(LogicalWindow.class, any())), "FilterWindowTransposeRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = (LogicalFilter) call.rel(0);
    final LogicalWindow window = (LogicalWindow) call.rel(1);
    List<RexNode> pushable = new ArrayList<>();
    List<RexNode> notPushable = new ArrayList<>();

    ImmutableBitSet.Builder keys = ImmutableBitSet.builder();
    keys.addAll(window.groups.get(0).keys);
    window.groups.forEach(g -> keys.intersect(g.keys));

    RelOptUtil.splitFilters(keys.build(), filter.getCondition(), pushable, notPushable);

    if (pushable.size() == 0) {
      return;
    }

    RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    RexNode combinedPushableCondition = RexUtil.composeConjunction(rexBuilder, pushable, true);
    final LogicalFilter filterBelowWindow = LogicalFilter.create(window.getInput(), combinedPushableCondition);

    final RelDataTypeFactory.Builder outputBuilder =
      window.getCluster().getTypeFactory().builder();
    outputBuilder.addAll(window.getRowType().getFieldList());

    final LogicalWindow newLogicalWindow =
      LogicalWindow.create(window.getTraitSet(), filterBelowWindow,
        window.constants, outputBuilder.build(), window.groups);

    if (notPushable.size() == 0) {
      call.transformTo(newLogicalWindow);
      return;
    }

    RexNode combinedNotPushableCondition = RexUtil.composeConjunction(rexBuilder, notPushable, true);
    final LogicalFilter filterAboveLogicalWindow = LogicalFilter.create(newLogicalWindow, combinedNotPushableCondition);

    call.transformTo(filterAboveLogicalWindow);
  }
}

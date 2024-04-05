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
package com.dremio.exec.planner.rules;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

/**
 * Dremio implementation of FilterSetOpTransposeRule. It makes an extra check while pushing filter
 * past union to avoid unnecessary pushdown and getting into an infinite loop. I.e., It makes two
 * extra checks for unions, 1> If the filter is already present in the specific input, it will not
 * push the filter onto that input. 2> In case of filter is not pushable to all the inputs, it will
 * not remove the filter on top of union.
 */
public class DremioFilterSetOpTransposeRule extends FilterSetOpTransposeRule {

  protected DremioFilterSetOpTransposeRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {

    Filter filterRel = call.rel(0);
    SetOp setOp = call.rel(1);

    if (!(setOp instanceof Union)) {
      super.onMatch(call);
    }

    RexNode condition = filterRel.getCondition();
    RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
    RelBuilder relBuilder = call.builder();
    List<RelDataTypeField> origFields = setOp.getRowType().getFieldList();
    int[] adjustments = new int[origFields.size()];
    List<RelNode> newSetOpInputs = new ArrayList<>();

    boolean shouldBePruned = true;
    boolean shouldBeTransformed = false;

    for (RelNode input : setOp.getInputs()) {
      RexNode newCondition =
          condition.accept(
              new RelOptUtil.RexInputConverter(
                  rexBuilder, origFields, input.getRowType().getFieldList(), adjustments));
      /*
        check if the condition is pushable to this input.
        E.g., Let's say we want to push the filter expression or(=(1, a), =(2, a)) down the Union.
        Now on left side of Union we have a filter =(1, a), then there is no meaning in pushing this filter expression.
        We also look for the same filter (or(=(1, a), =(2, a))) in the input as we do not want to push it down either.
      */
      if (MoreRelOptUtil.getPushableFilter(call.getMetadataQuery(), input, newCondition, rexBuilder)
          .isAlwaysTrue()) {
        // We found that filter is not pushable on this input, in this case, we do not want to prune
        // the top filter
        // as it could cause an infinite loop by pushing the same filter again and again by some
        // other rule.
        newSetOpInputs.add(input);
        shouldBePruned = false;
      } else {
        // We need to push condition to this input as it is beneficial to push filter further down.
        // We do not want to transform the rel node unless we have a filter expression to push down
        // as it invokes
        // unnecessary rule calls.
        newSetOpInputs.add(relBuilder.push(input).filter(new RexNode[] {newCondition}).build());
        shouldBeTransformed = true;
      }
    }

    SetOp newSetOp = setOp.copy(setOp.getTraitSet(), newSetOpInputs);
    if (shouldBeTransformed) {
      if (shouldBePruned) {
        call.transformTo(newSetOp);
      } else {
        // Maintain the top filter.
        call.transformTo(filterRel.copy(filterRel.getTraitSet(), newSetOp, condition));
      }
    }
  }
}

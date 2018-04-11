/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

/**
 * Pushes projections used in filter past flatten.
 */
public class PushFiltersProjectPastFlattenRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new PushFiltersProjectPastFlattenRule();

  private PushFiltersProjectPastFlattenRule() {
    super(RelOptHelper.any(FilterRel.class, FlattenRel.class), "PushFiltersProjectPastFlattenRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final FilterRel filter = call.rel(0);
    return !filter.isAlreadyPushedDown();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final FilterRel filter = call.rel(0);
    final FlattenRel flatten = call.rel(1);
    final RelNode child = flatten.getInput();

    RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    List<RexNode> rexProjExprs = new ArrayList<>();
    List<RexNode> rexItemExprs = new ArrayList<>();
    for (int index = 0; index < child.getRowType().getFieldCount(); index++) {
      rexProjExprs.add(rexBuilder.makeInputRef(child.getRowType().getFieldList().get(index).getType(), index));
      rexItemExprs.add(rexBuilder.makeInputRef(child.getRowType().getFieldList().get(index).getType(), index));
    }
    rexItemExprs.add(filter.getCondition());

    ProjectForFlattenRel projectForFlattenRel = new ProjectForFlattenRel(
        flatten.getCluster(),
        flatten.getTraitSet(),
        child,
        child.getRowType(),
        rexProjExprs,
        rexItemExprs);
    FlattenRel newFlatten = new FlattenRel(
        flatten.getCluster(),
        flatten.getTraitSet(),
        projectForFlattenRel,
        flatten.getToFlatten(),
        flatten.getNumProjectsPushed() + 1);
    FilterRel newFilter = new FilterRel(filter.getCluster(), filter.getTraitSet(), newFlatten, filter.getCondition(), true);
    call.transformTo(newFilter);
  }

}

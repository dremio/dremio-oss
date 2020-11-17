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
import org.apache.calcite.rex.RexNode;

public class MergeProjectForFlattenRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new MergeProjectForFlattenRule();

  private MergeProjectForFlattenRule() {
    super(RelOptHelper.some(ProjectForFlattenRel.class, RelOptHelper.any(ProjectForFlattenRel.class)), "MergeProjectForFlattenRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    ProjectForFlattenRel top = call.rel(0);
    ProjectForFlattenRel bottom = call.rel(1);

    try {
      ProjectRel temporary = ProjectRel.create(bottom.getCluster(), bottom.getTraitSet(), bottom.getInput(), bottom.getProjExprs(), bottom.getRowType());
      List<RexNode> newProjExprs = RelOptUtil.pushPastProject(top.getProjExprs(), temporary);
      List<RexNode> newItemExprs = new ArrayList<>(top.getStructuredColumnExprs().size() + bottom.getStructuredColumnExprs().size());
      newItemExprs.addAll(RelOptUtil.pushPastProject(top.getStructuredColumnExprs(), temporary));
      newItemExprs.addAll(bottom.getStructuredColumnExprs());

      ProjectForFlattenRel newProjectForFlatten = new ProjectForFlattenRel(
        top.getCluster(), top.getTraitSet(), bottom.getInput(), top.getRowType(), newProjExprs, newItemExprs);
      call.transformTo(newProjectForFlatten);
    } catch (Exception | AssertionError ignored) {}
  }
}

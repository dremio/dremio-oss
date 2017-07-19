/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexNode;

public class PushProjectForFlattenPastProjectRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new PushProjectForFlattenPastProjectRule();

  private PushProjectForFlattenPastProjectRule() {
    super(RelOptHelper.some(ProjectForFlattenRel.class, RelOptHelper.any(ProjectRel.class)), "PushProjectForFlattenPastProjectRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    ProjectForFlattenRel projectForFlattenRel = call.rel(0);
    ProjectRel project = call.rel(1);

    List<RexNode> newItemsExprs = RelOptUtil.pushPastProject(projectForFlattenRel.getItemExprs(), project);
    List<RexNode> newProjExprs = RelOptUtil.pushPastProject(projectForFlattenRel.getProjExprs(), project);
    ProjectForFlattenRel newProjectForFlattenRel = new ProjectForFlattenRel(
            projectForFlattenRel.getCluster(), projectForFlattenRel.getTraitSet(), project.getInput(), projectForFlattenRel.getRowType(), newProjExprs, newItemsExprs);
    call.transformTo(newProjectForFlattenRel);
  }
}

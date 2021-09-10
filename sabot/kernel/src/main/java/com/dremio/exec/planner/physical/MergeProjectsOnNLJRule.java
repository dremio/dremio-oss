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
package com.dremio.exec.planner.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import com.dremio.exec.planner.logical.RelOptHelper;

/**
 * Merge projects on NLJ.
 * This rule is to remove additional project generated after computing projected columns in join.
 */
public class MergeProjectsOnNLJRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new MergeProjectsOnNLJRule();

  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private MergeProjectsOnNLJRule() {
    super(RelOptHelper.some(ProjectPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(NestedLoopJoinPrel.class))), "MergeProjectsOnNLJRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectPrel topProject = call.rel(0);
    final ProjectPrel bottomProject = call.rel(1);
    final NestedLoopJoinPrel join = call.rel(2);
    final RelBuilder relBuilder = call.builder();

    final Permutation topPermutation = topProject.getPermutation();
    final Permutation bottomPermutation = bottomProject.getPermutation();
    if (topPermutation != null && bottomPermutation != null) {
      final Permutation product = topPermutation.product(bottomPermutation);
      relBuilder.push(bottomProject.getInput());
      List<RexNode> exprs = relBuilder.fields(product);

      call.transformTo(ProjectPrel.create(topProject.getCluster(), topProject.getTraitSet(), join, exprs, topProject.getRowType()));
    }

    final List<RexNode> newProjects =
      RelOptUtil.pushPastProject(topProject.getProjects(), bottomProject);
    final RelNode input = bottomProject.getInput();
    if (RexUtil.isIdentity(newProjects, input.getRowType())) {
      call.transformTo(input);
    }

    call.transformTo(ProjectPrel.create(topProject.getCluster(), topProject.getTraitSet(), join, newProjects, topProject.getRowType()));
  }
}

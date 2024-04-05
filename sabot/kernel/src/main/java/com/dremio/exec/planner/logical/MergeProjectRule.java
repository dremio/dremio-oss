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

import com.dremio.exec.planner.logical.FlattenVisitors.FlattenCounter;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Permutation;

public class MergeProjectRule extends RelOptRule {

  public static final MergeProjectRule CALCITE_INSTANCE =
      new MergeProjectRule(
          LogicalProject.class, RelFactories.LOGICAL_BUILDER, "ProjectMergeRuleCrel");
  public static final MergeProjectRule LOGICAL_INSTANCE =
      new MergeProjectRule(
          ProjectRel.class, DremioRelFactories.LOGICAL_BUILDER, "ProjectMergeRuleDrel");

  private MergeProjectRule(
      Class<? extends Project> class1, RelBuilderFactory relBuilderFactory, String name) {
    super(operand(class1, operand(class1, any())), relBuilderFactory, name);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  // TODO: go back to inheriting from Calcite's rule. Requires modifying the
  // construction signature to allow a user to set which project rels to match.
  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project topProject = call.rel(0);
    final Project bottomProject = call.rel(1);
    final RelBuilder relBuilder = call.builder();

    // merge projects assuming it doesn't alter the unique count of flattens.
    final FlattenCounter counter = new FlattenCounter();
    counter.add(topProject);
    counter.add(bottomProject);
    final int uniqueFlattens = counter.getCount();

    // If one or both projects are permutations, short-circuit the complex logic
    // of building a RexProgram.
    final Permutation topPermutation = topProject.getPermutation();
    if (topPermutation != null) {
      if (topPermutation.isIdentity()) {
        // Let ProjectRemoveRule handle this.
        return;
      }
      final Permutation bottomPermutation = bottomProject.getPermutation();
      if (bottomPermutation != null) {
        if (bottomPermutation.isIdentity()) {
          // Let ProjectRemoveRule handle this.
          return;
        }
        final Permutation product = topPermutation.product(bottomPermutation);
        relBuilder.push(bottomProject.getInput());
        List<RexNode> exprs = relBuilder.fields(product);
        relBuilder.project(exprs, topProject.getRowType().getFieldNames());

        if (FlattenVisitors.count(exprs) == uniqueFlattens) {
          call.transformTo(relBuilder.build());
        }
        return;
      }
    }

    final List<RexNode> newProjects =
        RelOptUtil.pushPastProject(topProject.getProjects(), bottomProject);
    final RelNode input = bottomProject.getInput();
    if (RexUtil.isIdentity(newProjects, input.getRowType()) && uniqueFlattens == 0) {
      call.transformTo(input);
      return;
    }

    // replace the two projects with a combined projection
    relBuilder.push(bottomProject.getInput());
    relBuilder.project(newProjects, topProject.getRowType().getFieldNames());
    if (FlattenVisitors.count(newProjects) == uniqueFlattens) {
      call.transformTo(relBuilder.build());
    }
  }
}

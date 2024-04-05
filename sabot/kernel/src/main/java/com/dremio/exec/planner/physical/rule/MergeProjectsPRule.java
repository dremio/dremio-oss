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
package com.dremio.exec.planner.physical.rule;

import com.dremio.exec.planner.physical.NestedLoopJoinPrel;
import com.dremio.exec.planner.physical.ProjectPrel;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

/** Merge projects */
public class MergeProjectsPRule extends RelRule<MergeProjectsPRule.Config> {
  /**
   * This rule is to remove additional project generated after computing projected columns in join.
   */
  public static final RelOptRule PROJECT_PROJECT_JOIN =
      Config.DEFAULT
          .withOperandSupplier(
              os1 ->
                  os1.operand(ProjectPrel.class)
                      .oneInput(
                          os2 ->
                              os2.operand(ProjectPrel.class)
                                  .oneInput(
                                      os3 -> os3.operand(NestedLoopJoinPrel.class).anyInputs())))
          .toRule();

  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private MergeProjectsPRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectPrel topProject = call.rel(0);
    final ProjectPrel bottomProject = call.rel(1);
    final RelBuilder relBuilder = call.builder();

    final Permutation topPermutation = topProject.getPermutation();
    final Permutation bottomPermutation = bottomProject.getPermutation();
    if (topPermutation != null && bottomPermutation != null) {
      final Permutation product = topPermutation.product(bottomPermutation);
      relBuilder.push(bottomProject.getInput());
      List<RexNode> exprs = relBuilder.fields(product);

      call.transformTo(
          ProjectPrel.create(
              topProject.getCluster(),
              topProject.getTraitSet(),
              bottomProject.getInput(),
              exprs,
              topProject.getRowType()));
    }

    final List<RexNode> newProjects =
        RelOptUtil.pushPastProject(topProject.getProjects(), bottomProject);
    final RelNode input = bottomProject.getInput();
    if (RexUtil.isIdentity(newProjects, input.getRowType())) {
      call.transformTo(input);
    }

    call.transformTo(
        ProjectPrel.create(
            topProject.getCluster(),
            topProject.getTraitSet(),
            bottomProject.getInput(),
            newProjects,
            topProject.getRowType()));
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        EMPTY
            .withDescription("MergeProjectsPRule")
            .withOperandSupplier(
                os1 ->
                    os1.operand(ProjectPrel.class)
                        .oneInput(os2 -> os2.operand(ProjectPrel.class).anyInputs()))
            .as(Config.class);

    @Override
    default MergeProjectsPRule toRule() {
      return new MergeProjectsPRule(this);
    }
  }
}

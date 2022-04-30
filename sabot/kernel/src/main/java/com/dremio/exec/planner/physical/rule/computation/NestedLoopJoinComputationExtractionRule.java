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
package com.dremio.exec.planner.physical.rule.computation;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

import com.dremio.exec.planner.physical.NestedLoopJoinPrel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.rules.DremioOptimizationRelRule;

/**
 * Visits the condition associated with NLJ, and pushes down expressions to projects below the join, reducing the amount
 * of computation required in the filter evaluation
 */
public class NestedLoopJoinComputationExtractionRule
  extends DremioOptimizationRelRule<NestedLoopJoinComputationExtractionRule.Config> {
  public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

  public NestedLoopJoinComputationExtractionRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final NestedLoopJoinPrel nestedLoopJoinPrel = call.rel(0);
    return !nestedLoopJoinPrel.hasVectorExpression();
  }

  @Override
  public void doOnMatch(RelOptRuleCall call) {
    final NestedLoopJoinPrel nlj = call.rel(0);

    JoinComputationExtractor.ExtractedComputation extractedComputation =
      JoinComputationExtractor.extractedComputation(nlj.getRowType(), nlj.getCondition(),
        nlj.getCondition(),
        nlj.getLeft(), nlj.getRight());
    if(null == extractedComputation) {
      return;
    }

    NestedLoopJoinPrel newJoin = NestedLoopJoinPrel.create(nlj.getCluster(), nlj.getTraitSet(),
      extractedComputation.left, extractedComputation.right,
      nlj.getJoinType(), extractedComputation.joinCondition);

    ProjectPrel projectPrel = ProjectPrel.create(newJoin.getCluster(),
      newJoin.getTraitSet(), newJoin, extractedComputation.topProject, nlj.getRowType());
    call.transformTo(projectPrel);
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
      .withDescription("NestedLoopJoinComputationExtractionRule")
      .withOperandSupplier(os1 -> os1.operand(NestedLoopJoinPrel.class).anyInputs())
      .as(Config.class);

    @Override default NestedLoopJoinComputationExtractionRule toRule() {
      return new NestedLoopJoinComputationExtractionRule(this);
    }
  }
}

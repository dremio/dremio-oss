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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCorrelate;

/**
 * Rule that converts a {@link org.apache.calcite.rel.logical.LogicalCorrelate} to a Dremio
 * equivalent {@link CorrelateRel}.
 */
public class CorrelateRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new CorrelateRule();

  private CorrelateRule() {
    super(RelOptHelper.any(LogicalCorrelate.class), "CorrelateRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalCorrelate correlate = call.rel(0);
    final RelTraitSet traits = correlate.getTraitSet().plus(Rel.LOGICAL);
    final RelNode left = correlate.getInput(0);
    final RelNode right = correlate.getInput(1);
    final RelNode convertedLeftInput =
        convert(left, left.getTraitSet().plus(Rel.LOGICAL).simplify());
    final RelNode convertedRightInput =
        convert(right, right.getTraitSet().plus(Rel.LOGICAL).simplify());
    CorrelateRel newRel =
        new CorrelateRel(
            correlate.getCluster(),
            traits,
            convertedLeftInput,
            convertedRightInput,
            correlate.getCorrelationId(),
            correlate.getRequiredColumns(),
            correlate.getJoinType());
    call.transformTo(newRel);
  }
}

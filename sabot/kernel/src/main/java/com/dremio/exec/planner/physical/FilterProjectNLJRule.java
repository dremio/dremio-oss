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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

/**
 * Capture Filter - Project - NLJ pattern and swap Filter and Project
 */
public class FilterProjectNLJRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new FilterProjectNLJRule();

  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private FilterProjectNLJRule() {
    super(operand(FilterPrel.class, operand(ProjectPrel.class, operand(NestedLoopJoinPrel.class, any()))), "FilterProjectNLJRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    FilterPrel filter = call.rel(0);
    ProjectPrel project = call.rel(1);
    if (RexOver.containsOver(project.getProjects(), null) ||
        RexUtil.containsCorrelation(filter.getCondition())) {
      return false;
    }

    NestedLoopJoinPrel join = call.rel(2);
    return join.getJoinType() == JoinRelType.INNER &&
        PrelUtil.getPlannerSettings(call.getPlanner()).getOptions().getOption(NestedLoopJoinPrel.VECTORIZED);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FilterPrel filter = call.rel(0);
    ProjectPrel project = call.rel(1);
    NestedLoopJoinPrel join = call.rel(2);
    RexNode newCondition = RelOptUtil.pushPastProject(filter.getCondition(), project);

    final RelBuilder relBuilder = call.builder();
    RelNode newFilterRel = filter.copy(filter.getTraitSet(),
      NestedLoopJoinPrel.create(join.getCluster(), join.getTraitSet(), join.getLeft(), join.getRight(), join.getJoinType(), join.getCondition(), join.getProjectedFields()),
    RexUtil.removeNullabilityCast(relBuilder.getTypeFactory(), newCondition));

    RelNode newProjRel = project.copy(project.getTraitSet(), newFilterRel, project.getProjects(), project.getRowType());
    call.transformTo(newProjRel);
  }
}

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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;


public class ProjectNLJMergeRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new ProjectNLJMergeRule();

  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private ProjectNLJMergeRule() {
    super(operand(ProjectPrel.class, operand(NestedLoopJoinPrel.class, any())), "ProjectNLJMergeRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    NestedLoopJoinPrel join = call.rel(1);
    return join.getJoinType() == JoinRelType.INNER &&
        PrelUtil.getPlannerSettings(call.getPlanner()).getOptions().getOption(NestedLoopJoinPrel.VECTORIZED);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    ProjectPrel project = call.rel(0);
    NestedLoopJoinPrel nlj = call.rel(1);

    ImmutableBitSet topProjectedColumns = InputFinder.bits(project.getProjects(), null);

    ImmutableBitSet bottomProjectedColumns = null;
    if (nlj.getProjectedFields() == null) {
      bottomProjectedColumns = ImmutableBitSet.range(nlj.getRowType().getFieldCount());
    } else {
      bottomProjectedColumns = nlj.getProjectedFields();
    }

    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    int field = 0;
    Mapping mapping = Mappings.create(MappingType.SURJECTION, bottomProjectedColumns.cardinality(), topProjectedColumns.cardinality());
    for (Ord<Integer> ord : Ord.zip(bottomProjectedColumns)) {
      if (topProjectedColumns.get(ord.i)) {
        builder.set(ord.e);
        mapping.set(ord.i, field);
        field++;
      }
    }

    if (builder.cardinality() == 0) {
      if (bottomProjectedColumns.cardinality() > 0) {
        //project at least one column
        builder.set(0);
      }
      else {
        return;
      }
    }

    ImmutableBitSet newJoinProjectedFields = builder.build();

    if (newJoinProjectedFields.equals(nlj.getProjectedFields())) {
      return;
    }

    RexShuttle shuttle = new RexPermuteInputsShuttle(mapping);
    List<RexNode> newProjects = shuttle.apply(project.getProjects());

    NestedLoopJoinPrel newJoin = (NestedLoopJoinPrel) nlj.copy(newJoinProjectedFields);
    ProjectPrel newProject = ProjectPrel.create(nlj.getCluster(), project.getTraitSet(), newJoin, newProjects, project.getRowType());
    call.transformTo(newProject);
  }
}

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

import static com.dremio.exec.planner.sql.handlers.RexFieldAccessUtils.STRUCTURED_WRAPPER;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

public class PushProjectForFlattenPastProjectRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new PushProjectForFlattenPastProjectRule();

  private PushProjectForFlattenPastProjectRule() {
    super(RelOptHelper.some(ProjectForFlattenRel.class, RelOptHelper.any(ProjectRel.class)), "PushProjectForFlattenPastProjectRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    ProjectForFlattenRel projectForFlattenRel = call.rel(0);
    ProjectRel project = call.rel(1);

    List<RexNode> newItemsExprs = pushPastProject(projectForFlattenRel.getStructuredColumnExprs(), project);
    List<RexNode> newProjExprs = pushPastProject(projectForFlattenRel.getProjExprs(), project);
    ProjectForFlattenRel newProjectForFlattenRel = new ProjectForFlattenRel(
      projectForFlattenRel.getCluster(), projectForFlattenRel.getTraitSet(), project.getInput(), projectForFlattenRel.getRowType(), newProjExprs, newItemsExprs);
    call.transformTo(newProjectForFlattenRel);
  }

  public static List<RexNode> pushPastProject(List<? extends RexNode> nodes, Project project) {
    final List<RexNode> list = new ArrayList<>();
    pushShuttle(project).visitList(nodes, list);
    return list;
  }

  /**
   * Shuttle that converts to equivalent expressions on the Project's input fields
   */
  private static RexShuttle pushShuttle(final Project project) {
    return new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef ref) {
        return project.getProjects().get(ref.getIndex());
      }

      @Override
      public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
        RexNode before = fieldAccess.getReferenceExpr();
        RexNode after = before.accept(this);
        if (before != after) {
          if (after instanceof RexCall) {
            if (((RexCall) after).getOperator().getName().equalsIgnoreCase(STRUCTURED_WRAPPER.getName())) {
              after = ((RexCall) after).getOperands().get(0);
            }
          }
          if (after instanceof RexInputRef) {
            RexBuilder rexBuilder = project.getCluster().getRexBuilder();
            RelDataType type = after.getType().getComponentType() == null ? after.getType() : after.getType().getComponentType();
            RexInputRef newInputRef = rexBuilder.makeInputRef(type, ((RexInputRef) after).getIndex());
            return rexBuilder.makeFieldAccess(newInputRef, fieldAccess.getField().getName(), true);
          }
          return after;
        }
        return fieldAccess;
      }
    };
  }
}

/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.PushProjector.ExprCondition;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

// TODO: modify calcite rule to support class types and remove this rule.
public class PushProjectPastJoinRule extends RelOptRule {

  public static final RelOptRule CALCITE_INSTANCE = new PushProjectPastJoinRule(
    LogicalProject.class,
    LogicalJoin.class,
    ExprCondition.TRUE,
    DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  public static final RelOptRule LOGICAL_INSTANCE = new PushProjectPastJoinRule(
    ProjectRel.class,
    JoinRel.class,
    ExprCondition.TRUE,
    DremioRelFactories.LOGICAL_BUILDER);

  private final PushProjector.ExprCondition preserveExprCondition;

  private PushProjectPastJoinRule(
      Class<? extends Project> projectClass,
      Class<? extends Join> joinClass,
      PushProjector.ExprCondition preserveExprCondition,
      RelBuilderFactory relFactory) {
    super(
        operand(projectClass,
            operand(joinClass, any())),
        relFactory, "PushProjectPastJoinRule" + projectClass.getSimpleName() + joinClass.getSimpleName());
    this.preserveExprCondition = preserveExprCondition;
  }

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    Project origProj = call.rel(0);
    final Join join = call.rel(1);

    if (join instanceof SemiJoin) {
      return; // TODO: support SemiJoin
    }
    // locate all fields referenced in the projection and join condition;
    // determine which inputs are referenced in the projection and
    // join condition; if all fields are being referenced and there are no
    // special expressions, no point in proceeding any further
    PushProjector pushProject =
        new PushProjector(
            origProj,
            join.getCondition(),
            join,
            preserveExprCondition,
            call.builder());
    if (pushProject.locateAllRefs()) {
      return;
    }

    // create left and right projections, projecting only those
    // fields referenced on each side
    RelNode leftProjRel =
        pushProject.createProjectRefsAndExprs(
            join.getLeft(),
            true,
            false);
    RelNode rightProjRel =
        pushProject.createProjectRefsAndExprs(
            join.getRight(),
            true,
            true);

    // convert the join condition to reference the projected columns
    RexNode newJoinFilter = null;
    int[] adjustments = pushProject.getAdjustments();
    if (join.getCondition() != null) {
      List<RelDataTypeField> projJoinFieldList =
          new ArrayList<>();
      projJoinFieldList.addAll(
          join.getSystemFieldList());
      projJoinFieldList.addAll(
          leftProjRel.getRowType().getFieldList());
      projJoinFieldList.addAll(
          rightProjRel.getRowType().getFieldList());
      newJoinFilter =
          pushProject.convertRefsAndExprs(
              join.getCondition(),
              projJoinFieldList,
              adjustments);
    }

    // create a new join with the projected children
    Join newJoinRel =
        join.copy(
            join.getTraitSet(),
            newJoinFilter,
            leftProjRel,
            rightProjRel,
            join.getJoinType(),
            join.isSemiJoinDone());

    // put the original project on top of the join, converting it to
    // reference the modified projection list
    RelNode topProject =
        pushProject.createNewProject(newJoinRel, adjustments);

    call.transformTo(topProject);
  }

}

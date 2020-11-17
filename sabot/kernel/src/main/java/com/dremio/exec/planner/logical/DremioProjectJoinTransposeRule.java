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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.PushProjector.ExprCondition;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilderFactory;

import com.dremio.exec.planner.sql.handlers.RexFieldAccessUtils;

public class DremioProjectJoinTransposeRule extends RelOptRule {
  public static final DremioProjectJoinTransposeRule INSTANCE;
  private final ExprCondition preserveExprCondition;

  private DremioProjectJoinTransposeRule(Class<? extends Project> projectClass, Class<? extends Join> joinClass, ExprCondition preserveExprCondition, RelBuilderFactory relFactory) {
    super(operand(projectClass, operand(joinClass, any()), new RelOptRuleOperand[0]), relFactory, (String)null);
    this.preserveExprCondition = preserveExprCondition;
  }

  public void onMatch(final RelOptRuleCall call) {
    Project origProj = (Project)call.rel(0);
    Join join = (Join)call.rel(1);
    Project wrappedProj = RexFieldAccessUtils.wrapProject(origProj, join, true);

    if (!(join instanceof SemiJoin)) {
      RexNode joinFilter = (RexNode)join.getCondition().accept(new RexShuttle() {
        public RexNode visitCall(RexCall rexCall) {
          RexNode node = super.visitCall(rexCall);
          return (RexNode)(!(node instanceof RexCall) ? node : RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall)node, call.builder().getRexBuilder()));
        }
      });
      PushProjector pushProject = new PushProjector(wrappedProj, joinFilter, join, this.preserveExprCondition, call.builder());
      if (!pushProject.locateAllRefs()) {
        RelNode leftProjRel = pushProject.createProjectRefsAndExprs(join.getLeft(), true, false);
        RelNode rightProjRel = pushProject.createProjectRefsAndExprs(join.getRight(), true, true);
        RexNode newJoinFilter = null;
        int[] adjustments = pushProject.getAdjustments();
        if (joinFilter != null) {
          List<RelDataTypeField> projJoinFieldList = new ArrayList();
          projJoinFieldList.addAll(join.getSystemFieldList());
          projJoinFieldList.addAll(leftProjRel.getRowType().getFieldList());
          projJoinFieldList.addAll(rightProjRel.getRowType().getFieldList());
          newJoinFilter = pushProject.convertRefsAndExprs(joinFilter, projJoinFieldList, adjustments);
        }

        Join newJoinRel = join.copy(join.getTraitSet(), newJoinFilter, leftProjRel, rightProjRel, join.getJoinType(), join.isSemiJoinDone());
        RelNode topProject = pushProject.createNewProject(newJoinRel, adjustments);
        call.transformTo(RexFieldAccessUtils.unwrap(topProject));
      }
    }
  }

  static {
    INSTANCE = new DremioProjectJoinTransposeRule(
      LogicalProject.class,
      LogicalJoin.class,
      ExprCondition.TRUE,
      DremioRelFactories.CALCITE_LOGICAL_BUILDER);
  }
}

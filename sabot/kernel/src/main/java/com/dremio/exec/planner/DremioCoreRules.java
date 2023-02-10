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
package com.dremio.exec.planner;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.UnionMergeRule;



public class DremioCoreRules {

  //just a workaround for the empty field list in projects. Need to find a formal way to deal with dummy projects.
  public static final RelOptRule JOIN_PROJECT_TRANSPOSE_LEFT = JoinProjectTransposeRule.Config.LEFT
    .withOperandSupplier(b0 ->
      b0.operand(Join.class).inputs(
        b1 -> b1.operand(Project.class).predicate(p->!p.getInput().getRowType().getFieldList().isEmpty()).anyInputs()))
    .toRule();

  public static final RelOptRule JOIN_PROJECT_TRANSPOSE_RIGHT = JoinProjectTransposeRule.Config.RIGHT
    .withOperandSupplier(b0 ->
      b0.operand(Join.class).inputs(
        b1 -> b1.operand(RelNode.class).predicate(p->!(p instanceof Project)).anyInputs(),
        b2 -> b2.operand(Project.class).predicate(p->!p.getInput().getRowType().getFieldList().isEmpty()).anyInputs()))
    .toRule();

  public static final RelOptRule UNION_MERGE_RULE = UnionMergeRule.Config.DEFAULT.withOperandFor(Union.class).toRule();

}

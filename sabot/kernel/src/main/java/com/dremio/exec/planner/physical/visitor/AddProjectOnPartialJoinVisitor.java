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

package com.dremio.exec.planner.physical.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.google.common.collect.Lists;

public class AddProjectOnPartialJoinVisitor extends BasePrelVisitor<Prel, Void, RuntimeException>{

  private static AddProjectOnPartialJoinVisitor INSTANCE = new AddProjectOnPartialJoinVisitor();

  public static Prel insertProject(Prel prel){
    return prel.accept(INSTANCE, null);
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, null);
      children.add(child);
    }

    return (Prel) prel.copy(prel.getTraitSet(), children);

  }

  @Override
  public Prel visitJoin(JoinPrel prel, Void value) throws RuntimeException {

    List<RelNode> children = Lists.newArrayList();

    for(Prel child : prel){
      child = child.accept(this, null);
      children.add(child);
    }

    if ((prel.getRowType().getFieldCount() != prel.getInputRowType().getFieldCount())
        || !(prel.getRowType().getFieldNames().equals(prel.getInputRowType().getFieldNames()))) {
      List<String> outputFieldNames = new ArrayList<>();
      List<RexNode> exprs = new ArrayList<>();
      List<RelDataTypeField> fields = prel.getRowType().getFieldList();
      for (RelDataTypeField field : fields) {
        RexNode expr = prel.getCluster().getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        exprs.add(expr);
        outputFieldNames.add(field.getName());
      }

      RelDataType rowType = RexUtil.createStructType(prel.getCluster().getTypeFactory(), exprs, outputFieldNames);

      JoinPrel newJoin = (JoinPrel) prel.copy(prel.getTraitSet(), children);
      ProjectPrel project = ProjectPrel.create(newJoin.getCluster(), newJoin.getTraitSet(), newJoin, exprs, rowType);
      return (Prel) project;
    }

    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

}

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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelConversionException;

import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.StarColumnHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.sql.OperatorTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SplitUpComplexExpressions {

  public static class SplitUpComplexExpressionsVisitor extends BasePrelVisitor<Prel, Object, RelConversionException> {
    OperatorTable table;
    FunctionImplementationRegistry funcReg;

    public SplitUpComplexExpressionsVisitor(OperatorTable table, FunctionImplementationRegistry funcReg) {
      super();
      this.table = table;
      this.funcReg = funcReg;
    }

    @Override
    public Prel visitPrel(Prel prel, Object value) throws RelConversionException {
      List<RelNode> children = Lists.newArrayList();
      for(Prel child : prel){
        child = child.accept(this, null);
        children.add(child);
      }
      return (Prel) prel.copy(prel.getTraitSet(), children);
    }

    @Override
    public Prel visitProject(ProjectPrel project, Object unused) throws RelConversionException {
      RelNode transformedInput = ((Prel)project.getInput()).accept(this, null);
      Prel accepted = (Prel) SplitUpComplexExpressions.visitProject(project, transformedInput, funcReg);
      if (accepted == null) {
        return (ProjectPrel) project.copy(project.getTraitSet(), Lists.newArrayList(transformedInput));
      } else {
        return accepted;
      }
    }

  }


  private static RelNode visitProject(Project project, RelNode input, FunctionImplementationRegistry funcReg) throws RelConversionException {
    // Apply the rule to the child
    final RelDataTypeFactory factory = project.getCluster().getTypeFactory();
    int i = 0;
    final int lastColumnReferenced = PrelUtil.getLastUsedColumnReference(project.getProjects());

    final int lastRexInput = lastColumnReferenced + 1;
    RexVisitorComplexExprSplitter exprSplitter = new RexVisitorComplexExprSplitter(project.getCluster(), funcReg, lastRexInput);


    List<RelDataTypeField> origRelDataTypes = new ArrayList<>();
    List<RexNode> exprList = new ArrayList<>();
    for (RexNode rex : project.getChildExps()) {
      origRelDataTypes.add(project.getRowType().getFieldList().get(i));
      i++;
      exprList.add(rex.accept(exprSplitter));
    }
    List<RexNode> complexExprs = exprSplitter.getComplexExprs();

    if (complexExprs.size() == 1 && findTopComplexFunc(funcReg, project.getChildExps()).size() == 1) {
      return null;
    }

    final RexBuilder builder = project.getCluster().getRexBuilder();
    List<RelDataTypeField> relDataTypes = new ArrayList<>();
    List<RexNode> allExprs = new ArrayList<>();
    int exprIndex = 0;
    List<String> fieldNames = input.getRowType().getFieldNames();
    for (int index = 0; index < lastRexInput; index++) {

      allExprs.add(builder.makeInputRef( factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.ANY), true), index));

      if(fieldNames.get(index).contains(StarColumnHelper.STAR_COLUMN)) {
        relDataTypes.add(new RelDataTypeFieldImpl(fieldNames.get(index), allExprs.size(), factory.createSqlType(SqlTypeName.ANY)));
      } else {
        relDataTypes.add(new RelDataTypeFieldImpl("EXPR$" + exprIndex, allExprs.size(), factory.createSqlType(SqlTypeName.ANY)));
        exprIndex++;
      }
    }
    RexNode currRexNode;
    int index = lastRexInput - 1;

    // if the projection expressions contained complex outputs, split them into their own individual projects
    if (complexExprs.size() > 0 ) {
      while (complexExprs.size() > 0) {
        if ( index >= lastRexInput ) {
          allExprs.remove(allExprs.size() - 1);
          allExprs.add(builder.makeInputRef( factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.ANY), true), index));
        }
        index++;
        exprIndex++;

        currRexNode = complexExprs.remove(0);
        allExprs.add(currRexNode);
        relDataTypes.add(new RelDataTypeFieldImpl("EXPR$" + exprIndex, allExprs.size(), factory.createSqlType(SqlTypeName.ANY)));
        input = project.copy(project.getTraitSet(), input, ImmutableList.copyOf(allExprs), new RelRecordType(relDataTypes));
      }
      // copied from above, find a better way to do this
      allExprs.remove(allExprs.size() - 1);
      allExprs.add(builder.makeInputRef( factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.ANY), true), index));
      relDataTypes.add(new RelDataTypeFieldImpl("EXPR$" + index, allExprs.size(), factory.createSqlType(SqlTypeName.ANY) ));
    }
    return project.copy(project.getTraitSet(), input, exprList, new RelRecordType(origRelDataTypes));
  }

  /**
   *  Find the list of expressions where Complex type function is at top level.
   */
  private static List<RexNode> findTopComplexFunc(FunctionImplementationRegistry funcReg, List<RexNode> exprs) {
    final List<RexNode> topComplexFuncs = new ArrayList<>();

    for (RexNode exp : exprs) {
      if (exp instanceof RexCall) {
        RexCall call = (RexCall) exp;
        String functionName = call.getOperator().getName();

        if (funcReg.isFunctionComplexOutput(functionName) ) {
          topComplexFuncs.add(exp);
        }
      }
    }

    return topComplexFuncs;
  }

}

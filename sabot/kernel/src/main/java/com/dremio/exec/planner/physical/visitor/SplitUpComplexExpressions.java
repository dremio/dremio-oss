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
import org.apache.calcite.rel.core.Filter;
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
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SplitUpComplexExpressions {

  public static class SplitUpComplexExpressionsVisitor extends BasePrelVisitor<Prel, Object, RelConversionException> {
    private final FunctionImplementationRegistry funcReg;

    public SplitUpComplexExpressionsVisitor(FunctionImplementationRegistry funcReg) {
      super();
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

    @Override
    public Prel visitFilter(FilterPrel filterPrel, Object unused) throws RelConversionException {
      PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(filterPrel.getCluster());
      if(plannerSettings.isSplitComplexFilterExpressionEnabled()) {
        RelNode transformedInput = ((Prel) filterPrel.getInput()).accept(this, null);
        Prel accepted = (Prel) SplitUpComplexExpressions.visitFilter(filterPrel, transformedInput, funcReg);
        if (accepted == null) {
          return (Prel) filterPrel.copy(filterPrel.getTraitSet(), Lists.newArrayList(transformedInput));
        } else {
          return accepted;
        }
      }
      return visitPrel(filterPrel, unused);
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
    for (RexNode rex : project.getProjects()) {
      origRelDataTypes.add(project.getRowType().getFieldList().get(i));
      i++;
      exprList.add(rex.accept(exprSplitter));
    }
    List<RexNode> complexExprs = exprSplitter.getComplexExprs();

    if (complexExprs.size() == 1 && findTopComplexFunc(funcReg, project.getProjects()).size() == 1) {
      return project;
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


  private static RelNode visitFilter(Filter filter, RelNode input, FunctionImplementationRegistry funcReg) throws RelConversionException {
    final RelDataTypeFactory factory = filter.getCluster().getTypeFactory();

    // Create Expressions based on input reference
    List<RexNode> origExprs = Lists.newArrayList();
    List<RelDataTypeField> origRelDataTypes = new ArrayList<>();

    for (RelDataTypeField field : input.getRowType().getFieldList()) {
      origRelDataTypes.add(field);
      RexNode expr = input.getCluster().getRexBuilder().makeInputRef(field.getType(), field.getIndex());
      origExprs.add(expr);
    }

    final int lastColumnReferenced = PrelUtil.getLastUsedColumnReference(origExprs);
    final int lastRexInput = lastColumnReferenced + 1;
    RexVisitorComplexExprSplitter topLevelComplexFilterExpression = new RexVisitorComplexExprSplitter.
      TopLevelComplexFilterExpression(filter.getCluster(), funcReg, lastRexInput);
    RexNode updatedFilterCondition = filter.getCondition().accept(topLevelComplexFilterExpression);
    List<RexNode> complexExprs = topLevelComplexFilterExpression.getComplexExprs();
    if(complexExprs.size() == 0){
      return null;
    }
    List<RexNode> underlyingProjectExprs = new ArrayList<>(origExprs);
    underlyingProjectExprs.addAll(topLevelComplexFilterExpression.getComplexExprs());

    List<RelDataTypeField> underlyingProjectDataTypes = new ArrayList<>(origRelDataTypes);
    // Add the new complex fields used
    for(int i = lastColumnReferenced+1; i<topLevelComplexFilterExpression.lastUsedIndex; i++){
      underlyingProjectDataTypes.add(new RelDataTypeFieldImpl("EXPR$" + i, i, factory.createSqlType(SqlTypeName.ANY)));
    }

    // Create new Project+ Filter + Project and visit underlying project since we only split top level complex expressions
    RelNode underlyingProject = visitProject(
      ProjectPrel.create(
        input.getCluster(),
        input.getTraitSet(),
        input,
        underlyingProjectExprs,
        new RelRecordType(underlyingProjectDataTypes)),
      input,
      funcReg);
   return ProjectPrel.create(
     filter.getCluster(), filter.getTraitSet(),
     filter.copy(filter.getTraitSet(), underlyingProject, updatedFilterCondition),
     origExprs, filter.getRowType());

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

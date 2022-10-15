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
import java.util.Locale;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.expr.fn.FunctionImplementationRegistry;

public class RexVisitorComplexExprSplitter extends RexVisitorImpl<RexNode> {

  final RelDataTypeFactory factory;
  final RexBuilder builder;
  final FunctionImplementationRegistry funcReg;
  final List<RexNode> complexExprs;
  int lastUsedIndex;

  public RexVisitorComplexExprSplitter(RelOptCluster cluster, FunctionImplementationRegistry funcReg, int firstUnused) {
    super(true);
    this.factory = cluster.getTypeFactory();
    this.builder = cluster.getRexBuilder();
    this.funcReg = funcReg;
    this.complexExprs = new ArrayList<>();
    this.lastUsedIndex = firstUnused;
  }

  public  List<RexNode> getComplexExprs() {
    return complexExprs;
  }

  @Override
  public RexNode visitInputRef(RexInputRef inputRef) {
    return inputRef;
  }

  @Override
  public RexNode visitLocalRef(RexLocalRef localRef) {
    return localRef;
  }

  @Override
  public RexNode visitLiteral(RexLiteral literal) {
    return literal;
  }

  @Override
  public RexNode visitOver(RexOver over) {
    return over;
  }

  @Override
  public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
    return correlVariable;
  }

  @Override
  public RexNode visitCall(RexCall call) {

    String functionName = call.getOperator().getName();

    List<RexNode> newOps = new ArrayList<>();
    for (RexNode operand : call.operands) {
      newOps.add(operand.accept(this));
    }
    if (funcReg.isFunctionComplexOutput(functionName)) {
      RexNode ret = builder.makeInputRef( factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.ANY), true), lastUsedIndex);
      lastUsedIndex++;
      complexExprs.add(call.clone(factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.ANY), true), newOps));
      return ret;
    }
    return call.clone(call.getType(), newOps);
  }

  @Override
  public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
    return dynamicParam;
  }

  @Override
  public RexNode visitRangeRef(RexRangeRef rangeRef) {
    return rangeRef;
  }

  @Override
  public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
    return fieldAccess;
  }

// Only top level complex expression will be detected by this visitor
// For eg - COMPLEX1(COMPLEX2, 3) only Complex1 will be detected and inner COMPLEX 2 will not be detected
  public static class TopLevelComplexFilterExpression extends RexVisitorComplexExprSplitter{

    public TopLevelComplexFilterExpression(RelOptCluster cluster, FunctionImplementationRegistry funcReg, int firstUnused) {
      super(cluster, funcReg, firstUnused);
    }

    @Override
    public RexNode visitCall(RexCall call) {

      String functionName = call.getOperator().getName();

      if (funcReg.isFunctionComplexOutput(functionName) ||
        // Dremio Filter cannot handle item on list Also Item is a type of complexOutput but it is not a function in functionRegistry
        functionName.toLowerCase(Locale.ROOT).equals("item")) {
        RexNode ret = builder.makeInputRef( factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.ANY), true), lastUsedIndex);
        lastUsedIndex++;
        complexExprs.add(call.clone(factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.ANY), true), call.getOperands()));
        return ret;
      }
      List<RexNode> newOps = new ArrayList<>();
      for (RexNode operand : call.operands) {
        newOps.add(operand.accept(this));
      }
      return call.clone(call.getType(), newOps);
    }

  }

}

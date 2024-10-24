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

import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
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
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;

public class ComplexOutputFuncVisitors {

  public static boolean hasComplexOutputFunc(RexNode node, FunctionImplementationRegistry funcReg) {
    return node.accept(new ComplexOutputFuncFinder(funcReg));
  }

  public static class ComplexOutputFuncFinder extends RexVisitorImpl<Boolean> {
    private final FunctionImplementationRegistry funcReg;

    protected ComplexOutputFuncFinder(FunctionImplementationRegistry funcReg) {
      super(true);
      this.funcReg = funcReg;
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
      return false;
    }

    @Override
    public Boolean visitLocalRef(RexLocalRef localRef) {
      return doUnknown(localRef);
    }

    @Override
    public Boolean visitLiteral(RexLiteral literal) {
      return doUnknown(literal);
    }

    @Override
    public Boolean visitOver(RexOver over) {
      return doUnknown(over);
    }

    @Override
    public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      return doUnknown(correlVariable);
    }

    @Override
    public Boolean visitCall(RexCall call) {
      for (RexNode exp : call.getOperands()) {
        if (exp.accept(this)) {
          return true;
        }
      }
      String functionName = call.getOperator().getName();
      return funcReg.isFunctionComplexOutput(functionName);
    }

    @Override
    public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
      return doUnknown(dynamicParam);
    }

    @Override
    public Boolean visitRangeRef(RexRangeRef rangeRef) {
      return doUnknown(rangeRef);
    }

    @Override
    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess.getReferenceExpr().accept(this);
    }

    @Override
    public Boolean visitSubQuery(RexSubQuery subQuery) {
      for (RexNode sub : subQuery.getOperands()) {
        if (sub.accept(this)) {
          return true;
        }
      }
      return false;
    }

    private boolean doUnknown(Object o) {
      return false;
    }
  }
}

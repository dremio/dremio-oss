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
package com.dremio.exec.expr;

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.IfExpression.IfCondition;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.NullExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.expression.ValueExpressions.BooleanExpression;
import com.dremio.common.expression.ValueExpressions.DateExpression;
import com.dremio.common.expression.ValueExpressions.DecimalExpression;
import com.dremio.common.expression.ValueExpressions.DoubleExpression;
import com.dremio.common.expression.ValueExpressions.FloatExpression;
import com.dremio.common.expression.ValueExpressions.IntExpression;
import com.dremio.common.expression.ValueExpressions.IntervalDayExpression;
import com.dremio.common.expression.ValueExpressions.IntervalYearExpression;
import com.dremio.common.expression.ValueExpressions.LongExpression;
import com.dremio.common.expression.ValueExpressions.QuotedString;
import com.dremio.common.expression.ValueExpressions.TimeExpression;
import com.dremio.common.expression.ValueExpressions.TimeStampExpression;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.google.common.collect.Lists;

/**
 * Creates a deep copy of a LogicalExpression. Specifically, it creates new instances of the literal expressions
 */
public class CloneVisitor extends AbstractExprVisitor<LogicalExpression,Void,RuntimeException> {
  @Override
  public LogicalExpression visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    List<LogicalExpression> args = Lists.newArrayList();
    for (LogicalExpression arg : call.args) {
      args.add(arg.accept(this, null));
    }

    return new FunctionCall(call.getName(), args);
  }

  @Override
  public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression holder, Void value) throws RuntimeException {
    if (holder instanceof FunctionHolderExpr) {
      List<LogicalExpression> args = Lists.newArrayList();
      for (LogicalExpression arg : holder.args) {
        args.add(arg.accept(this, null));
      }
      return new FunctionHolderExpr(holder.getName(), (BaseFunctionHolder) holder.getHolder(), args);
    }
    return null;
  }

  @Override
  public LogicalExpression visitIfExpression(IfExpression ifExpr, Void value) throws RuntimeException {
    LogicalExpression ifCondition = ifExpr.ifCondition.condition.accept(this, null);
    LogicalExpression ifExpression = ifExpr.ifCondition.expression.accept(this, null);
    LogicalExpression elseExpression = ifExpr.elseExpression.accept(this, null);
    IfExpression.IfCondition condition = new IfCondition(ifCondition, ifExpression);
    return IfExpression.newBuilder().setIfCondition(condition).setElse(elseExpression).build();
  }

  @Override
  public LogicalExpression visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return visitUnknown(op, value);
  }

  @Override
  public LogicalExpression visitSchemaPath(SchemaPath path, Void value) throws RuntimeException {
    return path;
  }

  @Override
  public LogicalExpression visitFloatConstant(FloatExpression fExpr, Void value) throws RuntimeException {
    return visitUnknown(fExpr, value);
  }

  @Override
  public LogicalExpression visitIntConstant(IntExpression intExpr, Void value) throws RuntimeException {
    return new IntExpression(intExpr.getInt());
  }

  @Override
  public LogicalExpression visitLongConstant(LongExpression intExpr, Void value) throws RuntimeException {
    return visitUnknown(intExpr, value);
  }


  @Override
  public LogicalExpression visitDecimalConstant(DecimalExpression decExpr, Void value) throws RuntimeException {
    return visitUnknown(decExpr, value);
  }

  @Override
  public LogicalExpression visitDateConstant(DateExpression intExpr, Void value) throws RuntimeException {
    return visitUnknown(intExpr, value);
  }

  @Override
  public LogicalExpression visitTimeConstant(TimeExpression intExpr, Void value) throws RuntimeException {
    return visitUnknown(intExpr, value);
  }

  @Override
  public LogicalExpression visitTimeStampConstant(TimeStampExpression intExpr, Void value) throws RuntimeException {
    return visitUnknown(intExpr, value);
  }

  @Override
  public LogicalExpression visitIntervalYearConstant(IntervalYearExpression intExpr, Void value) throws RuntimeException {
    return visitUnknown(intExpr, value);
  }

  @Override
  public LogicalExpression visitIntervalDayConstant(IntervalDayExpression intExpr, Void value) throws RuntimeException {
    return visitUnknown(intExpr, value);
  }

  @Override
  public LogicalExpression visitDoubleConstant(DoubleExpression dExpr, Void value) throws RuntimeException {
    return visitUnknown(dExpr, value);
  }

  @Override
  public LogicalExpression visitBooleanConstant(BooleanExpression e, Void value) throws RuntimeException {
    return visitUnknown(e, value);
  }

  @Override
  public LogicalExpression visitQuotedStringConstant(QuotedString e, Void value) throws RuntimeException {
    return visitUnknown(e, value);
  }

  @Override
  public LogicalExpression visitCastExpression(CastExpression e, Void value) throws RuntimeException {
    return visitUnknown(e, value);
  }

  @Override
  public LogicalExpression visitConvertExpression(ConvertExpression e, Void value) throws RuntimeException {
    return visitUnknown(e, value);
  }

  @Override
  public LogicalExpression visitCaseExpression(CaseExpression caseExpression, Void value) throws RuntimeException {
    List<CaseExpression.CaseConditionNode> caseConditions = new ArrayList<>();
    for (CaseExpression.CaseConditionNode conditionNode : caseExpression.caseConditions) {
      caseConditions.add(new CaseExpression.CaseConditionNode(
        conditionNode.whenExpr.accept(this, null),
        conditionNode.thenExpr.accept(this, null)));
    }
    LogicalExpression elseExpr = caseExpression.elseExpr.accept(this, null);
    return CaseExpression.newBuilder().setCaseConditions(caseConditions).setElseExpr(elseExpr).build();
  }

  @Override
  public LogicalExpression visitNullConstant(TypedNullConstant e, Void value) throws RuntimeException {
    return visitUnknown(e, value);
  }

  @Override
  public LogicalExpression visitNullExpression(NullExpression e, Void value) throws RuntimeException {
    return visitUnknown(e, value);
  }

  @Override
  public LogicalExpression visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    return e;
  }
}

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
package com.dremio.common.expression.visitors;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.IfExpression.IfCondition;
import com.dremio.common.expression.InputReference;
import com.dremio.common.expression.ListAggExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.NullExpression;
import com.dremio.common.expression.Ordering;
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

public final class AggregateChecker implements ExprVisitor<Boolean, ErrorCollector, RuntimeException>{

  public static final AggregateChecker INSTANCE = new AggregateChecker();

  public static boolean isAggregating(LogicalExpression e, ErrorCollector errors) {
    return e.accept(INSTANCE, errors);
  }

  @Override
  public Boolean visitFunctionCall(FunctionCall call, ErrorCollector errors) {
    throw new UnsupportedOperationException("FunctionCall is not expected here. "+
      "It should have been converted to FunctionHolderExpression in materialization");
  }

  @Override
  public Boolean visitFunctionHolderExpression(FunctionHolderExpression holder, ErrorCollector errors) {
    if (holder.isAggregating()) {
      for (int i = 0; i < holder.args.size(); i++) {
        LogicalExpression e = holder.args.get(i);
        if(e.accept(this, errors)) {
          errors.addGeneralError("Aggregating function call %s includes nested aggregations at arguments number %d. This isn't allowed.", holder.getName(), i);
        }
      }
      return true;
    } else {
      for (LogicalExpression e : holder.args) {
        if (e.accept(this, errors)) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public Boolean visitBooleanOperator(BooleanOperator op, ErrorCollector errors) {
    for (LogicalExpression arg : op.args) {
      if (arg.accept(this, errors)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Boolean visitIfExpression(IfExpression ifExpr, ErrorCollector errors) {
    IfCondition c = ifExpr.ifCondition;
    if (c.condition.accept(this, errors) || c.expression.accept(this, errors)) {
      return true;
    }
    return ifExpr.elseExpression.accept(this, errors);
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitInputReference(InputReference input, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitCaseExpression(CaseExpression caseExpression, ErrorCollector value) throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitIntConstant(IntExpression intExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitFloatConstant(FloatExpression fExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitLongConstant(LongExpression intExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitDoubleConstant(DoubleExpression dExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitBooleanConstant(BooleanExpression e, ErrorCollector errors) {
    return false;
  }
  public Boolean visitDecimalConstant(DecimalExpression decExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitQuotedStringConstant(QuotedString e, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitCastExpression(CastExpression e, ErrorCollector errors) {
    return e.getInput().accept(this, errors);
  }

  @Override
  public Boolean visitConvertExpression(ConvertExpression e, ErrorCollector errors) throws RuntimeException {
    return e.getInput().accept(this, errors);
  }

  @Override
  public Boolean visitDateConstant(DateExpression intExpr, ErrorCollector errors) {
      return false;
  }

  @Override
  public Boolean visitTimeConstant(TimeExpression intExpr, ErrorCollector errors) {
      return false;
  }

  @Override
  public Boolean visitTimeStampConstant(TimeStampExpression intExpr, ErrorCollector errors) {
      return false;
  }

  @Override
  public Boolean visitIntervalYearConstant(IntervalYearExpression intExpr, ErrorCollector errors) {
      return false;
  }

  @Override
  public Boolean visitIntervalDayConstant(IntervalDayExpression intExpr, ErrorCollector errors) {
      return false;
  }

  @Override
  public Boolean visitNullConstant(TypedNullConstant e, ErrorCollector value) throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitNullExpression(NullExpression e, ErrorCollector value) throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitListAggExpression(ListAggExpression e, ErrorCollector value) throws RuntimeException {
    return true;
  }

  @Override
  public Boolean visitOrdering(Ordering e, ErrorCollector value) throws RuntimeException {
    return false;
  }

}

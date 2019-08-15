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
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.InputReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.NullExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.ValueExpressions.BooleanExpression;
import com.dremio.common.expression.ValueExpressions.DateExpression;
import com.dremio.common.expression.ValueExpressions.DecimalExpression;
import com.dremio.common.expression.ValueExpressions.DoubleExpression;
import com.dremio.common.expression.ValueExpressions.IntervalDayExpression;
import com.dremio.common.expression.ValueExpressions.IntervalYearExpression;
import com.dremio.common.expression.ValueExpressions.LongExpression;
import com.dremio.common.expression.ValueExpressions.QuotedString;
import com.dremio.common.expression.ValueExpressions.TimeExpression;
import com.dremio.common.expression.ValueExpressions.TimeStampExpression;
import com.dremio.common.types.TypeProtos.MinorType;

public class ExpressionValidator implements ExprVisitor<Void, ErrorCollector, RuntimeException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionValidator.class);

  @Override
  public Void visitFunctionCall(FunctionCall call, ErrorCollector errors) throws RuntimeException {
    // we throw an exception here because this is a fundamental operator programming problem as opposed to an expression
    // problem. At this point in an expression's lifecycle, all function calls should have been converted into
    // FunctionHolders.
    throw new UnsupportedOperationException("FunctionCall is not expected here. "
        + "It should have been converted to FunctionHolderExpression in materialization");
  }

  @Override
  public Void visitFunctionHolderExpression(FunctionHolderExpression holder, ErrorCollector errors)
      throws RuntimeException {
    // make sure aggregate functions are not nested inside aggregate functions
    AggregateChecker.isAggregating(holder, errors);

    // make sure arguments are constant if the function implementation expects constants for any arguments
    ConstantChecker.checkConstants(holder, errors);

    return null;
  }

  @Override
  public Void visitBooleanOperator(BooleanOperator op, ErrorCollector errors) throws RuntimeException {
    int i = 0;
    for (LogicalExpression arg : op.args) {
      if ( arg.getCompleteType().toMinorType() != MinorType.BIT) {
        errors.addGeneralError(
            "Failure composing boolean operator %s.  All conditions must return a boolean type.  Condition %d was of Type %s.",
            op.getName(), i, arg.getCompleteType());
      }
      i++;
    }

    return null;
  }


  @Override
  public Void visitInputReference(InputReference e, ErrorCollector value) throws RuntimeException {
    return e.getReference().accept(this, value);
  }

  @Override
  public Void visitIfExpression(IfExpression ifExpr, ErrorCollector errors) throws RuntimeException {
    // confirm that all conditions are required boolean values.
    final CompleteType conditionType = ifExpr.ifCondition.condition.getCompleteType();
    if ( conditionType.toMinorType() != MinorType.BIT) {
      errors.addGeneralError("Failure composing If Expression.  All conditions must return a boolean type.  Condition was of Type %s.", conditionType);
    }

    // since we don't support unioning, no need to validate this.
//    final CompleteType elseType = ifExpr.elseExpression.getCompleteType();
//    final CompleteType ifType = ifExpr.ifCondition.expression.getCompleteType();
    return null;
  }

  @Override
  public Void visitSchemaPath(SchemaPath path, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitIntConstant(ValueExpressions.IntExpression intExpr, ErrorCollector value) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitFloatConstant(ValueExpressions.FloatExpression fExpr, ErrorCollector value) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitLongConstant(LongExpression intExpr, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitDecimalConstant(DecimalExpression decExpr, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitDateConstant(DateExpression intExpr, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitTimeConstant(TimeExpression intExpr, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitIntervalYearConstant(IntervalYearExpression intExpr, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitIntervalDayConstant(IntervalDayExpression intExpr, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitTimeStampConstant(TimeStampExpression intExpr, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitDoubleConstant(DoubleExpression dExpr, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitBooleanConstant(BooleanExpression e, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitQuotedStringConstant(QuotedString e, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitUnknown(LogicalExpression e, ErrorCollector value) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitCastExpression(CastExpression e, ErrorCollector value) throws RuntimeException {
    return e.getInput().accept(this, value);
  }

  @Override
  public Void visitNullConstant(TypedNullConstant e, ErrorCollector value) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitNullExpression(NullExpression e, ErrorCollector value) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitConvertExpression(ConvertExpression e, ErrorCollector value)
      throws RuntimeException {
    return e.getInput().accept(this, value);
  }

}

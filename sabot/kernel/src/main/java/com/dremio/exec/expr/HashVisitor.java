/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
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

public class HashVisitor extends AbstractExprVisitor<Integer,Void,RuntimeException> {
  @Override
  public Integer visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    return compute(call, 1);
  }

  @Override
  public Integer visitFunctionHolderExpression(FunctionHolderExpression holder, Void value) throws RuntimeException {
    return compute(holder, 2);
  }

  @Override
  public Integer visitIfExpression(IfExpression ifExpr, Void value) throws RuntimeException {
    return compute(ifExpr, 3);
  }

  @Override
  public Integer visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return compute(op, 4);
  }

  @Override
  public Integer visitSchemaPath(SchemaPath path, Void value) throws RuntimeException {
    return compute(path, 5);
  }

  @Override
  public Integer visitFloatConstant(FloatExpression fExpr, Void value) throws RuntimeException {
    return compute(fExpr, 6);
  }

  @Override
  public Integer visitIntConstant(IntExpression intExpr, Void value) throws RuntimeException {
    return compute(intExpr, 7);
  }

  @Override
  public Integer visitLongConstant(LongExpression intExpr, Void value) throws RuntimeException {
    return compute(intExpr, 8);
  }


  @Override
  public Integer visitDecimalConstant(DecimalExpression decExpr, Void value) throws RuntimeException {
    return compute(decExpr, 9);
  }

  @Override
  public Integer visitDateConstant(DateExpression intExpr, Void value) throws RuntimeException {
    return compute(intExpr, 13);
  }

  @Override
  public Integer visitTimeConstant(TimeExpression intExpr, Void value) throws RuntimeException {
    return compute(intExpr, 14);
  }

  @Override
  public Integer visitTimeStampConstant(TimeStampExpression intExpr, Void value) throws RuntimeException {
    return compute(intExpr, 15);
  }

  @Override
  public Integer visitIntervalYearConstant(IntervalYearExpression intExpr, Void value) throws RuntimeException {
    return compute(intExpr, 16);
  }

  @Override
  public Integer visitIntervalDayConstant(IntervalDayExpression intExpr, Void value) throws RuntimeException {
    return compute(intExpr, 17);
  }

  @Override
  public Integer visitDoubleConstant(DoubleExpression dExpr, Void value) throws RuntimeException {
    return compute(dExpr, 18);
  }

  @Override
  public Integer visitBooleanConstant(BooleanExpression e, Void value) throws RuntimeException {
    return compute(e, 19);
  }

  @Override
  public Integer visitQuotedStringConstant(QuotedString e, Void value) throws RuntimeException {
    return compute(e, 20);
  }

  @Override
  public Integer visitCastExpression(CastExpression e, Void value) throws RuntimeException {
    return compute(e, 21);
  }

  @Override
  public Integer visitConvertExpression(ConvertExpression e, Void value) throws RuntimeException {
    return compute(e, 22);
  }

  @Override
  public Integer visitNullConstant(TypedNullConstant e, Void value) throws RuntimeException {
    return compute(e, 23);
  }

  @Override
  public Integer visitNullExpression(NullExpression e, Void value) throws RuntimeException {
    return compute(e, 24);
  }

  @Override
  public Integer visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    return compute(e, 25);
  }

  private int compute(LogicalExpression e, int seed) {
    int hash = seed;
    for (LogicalExpression child : e) {
      hash = hash * 31 + child.accept(this, null);
    }
    return hash;
  }
}

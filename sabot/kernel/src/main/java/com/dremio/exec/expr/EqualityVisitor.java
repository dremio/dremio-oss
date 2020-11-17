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

import java.util.List;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.InExpression;
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
import com.google.common.collect.Lists;

class EqualityVisitor extends AbstractExprVisitor<Boolean,LogicalExpression,RuntimeException> {

  @Override
  public Boolean visitFunctionCall(FunctionCall call, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof FunctionCall)) {
      return false;
    }
    if (!checkType(call, value)) {
      return false;
    }
    if (!call.getName().equals(((FunctionCall) value).getName())) {
      return false;
    }
    return checkChildren(call, value);
  }

  @Override
  public Boolean visitFunctionHolderExpression(FunctionHolderExpression holder, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof FunctionHolderExpression)) {
      return false;
    }
    if (!checkType(holder, value)) {
      return false;
    }
    if (!holder.getName().equals(((FunctionHolderExpression) value).getName())) {
      return false;
    }
    if (holder.isRandom()) {
      return false;
    }
    return checkChildren(holder, value);
  }

  @Override
  public Boolean visitIfExpression(IfExpression ifExpr, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof IfExpression)) {
      return false;
    }
    return checkChildren(ifExpr, value);
  }

  @Override
  public Boolean visitBooleanOperator(BooleanOperator call, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof BooleanOperator)) {
      return false;
    }
    if (!call.getName().equals(((BooleanOperator) value).getName())) {
      return false;
    }
    return checkChildren(call, value);
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof SchemaPath)) {
      return false;
    }
    return path.equals(value);
  }

  @Override
  public Boolean visitIntConstant(IntExpression intExpr, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof IntExpression)) {
      return false;
    }
    return intExpr.getInt() == ((IntExpression) value).getInt();
  }

  @Override
  public Boolean visitFloatConstant(FloatExpression fExpr, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof FloatExpression)) {
      return false;
    }
    return fExpr.getFloat() == ((FloatExpression) value).getFloat();
  }

  @Override
  public Boolean visitLongConstant(LongExpression intExpr, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof LongExpression)) {
      return false;
    }
    return intExpr.getLong() == ((LongExpression) value).getLong();
  }

  @Override
  public Boolean visitDateConstant(DateExpression intExpr, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof DateExpression)) {
      return false;
    }
    return intExpr.getDate() == ((DateExpression) value).getDate();
  }

  @Override
  public Boolean visitTimeConstant(TimeExpression intExpr, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof TimeExpression)) {
      return false;
    }
    return intExpr.getTime() == ((TimeExpression) value).getTime();
  }

  @Override
  public Boolean visitTimeStampConstant(TimeStampExpression intExpr, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof TimeStampExpression)) {
      return false;
    }
    return intExpr.getTimeStamp() == ((TimeStampExpression) value).getTimeStamp();
  }

  @Override
  public Boolean visitIntervalYearConstant(IntervalYearExpression intExpr, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof IntervalYearExpression)) {
      return false;
    }
    return (intExpr.getIntervalYear() == ((IntervalYearExpression) value).getIntervalYear());
  }

  @Override
  public Boolean visitIntervalDayConstant(IntervalDayExpression intExpr, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof IntervalDayExpression)) {
      return false;
    }
    return (intExpr.getIntervalDay() == ((IntervalDayExpression) value).getIntervalDay()
        && intExpr.getIntervalMillis() == ((IntervalDayExpression) value).getIntervalMillis());
  }

  @Override
  public Boolean visitDecimalConstant(DecimalExpression decExpr, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof DecimalExpression)) {
      return false;
    }
    return decExpr.getDecimal().equals(((DecimalExpression) value).getDecimal());
  }

  @Override
  public Boolean visitDoubleConstant(DoubleExpression dExpr, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof DoubleExpression)) {
      return false;
    }
    return dExpr.getDouble() == ((DoubleExpression) value).getDouble();
  }

  @Override
  public Boolean visitBooleanConstant(BooleanExpression e, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof BooleanExpression)) {
      return false;
    }
    return e.getBoolean() == ((BooleanExpression) value).getBoolean();
  }

  @Override
  public Boolean visitQuotedStringConstant(QuotedString e, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof QuotedString)) {
      return false;
    }
    return e.getString().equals(((QuotedString) value).getString());
  }

  @Override
  public Boolean visitNullExpression(NullExpression e, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof NullExpression)) {
      return false;
    }
    return e.getCompleteType().equals(value.getCompleteType());
  }

  @Override
  public Boolean visitCastExpression(CastExpression e, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof CastExpression)) {
      return false;
    }
    if (!e.getCompleteType().equals(value.getCompleteType())) {
      return false;
    }
    return checkChildren(e, value);
  }

  @Override
  public Boolean visitConvertExpression(ConvertExpression e, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof ConvertExpression)) {
      return false;
    }
    if (!e.getConvertFunction().equals(((ConvertExpression) value).getConvertFunction())) {
      return false;
    }
    return checkChildren(e, value);
  }

  @Override
  public Boolean visitNullConstant(TypedNullConstant e, LogicalExpression value) throws RuntimeException {
    if (!(value instanceof TypedNullConstant)) {
      return false;
    }
    return value.getCompleteType().equals(e.getCompleteType());
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, LogicalExpression value) throws RuntimeException {
    if (e instanceof ValueVectorReadExpression && value instanceof ValueVectorReadExpression) {
      return visitValueVectorReadExpression((ValueVectorReadExpression) e, (ValueVectorReadExpression) value);
    } else if (e instanceof InExpression && value instanceof InExpression) {
      return visitInExpression((InExpression) e, (InExpression) value);
    }
    return false;
  }

  /**
   * An in expression is equal if it has the same eval block and equal constants.
   */
  private Boolean visitInExpression(InExpression e1, InExpression e2) {
    if(!e1.getEval().accept(this, e2)) {
      return false;
    }

    if(e1.getConstants().size() != e2.getConstants().size()) {
      return false;
    }
    final List<LogicalExpression> le1 = e1.getConstants();
    final List<LogicalExpression> le2 = e2.getConstants();
    final int size = le1.size();

    for(int i = 0; i < size; i++) {
      LogicalExpression se1 = le1.get(i);
      LogicalExpression se2 = le2.get(i);
      if(!se1.accept(this, se2)) {
        return false;
      }
    }

    return true;
  }

  private Boolean visitValueVectorReadExpression(ValueVectorReadExpression e, ValueVectorReadExpression value) {
    return e.getTypedFieldId().equals(value.getTypedFieldId());
  }


  private boolean checkChildren(LogicalExpression thisExpr, LogicalExpression thatExpr) {
    List<LogicalExpression> theseChildren = Lists.newArrayList(thisExpr);
    List<LogicalExpression> thoseChildren = Lists.newArrayList(thatExpr);

    if (theseChildren.size() != thoseChildren.size()) {
      return false;
    }
    for (int i = 0; i < theseChildren.size(); i++) {
      if (!theseChildren.get(i).accept(this, thoseChildren.get(i))) {
        return false;
      }
    }
    return true;
  }

  private boolean checkType(LogicalExpression e1, LogicalExpression e2) {
    return e1.getCompleteType().equals(e2.getCompleteType());
  }
}

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
package com.dremio.common.expression;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.joda.time.Period;

import com.dremio.common.expression.IfExpression.IfCondition;
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
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.Types;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class ExpressionStringBuilder extends AbstractExprVisitor<Void, StringBuilder, RuntimeException>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionStringBuilder.class);

  static final ExpressionStringBuilder INSTANCE = new ExpressionStringBuilder();

  public static String toString(LogicalExpression expr) {
    StringBuilder sb = new StringBuilder();
    expr.accept(INSTANCE, sb);
    return sb.toString();
  }

  public static void toString(LogicalExpression expr, StringBuilder sb) {
    expr.accept(INSTANCE, sb);
  }

  public static String escapeSingleQuote(String input) {
    return input.replaceAll("(['\\\\])", "\\\\$1");
  }

  public static String escapeBackTick(String input) {
    return input.replaceAll("([`\\\\])", "\\\\$1");
  }

  @Override
  public Void visitFunctionCall(FunctionCall call, StringBuilder sb) throws RuntimeException {
    ImmutableList<LogicalExpression> args = call.args;
    sb.append(call.getName());
    sb.append("(");
    for (int i = 0; i < args.size(); i++) {
      if (i != 0) {
        sb.append(", ");
      }
      args.get(i).accept(this, sb);
    }
    sb.append(") ");
    return null;
  }


  @Override
  public Void visitInputReference(InputReference sideExpr, StringBuilder sb) throws RuntimeException {
    sb.append("INPUT_REFERENCE(");
    sb.append(sideExpr.getInputOrdinal());
    sb.append(",");
    sideExpr.getReference().accept(this,  sb);
    sb.append(")");
    return null;
  }

  @Override
  public Void visitBooleanOperator(BooleanOperator op, StringBuilder sb) throws RuntimeException {
    return visitFunctionCall(op, sb);
  }

  @Override
  public Void visitFunctionHolderExpression(FunctionHolderExpression holder, StringBuilder sb) throws RuntimeException {
    ImmutableList<LogicalExpression> args = holder.args;
    sb.append(holder.getName());
    sb.append("(");
    for (int i = 0; i < args.size(); i++) {
      if (i != 0) {
        sb.append(", ");
      }
      args.get(i).accept(this, sb);
    }
    sb.append(") ");
    return null;
  }

  @Override
  public Void visitIfExpression(IfExpression ifExpr, StringBuilder sb) throws RuntimeException {

    // serialize the if expression
    sb.append(" ( ");
    IfCondition c = ifExpr.ifCondition;
    sb.append("if (");
    c.condition.accept(this, sb);
    sb.append(" ) then (");
    c.expression.accept(this, sb);
    sb.append(" ) ");

    sb.append(" else (");
    ifExpr.elseExpression.accept(this, sb);
    sb.append(" ) ");
    sb.append(" end ");
    sb.append(" ) ");
    return null;
  }

  @Override
  public Void visitSchemaPath(SchemaPath path, StringBuilder sb) throws RuntimeException {
    PathSegment seg = path.getRootSegment();
    if (seg.isArray()) {
      throw new IllegalStateException("Dremio doesn't currently support top level arrays");
    }
    sb.append('`');
    sb.append(escapeBackTick(seg.getNameSegment().getPath()));
    sb.append('`');

    while ( (seg = seg.getChild()) != null) {
      if (seg.isNamed()) {
        sb.append('.');
        sb.append('`');
        sb.append(escapeBackTick(seg.getNameSegment().getPath()));
        sb.append('`');
      } else {
        sb.append('[');
        sb.append(seg.getArraySegment().getIndex());
        sb.append(']');
      }
    }
    return null;
  }

  @Override
  public Void visitLongConstant(LongExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append(lExpr.getLong());
    sb.append("l");
    return null;
  }

  @Override
  public Void visitDateConstant(DateExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append("cast( ");
    sb.append(lExpr.getDate());
    sb.append("l as DATE)");
    return null;
  }

  @Override
  public Void visitTimeConstant(TimeExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append("cast( ");
    sb.append(lExpr.getTime());
    sb.append("i as TIME)");
    return null;
  }

  @Override
  public Void visitTimeStampConstant(TimeStampExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append("cast( ");
    sb.append(lExpr.getTimeStamp());
    sb.append("l as TIMESTAMP)");
    return null;
  }

  @Override
  public Void visitIntervalYearConstant(IntervalYearExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append("cast( '");
    sb.append(Period.months(lExpr.getIntervalYear()).toString());
    sb.append("' as INTERVALYEAR)");
    return null;
  }

  @Override
  public Void visitIntervalDayConstant(IntervalDayExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append("cast( '");
    sb.append(Period.days(lExpr.getIntervalDay()).plusMillis(lExpr.getIntervalMillis()).toString());
    sb.append("' as INTERVALDAY)");
    return null;
  }

  static String serializeDecimalConstant(DecimalExpression decExpr) {
    // for decimals, precision and scale also need to be tagged along. The json de-parsing expects a
    // numeric value that doesn't start with 0.
    StringBuilder builder = new StringBuilder();
    BigInteger bi = decExpr.getDecimal().setScale(decExpr.getScale()).unscaledValue();
    if (!bi.equals(BigInteger.ZERO)) {
      builder.append(bi.toString());
    }
    return builder
      .append(String.format("%02d", decExpr.getPrecision() + CompleteType.MAX_DECIMAL_PRECISION))
      .append(String.format("%02d", decExpr.getScale() + CompleteType.MAX_DECIMAL_PRECISION))
      .toString();
  }

  static DecimalExpression deserializeDecimalConstant(String numStr) {
    // extract decimal value
    int offsetInString = 0;
    int biLen = numStr.length() - 4;
    BigInteger bi = BigInteger.ZERO;
    if (biLen > 0) {
      bi = new BigInteger(numStr.substring(offsetInString, offsetInString + biLen));
    }

    // extract precision
    offsetInString += biLen;
    int precision = Integer.parseInt(numStr.substring(offsetInString, offsetInString + 2)) -
      CompleteType.MAX_DECIMAL_PRECISION;

    // extract scale
    offsetInString += 2;
    int scale = Integer.parseInt(numStr.substring(offsetInString, offsetInString + 2)) -
      CompleteType.MAX_DECIMAL_PRECISION;
    return new DecimalExpression(new BigDecimal(bi, scale), precision, scale);
  }

  @Override
  public Void visitDecimalConstant(DecimalExpression decExpr, StringBuilder sb) throws RuntimeException {
    sb.append(serializeDecimalConstant(decExpr));
    sb.append('m');
    return null;
  }

  @Override
  public Void visitDoubleConstant(DoubleExpression dExpr, StringBuilder sb) throws RuntimeException {
    sb.append(dExpr.getDouble());
    sb.append("d");
    return null;
  }

  @Override
  public Void visitBooleanConstant(BooleanExpression e, StringBuilder sb) throws RuntimeException {
    sb.append(e.getBoolean());
    return null;
  }

  @Override
  public Void visitQuotedStringConstant(QuotedString e, StringBuilder sb) throws RuntimeException {
    sb.append("'");
    sb.append(escapeSingleQuote(e.value));
    sb.append("'");
    return null;
  }

  @Override
  public Void visitConvertExpression(ConvertExpression e, StringBuilder sb) throws RuntimeException {
    sb.append(e.getConvertFunction()).append("(");
    e.getInput().accept(this, sb);
    sb.append(", '").append(e.getEncodingType()).append("')");
    return null;
  }

  @Override
  public Void visitCastExpression(CastExpression e, StringBuilder sb) throws RuntimeException {
    final MajorType mt = e.retrieveMajorType();

    sb.append("cast( (");
    e.getInput().accept(this, sb);
    sb.append(" ) as ");
    sb.append(mt.getMinorType().name());

    switch(mt.getMinorType()) {
    case FLOAT4:
    case FLOAT8:
    case BIT:
    case INT:
    case TINYINT:
    case SMALLINT:
    case BIGINT:
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:
    case DATE:
    case TIMESTAMP:
    case TIMESTAMPTZ:
    case TIME:
    case INTERVAL:
    case INTERVALDAY:
    case INTERVALYEAR:
    case STRUCT:
    case LIST:
      // do nothing else.
      break;
    case VAR16CHAR:
    case VARBINARY:
    case VARCHAR:
    case FIXED16CHAR:
    case FIXEDSIZEBINARY:
    case FIXEDCHAR:

      // add size in parens
      sb.append("(");
      sb.append(mt.getWidth());
      sb.append(")");
      break;
    case DECIMAL:

      // add scale and precision
      sb.append("(");
      sb.append(mt.getPrecision());
      sb.append(", ");
      sb.append(mt.getScale());
      sb.append(")");
      break;
    default:
      throw new UnsupportedOperationException(String.format("Unable to convert cast expression %s into string.", e));
    }
    sb.append(" )");
    return null;
  }


  @Override
  public Void visitFloatConstant(FloatExpression fExpr, StringBuilder sb) throws RuntimeException {
    sb.append(fExpr.getFloat());
    sb.append("f");
    return null;
  }

  @Override
  public Void visitIntConstant(IntExpression intExpr, StringBuilder sb) throws RuntimeException {
    sb.append(intExpr.getInt());
    sb.append("i");
    return null;
  }

  @Override
  public Void visitNullConstant(TypedNullConstant e, StringBuilder sb) throws RuntimeException {
    MajorType type = Types.optional(e.getCompleteType().toMinorType());
    if (e.getCompleteType().isDecimal()) {
      type = MajorType.newBuilder().setMode(TypeProtos.DataMode.OPTIONAL).setMinorType(e.getCompleteType().toMinorType())
        .setPrecision(e.getCompleteType().getPrecision()).setScale(e.getCompleteType().getScale()).build();
    }
    CastExpression cast = new CastExpression(NullExpression.INSTANCE, type);
    cast.accept(this, sb);
    return null;
  }

  @Override
  public Void visitNullExpression(NullExpression e, StringBuilder sb) throws RuntimeException {
    sb.append("__$INTERNAL_NULL$__");
    return null;
  }

  @Override  public Void visitInExpression(InExpression e, StringBuilder sb) throws RuntimeException {
    e.getEval().accept(this, sb);
    sb.append(" in ( ");
    boolean first = true;
    for(LogicalExpression expression: e.getConstants()) {
      if(first) {
        first = false;
      } else {
        sb.append(", ");
      }
      expression.accept(this, sb);
    }
    sb.append(") ");

    return null;
  }

  @Override
  public Void visitUnknown(LogicalExpression e, StringBuilder sb) {
    sb.append(e.toString());
    return null;
  }
}

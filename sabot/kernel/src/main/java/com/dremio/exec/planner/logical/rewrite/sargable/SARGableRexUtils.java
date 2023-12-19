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
package com.dremio.exec.planner.logical.rewrite.sargable;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.sql.Checker;
import com.google.common.collect.ImmutableList;

public final class SARGableRexUtils {
  private SARGableRexUtils() {}

  public static final SqlFunction DATE_ADD = new SqlFunction(
    "DATE_ADD",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0,
    null,
    Checker.of(2),
    SqlFunctionCategory.USER_DEFINED_FUNCTION
  );

  public static final SqlFunction DATE_SUB = new SqlFunction(
    "DATE_SUB",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0,
    null,
    Checker.of(2),
    SqlFunctionCategory.USER_DEFINED_FUNCTION
  );

  public static final SqlFunction DATE_TRUNC = new SqlFunction(
    "DATE_TRUNC",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG1,
    null,
    Checker.of(2),
    SqlFunctionCategory.USER_DEFINED_FUNCTION
  );

  public static final SqlFunction EXTRACT = new SqlFunction(
    "EXTRACT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.INTEGER,
    null,
    Checker.of(2),
    SqlFunctionCategory.USER_DEFINED_FUNCTION
  );

  public static final SqlFunction NEXT_DAY = new SqlFunction(
    "NEXT_DAY",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0,
    null,
    Checker.of(2),
    SqlFunctionCategory.USER_DEFINED_FUNCTION
  );

  public static RexNode eq(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      left,
      right
    );
  }

  public static RexNode neq(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.NOT_EQUALS,
      left,
      right
    );
  }

  public static RexNode gt(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.GREATER_THAN,
      left,
      right
    );
  }

  public static RexNode gte(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
      left,
      right
    );
  }

  public static RexNode lt(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.LESS_THAN,
      left,
      right
    );
  }

  public static RexNode lte(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
      left,
      right
    );
  }

  public static RexNode in(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.IN,
      left,
      right
    );
  }

  public static RexNode notIn(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.NOT_IN,
      left,
      right
    );
  }

  public static RexNode between(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.BETWEEN,
      left,
      right
    );
  }

  public static RexNode notBetween(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.NOT_BETWEEN,
      left,
      right
    );
  }

  public static RexNode and(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return and(ImmutableList.of(left, right), rexBuilder);
  }

  public static RexNode and(List<? extends RexNode> exprs, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.AND,
      exprs
    );
  }

  public static RexNode or(RexNode left, RexNode right, RexBuilder rexBuilder) {
    return or(ImmutableList.of(left, right), rexBuilder);
  }

  public static RexNode or(List<? extends RexNode> exprs, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      SqlStdOperatorTable.OR,
      exprs
    );
  }

  public static RexNode dateAdd(RexNode dateExpr, RexNode interval, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      DATE_ADD,
      dateExpr,
      interval
    );
  }

  public static RexNode dateAdd(RexNode dateExpr, int days, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      DATE_ADD,
      dateExpr,
      rexBuilder.makeLiteral(String.valueOf(days))
    );
  }

  public static RexNode dateTrunc(TimeUnit timeUnit, RexNode dateExpr, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      DATE_TRUNC,
      rexBuilder.makeLiteral(timeUnit.toString()),
      dateExpr
    );
  }

  public static RexNode nextDay(RexNode dateExpr, RexNode literal, RexBuilder rexBuilder) {
    return rexBuilder.makeCall(
      NEXT_DAY,
      dateExpr,
      literal
    );
  }

  public static RexNode toInterval(Long num, RexLiteral rexTimeUnit, RexBuilder rexBuilder) {
    return toInterval(num, rexTimeUnit.getValue2().toString(), rexBuilder);
  }

  public static RexNode toInterval(RexLiteral literal, String strTimeUnit, RexBuilder rexBuilder) {
    if (SqlTypeName.NUMERIC_TYPES.contains(literal.getTypeName())) {
      return toInterval(((BigDecimal) literal.getValue()).longValue(), strTimeUnit, rexBuilder);
    }
    return literal;
  }

  public static RexNode toInterval(RexLiteral literal, TimeUnit timeUnit, RexBuilder rexBuilder) {
    if (SqlTypeName.NUMERIC_TYPES.contains(literal.getTypeName())) {
      return toInterval(((BigDecimal) literal.getValue()).longValue(), timeUnit, rexBuilder);
    }
    return literal;
  }

  public static RexNode toInterval(Long num, String strTimeUnit, RexBuilder rexBuilder) {
    TimeUnit timeUnit;
    switch (strTimeUnit.toUpperCase(Locale.ROOT)) {
      case "YEAR":
        timeUnit = TimeUnit.YEAR;
        break;
      case "MONTH":
        timeUnit = TimeUnit.MONTH;
        break;
      case "DAY":
        timeUnit = TimeUnit.DAY;
        break;
      case "HOUR":
        timeUnit = TimeUnit.HOUR;
        break;
      case "MINUTE":
        timeUnit = TimeUnit.MINUTE;
        break;
      case "SECOND":
        timeUnit = TimeUnit.SECOND;
        break;
      case "QUARTER":
        timeUnit = TimeUnit.QUARTER;
        break;
      case "WEEK":
        timeUnit = TimeUnit.WEEK;
        break;
      default:
        timeUnit = null;
        break;
    }
    return toInterval(num, timeUnit, rexBuilder);
  }

  public static RexNode toInterval(Long num, TimeUnit timeUnit, RexBuilder rexBuilder) {
    return (num == null || timeUnit == null) ? null :
      rexBuilder.makeIntervalLiteral(
        BigDecimal.valueOf(num * timeUnit.multiplier.longValue()),
        new SqlIntervalQualifier(timeUnit, null, SqlParserPos.ZERO));
  }
}

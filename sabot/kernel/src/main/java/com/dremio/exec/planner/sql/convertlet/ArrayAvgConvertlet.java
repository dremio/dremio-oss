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
package com.dremio.exec.planner.sql.convertlet;

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_AVG;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.sql.DremioSqlOperatorTable;

/**
 * Convertlet to rewrite ARRAY_AVG(arr) as ARRAY_SUM(arr) / ARRAY_LENGTH(arr)
 */
public final class ArrayAvgConvertlet implements FunctionConvertlet {
  public static final FunctionConvertlet INSTANCE = new NullableArrayFunctionConvertlet(new ArrayAvgConvertlet());

  private ArrayAvgConvertlet() {}

  @Override
  public boolean matches(RexCall call) {
    return call.getOperator() == ARRAY_AVG;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    // ARRAY_AVG(arr) => ARRAY_SUM(arr) / ARRAY_LENGTH(arr)
    RexBuilder rexBuilder = cx.getRexBuilder();

    // We need to cast to DECIMAL, since the DIVIDE function expects DECIMALs
    RelDataType defaultDecimal = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL);

    RexNode arr = call.getOperands().get(0);
    RexNode arraySum = rexBuilder.makeCast(
      defaultDecimal,
      rexBuilder.makeCall(DremioSqlOperatorTable.ARRAY_SUM, arr));
    RexNode arrayLength = rexBuilder.makeCast(
      defaultDecimal,
      rexBuilder.makeCall(DremioSqlOperatorTable.CARDINALITY, arr));
    RexNode arrayAverage = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, arraySum, arrayLength);
    RexNode isDivisionByZero = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, arrayLength, rexBuilder.makeZeroLiteral(defaultDecimal));
    RexNode nullLiteral = rexBuilder.makeNullLiteral(arrayAverage.getType());
    RexNode caseForDivisionByZero = rexBuilder.makeCall(SqlStdOperatorTable.CASE, isDivisionByZero, nullLiteral, arrayAverage);

    return (RexCall) caseForDivisionByZero;
  }
}

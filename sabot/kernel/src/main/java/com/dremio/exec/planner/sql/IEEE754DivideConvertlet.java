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
package com.dremio.exec.planner.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;

import com.google.common.collect.ImmutableList;

public class IEEE754DivideConvertlet implements SqlRexConvertlet {
  public static final SqlRexConvertlet INSTANCE = new IEEE754DivideConvertlet();

  @Override
  public RexNode convertCall(SqlRexContext sqlRexContext,
      SqlCall sqlCall) {
    assert sqlCall.getOperandList().size() == 2;
    RexBuilder rexBuilder = sqlRexContext.getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    RexNode numerator = sqlRexContext.convertExpression(sqlCall.getOperandList().get(0));
    RexNode denominator = sqlRexContext.convertExpression(sqlCall.getOperandList().get(1));
    RelDataType type =
      typeFactory.leastRestrictive(ImmutableList.of(numerator.getType(), denominator.getType()));

    switch (type.getSqlTypeName()) {
      case FLOAT:
      case DOUBLE: {
        RexNode zero = rexBuilder.makeZeroLiteral(type);
        RexNode infinity = rexBuilder.makeCast(type, rexBuilder.makeLiteral("Infinity"));
        RexNode negativeInfinity =
          rexBuilder.makeCast(type, rexBuilder.makeLiteral("-Infinity"));
        RexNode nan = rexBuilder.makeCast(type, rexBuilder.makeLiteral("NaN"));
        return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
          rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, denominator, zero),
            rexBuilder.makeCall(SqlStdOperatorTable.CASE,
              rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, numerator, zero), infinity,
              rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, numerator, zero), negativeInfinity,
              nan),
          rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, numerator, denominator));
      }
      default:
        return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, numerator, denominator);
    }
  }
}

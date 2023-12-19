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


import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;


/**
 * Convertlet to rewrite LOG2(value) as LOG(2.0,value)
 */
public final class DatePartFunctionsConvertlet implements SqlRexConvertlet {
  public static final DatePartFunctionsConvertlet SECOND_INSTANCE = new DatePartFunctionsConvertlet(
    TimeUnitRange.SECOND);
public static final DatePartFunctionsConvertlet MINUTE_INSTANCE = new DatePartFunctionsConvertlet(
    TimeUnitRange.MINUTE);
public static final DatePartFunctionsConvertlet HOUR_INSTANCE = new DatePartFunctionsConvertlet(
    TimeUnitRange.HOUR);
public static final DatePartFunctionsConvertlet DAY_INSTANCE = new DatePartFunctionsConvertlet(
    TimeUnitRange.DAY);
public static final DatePartFunctionsConvertlet MONTH_INSTANCE = new DatePartFunctionsConvertlet(
    TimeUnitRange.MONTH);
public static final DatePartFunctionsConvertlet YEAR_INSTANCE = new DatePartFunctionsConvertlet(
    TimeUnitRange.YEAR);


  private final TimeUnitRange unit;

  private DatePartFunctionsConvertlet(TimeUnitRange unit) {
    this.unit = unit;
  }

  @Override
  public RexNode convertCall(SqlRexContext sqlRexContext, SqlCall sqlCall) {
    final RexBuilder rexBuilder = sqlRexContext.getRexBuilder();
    List<RexNode> exprs = new ArrayList<>();
    exprs.add(rexBuilder.makeFlag(unit));
    exprs.add(sqlRexContext.convertExpression(sqlCall.getOperandList().get(0)));

    RelDataTypeFactory typeFactory = sqlRexContext.getTypeFactory();
    final RelDataType returnType
      = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), exprs.get(1).getType().isNullable());
    return rexBuilder.makeCall(returnType, SqlStdOperatorTable.EXTRACT, exprs);
  }
}

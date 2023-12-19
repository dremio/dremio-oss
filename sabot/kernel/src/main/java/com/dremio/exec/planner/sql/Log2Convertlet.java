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


import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;

import com.google.common.base.Preconditions;

/**
 * Convertlet to rewrite LOG2(value) as LOG(2.0,value)
 */
public final class Log2Convertlet implements SqlRexConvertlet {
  public static final Log2Convertlet INSTANCE = new Log2Convertlet();

  private Log2Convertlet() {}

  @Override
  public RexNode convertCall(SqlRexContext sqlRexContext, SqlCall sqlCall) {
    Preconditions.checkArgument(sqlCall.getOperandList().size() == 1);

    RexBuilder rexBuilder = sqlRexContext.getRexBuilder();
    RexNode value = sqlRexContext.convertExpression(sqlCall.getOperandList().get(0));

    RexNode base = rexBuilder.makeLiteral(2.0, value.getType(), false);

    return rexBuilder.makeCall(DremioSqlOperatorTable.LOG, base, value);
  }
}

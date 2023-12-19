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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_CONCAT;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_DISTINCT;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.SET_UNION;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

public final class SetUnionConvertlet implements FunctionConvertlet {
  public static final FunctionConvertlet INSTANCE = new NullableArrayFunctionConvertlet(new SetUnionConvertlet());

  private SetUnionConvertlet() {}

  @Override
  public boolean matches(RexCall call) {
    return call.getOperator() == SET_UNION;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    // SET_UNION(arr1, arr2) -> ARRAY_DISTINCT(ARRAY_CONCAT(arr1, arr2))
    RexNode arr1 = call.getOperands().get(0);
    RexNode arr2 = call.getOperands().get(1);

    RexBuilder rexBuilder = cx.getRexBuilder();
    RexNode arrayConcat = rexBuilder.makeCall(ARRAY_CONCAT, arr1, arr2);
    return (RexCall) rexBuilder.makeCall(ARRAY_DISTINCT, arrayConcat);
  }
}

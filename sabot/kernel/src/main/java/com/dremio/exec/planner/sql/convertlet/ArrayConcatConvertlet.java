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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_CAT;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_CONCAT;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

public final class ArrayConcatConvertlet implements FunctionConvertlet {
  public static final FunctionConvertlet INSTANCE = new NullableArrayFunctionConvertlet(new ArrayConcatConvertlet());

  private ArrayConcatConvertlet() {}

  @Override
  public boolean matches(RexCall call) {
    return call.getOperator() == ARRAY_CONCAT;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    /*
      If a user writes a query like:

      SELECT ARRAY_CONCAT(ARRAY[1, 2, 3], ARRAY[4, 5, 6], ARRAY[7, 8, 9])

      Then we get a runtime error, since our execution for ARRAY_CONCAT only supports 2 parameters.
      (There is no variadic function in execution).

      One solution is to just nest the arguments:

      SELECT ARRAY_CONCAT(ARRAY_CONCAT(ARRAY[1, 2, 3], ARRAY[4, 5, 6]), ARRAY[7, 8, 9])
     */
    RexBuilder rexBuilder = cx.getRexBuilder();

    RexNode nestedArrayConcat = rexBuilder.makeCall(
      ARRAY_CAT,
      call.getOperands().get(0),
      call.getOperands().get(1));
    for (int i = 2; i < call.getOperands().size(); i++) {
      nestedArrayConcat = rexBuilder.makeCall(
        ARRAY_CAT,
        nestedArrayConcat,
        call.getOperands().get(i));
    }

    return (RexCall) nestedArrayConcat;
  }
}

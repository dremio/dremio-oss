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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_INTERSECTION;

import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

public final class ArrayIntersectionConvertlet implements FunctionConvertlet {
  public static final FunctionConvertlet INSTANCE =
      new NullableArrayFunctionConvertlet(new ArrayIntersectionConvertlet());

  private ArrayIntersectionConvertlet() {}

  @Override
  public boolean matches(RexCall call) {
    return call.getOperator() == ARRAY_INTERSECTION;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    // ARRAY_INTERSECTION(arr1, arr2) -> ARRAY(
    //   SELECT item1
    //   FROM UNNEST(arr1) as t(item1)
    //   WHERE ARRAY_CONTAINS(arr2, item1)
    // )
    RexNode arr1 = call.getOperands().get(0);
    RexNode arr2 = call.getOperands().get(1);

    RexBuilder rexBuilder = cx.getRexBuilder();

    return CorrelatedUnnestQueryBuilder.create(cx)
        .unnest(arr1)
        .transform(
            builder -> {
              RexNode item1 = rexBuilder.makeInputRef(builder.peek(), 0);
              RexCall arrayContains =
                  (RexCall) rexBuilder.makeCall(DremioSqlOperatorTable.ARRAY_CONTAINS, arr2, item1);
              arrayContains = ArrayContainsConvertlet.INSTANCE.convertCall(cx, arrayContains);
              builder.filter(arrayContains);
            })
        .array();
  }
}

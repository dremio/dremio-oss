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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAYS_OVERLAP;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.tools.RelBuilder;

public final class ArraysOverlapConvertlet implements FunctionConvertlet {
  public static final FunctionConvertlet INSTANCE =
      new NullableArrayFunctionConvertlet(new ArraysOverlapConvertlet());

  private ArraysOverlapConvertlet() {}

  @Override
  public boolean matches(RexCall call) {
    return call.getOperator() == ARRAYS_OVERLAP;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    // ARRAYS_OVERLAP(arr1, arr2) -> EXISTS(
    //   SELECT item1
    //   FROM UNNEST(arr1) as t1(item1)
    //   WHERE item1 IN (SELECT item2 FROM UNNEST(arr2) as t2(item2))
    // )
    RexNode arr1 = call.getOperands().get(0);
    RexNode arr2 = call.getOperands().get(1);

    RexBuilder rexBuilder = cx.getRexBuilder();
    RelBuilder relBuilder = cx.getRelBuilder();
    RexCorrelVariable rexCorrelVariable = cx.getRexCorrelVariable();
    return new CorrelatedUnnestQueryBuilder(rexCorrelVariable, relBuilder, rexBuilder)
        .unnest(arr1)
        .transform(
            builder -> {
              RexSubQuery inSubquery =
                  new CorrelatedUnnestQueryBuilder(rexCorrelVariable, relBuilder, rexBuilder)
                      .unnest(arr2)
                      .noOp()
                      .in(rexBuilder.makeInputRef(arr1.getType().getComponentType(), 0));
              builder.filter(inSubquery);
            })
        .exists();
  }
}

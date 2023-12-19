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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

public final class ArrayContainsConvertlet implements FunctionConvertlet {
  public static final FunctionConvertlet INSTANCE = new NullableArrayFunctionConvertlet(new ArrayContainsConvertlet());

  private ArrayContainsConvertlet() {}

  @Override
  public boolean matches(RexCall call) {
    // This convertlet is only used internally for ARRAYS_OVERLAP
    return false;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    // ARRAY_CONTAINS(arr, item) -> item IN (SELECT item2 FROM UNNEST(arr) as t(item2))
    RexNode array = call.getOperands().get(0);
    RexNode item = call.getOperands().get(1);

    return CorrelatedUnnestQueryBuilder.create(cx)
      .unnest(array)
      .noOp()
      .in(item);
  }
}

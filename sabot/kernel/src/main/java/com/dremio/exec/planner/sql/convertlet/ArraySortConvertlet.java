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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_SORT;

import com.dremio.common.exceptions.UserException;
import org.apache.calcite.rex.RexCall;

public final class ArraySortConvertlet implements FunctionConvertlet {
  public static final FunctionConvertlet INSTANCE =
      new NullableArrayFunctionConvertlet(new ArraySortConvertlet());

  private ArraySortConvertlet() {}

  @Override
  public boolean matches(RexCall call) {
    return call.getOperator() == ARRAY_SORT;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    throw UserException.planError()
        .message("ARRAY_SORT is currently not supported.")
        .buildSilently();
  }
}

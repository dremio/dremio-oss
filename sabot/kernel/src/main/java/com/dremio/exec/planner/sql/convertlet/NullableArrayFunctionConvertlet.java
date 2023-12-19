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

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;

import java.util.stream.Collectors;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;

public final class NullableArrayFunctionConvertlet extends NullableFunctionConvertlet {
  public NullableArrayFunctionConvertlet(FunctionConvertlet innerConvertlet) {
    super(innerConvertlet);
  }

  @Override
  public RexNode whenReturnNull(final RexBuilder rexBuilder, final RexCall originalCall) {
    return RexUtil.composeDisjunction(rexBuilder, originalCall
      .getOperands()
      .stream()
      .filter(operand -> operand.getType().getSqlTypeName() == SqlTypeName.ARRAY)
      .filter(operand -> operand.getType().isNullable())
      .map(operand -> rexBuilder.makeCall(IS_NULL, operand))
      .collect(Collectors.toList()));
  }
}

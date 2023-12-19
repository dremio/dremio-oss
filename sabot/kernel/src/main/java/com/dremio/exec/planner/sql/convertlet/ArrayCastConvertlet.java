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

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.type.SqlTypeName.ARRAY;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.planner.logical.RelDataTypeEqualityUtil;

public final class ArrayCastConvertlet implements FunctionConvertlet {
  public static final FunctionConvertlet INSTANCE = new NullableArrayFunctionConvertlet(new ArrayCastConvertlet());

  private ArrayCastConvertlet() {}

  @Override
  public boolean matches(RexCall call) {
    if (call.getOperator() != CAST) {
      return false;
    }

    if (!call.getType().getSqlTypeName().equals(ARRAY)) {
      return false;
    }

    RelDataType arrayType = call.getOperands().get(0).getType();
    RelDataType castType = call.getType();

    boolean castingNeeded = !RelDataTypeEqualityUtil.areEquals(
      castType,
      arrayType,
      false,
      false);

    return castingNeeded;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    // CAST(arr AS TYPE ARRAY) -> ARRAY(CAST(item as TYPE) FROM UNNEST(arr) as t(item))
    RexNode array = call.getOperands().get(0);
    RexBuilder rexBuilder = cx.getRexBuilder();

    RelDataType castType = call.getType();
    RexCall castedArray = CorrelatedUnnestQueryBuilder.create(cx)
      .unnest(array)
      .transform(builder -> builder
        .project(
          rexBuilder.makeCast(
            call.getType().getComponentType(),
            rexBuilder.makeInputRef(builder.peek(), 0))))
      .array();

    boolean hasNullabilityMismatch = castType.isNullable() && !castedArray.getType().isNullable();
    if (hasNullabilityMismatch) {
      /*
       * Casts an array to be nullable without using the CAST function.
       * If we did, then we would have infinite recursion in the CAST rewrite
       *
       * CASE
       *  WHEN false THEN null
       *  ELSE rexNode
       *  END
       *
       * This code eventually gets optimized out when we run it through the reduce rule.
       */
      RexNode falseLiteral = rexBuilder.makeLiteral(false);
      RexNode nullLiteral = rexBuilder.makeNullLiteral(call.getType());
      castedArray = (RexCall) rexBuilder.makeCall(
        CASE,
        falseLiteral,
        nullLiteral,
        castedArray);
    }

    return castedArray;
  }
}

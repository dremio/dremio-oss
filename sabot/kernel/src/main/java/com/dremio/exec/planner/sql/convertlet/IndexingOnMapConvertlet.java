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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.LAST_MATCHING_MAP_ENTRY_FOR_KEY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ITEM;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

public final class IndexingOnMapConvertlet implements FunctionConvertlet {
  public static final IndexingOnMapConvertlet INSTANCE = new IndexingOnMapConvertlet();

  private IndexingOnMapConvertlet() {}

  @Override
  public boolean matches(RexCall call) {
    if (call.getOperator() != ITEM) {
      return false;
    }

    if (call.getOperands().get(0).getType().getSqlTypeName() != SqlTypeName.MAP) {
      return false;
    }

    return true;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    RexBuilder rexBuilder = cx.getRexBuilder();
    RexNode map = call.getOperands().get(0);
    RexNode index = call.getOperands().get(1);
    RexNode lastMatchingMapEntryCall =
        rexBuilder.makeCall(LAST_MATCHING_MAP_ENTRY_FOR_KEY, map, index);
    RexNode valueStringLiteral = rexBuilder.makeLiteral("value");
    RexNode itemCall = rexBuilder.makeCall(ITEM, lastMatchingMapEntryCall, valueStringLiteral);
    return (RexCall) itemCall;
  }
}

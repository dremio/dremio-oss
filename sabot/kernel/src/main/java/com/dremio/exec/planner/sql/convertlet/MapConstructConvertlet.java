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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.DREMIO_INTERNAL_BUILDMAP;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.MAP_CONSTRUCT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;

import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

public class MapConstructConvertlet extends RexCallConvertlet {
  public static final MapConstructConvertlet INSTANCE = new MapConstructConvertlet();

  @Override
  public boolean matchesCall(RexCall call) {
    return call.getOperator() == MAP_CONSTRUCT;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    List<RexNode> keys = new LinkedList<>();
    List<RexNode> values = new LinkedList<>();
    List<RexNode> operands = call.getOperands();
    for (int i = 0; i < operands.size(); i++) {
      if (i % 2 == 0) {
        keys.add(operands.get(i));
      } else {
        values.add(operands.get(i));
      }
    }

    RexBuilder rexBuilder = cx.getRexBuilder();
    RexNode keysArrayConstructor = rexBuilder.makeCall(ARRAY_VALUE_CONSTRUCTOR, keys);
    RexNode valuesArrayConstructor = rexBuilder.makeCall(ARRAY_VALUE_CONSTRUCTOR, values);

    return (RexCall)
        rexBuilder.makeCall(DREMIO_INTERNAL_BUILDMAP, keysArrayConstructor, valuesArrayConstructor);
  }
}

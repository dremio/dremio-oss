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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.CONVERT_FROM;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.CONVERT_REPLACEUTF8;

import com.dremio.exec.planner.sql.ConvertFromOperators;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.NlsString;

/** Rewrites CONVERT_FROM(x, TYPE) to CONVERT_FROMTYPE(x) */
public final class ConvertFromConvertlet extends RexCallConvertlet {
  public static final RexCallConvertlet INSTANCE = new ConvertFromConvertlet();

  private ConvertFromConvertlet() {}

  @Override
  public boolean matchesCall(RexCall call) {
    return call.getOperator() == CONVERT_FROM;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    if (call.getOperands().size() == 3) {
      return (RexCall)
          cx.getRexBuilder()
              .makeCall(CONVERT_REPLACEUTF8, call.getOperands().get(0), call.getOperands().get(2));
    }

    RexNode typeNode = call.getOperands().get(1);
    String type = ((NlsString) (((RexLiteral) typeNode).getValue())).getValue();
    SqlOperator operator = ConvertFromOperators.convertTypeToOperator(type);
    return (RexCall) cx.getRexBuilder().makeCall(operator, call.getOperands().get(0));
  }
}

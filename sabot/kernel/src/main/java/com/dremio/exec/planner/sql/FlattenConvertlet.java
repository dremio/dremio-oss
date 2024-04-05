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
package com.dremio.exec.planner.sql;

import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;

public class FlattenConvertlet implements SqlRexConvertlet {

  public static final FlattenConvertlet INSTANCE = new FlattenConvertlet();

  private FlattenConvertlet() {}

  /*
   * Convert Flatten operators into distinct flatten calls.
   */
  @Override
  public RexNode convertCall(SqlRexContext cx, SqlCall call) {
    SqlFlattenOperator operator = (SqlFlattenOperator) call.getOperator();
    final List<RexNode> exprs = new LinkedList<>();

    for (SqlNode node : call.getOperandList()) {
      exprs.add(cx.convertExpression(node));
    }

    SqlFlattenOperator indexedOperator =
        operator.withIndex(((SqlValidatorImpl) cx.getValidator()).nextFlattenIndex());
    return cx.getRexBuilder().makeCall(indexedOperator, exprs);
  }
}

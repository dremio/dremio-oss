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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;

public class FlattenConvertlet implements SqlRexConvertlet {

  public final static FlattenConvertlet INSTANCE = new FlattenConvertlet();

  private FlattenConvertlet() {
  }

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

    SqlFlattenOperator indexedOperator = operator.withIndex(((SqlValidatorImpl)cx.getValidator()).nextFlattenIndex());
    final RexBuilder rexBuilder = cx.getRexBuilder();
    // Since we don't have any way of knowing if the output of the flatten is nullable, we should always assume it is.
    // This is especially important when accelerating a count(column) query, because the normalizer will convert it to
    // a count(1) if it thinks this column is non-nullable, and then remove the flatten altogether. This is actually a
    // problem with the fact that flatten is not really a project operator (because it can output more than one row per input).
    RelDataType type;
    if (exprs.get(0).getType() instanceof ArraySqlType) {
      type = exprs.get(0).getType().getComponentType();
    } else {
      type = rexBuilder
      .getTypeFactory()
      .createTypeWithNullability(
        rexBuilder
          .getTypeFactory()
          .createSqlType(SqlTypeName.ANY),
        true
      );
    }
    return rexBuilder.makeCall(type, indexedOperator, exprs);
  }


}


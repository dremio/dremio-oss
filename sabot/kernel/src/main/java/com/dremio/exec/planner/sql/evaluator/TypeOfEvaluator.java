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
package com.dremio.exec.planner.sql.evaluator;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/** Convertlet to implement TYPEOF by passing it to Calcite's type system. */
public final class TypeOfEvaluator implements FunctionEval {
  public static final TypeOfEvaluator INSTANCE = new TypeOfEvaluator();

  private TypeOfEvaluator() {}

  @Override
  public RexNode evaluate(EvaluationContext cx, RexCall call) {
    RexNode argument = call.getOperands().get(0);
    RelDataType relDataType = argument.getType();
    String relDataTypeString = relDataTypeToString(relDataType);
    RexNode type = cx.getRexBuilder().makeLiteral(relDataTypeString);
    RexNode casted =
        cx.getRexBuilder()
            .makeCast(cx.getRexBuilder().getTypeFactory().createSqlType(SqlTypeName.VARCHAR), type);
    return casted;
  }

  private static String relDataTypeToString(RelDataType relDataType) {
    String nullableMarker = (relDataType.isNullable() ? "?" : "");

    String toString;
    switch (relDataType.getSqlTypeName()) {
      case ARRAY:
        toString =
            "ARRAY<" + relDataTypeToString(relDataType.getComponentType()) + ">" + nullableMarker;
        break;

      case NULL:
        toString = "NULL";
        break;

      default:
        toString = relDataType.getSqlTypeName().toString() + nullableMarker;
    }

    return toString;
  }
}

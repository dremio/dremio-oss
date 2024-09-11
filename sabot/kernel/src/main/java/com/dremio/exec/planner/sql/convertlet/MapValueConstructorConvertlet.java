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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR;

import com.dremio.common.exceptions.UserException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;

public final class MapValueConstructorConvertlet extends RexCallConvertlet {
  public static final MapValueConstructorConvertlet INSTANCE = new MapValueConstructorConvertlet();
  private final List<SqlTypeFamily> SUPPORTED_FAMILIES =
      Arrays.asList(SqlTypeFamily.BOOLEAN, SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING);

  private MapValueConstructorConvertlet() {}

  @Override
  public boolean matchesCall(RexCall call) {
    return call.getOperator() == MAP_VALUE_CONSTRUCTOR;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    List<RexNode> keys = new LinkedList<>();
    List<RexNode> values = new LinkedList<>();

    List<RexNode> operands = call.getOperands();
    // These validations block calls of MAP[] with columns and functions arguments as per
    // requirements.
    // SqlOperandTypeChecker is not called for MAP[] so validations can't be put there
    if (operands.stream().anyMatch(op -> !op.isA(EnumSet.of(SqlKind.LITERAL, SqlKind.CAST)))) {
      throw UserException.validationError()
          .message("MAP expects all arguments to be literals.")
          .build();
    }

    for (int i = 0; i < operands.size(); i++) {
      (i % 2 == 0 ? keys : values).add(operands.get(i));
    }

    if (keys.stream().anyMatch(key -> !SqlTypeFamily.STRING.contains(key.getType()))) {
      throw UserException.validationError().message("MAP expects keys to be Characters.").build();
    }

    if (values.stream()
        .anyMatch(
            value ->
                SUPPORTED_FAMILIES.stream().noneMatch(type -> type.contains(value.getType()))
                    || ((RexLiteral) value).isNull())) {
      throw UserException.validationError()
          .message("MAP expects values to be Numbers, Booleans or Characters.")
          .build();
    }

    RelDataType valueRelType =
        cx.getRexBuilder()
            .getTypeFactory()
            .leastRestrictive(values.stream().map(RexNode::getType).collect(Collectors.toList()));
    if (valueRelType == null) {
      throw UserException.validationError()
          .message("MAP expects all values to have same type.")
          .build();
    }

    RexBuilder rexBuilder = cx.getRexBuilder();
    RexNode keysArrayConstructor = rexBuilder.makeCall(ARRAY_VALUE_CONSTRUCTOR, keys);
    RexNode valuesArrayConstructor = rexBuilder.makeCall(ARRAY_VALUE_CONSTRUCTOR, values);

    return (RexCall)
        rexBuilder.makeCall(DREMIO_INTERNAL_BUILDMAP, keysArrayConstructor, valuesArrayConstructor);
  }
}

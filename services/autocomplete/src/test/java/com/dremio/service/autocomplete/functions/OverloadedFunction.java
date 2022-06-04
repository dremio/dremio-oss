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
package com.dremio.service.autocomplete.functions;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.google.common.collect.ImmutableList;

/**
 * A SqlFunction that takes a variable number of arguments.
 */
public final class OverloadedFunction extends SqlFunction {
  public static final OverloadedFunction INSTANCE = new OverloadedFunction();

  private static final class OperandTypes {
    private OperandTypes() {}

    public static final List<SqlTypeName> TWO_ARG_1 = new ImmutableList.Builder<SqlTypeName>()
      .add(SqlTypeName.BINARY, SqlTypeName.BOOLEAN)
      .build();

    public static final List<SqlTypeName> TWO_ARG_2 = new ImmutableList.Builder<SqlTypeName>()
      .add(SqlTypeName.DATE, SqlTypeName.BINARY)
      .build();

    public static final List<SqlTypeName> THREE_ARG_1 = new ImmutableList.Builder<SqlTypeName>()
      .add(SqlTypeName.BOOLEAN, SqlTypeName.DATE, SqlTypeName.BINARY)
      .build();
  }

  private OverloadedFunction() {
    super("OVERLOADED_FUNCTION", SqlKind.OTHER, null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  public boolean checkOperandTypes(SqlCallBinding sqlCallBinding, boolean throwOnFailure) {
    switch (sqlCallBinding.operands().size()) {
    case 2:
      return checkArgTypes(sqlCallBinding, OperandTypes.TWO_ARG_1) || checkArgTypes(sqlCallBinding, OperandTypes.TWO_ARG_2);
    case 3:
      return checkArgTypes(sqlCallBinding, OperandTypes.THREE_ARG_1);
    default:
      return false;
    }
  }

  private static boolean checkArgTypes(SqlCallBinding sqlCallBinding, List<SqlTypeName> types) {
    if (sqlCallBinding.getOperandCount() != types.size()) {
      return false;
    }

    for (int i = 0; i < sqlCallBinding.getOperandCount(); i++) {
      RelDataType operand = sqlCallBinding.getOperandType(i);
      SqlTypeName operandSqlTypeName = operand.getSqlTypeName();
      if (operandSqlTypeName != types.get(i)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 3);
  }

  public RelDataType inferReturnType(
    SqlOperatorBinding opBinding) {
    return JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.DOUBLE);
  }
}

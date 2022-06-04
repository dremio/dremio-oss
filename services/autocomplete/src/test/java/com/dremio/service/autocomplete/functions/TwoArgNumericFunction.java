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

/**
 * A SqlFunction that takes two numeric operands and return a double.
 */
public final class TwoArgNumericFunction extends SqlFunction {
  public static final TwoArgNumericFunction INSTANCE = new TwoArgNumericFunction();

  private TwoArgNumericFunction() {
    super("TWO_ARG_NUMERIC_FUNCTION", SqlKind.OTHER, null, null, null, SqlFunctionCategory.NUMERIC);
  }

  @Override
  public String getAllowedSignatures(String opNameToUse) {
    return "TWO_ARG_NUMERIC_FUNCTION(<NUMERIC>)";
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  public boolean checkOperandTypes(SqlCallBinding sqlCallBinding, boolean throwOnFailure) {
    if (sqlCallBinding.operands().size() != 2) {
      return false;
    }

    RelDataType operand1 = sqlCallBinding.getOperandType(0);
    if(!SqlTypeName.NUMERIC_TYPES.contains(operand1.getSqlTypeName())) {
      return false;
    }

    RelDataType operand2 = sqlCallBinding.getOperandType(1);
    if(!SqlTypeName.NUMERIC_TYPES.contains(operand2.getSqlTypeName())) {
      return false;
    }

    return true;
  }

  public RelDataType inferReturnType(
    SqlOperatorBinding opBinding) {
    return JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.DOUBLE);
  }
}

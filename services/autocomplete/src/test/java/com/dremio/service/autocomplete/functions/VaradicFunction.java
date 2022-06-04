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
 * A SqlFunction that takes a variable number of arguments.
 */
public final class VaradicFunction extends SqlFunction {
  public static final VaradicFunction INSTANCE = new VaradicFunction();

  private VaradicFunction() {
    super("VARADIC_FUNCTION", SqlKind.OTHER, null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  @Override
  public String getAllowedSignatures(String opNameToUse) {
    return "VARADIC_FUNCTION(<VARCHAR>, <VARCHAR>, <VARCHAR>*)";
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.from(2);
  }

  public boolean checkOperandTypes(SqlCallBinding sqlCallBinding, boolean throwOnFailure) {
    if (sqlCallBinding.operands().size() < 2) {
      return false;
    }

    for (int i = 0; i < sqlCallBinding.operands().size(); i++) {
      RelDataType operand = sqlCallBinding.getOperandType(i);
      SqlTypeName operandSqlTypeName = operand.getSqlTypeName();
      if (operandSqlTypeName != SqlTypeName.VARCHAR) {
        return false;
      }
    }

    return true;
  }

  public RelDataType inferReturnType(
    SqlOperatorBinding opBinding) {
    return JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.DOUBLE);
  }
}

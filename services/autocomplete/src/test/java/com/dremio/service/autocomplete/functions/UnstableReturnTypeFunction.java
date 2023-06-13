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
 * A SqlFunction that is designed to return a different type depending on the input type.
 */
public final class UnstableReturnTypeFunction extends SqlFunction {
  public static final UnstableReturnTypeFunction INSTANCE = new UnstableReturnTypeFunction();

  private UnstableReturnTypeFunction() {
    super("UNSTABLE_RETURN_TYPE_FUNCTION", SqlKind.OTHER, null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  @Override
  public String getAllowedSignatures(String opNameToUse) {
    return "UNSTABLE_RETURN_TYPE_FUNCTION(<EXACT_TYPES>), UNSTABLE_RETURN_TYPE_FUNCTION(<ANY>)";
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(1);
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding sqlCallBinding, boolean throwOnFailure) {
    return sqlCallBinding.operands().size() == 1;
  }

  @Override
  public RelDataType inferReturnType(
    SqlOperatorBinding opBinding) {
    RelDataType relDataType = opBinding.getOperandType(0);

    RelDataType returnType;
    if (SqlTypeName.EXACT_TYPES.contains(relDataType.getSqlTypeName())) {
      returnType = JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.INTEGER);
    } else {
      returnType = JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.DECIMAL);
    }

    return returnType;
  }
}

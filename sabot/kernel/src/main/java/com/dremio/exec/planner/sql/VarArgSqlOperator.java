/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;

public class VarArgSqlOperator extends SqlFunction {
  private static final MajorType NONE = MajorType.getDefaultInstance();
  private final MajorType returnType;
  private final boolean isDeterministic;

  public VarArgSqlOperator(String name, MajorType returnType, boolean isDeterministic) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO), DynamicReturnType.INSTANCE, null, new Checker(), null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.returnType = returnType;
    this.isDeterministic = isDeterministic;
  }

  private static class Checker implements SqlOperandTypeChecker {

    private SqlOperandCountRange range = SqlOperandCountRanges.any();

    @Override
    public boolean checkOperandTypes(SqlCallBinding sqlCallBinding, boolean b) {
      return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return range;
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
      return opName + "(Dremio - Opaque)";
    }

    @Override
    public Consistency getConsistency() {
      return Consistency.NONE;
    }

    @Override
    public boolean isOptional(int i) {
      return true;
    }
  }

  @Override
  public boolean isDeterministic() {
    return isDeterministic;
  }

  protected RelDataType getReturnDataType(final RelDataTypeFactory factory) {
    if (MinorType.BIT.equals(returnType.getMinorType())) {
      return factory.createSqlType(SqlTypeName.BOOLEAN);
    }
    return factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.ANY), true);
  }

  private RelDataType getNullableReturnDataType(final RelDataTypeFactory factory) {
    return factory.createTypeWithNullability(getReturnDataType(factory), true);
  }

  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    if (NONE.equals(returnType)) {
      return validator.getTypeFactory().createSqlType(SqlTypeName.ANY);
    }
    /*
     * We return a nullable output type both in validation phase and in
     * Sql to Rel phase. We don't know the type of the output until runtime
     * hence have to choose the least restrictive type to avoid any wrong
     * results.
     */
    return getNullableReturnDataType(validator.getTypeFactory());
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    return getNullableReturnDataType(opBinding.getTypeFactory());
  }
}

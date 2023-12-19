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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.base.Objects;

public class SqlFlattenOperator extends SqlFunction {

  private int index;

  public SqlFlattenOperator(int index) {
    super(new SqlIdentifier("FLATTEN", SqlParserPos.ZERO),
      new SqlReturnTypeInference() {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
          final RelDataType operandType = opBinding.getOperandType(0);
          if (operandType instanceof ArraySqlType) {
            return ((ArraySqlType) operandType).getComponentType();
          } else {
            // Since we don't have any way of knowing if the output of the flatten is nullable, we should always assume it is.
            // This is especially important when accelerating a count(column) query, because the normalizer will convert it to
            // a count(1) if it thinks this column is non-nullable, and then remove the flatten altogether. This is actually a
            // problem with the fact that flatten is not really a project operator (because it can output more than one row per input).
            return opBinding.getTypeFactory()
              .createTypeWithNullability(opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY),true);
          }
        }
      },
      null,
      OperandTypes.ARRAY,
      null,
      SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.index = index;
  }

  public SqlFlattenOperator withIndex(int index){
    return new SqlFlattenOperator(index);
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof SqlFlattenOperator)) {
      return false;
    }
    SqlFlattenOperator castOther = (SqlFlattenOperator) other;
    return Objects.equal(index, castOther.index);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(index);
  }

  public int getIndex(){
    return index;
  }

  // Making flatten non-deterministic as reduce expression rule was replacing it as a constant
  // expression and producing the wrong results
  @Override
  public boolean isDeterministic() {
    return false;
  }

}

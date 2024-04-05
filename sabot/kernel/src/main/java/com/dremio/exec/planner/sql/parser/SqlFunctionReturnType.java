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
package com.dremio.exec.planner.sql.parser;

import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * This class is hold information on the return type for a UDF Function. It is implemented as an
 * Either Monad on Tabular and Scalar Function return types.
 */
public final class SqlFunctionReturnType extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("FUNTION_RETURN_TYPE", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          if (operands.length == 2) {
            return new SqlFunctionReturnType(
                pos, (SqlDataTypeSpec) operands[0], (SqlNodeList) operands[1]);
          }
          throw new IllegalArgumentException(
              "SqlFunctionReturnType.createCall() has to get 2 operands!");
        }
      };

  private SqlNodeList tabularReturnType;
  private SqlDataTypeSpec scalarReturnType;

  public SqlFunctionReturnType(
      SqlParserPos pos, SqlDataTypeSpec scalarReturnType, SqlNodeList tabularReturnType) {
    super(pos);
    final boolean isTabularEmpty = tabularReturnType == null || tabularReturnType.size() == 0;
    Preconditions.checkArgument(
        (!isTabularEmpty && scalarReturnType == null)
            || (isTabularEmpty && scalarReturnType != null),
        "Function return type should be either a scalar type or a tabular type");
    this.tabularReturnType = tabularReturnType;
    this.scalarReturnType = scalarReturnType;
  }

  public boolean isTabular() {
    return tabularReturnType != null && tabularReturnType.size() != 0;
  }

  public SqlDataTypeSpec getScalarReturnType() {
    return scalarReturnType;
  }

  public SqlNodeList getTabularReturnType() {
    return tabularReturnType;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    if (scalarReturnType != null) {
      return ImmutableList.of(scalarReturnType);
    } else {
      return ImmutableList.of(tabularReturnType);
    }
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (scalarReturnType != null) {
      scalarReturnType.unparse(writer, leftPrec, rightPrec);
    } else {
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, tabularReturnType);
    }
  }
}

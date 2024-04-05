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

import com.dremio.catalog.model.VersionContext;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

public class SqlDescribeDremioTable extends SqlDescribeTable {

  private final SqlTableVersionSpec sqlTableVersionSpec;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DESCRIBE_TABLE", SqlKind.DESCRIBE_TABLE) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 3, "SqlDescribeDremioTable.createCall() has to get 3 operands!");
          return new SqlDescribeDremioTable(
              pos,
              (SqlIdentifier) operands[0],
              (SqlTableVersionSpec) operands[1],
              (SqlIdentifier) operands[2]);
        }
      };

  public SqlDescribeDremioTable(
      SqlParserPos pos,
      SqlIdentifier table,
      SqlTableVersionSpec sqlTableVersionSpec,
      SqlIdentifier column) {
    super(pos, table, column);
    this.sqlTableVersionSpec = sqlTableVersionSpec;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(super.getTable(), getSqlTableVersionSpec(), super.getColumn());
  }

  public SqlTableVersionSpec getSqlTableVersionSpec() {
    return sqlTableVersionSpec;
  }

  public VersionContext.Type getRefType() {
    if (sqlTableVersionSpec != null) {
      return sqlTableVersionSpec
          .getTableVersionSpec()
          .getTableVersionContext()
          .asVersionContext()
          .getType();
    }
    return null;
  }

  public String getRefValue() {
    if (sqlTableVersionSpec != null) {
      return sqlTableVersionSpec
          .getTableVersionSpec()
          .getTableVersionContext()
          .asVersionContext()
          .getValue();
    }
    return null;
  }
}

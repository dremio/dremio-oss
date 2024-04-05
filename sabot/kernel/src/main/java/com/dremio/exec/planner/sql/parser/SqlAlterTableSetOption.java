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

import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/** ALTER TABLE table SET option = value */
public class SqlAlterTableSetOption extends SqlSetOption {

  public static final SqlSpecialOperator SET_TABLE_OPTION_OPERATOR =
      new SqlSpecialOperator("SET_TABLE_OPTION", SqlKind.SET_OPTION) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 5, "SqlSetTableOption.createCall() has to get 5 operands!");
          return new SqlAlterTableSetOption(
              pos,
              (SqlIdentifier) operands[0],
              (SqlIdentifier) operands[1],
              (SqlIdentifier) operands[2],
              operands[3],
              (SqlTableVersionSpec) operands[4]);
        }
      };

  private SqlIdentifier table;
  private final SqlTableVersionSpec tableVersionSpec;

  public SqlAlterTableSetOption(
      SqlParserPos pos,
      SqlIdentifier table,
      SqlSetOption sqlSetOption,
      SqlTableVersionSpec tableVersionSpec) {
    super(pos, sqlSetOption.getScope(), sqlSetOption.getName(), sqlSetOption.getValue());
    this.table = table;
    this.tableVersionSpec = tableVersionSpec;
  }

  private SqlAlterTableSetOption(
      SqlParserPos pos,
      SqlIdentifier table,
      SqlIdentifier scope,
      SqlIdentifier name,
      SqlNode value,
      SqlTableVersionSpec tableVersionSpec) {
    super(pos, scope.getSimple(), name, value);
    this.table = table;
    this.tableVersionSpec = tableVersionSpec;
  }

  @Override
  public SqlOperator getOperator() {
    return SET_TABLE_OPTION_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.<SqlNode>builder()
        .add(table)
        .addAll(super.getOperandList())
        .add(tableVersionSpec)
        .build();
  }

  public NamespaceKey getTable() {
    return new NamespaceKey(table.names);
  }

  public SqlTableVersionSpec getSqlTableVersionSpec() {
    return tableVersionSpec;
  }
}

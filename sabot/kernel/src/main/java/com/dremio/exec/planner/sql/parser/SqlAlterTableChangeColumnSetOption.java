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

import java.util.ArrayList;
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

import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;

/**
 * ALTER TABLE table CHANGE column SET option = value
 */
public class SqlAlterTableChangeColumnSetOption extends SqlSetOption {

  public static final SqlSpecialOperator SET_COLUMN_OPTION_OPERATOR = new SqlSpecialOperator("SET_COLUMN_OPTION", SqlKind.SET_OPTION) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 5, "SqlAlterTableChangeColumnSetOption.createCall() has to get 5 operands!");
      return new SqlAlterTableChangeColumnSetOption(pos, (SqlIdentifier) operands[0], (SqlIdentifier) operands[1], (SqlIdentifier) operands[2],
        (SqlIdentifier) operands[3], operands[4]);
    }
  };

  private SqlIdentifier table;
  private SqlIdentifier column;

  public SqlAlterTableChangeColumnSetOption(SqlParserPos pos, SqlIdentifier table, SqlIdentifier column, SqlSetOption sqlSetOption) {
    super(pos, sqlSetOption.getScope(), sqlSetOption.getName(), sqlSetOption.getValue());
    this.table = table;
    this.column = column;
  }

  private SqlAlterTableChangeColumnSetOption(SqlParserPos pos, SqlIdentifier table, SqlIdentifier column, SqlIdentifier scope, SqlIdentifier name, SqlNode value) {
    super(pos, scope.getSimple(), name, value);
    this.table = table;
    this.column = column;
  }

  @Override
  public SqlOperator getOperator() {
    return SET_COLUMN_OPTION_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    // Note: cannot use ImmutableList because we permit null for the option value.
    final List<SqlNode> nodes = new ArrayList<>();
    nodes.add(table);
    nodes.add(column);
    nodes.addAll(super.getOperandList());
    return nodes;
  }

  public NamespaceKey getTable() {
    return new NamespaceKey(table.names);
  }

  public String getColumn() {
    return column.getSimple();
  }
}

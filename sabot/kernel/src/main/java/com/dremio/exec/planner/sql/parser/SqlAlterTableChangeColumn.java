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

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * ALTER TABLE name CHANGE column_name new_column_spec
 */
public class SqlAlterTableChangeColumn extends SqlAlterTable {

  public static final SqlSpecialOperator CHANGE_COLUMN_OPERATOR = new SqlSpecialOperator("CHANGE_COLUMN", SqlKind.ALTER_TABLE) {

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 3, "SqlAlterTableChangeColumn.createCall() " +
          "has to get 3 operands!");

      return new SqlAlterTableChangeColumn(
          pos,
          (SqlIdentifier) operands[0],
          (SqlIdentifier) operands[1],
          (DremioSqlColumnDeclaration) operands[2]);
    }
  };

  protected final SqlIdentifier column;
  protected final DremioSqlColumnDeclaration newColumnSpec;

  public SqlAlterTableChangeColumn(SqlParserPos pos, SqlIdentifier tblName, SqlIdentifier column, DremioSqlColumnDeclaration newColumnSpec) {
    super(pos, tblName);
    this.column = column;
    this.newColumnSpec = newColumnSpec;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    writer.keyword("CHANGE");
    column.unparse(writer, leftPrec, rightPrec);
    newColumnSpec.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SqlOperator getOperator() {
    return CHANGE_COLUMN_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(tblName, column, newColumnSpec);
  }

  public String getColumnToChange() {
    return column.getSimple();
  }

  public DremioSqlColumnDeclaration getNewColumnSpec() {
    return newColumnSpec;
  }
}

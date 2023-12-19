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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * ALTER TABLE tblname ADD COLUMNS (colspec1 [, colspec2, colspec3])
 */
public class SqlAlterTableAddColumns extends SqlAlterTable {

  public static final SqlSpecialOperator ADD_COLUMNS_OPERATOR = new SqlSpecialOperator("ADD_COLUMNS", SqlKind.ALTER_TABLE) {

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 3, "SqlAlterTableAddColumns.createCall() " +
          "has to get 3 operands!");

      if (((SqlNodeList) operands[1]).getList().size() == 0) {
        throw UserException.parseError().message("Columns not specified.").buildSilently();
      }

      return new SqlAlterTableAddColumns(
        pos,
        (SqlIdentifier) operands[0],
        (SqlNodeList) operands[1],
        (SqlTableVersionSpec) operands[2]);
    }
  };

  protected final SqlNodeList columnList;
  private final SqlTableVersionSpec tableVersionSpec;

  public SqlAlterTableAddColumns(SqlParserPos pos, SqlIdentifier tblName, SqlNodeList columnList, SqlTableVersionSpec tableVersionSpec) {
    super(pos, tblName);
    this.columnList = columnList;
    this.tableVersionSpec = tableVersionSpec;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    writer.keyword("ADD");
    writer.keyword("COLUMNS");
    SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, columnList);
  }

  @Override
  public SqlOperator getOperator() {
    return ADD_COLUMNS_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(tblName, columnList, tableVersionSpec);
  }

  public SqlNodeList getColumnList() {
    return columnList;
  }

  public SqlTableVersionSpec getSqlTableVersionSpec() {
    return tableVersionSpec;
  }
}

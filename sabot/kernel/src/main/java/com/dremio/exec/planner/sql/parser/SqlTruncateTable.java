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

import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class SqlTruncateTable extends SqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("TRUNCATE_TABLE", SqlKind.OTHER_DDL) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 3, "SqlTruncateTable.createCall() " +
          "has to get 3 operands!");
      return new SqlTruncateTable(pos, (SqlIdentifier) operands[0], (SqlLiteral) operands[1], (SqlLiteral) operands[2]);
    }
  };

  private SqlIdentifier tableName;
  private boolean tableExistenceCheck;
  private boolean tableKeywordPresent;

  public SqlTruncateTable(SqlParserPos pos, SqlIdentifier tableName, SqlLiteral tableExistenceCheck,
                          SqlLiteral tableKeywordPresent) {
    this(pos, tableName, tableExistenceCheck.booleanValue(), tableKeywordPresent.booleanValue());
  }

  public SqlTruncateTable(SqlParserPos pos, SqlIdentifier tableName, boolean tableExistenceCheck,
                          boolean tableKeywordPresent) {
    super(pos);
    this.tableName = tableName;
    this.tableExistenceCheck = tableExistenceCheck;
    this.tableKeywordPresent = tableKeywordPresent;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(
        tableName,
        SqlLiteral.createBoolean(tableExistenceCheck, SqlParserPos.ZERO),
        SqlLiteral.createBoolean(tableKeywordPresent, SqlParserPos.ZERO)
    );
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("TRUNCATE");
    if (tableKeywordPresent) {
      writer.keyword("TABLE");
    }
    if (tableExistenceCheck) {
      writer.keyword("IF");
      writer.keyword("EXISTS");
    }
    tableName.unparse(writer, leftPrec, rightPrec);
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(tableName.names);
  }

  public boolean checkTableExistence() {
    return tableExistenceCheck;
  }

}

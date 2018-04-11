/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import com.google.common.collect.ImmutableList;

public class SqlDropTable extends SqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DROP_TABLE", SqlKind.DROP_TABLE) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlDropTable(pos, (SqlIdentifier) operands[0], (SqlLiteral) operands[1]);
    }
  };

  private SqlIdentifier tableName;
  private boolean tableExistenceCheck;

  public SqlDropTable(SqlParserPos pos, SqlIdentifier tableName, SqlLiteral tableExistenceCheck) {
    this(pos, tableName, tableExistenceCheck.booleanValue());
  }

  public SqlDropTable(SqlParserPos pos, SqlIdentifier tableName, boolean tableExistenceCheck) {
    super(pos);
    this.tableName = tableName;
    this.tableExistenceCheck = tableExistenceCheck;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    final List<SqlNode> ops =
        ImmutableList.of(
            tableName,
            SqlLiteral.createBoolean(tableExistenceCheck, SqlParserPos.ZERO)
        );
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    writer.keyword("TABLE");
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

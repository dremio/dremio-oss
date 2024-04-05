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

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.DropPrimaryKeyHandler;
import com.dremio.exec.planner.sql.handlers.direct.SimpleDirectHandler;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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

/** ALTER TABLE tblname DROP PRIMARY KEY */
public class SqlAlterTableDropPrimaryKey extends SqlAlterTable
    implements SimpleDirectHandler.Creator {

  public static final SqlSpecialOperator DROP_PRIMARY_KEY_OPERATOR =
      new SqlSpecialOperator("DROP_PRIMARY_KEY", SqlKind.ALTER_TABLE) {

        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 2,
              "SqlAlterTableDropPrimaryKey.createCall() " + "has to get 2 operands!");

          return new SqlAlterTableDropPrimaryKey(
              pos, (SqlIdentifier) operands[0], (SqlTableVersionSpec) operands[1]);
        }
      };
  protected final SqlTableVersionSpec sqlTableVersionSpec;

  public SqlAlterTableDropPrimaryKey(
      SqlParserPos pos, SqlIdentifier tblName, SqlTableVersionSpec sqlTableVersionSpec) {
    super(pos, tblName);
    this.sqlTableVersionSpec = sqlTableVersionSpec;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SqlOperator getOperator() {
    return DROP_PRIMARY_KEY_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(tblName, sqlTableVersionSpec);
  }

  @Override
  public SimpleDirectHandler toDirectHandler(QueryContext context) {
    return new DropPrimaryKeyHandler(context.getCatalog());
  }

  public SqlTableVersionSpec getSqlTableVersionSpec() {
    return sqlTableVersionSpec;
  }
}

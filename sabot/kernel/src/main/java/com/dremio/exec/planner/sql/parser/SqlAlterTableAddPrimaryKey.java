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
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.direct.AddPrimaryKeyHandler;
import com.dremio.exec.planner.sql.handlers.direct.SimpleDirectHandler;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * ALTER TABLE tblname ADD PRIMARY KEY (colspec1 [, colspec2, colspec3])
 */
public class SqlAlterTableAddPrimaryKey extends SqlAlterTable implements SimpleDirectHandler.Creator {

  public static final SqlSpecialOperator ADD_PRIMARY_KEY_OPERATOR = new SqlSpecialOperator("ADD_PRIMARY_KEY", SqlKind.ALTER_TABLE) {

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 3, "SqlAlterTableAddPrimaryKey.createCall() " +
          "has to get 3 operands!");

      if (((SqlNodeList) operands[1]).getList().size() == 0) {
        throw UserException.parseError().message("Columns not specified.").buildSilently();
      }

      return new SqlAlterTableAddPrimaryKey(
          pos,
          (SqlIdentifier) operands[0],
          (SqlNodeList) operands[1],
          (SqlTableVersionSpec) operands[2]);
    }
  };

  protected final SqlNodeList columnList;
  protected final SqlTableVersionSpec sqlTableVersionSpec;

  public SqlAlterTableAddPrimaryKey(SqlParserPos pos, SqlIdentifier tblName, SqlNodeList columnList, SqlTableVersionSpec sqlTableVersionSpec) {
    super(pos, tblName);
    this.columnList = columnList;
    this.sqlTableVersionSpec = sqlTableVersionSpec;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    writer.keyword("ADD");
    writer.keyword("PRIMARY");
    writer.keyword("KEY");
    SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, columnList);
  }

  @Override
  public SqlOperator getOperator() {
    return ADD_PRIMARY_KEY_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(tblName, columnList, sqlTableVersionSpec);
  }

  public SqlNodeList getColumnList() {
    return columnList;
  }

  @Override
  public SimpleDirectHandler toDirectHandler(QueryContext context) {
    return new AddPrimaryKeyHandler(context.getCatalog());
  }

  public SqlTableVersionSpec getSqlTableVersionSpec() {
    return sqlTableVersionSpec;
  }
}

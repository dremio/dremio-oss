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
import com.google.common.collect.ImmutableList;

public class SqlDropView extends SqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DROP_VIEW", SqlKind.DROP_VIEW) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlDropView(pos, (SqlIdentifier) operands[0], (SqlLiteral) operands[1]);
    }
  };

  private SqlIdentifier viewName;
  private boolean viewExistenceCheck;

  public SqlDropView(SqlParserPos pos, SqlIdentifier viewName, SqlLiteral viewExistenceCheck) {
    this(pos, viewName, viewExistenceCheck.booleanValue());
  }

  public SqlDropView(SqlParserPos pos, SqlIdentifier viewName, boolean viewExistenceCheck) {
    super(pos);
    this.viewName = viewName;
    this.viewExistenceCheck = viewExistenceCheck;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    final List<SqlNode> ops =
        ImmutableList.of(
            viewName,
            SqlLiteral.createBoolean(viewExistenceCheck, SqlParserPos.ZERO)
        );
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    writer.keyword("VIEW");
    if (viewExistenceCheck) {
      writer.keyword("IF");
      writer.keyword("EXISTS");
    }
    viewName.unparse(writer, leftPrec, rightPrec);
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(viewName.names);
  }

  public boolean checkViewExistence() {
    return viewExistenceCheck;
  }

}

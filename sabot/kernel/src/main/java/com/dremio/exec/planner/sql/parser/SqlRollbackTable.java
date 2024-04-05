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

import com.dremio.exec.catalog.RollbackOption;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
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
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlRollbackTable extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("ROLLBACK", SqlKind.ROLLBACK) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 3, "SqlRollbackTable.createCall() " + "has to get 3 operands!");
          return new SqlRollbackTable(
              pos, (SqlIdentifier) operands[0], (SqlLiteral) operands[1], operands[2]);
        }
      };

  private final SqlIdentifier tableName;
  private final boolean snapshotKeywordPresent;
  private final SqlNode specifier;

  public SqlRollbackTable(
      SqlParserPos pos,
      SqlIdentifier tableName,
      SqlLiteral snapshotKeywordPresent,
      SqlNode specifier) {
    this(pos, tableName, snapshotKeywordPresent.booleanValue(), specifier);
  }

  public SqlRollbackTable(
      SqlParserPos pos,
      SqlIdentifier tableName,
      boolean snapshotKeywordPresent,
      SqlNode specifier) {
    super(pos);
    this.tableName = tableName;
    this.snapshotKeywordPresent = snapshotKeywordPresent;
    this.specifier = specifier;
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
            SqlLiteral.createBoolean(snapshotKeywordPresent, SqlParserPos.ZERO),
            specifier);
    return ops;
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(tableName.names);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ROLLBACK");
    writer.keyword("TABLE");
    tableName.unparse(writer, leftPrec, rightPrec);
    writer.keyword("TO");
    if (snapshotKeywordPresent) {
      writer.keyword("SNAPSHOT");
      specifier.unparse(writer, leftPrec, rightPrec);
    } else {
      writer.keyword("TIMESTAMP");
      specifier.unparse(writer, leftPrec, rightPrec);
    }
  }

  public RollbackOption getRollbackOption() throws Exception {
    String literal = ((SqlLiteral) specifier).getValueAs(String.class);
    RollbackOption.Type type;
    Long rollbackValue = null;
    if (snapshotKeywordPresent) {
      type = RollbackOption.Type.SNAPSHOT;
      try {
        rollbackValue = Long.parseLong(literal);
      } catch (Exception e) {
        throw com.dremio.common.exceptions.UserException.parseError(e)
            .message("Literal %s is an invalid snapshot id", literal)
            .buildSilently();
      }
    } else {
      type = RollbackOption.Type.TIME;
      Object value = SqlHandlerUtil.convertToTimeInMillis(literal, specifier.getParserPosition());
      rollbackValue = Long.parseLong(String.valueOf(value));
    }

    return new RollbackOption(type, rollbackValue, literal);
  }
}

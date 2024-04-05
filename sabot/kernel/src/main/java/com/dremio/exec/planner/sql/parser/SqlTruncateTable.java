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

public class SqlTruncateTable extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("TRUNCATE TABLE", SqlKind.OTHER_DDL) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 5, "SqlTruncateTable.createCall() " + "has to get 5 operands!");
          return new SqlTruncateTable(
              pos,
              (SqlLiteral) operands[0],
              (SqlLiteral) operands[1],
              (SqlIdentifier) operands[2],
              ((SqlLiteral) operands[3]).symbolValue(ReferenceType.class),
              (SqlIdentifier) operands[4]);
        }
      };
  private static final SqlLiteral sqlLiteralNull = SqlLiteral.createNull(SqlParserPos.ZERO);
  private SqlIdentifier tableName;
  private boolean shouldErrorIfTableDoesNotExist;
  private boolean tableKeywordPresent;

  private final ReferenceType refType;
  private final SqlIdentifier refValue;

  public SqlTruncateTable(
      SqlParserPos pos,
      SqlLiteral shouldErrorIfTableDoesNotExist,
      SqlLiteral tableKeywordPresent,
      SqlIdentifier tableName,
      ReferenceType refType,
      SqlIdentifier refValue) {
    this(
        pos,
        shouldErrorIfTableDoesNotExist.booleanValue(),
        tableKeywordPresent.booleanValue(),
        tableName,
        refType,
        refValue);
  }

  public SqlTruncateTable(
      SqlParserPos pos,
      boolean shouldErrorIfTableDoesNotExist,
      boolean tableKeywordPresent,
      SqlIdentifier tableName,
      ReferenceType refType,
      SqlIdentifier refValue) {
    super(pos);
    this.tableName = tableName;
    this.shouldErrorIfTableDoesNotExist = shouldErrorIfTableDoesNotExist;
    this.tableKeywordPresent = tableKeywordPresent;
    this.refType = refType;
    this.refValue = refValue;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(SqlLiteral.createBoolean(shouldErrorIfTableDoesNotExist, SqlParserPos.ZERO));
    ops.add(SqlLiteral.createBoolean(tableKeywordPresent, SqlParserPos.ZERO));
    ops.add(tableName);
    if (refType == null) {
      ops.add(sqlLiteralNull);
    } else {
      ops.add(SqlLiteral.createSymbol(getRefType(), SqlParserPos.ZERO));
    }
    ops.add(refValue);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("TRUNCATE");
    if (tableKeywordPresent) {
      writer.keyword("TABLE");
    }
    if (!shouldErrorIfTableDoesNotExist) {
      writer.keyword("IF");
      writer.keyword("EXISTS");
    }
    tableName.unparse(writer, leftPrec, rightPrec);

    if (refType != null && refValue != null) {
      writer.keyword("AT");
      writer.keyword(refType.toString());
      refValue.unparse(writer, leftPrec, rightPrec);
    }
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(tableName.names);
  }

  public boolean shouldErrorIfTableDoesNotExist() {
    return shouldErrorIfTableDoesNotExist;
  }

  public ReferenceType getRefType() {
    return refType;
  }

  public SqlIdentifier getRefValue() {
    return refValue;
  }
}

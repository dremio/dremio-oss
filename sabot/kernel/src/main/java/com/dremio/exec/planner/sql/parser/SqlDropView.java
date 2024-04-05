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

public class SqlDropView extends SqlCall {
  private static final SqlLiteral sqlLiteralNull = SqlLiteral.createNull(SqlParserPos.ZERO);

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DROP_VIEW", SqlKind.DROP_VIEW) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return new SqlDropView(
              pos,
              (SqlIdentifier) operands[0],
              (SqlLiteral) operands[1],
              ((SqlLiteral) operands[2]).symbolValue(ReferenceType.class),
              (SqlIdentifier) operands[3]);
        }
      };

  private SqlIdentifier viewName;
  private boolean shouldErrorIfViewDoesNotExist;
  private ReferenceType refType;
  private SqlIdentifier refValue;

  public SqlDropView(
      SqlParserPos pos,
      SqlIdentifier viewName,
      SqlLiteral shouldErrorIfViewDoesNotExist,
      ReferenceType referenceType,
      SqlIdentifier refValue) {
    this(pos, viewName, shouldErrorIfViewDoesNotExist.booleanValue(), referenceType, refValue);
  }

  public SqlDropView(
      SqlParserPos pos,
      SqlIdentifier viewName,
      boolean shouldErrorIfViewDoesNotExist,
      ReferenceType referenceType,
      SqlIdentifier refValue) {
    super(pos);
    this.viewName = viewName;
    this.shouldErrorIfViewDoesNotExist = shouldErrorIfViewDoesNotExist;
    this.refType = referenceType;
    this.refValue = refValue;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(viewName);
    ops.add(SqlLiteral.createBoolean(shouldErrorIfViewDoesNotExist, SqlParserPos.ZERO));
    ops.add(
        getRefType() == null
            ? sqlLiteralNull
            : SqlLiteral.createSymbol(getRefType(), SqlParserPos.ZERO));
    ops.add(refValue);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    writer.keyword("VIEW");
    if (!shouldErrorIfViewDoesNotExist) {
      writer.keyword("IF");
      writer.keyword("EXISTS");
    }
    viewName.unparse(writer, leftPrec, rightPrec);
    if (refType != null && refValue != null) {
      writer.keyword("AT");
      writer.keyword(refType.toString());
      refValue.unparse(writer, leftPrec, rightPrec);
    }
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(viewName.names);
  }

  public ReferenceType getRefType() {
    return refType;
  }

  public SqlIdentifier getRefValue() {
    return refValue;
  }

  public boolean shouldErrorIfViewDoesNotExist() {
    return shouldErrorIfViewDoesNotExist;
  }
}

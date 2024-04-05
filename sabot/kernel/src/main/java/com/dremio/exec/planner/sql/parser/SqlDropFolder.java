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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.lang.reflect.Constructor;
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

/**
 * Implements SQL DROP FOLDER to drop folder under Nessie Repository. Represents statements like:
 * DROP FOLDER [ IF NOT EXISTS ] [source.]parentFolderName[.childFolder] [ AT BRANCH refValue ]
 */
public class SqlDropFolder extends SqlCall {
  private static final SqlLiteral sqlLiteralNull = SqlLiteral.createNull(SqlParserPos.ZERO);

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DROP_FOLDER", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 4, "SqlDropFolder.createCall() has to get 4 operands!");
          return new SqlDropFolder(
              pos,
              (SqlLiteral) operands[0],
              (SqlIdentifier) operands[1],
              ((SqlLiteral) operands[2]).symbolValue(ReferenceType.class),
              (SqlIdentifier) operands[3]);
        }
      };
  private final SqlLiteral existenceCheck;
  private final SqlIdentifier folderName;
  private final ReferenceType refType;
  private final SqlIdentifier refValue;

  public SqlDropFolder(
      SqlParserPos pos,
      SqlLiteral existenceCheck,
      SqlIdentifier folderName,
      ReferenceType refType,
      SqlIdentifier refValue) {
    super(pos);
    this.existenceCheck = existenceCheck;
    this.folderName = Preconditions.checkNotNull(folderName);
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
    ops.add(existenceCheck);
    ops.add(folderName);
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
    writer.keyword("DROP");
    writer.keyword("FOLDER");

    if (existenceCheck.booleanValue()) {
      writer.keyword("IF");
      writer.keyword("NOT");
      writer.keyword("EXISTS");
    }

    folderName.unparse(writer, leftPrec, rightPrec);

    if (refType != null && refValue != null) {
      writer.keyword("AT");
      writer.keyword(refType.toString());
      refValue.unparse(writer, leftPrec, rightPrec);
    }
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(folderName.names);
  }

  public ReferenceType getRefType() {
    return refType;
  }

  public SqlIdentifier getRefValue() {
    return refValue;
  }

  public SqlLiteral getExistenceCheck() {
    return existenceCheck;
  }

  public SqlDirectHandler<?> toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.DropFolderHandler");
      final Constructor<?> ctor = cl.getConstructor(Catalog.class, UserSession.class);

      return (SqlDirectHandler<?>) ctor.newInstance(context.getCatalog(), context.getSession());
    } catch (ClassNotFoundException e) {
      throw UserException.unsupportedError(e)
          .message("DROP FOLDER action is not supported.")
          .buildSilently();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}

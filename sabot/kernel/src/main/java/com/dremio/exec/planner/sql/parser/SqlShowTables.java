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
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.Lists;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Optional;
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
 * Sql parse tree node to represent statement: SHOW TABLES [ AT ( REF[ERENCE] | BRANCH | TAG |
 * COMMIT ) refValue [AS OF timestamp] ] [ ( FROM | IN ) source] [ LIKE 'pattern']
 */
public class SqlShowTables extends SqlVersionSourceRefBase {

  private final SqlNode likePattern;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SHOW_TABLES", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return new SqlShowTables(
              pos,
              operands[0] != null
                  ? ((SqlLiteral) operands[0]).symbolValue(ReferenceType.class)
                  : null,
              (SqlIdentifier) operands[1],
              operands[2],
              (SqlIdentifier) operands[3],
              operands[4]);
        }
      };

  public SqlShowTables(
      SqlParserPos pos,
      ReferenceType refType,
      SqlIdentifier refValue,
      SqlNode timestamp,
      SqlIdentifier source,
      SqlNode likePattern) {
    super(pos, source, refType, refValue, timestamp);
    this.likePattern = likePattern;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> opList = Lists.newArrayList();
    SqlLiteral refTypeSqlLiteral = null;
    if (getRefType() != null) { // SqlLiteral.createSymbol has asserts preventing null parameters.
      refTypeSqlLiteral = SqlLiteral.createSymbol(getRefType(), SqlParserPos.ZERO);
    }
    opList.add(refTypeSqlLiteral);
    opList.add(getRefValue());
    opList.add(getTimestampAsSqlNode());
    opList.add(getSourceName());
    opList.add(likePattern);
    return opList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("TABLES");

    unparseRef(writer, leftPrec, rightPrec, "AT");
    unparseSourceName(writer, leftPrec, rightPrec);

    if (likePattern != null) {
      writer.keyword("LIKE");
      likePattern.unparse(writer, leftPrec, rightPrec);
    }
  }

  public SqlNode getLikePattern() {
    return likePattern;
  }

  public Optional<NamespaceKey> getSourcePath() {
    return (getSourceName() != null)
        ? Optional.of(new NamespaceKey(getSourceName().names))
        : Optional.empty();
  }

  @Override
  public SqlDirectHandler<?> toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl =
          Class.forName("com.dremio.exec.planner.sql.handlers.direct.ShowTablesHandler");
      final Constructor<?> ctor =
          cl.getConstructor(Catalog.class, OptionResolver.class, UserSession.class);
      return (SqlDirectHandler<?>)
          ctor.newInstance(context.getCatalog(), context.getOptions(), context.getSession());
    } catch (ClassNotFoundException e) {
      throw UserException.unsupportedError(e)
          .message("SHOW TABLES action is not supported.")
          .buildSilently();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}

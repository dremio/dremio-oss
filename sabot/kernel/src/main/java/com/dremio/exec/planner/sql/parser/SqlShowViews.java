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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.collect.Lists;

/**
 * Sql parse tree node to represent statement:
 * SHOW VIEWS
 * [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue [AS OF timestamp] ]
 * [ ( FROM | IN ) source]
 * [ LIKE 'pattern']
 */

public class SqlShowViews extends SqlVersionSourceRefBase {

  private final SqlNode likePattern;
  public static final SqlSpecialOperator OPERATOR =
    new SqlSpecialOperator("SHOW_VIEWS", SqlKind.OTHER) {
      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos,
                                SqlNode... operands) {
        return new SqlShowViews(pos,
          operands[0] != null ? ((SqlLiteral) operands[0]).symbolValue(ReferenceType.class) : null,
          (SqlIdentifier) operands[1],
          operands[2],
          (SqlIdentifier) operands[3],
          operands[4]);
      }
    };

  public SqlShowViews(SqlParserPos pos, ReferenceType refType, SqlIdentifier refValue, SqlNode timestamp, SqlIdentifier source, SqlNode likePattern) {
    super(pos, source, refType, refValue, timestamp);
    this.likePattern = likePattern;
  }

  public SqlNode getLikePattern() { return likePattern; }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> opList = Lists.newArrayList();
    SqlLiteral refTypeSqlLiteral = null;
    if(getRefType() != null) { // SqlLiteral.createSymbol has asserts preventing null parameters.
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
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("VIEWS");

    unparseRef(writer, leftPrec, rightPrec, "AT");
    unparseSourceName(writer, leftPrec, rightPrec);

    if (likePattern != null) {
      writer.keyword("LIKE");
      likePattern.unparse(writer, leftPrec, rightPrec);
    }
  }
  @Override
  public SqlDirectHandler<?> toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.direct.ShowViewsHandler");
      final Constructor<?> ctor = cl.getConstructor(
        Catalog.class,
        OptionResolver.class,
        UserSession.class);
      return (SqlDirectHandler<?>) ctor.newInstance(
        context.getCatalog(),
        context.getOptions(),
        context.getSession());
    } catch (ClassNotFoundException e) {
      throw UserException.unsupportedError(e)
        .message("SHOW VIEWS action is not supported.")
        .buildSilently();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}

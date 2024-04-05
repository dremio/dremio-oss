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
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
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
 * Implements SQL ALTER TAG ASSIGN to assign a new reference to the given tag.
 *
 * <p>ALTER TAG tagName ASSIGN ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue [ AS OF timestamp ]
 * [ IN sourceName ]
 */
public final class SqlAssignTag extends SqlVersionSourceRefBase {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("ASSIGN_TAG", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 5, "SqlAssignTag.createCall() has to get 5 operands!");
          return new SqlAssignTag(
              pos,
              (SqlIdentifier) operands[0],
              ((SqlLiteral) operands[1]).symbolValue(ReferenceType.class),
              (SqlIdentifier) operands[2],
              operands[3],
              (SqlIdentifier) operands[4]);
        }
      };

  private final SqlIdentifier tagName;

  public SqlAssignTag(
      SqlParserPos pos,
      SqlIdentifier tagName,
      ReferenceType refType,
      SqlIdentifier refValue,
      SqlNode timestamp,
      SqlIdentifier sourceName) {
    super(pos, sourceName, refType, refValue, timestamp);
    this.tagName = tagName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(tagName);
    ops.add(SqlLiteral.createSymbol(getRefType(), SqlParserPos.ZERO));
    ops.add(getRefValue());
    ops.add(getTimestampAsSqlNode());
    ops.add(getSourceName());
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER");
    writer.keyword("TAG");
    tagName.unparse(writer, leftPrec, rightPrec);

    unparseRef(writer, leftPrec, rightPrec, "ASSIGN");
    unparseSourceName(writer, leftPrec, rightPrec);
  }

  @Override
  public SqlDirectHandler<?> toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.AssignTagHandler");
      final Constructor<?> ctor = cl.getConstructor(QueryContext.class);

      return (SqlDirectHandler<?>) ctor.newInstance(context);
    } catch (ClassNotFoundException e) {
      throw UserException.unsupportedError(e)
          .message("ALTER TAG ASSIGN action is not supported.")
          .buildSilently();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  public SqlIdentifier getTagName() {
    return tagName;
  }
}

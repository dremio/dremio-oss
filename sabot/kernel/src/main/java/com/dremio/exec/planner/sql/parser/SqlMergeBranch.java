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
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Implements SQL ALTER BRANCH MERGE to merge a source branch into a target branch. Represents
 * statements like: ALTER BRANCH MERGE sourceBranchName [ INTO targetBranchName ] IN source
 */
public final class SqlMergeBranch extends SqlVersionBase {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("MERGE_BRANCH", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 3, "SqlMergeBranch.createCall() has to get 3 operands!");
          return new SqlMergeBranch(
              pos,
              (SqlIdentifier) operands[0],
              (SqlIdentifier) operands[1],
              (SqlIdentifier) operands[2]);
        }
      };

  private final SqlIdentifier sourceBranchName;
  private final SqlIdentifier targetBranchName;

  public SqlMergeBranch(
      SqlParserPos pos,
      SqlIdentifier sourceBranchName,
      SqlIdentifier targetBranchName,
      SqlIdentifier source) {
    super(pos, source);
    this.sourceBranchName = sourceBranchName;
    this.targetBranchName = targetBranchName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(sourceBranchName);
    ops.add(targetBranchName);
    ops.add(getSourceName());

    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER");
    writer.keyword("BRANCH");
    writer.keyword("MERGE");
    sourceBranchName.unparse(writer, leftPrec, rightPrec);

    if (targetBranchName != null) {
      writer.keyword("INTO");
      targetBranchName.unparse(writer, leftPrec, rightPrec);
    }

    writer.keyword("IN");
    getSourceName().unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SqlDirectHandler<?> toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.MergeBranchHandler");
      final Constructor<?> ctor = cl.getConstructor(QueryContext.class);

      return (SqlDirectHandler<?>) ctor.newInstance(context);
    } catch (ClassNotFoundException e) {
      throw UserException.unsupportedError(e)
          .message("ALTER BRANCH MERGE action is not supported.")
          .buildSilently();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  public SqlIdentifier getSourceBranchName() {
    return sourceBranchName;
  }

  public SqlIdentifier getTargetBranchName() {
    return targetBranchName;
  }
}

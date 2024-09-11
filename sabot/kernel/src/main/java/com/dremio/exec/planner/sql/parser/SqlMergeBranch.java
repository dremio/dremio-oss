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

import static com.dremio.exec.planner.sql.parser.ParserUtil.mergeBehaviorToSql;
import static com.dremio.exec.planner.sql.parser.ParserUtil.sqlToMergeBehavior;

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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.projectnessie.model.MergeBehavior;

/**
 * Implements SQL MERGE BRANCH to merge a source branch into a target branch.
 *
 * <p>MERGE BRANCH sourceBranchName [INTO targetBranchName] [IN <sourceName>]
 */
public final class SqlMergeBranch extends SqlVersionBase {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("MERGE_BRANCH", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 9, "SqlMergeBranch.createCall() has to get 4 operands!");
          return new SqlMergeBranch(
              pos,
              (SqlLiteral) operands[0],
              (SqlIdentifier) operands[1],
              (SqlIdentifier) operands[2],
              (SqlIdentifier) operands[3],
              sqlToMergeBehavior((SqlLiteral) operands[4]),
              sqlToMergeBehavior((SqlLiteral) operands[5]),
              sqlToMergeBehavior((SqlLiteral) operands[6]),
              (SqlNodeList) operands[7],
              (SqlNodeList) operands[8]);
        }
      };

  private final SqlLiteral isDryRun;

  private final SqlIdentifier sourceBranchName;
  private final SqlIdentifier targetBranchName;
  private final MergeBehavior defaultMergeBehavior;

  private final MergeBehavior mergeBehavior1;

  private final MergeBehavior mergeBehavior2;

  private final SqlNodeList exceptContentList1;
  private final SqlNodeList exceptContentList2;

  public SqlMergeBranch(
      SqlParserPos pos,
      SqlLiteral isDryRun,
      SqlIdentifier sourceBranchName,
      SqlIdentifier targetBranchName,
      SqlIdentifier sourceName,
      MergeBehavior defaultMergeBehavior,
      MergeBehavior mergeBehavior1,
      MergeBehavior mergeBehavior2,
      SqlNodeList exceptContentList1,
      SqlNodeList exceptContentList2) {
    super(pos, sourceName);
    this.isDryRun = isDryRun;
    this.sourceBranchName = sourceBranchName;
    this.targetBranchName = targetBranchName;
    this.defaultMergeBehavior = defaultMergeBehavior;
    this.mergeBehavior1 = mergeBehavior1;
    this.mergeBehavior2 = mergeBehavior2;
    this.exceptContentList1 = exceptContentList1;
    this.exceptContentList2 = exceptContentList2;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(isDryRun);
    ops.add(sourceBranchName);
    ops.add(targetBranchName);
    ops.add(getSourceName());
    ops.add(createMergeBehaviorSqlLiteral(defaultMergeBehavior));
    ops.add(createMergeBehaviorSqlLiteral(mergeBehavior1));
    ops.add(createMergeBehaviorSqlLiteral(mergeBehavior2));
    ops.add(exceptContentList1);
    ops.add(exceptContentList2);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("MERGE");
    writer.keyword("BRANCH");

    if (isDryRun.booleanValue()) {
      writer.keyword("DRY");
      writer.keyword("RUN");
    }

    sourceBranchName.unparse(writer, leftPrec, rightPrec);

    if (targetBranchName != null) {
      writer.keyword("INTO");
      targetBranchName.unparse(writer, leftPrec, rightPrec);
    }

    unparseSourceName(writer, leftPrec, rightPrec);

    if (defaultMergeBehavior != null) {
      writer.keyword("ON");
      writer.keyword("CONFLICT");
      writer.keyword(mergeBehaviorToSql(defaultMergeBehavior));

      if (mergeBehavior1 != null) {
        writer.keyword("EXCEPT");
        writer.keyword(mergeBehaviorToSql(mergeBehavior1));
        exceptContentList1.unparse(writer, leftPrec, rightPrec);
      }

      if (mergeBehavior2 != null) {
        writer.keyword("EXCEPT");
        writer.keyword(mergeBehaviorToSql(mergeBehavior2));
        exceptContentList2.unparse(writer, leftPrec, rightPrec);
      }
    }
  }

  private SqlLiteral createMergeBehaviorSqlLiteral(MergeBehavior mergeBehavior) {
    if (mergeBehavior == null) {
      return SqlLiteral.createNull(SqlParserPos.ZERO);
    }
    return SqlLiteral.createCharString(mergeBehaviorToSql(mergeBehavior), SqlParserPos.ZERO);
  }

  @Override
  public SqlDirectHandler<?> toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.MergeBranchHandler");
      final Constructor<?> ctor = cl.getConstructor(QueryContext.class);

      return (SqlDirectHandler<?>) ctor.newInstance(context);
    } catch (ClassNotFoundException e) {
      throw UserException.unsupportedError(e)
          .message("MERGE BRANCH action is not supported.")
          .buildSilently();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  public SqlLiteral getIsDryRun() {
    return isDryRun;
  }

  public SqlIdentifier getSourceBranchName() {
    return sourceBranchName;
  }

  public SqlIdentifier getTargetBranchName() {
    return targetBranchName;
  }

  public MergeBehavior getDefaultMergeBehavior() {
    return defaultMergeBehavior;
  }

  public MergeBehavior getMergeBehavior1() {
    return mergeBehavior1;
  }

  public MergeBehavior getMergeBehavior2() {
    return mergeBehavior2;
  }

  public SqlNodeList getExceptContentList1() {
    return exceptContentList1;
  }

  public SqlNodeList getExceptContentList2() {
    return exceptContentList2;
  }
}

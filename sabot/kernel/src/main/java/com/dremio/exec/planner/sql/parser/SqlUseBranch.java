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

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SimpleDirectHandler;
import com.dremio.exec.planner.sql.handlers.direct.UseBranchHandler;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Implements SQL Branch use to set a Nessie branch as current default branch from the system. Represents statements like:
 * USE BRANCH branch_name
 */
public final class SqlUseBranch extends SqlCall implements SimpleDirectHandler.Creator {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("USE_BRANCH", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 1, "SqlUseBranch.createCall() has to get 1 operands!");
      return new SqlUseBranch(
        pos,
        (SqlIdentifier) operands[0]
      );
    }
  };
  private final SqlIdentifier branchName;

  public SqlUseBranch(SqlParserPos pos,
                      SqlIdentifier branchName) {
    super(pos);
    this.branchName = Preconditions.checkNotNull(branchName);
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(branchName);

    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("USE");
    writer.keyword("BRANCH");
    branchName.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SimpleDirectHandler toDirectHandler(QueryContext context) {
    return new UseBranchHandler(context.getSession(), context.getOptions());
  }

  public SqlIdentifier getBranchName() {
    return branchName;
  }
}


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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
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
import org.jetbrains.annotations.NotNull;

public class SqlAlterPipeStatus extends SqlCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("ALTER_PIPE_STATUS", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(operands.length == 2);
          return new SqlAlterPipeStatus(
              pos, (SqlIdentifier) operands[0], ((SqlLiteral) operands[1]).booleanValue());
        }
      };
  protected final SqlIdentifier pipeName;
  private final boolean pipeExecutionStatus;

  public SqlAlterPipeStatus(SqlParserPos pos, SqlIdentifier pipeName, boolean pipeExecutionStatus) {
    super(pos);
    this.pipeName = pipeName;
    this.pipeExecutionStatus = pipeExecutionStatus;
  }

  @NotNull
  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @NotNull
  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> operands = new ArrayList<>();
    operands.add(pipeName);
    operands.add(SqlLiteral.createBoolean(pipeExecutionStatus, SqlParserPos.ZERO));
    return operands;
  }

  public SqlIdentifier getPipeName() {
    return pipeName;
  }

  public boolean getPipeExecutionStatus() {
    return pipeExecutionStatus;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER");
    writer.keyword("PIPE");
    pipeName.unparse(writer, leftPrec, rightPrec);
    writer.keyword("SET");
    writer.keyword("PIPE_EXECUTION_RUNNING");
    writer.keyword("=");
    if (pipeExecutionStatus) {
      writer.keyword("TRUE");
    } else {
      writer.keyword("FALSE");
    }
  }
}

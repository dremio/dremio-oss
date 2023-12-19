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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.base.Preconditions;

/**
 * Sql node representing DESCRIBE PIPE command
 */
public class SqlDescribePipe extends SqlCall {

  private final SqlIdentifier pipeName;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DESCRIBE PIPE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 1, String.format("Invalid number of operands : %d", operands.length));
      return new SqlDescribePipe(
        pos,
        (SqlIdentifier) operands[0]
      );
    }
  };

  public SqlDescribePipe(SqlParserPos pos, SqlIdentifier pipeName) {
    super(pos);
    this.pipeName = pipeName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public SqlIdentifier getPipeName() {
    return pipeName;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> operands = new ArrayList<>();
    operands.add(pipeName);
    return operands;
  }
}

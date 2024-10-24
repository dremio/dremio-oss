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
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.NotNull;

public class SqlAlterEngine extends SqlManageEngine {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("ALTER_ENGINE", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(operands.length == 3);
          return new SqlAlterEngine(
              pos,
              (SqlIdentifier) operands[0],
              (SqlNumericLiteral) operands[1],
              (SqlNumericLiteral) operands[2]);
        }
      };

  public SqlAlterEngine(
      SqlParserPos pos,
      SqlIdentifier engineName,
      SqlNumericLiteral minReplicas,
      SqlNumericLiteral maxReplicas) {
    super(pos, engineName, minReplicas, maxReplicas);
  }

  @Override
  public @NotNull SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public @NotNull List<SqlNode> getOperandList() {
    List<SqlNode> operands = new ArrayList<>();
    operands.add(engineName);
    operands.add(minReplicas);
    operands.add(maxReplicas);
    return operands;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER");
    writer.keyword("ENGINE");
    engineName.unparse(writer, leftPrec, rightPrec);
    writer.keyword("SET");
    boolean addComma = minReplicas != null && maxReplicas != null;
    if (minReplicas != null) {
      writer.keyword("MIN_REPLICAS = ");
      writer.keyword(minReplicas.toString());
    }
    if (addComma) {
      writer.keyword(",");
    }
    if (maxReplicas != null) {
      writer.keyword("MAX_REPLICAS = ");
      writer.keyword(maxReplicas.toString());
    }
  }
}

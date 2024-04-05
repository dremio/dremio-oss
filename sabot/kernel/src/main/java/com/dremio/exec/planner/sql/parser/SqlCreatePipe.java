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

public class SqlCreatePipe extends SqlManagePipe {

  private final boolean ifNotExists;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE_PIPE", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(operands.length == 6);
          return new SqlCreatePipe(
              pos,
              (SqlIdentifier) operands[0],
              operands[1],
              (SqlNumericLiteral) operands[2],
              (SqlIdentifier) operands[3],
              (SqlIdentifier) operands[4],
              ((SqlLiteral) operands[5]).booleanValue());
        }
      };

  public SqlCreatePipe(
      SqlParserPos pos,
      SqlIdentifier pipeName,
      SqlNode sqlCopyInto,
      SqlNumericLiteral dedupLookbackPeriod,
      SqlIdentifier notificationProvider,
      SqlIdentifier notificationQueueRef,
      boolean ifNotExists) {
    super(
        pos,
        pipeName,
        sqlCopyInto,
        dedupLookbackPeriod,
        notificationProvider,
        notificationQueueRef);
    this.ifNotExists = ifNotExists;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public boolean hasIfNotExists() {
    return ifNotExists;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> operands = new ArrayList<>();
    operands.add(pipeName);
    operands.add(sqlCopyInto);
    operands.add(dedupLookbackPeriod);
    operands.add(notificationProvider);
    operands.add(notificationQueueRef);
    operands.add(SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO));
    return operands;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("PIPE");
    if (ifNotExists) {
      writer.keyword("IF");
      writer.keyword("NOT");
      writer.keyword("EXISTS");
    }
    pipeName.unparse(writer, leftPrec, rightPrec);
    if (dedupLookbackPeriod != null) {
      writer.keyword("DEDUPE_LOOKBACK_PERIOD");
      writer.keyword(dedupLookbackPeriod.toString());
    }
    if (notificationProvider != null) {
      writer.keyword("NOTIFICATION_PROVIDER");
      notificationProvider.unparse(writer, leftPrec, rightPrec);
      writer.keyword("NOTIFICATION_QUEUE_REFERENCE");
      notificationQueueRef.unparse(writer, leftPrec, rightPrec);
    }
    writer.keyword("AS");
    sqlCopyInto.unparse(writer, leftPrec, rightPrec);
  }
}

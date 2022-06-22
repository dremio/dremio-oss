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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.base.Preconditions;

/**
 * Extends Calcite's SqlDelete to add the ability to extend the table columns with system columns.
 */
public class SqlDeleteFromTable extends SqlDelete implements SqlDmlOperator {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DELETE", SqlKind.DELETE) {

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 3, "SqlDelete.createCall() has to get 3 operands!");

      // We ignore operands[2] which contains the ALIAS since we don't allow that for now.
      return new SqlDeleteFromTable(pos, operands[0], operands[1], (SqlIdentifier)operands[2]);
    }
  };

  public SqlDeleteFromTable(SqlParserPos pos, SqlNode targetTable, SqlNode condition, SqlIdentifier alias) {
    super(pos, targetTable, condition, null, alias);
  }

  public void extendTableWithDataFileSystemColumns() {
    if (getTargetTable().getKind() == SqlKind.IDENTIFIER) {
      setOperand(0, DmlUtils.extendTableWithDataFileSystemColumns(getTargetTable()));
    }
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }
}

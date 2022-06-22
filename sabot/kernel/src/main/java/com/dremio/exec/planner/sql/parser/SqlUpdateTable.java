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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.base.Preconditions;

/**
 * Extends Calcite's SqlUpdate to add the ability to:
 *  1. extend the table columns with system columns.
 *  2. do custom parsing (like remove ALIAS).
 */
public class SqlUpdateTable extends SqlUpdate implements SqlDmlOperator {

  // Create a separate `extendedTargetTable` to handle extended columns, as there's
  // no way to set SqlUpdate::targetTable without an assertion being thrown.
  private SqlNode extendedTargetTable;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("UPDATE", SqlKind.UPDATE) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 5, "SqlUpdateTable.createCall() has to get 5 operands!");

      // We ignore operands[4] which contains the ALIAS since we don't allow that for now.
      return new SqlUpdateTable(pos, operands[0], (SqlNodeList) operands[1], (SqlNodeList) operands[2], operands[3], (SqlIdentifier)operands[4]);
    }
  };

  public SqlUpdateTable(SqlParserPos pos,
                        SqlNode targetTable,
                        SqlNodeList targetColumnList,
                        SqlNodeList sourceExpressionList,
                        SqlNode condition,
                        SqlIdentifier alias) {
    super(pos, targetTable, targetColumnList, sourceExpressionList, condition, null, alias);
  }

  public void extendTableWithDataFileSystemColumns() {
    if (extendedTargetTable == null) {
      extendedTargetTable = DmlUtils.extendTableWithDataFileSystemColumns(getTargetTable());
    }
  }

  @Override
  public SqlNode getTargetTable() {
    return extendedTargetTable == null ? super.getTargetTable() : extendedTargetTable;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }
}

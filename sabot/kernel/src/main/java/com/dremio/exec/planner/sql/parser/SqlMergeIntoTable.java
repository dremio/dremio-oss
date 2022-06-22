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
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Extends Calcite's SqlMerge to add the ability to:
 *  1. extend the table columns with system columns.
 *  2. do custom parsing (like remove ALIAS, keywords).
 */
public class SqlMergeIntoTable extends SqlMerge implements SqlDmlOperator {

  // Create a separate `extendedTargetTable` to handle extended columns, as there's
  // no way to set SqlUpdate::targetTable without an assertion being thrown.
  private SqlNode extendedTargetTable;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("MERGE", SqlKind.MERGE) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 6, "SqlMergeIntoTable.createCall() has to get 6 operands!");
      return new SqlMergeIntoTable(pos, operands[0], operands[1], operands[2], (SqlUpdate)operands[3], (SqlInsert)operands[4], (SqlIdentifier)operands[5]);
    }
  };

  public SqlMergeIntoTable(SqlParserPos pos,
                           SqlNode targetTable,
                           SqlNode condition,
                           SqlNode source,
                           SqlUpdate updateCall,
                           SqlInsert insertCall,
                           SqlIdentifier alias)  {
    super(pos, targetTable, condition, source, updateCall, insertCall, null, alias);
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

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(getTargetTable(),  getCondition(), getSourceTableRef(), getUpdateCall(), getInsertCall(), getAlias());
  }
}

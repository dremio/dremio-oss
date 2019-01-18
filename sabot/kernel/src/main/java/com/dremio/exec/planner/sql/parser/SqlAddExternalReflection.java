/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class SqlAddExternalReflection extends SqlSystemCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("ADD_EXTERNAL_REFLECTION", SqlKind.OTHER_DDL) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 3, "SqlAddExternalReflection.createCall() has to get 3 operands!");
      return new SqlAddExternalReflection(
        pos,
        (SqlIdentifier) operands[0],
        (SqlIdentifier) operands[1],
        (SqlIdentifier) operands[2]
      );
    }
  };

  private final SqlIdentifier tblName;
  private final SqlIdentifier name;
  private final SqlIdentifier targetTable;

  public SqlAddExternalReflection(SqlParserPos pos, SqlIdentifier tblName, SqlIdentifier name, SqlIdentifier targetTable) {
    super(pos);
    this.tblName = tblName;
    this.name = name;
    this.targetTable = targetTable;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.<SqlNode>of(tblName, name, targetTable);
  }

  public SqlIdentifier getTblName() {
    return tblName;
  }

  public SqlIdentifier getName() {
    return name;
  }

  public SqlIdentifier getTargetTable() {
    return targetTable;
  }

}

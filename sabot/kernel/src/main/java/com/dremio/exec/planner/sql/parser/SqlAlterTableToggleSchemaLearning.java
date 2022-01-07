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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * ALTER TABLE tblname ENABLE/DISABLE SCHEMA LEARNING
 */
public class SqlAlterTableToggleSchemaLearning extends SqlAlterTable {

  public static final SqlSpecialOperator TOGGLE_SCHEMA_LEARNING_OPERATOR = new SqlSpecialOperator("TOGGLE_SCHEMA_LEARNING", SqlKind.ALTER_TABLE) {

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 2, "SqlToggleSchemaLearning.createCall() " +
        "has to get 2 operands!");

      return new SqlAlterTableToggleSchemaLearning(
        pos,
        (SqlIdentifier) operands[0],
        (SqlLiteral) operands[1]);
    }
  };

  private SqlLiteral enableSchemaLearning;

  public SqlAlterTableToggleSchemaLearning(SqlParserPos pos, SqlIdentifier tblName, SqlLiteral enableSchemaLearning) {
    super(pos, tblName);
    this.enableSchemaLearning = enableSchemaLearning;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    if ((boolean) enableSchemaLearning.getValue()) {
      writer.keyword("ENABLE");
    } else {
      writer.keyword("DISABLE");
    }
    writer.keyword("SCHEMA");
    writer.keyword("LEARNING");
  }

  @Override
  public SqlOperator getOperator() {
    return TOGGLE_SCHEMA_LEARNING_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(tblName, enableSchemaLearning);
  }

}

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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

/**
 * ANALYZE TABLE <name> [ for_clause ] statistics_clause
 *
 * for_clause: FOR  [ ALL COLUMNS | COLUMNS ( <list_of_columns> ) ]
 *
 * statistics_clause: COMPUTE STATISTICS | DELETE STATISTICS
 */
public class SqlAnalyzeTableStatistics extends SqlCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("ANALYZE_TABLE_STATISTICS", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlAnalyzeTableStatistics(pos, (SqlIdentifier) operands[0], (SqlLiteral) operands[1], (SqlNodeList) operands[2]);
    }
  };

  private final SqlIdentifier table;
  private final SqlNodeList columns;
  private final SqlLiteral isAnalyze;

  public SqlAnalyzeTableStatistics(SqlParserPos pos, SqlIdentifier table, SqlLiteral isAnalyze, SqlNodeList columns) {
    super(pos);
    this.table = table;
    this.isAnalyze = isAnalyze;
    this.columns = columns;
  }

  public SqlIdentifier getTable() {
    return table;
  }

  public SqlNodeList getColumns() {
    return columns;
  }

  public SqlLiteral isAnalyze() {
    return isAnalyze;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    if (columns != null) {
      return ImmutableList.<SqlNode>builder().add(table).add(isAnalyze).add(columns).build();
    }
    return ImmutableList.<SqlNode>builder().add(table).add(isAnalyze).build();
  }
}

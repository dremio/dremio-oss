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

import com.google.common.collect.Lists;

/**
 * SqlNode for EXPLAIN JSON
 */
public class SqlExplainJson extends SqlCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("EXPLAIN_JSON", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlExplainJson(pos, operands[0], (SqlIdentifier) operands[1]);
    }
  };

  private SqlNode query;
  private SqlIdentifier phase;

  public SqlExplainJson(
      SqlParserPos pos,
      SqlNode query,
      SqlIdentifier phase) {
    super(pos);
    this.query = query;
    this.phase = phase;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(query);
    ops.add(phase);
    return ops;
  }

  public String getPhase() {
    if(phase != null) {
      return phase.getSimple().toUpperCase();
    } else {
      return "ORIGINAL";
    }
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("EXPLAIN");
    writer.keyword("JSON");
    if(phase != null) {
      phase.unparse(writer, 0, 0);
    }
    writer.keyword("FOR");
    query.unparse(writer, leftPrec, rightPrec);
  }

  public SqlNode getQuery() {
    return query;
  }

}

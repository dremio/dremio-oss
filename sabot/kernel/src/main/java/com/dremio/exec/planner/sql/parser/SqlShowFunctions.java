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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Sql parse tree node to represent statement: SHOW Functions [ LIKE 'pattern'] */
public class SqlShowFunctions extends SqlCall {

  private final SqlNode likePattern;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SHOW_FUNCTIONS", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return new SqlShowFunctions(pos, operands[0]);
        }
      };

  public SqlShowFunctions(SqlParserPos pos, SqlNode likePattern) {
    super(pos);
    this.likePattern = likePattern;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> opList = Lists.newArrayList();
    opList.add(likePattern);
    return opList;
  }

  public SqlNode getLikePattern() {
    return likePattern;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("Functions");

    if (likePattern != null) {
      writer.keyword("LIKE");
      likePattern.unparse(writer, leftPrec, rightPrec);
    }
  }
}

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
import com.google.common.collect.ImmutableList;

/**
 * A <code>SqlDescribeFunction</code> is a node of a parse tree that represents a
 * {@code DESCRIBE Function} statement.
 */
public class SqlDescribeFunction extends SqlCall {

  private final SqlIdentifier Function;

  public static final SqlSpecialOperator OPERATOR =
    new SqlSpecialOperator("DESCRIBE_FUNCTION", SqlKind.OTHER) {
      @Override public SqlCall createCall(SqlLiteral functionQualifier,
        SqlParserPos pos, SqlNode... operands) {
        Preconditions.checkArgument(operands.length == 1, "SqlDescribeFunction.createCall() has to get 2 operands!");
        return new SqlDescribeFunction(pos, (SqlIdentifier) operands[0]);
      }
    };

  /** Creates a SqlDescribeFunction. */
  public SqlDescribeFunction(SqlParserPos pos,
    SqlIdentifier Function) {
    super(pos);
    this.Function = Function;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DESCRIBE");
    writer.keyword("FUNCTION");
    Function.unparse(writer, leftPrec, rightPrec);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(Function);
  }

  public SqlIdentifier getFunction() {
    return Function;
  }

}

// End SqlDescribeFunction.java

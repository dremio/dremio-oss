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

import java.util.ArrayList;
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

/**
 * A representation of a partition transform in the SQL parse tree.
 */
public class SqlPartitionTransform extends SqlCall {

  public static final SqlSpecialOperator PARTITION_TRANSFORM_OPERATOR = new SqlSpecialOperator(
    "PARTITION_TRANSFORM", SqlKind.OTHER_FUNCTION);

  public static final String IDENTITY = "identity";

  private final SqlIdentifier columnName;
  private final SqlIdentifier transformName;
  private final List<SqlLiteral> transformArguments;

  public SqlPartitionTransform(SqlIdentifier columnName, SqlParserPos pos) {
    this(columnName, new SqlIdentifier(IDENTITY, pos), new ArrayList<>(), pos);
  }

  public SqlPartitionTransform(SqlIdentifier columnName, SqlIdentifier transformName,
                               List<SqlLiteral> transformArguments, SqlParserPos pos) {
    super(pos);
    this.columnName = Preconditions.checkNotNull(columnName);
    this.transformName = Preconditions.checkNotNull(transformName);
    this.transformArguments = Preconditions.checkNotNull(transformArguments);
  }

  public SqlIdentifier getColumnName() {
    return columnName;
  }

  public SqlIdentifier getTransformName() {
    return transformName;
  }

  public List<SqlLiteral> getTransformArguments() {
    return transformArguments;
  }

  public boolean isIdentityTransform() {
    return transformName.isSimple() && transformName.getSimple().equals(IDENTITY);
  }

  @Override
  public SqlOperator getOperator() {
    return PARTITION_TRANSFORM_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> operands = new ArrayList<>(transformArguments);
    operands.add(columnName);
    return operands;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (isIdentityTransform()) {
      columnName.unparse(writer, leftPrec, rightPrec);
    } else {
      transformName.unparse(writer, leftPrec, rightPrec);
      writer.keyword("(");
      for (SqlLiteral arg : transformArguments) {
        arg.unparse(writer, leftPrec, rightPrec);
        writer.keyword(",");
      }
      columnName.unparse(writer, leftPrec, rightPrec);
      writer.keyword(")");
    }
  }
}

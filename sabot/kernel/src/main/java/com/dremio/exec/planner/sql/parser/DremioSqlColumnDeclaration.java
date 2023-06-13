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

import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

/**
 * Parse tree for {@code UNIQUE}, {@code PRIMARY KEY} constraints.
 * <p>And {@code FOREIGN KEY}, when we support it.
 */
public class DremioSqlColumnDeclaration extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
    new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL) {
      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        if (operands.length == 2) {
          return new DremioSqlColumnDeclaration(pos, (SqlColumnPolicyPair) operands[0], (SqlDataTypeSpec) operands[1], null);
        } else if (operands.length == 3) {
          return new DremioSqlColumnDeclaration(pos, (SqlColumnPolicyPair) operands[0], (SqlDataTypeSpec) operands[1], (SqlNode) operands[2]);
        }
        throw new IllegalArgumentException("SqlSpecialOperator.createCall() has to get 2 or 3 operands!");
      }
  };

  private final SqlIdentifier name;
  private final SqlDataTypeSpec dataType;
  private final SqlPolicy policy;
  private final SqlNode expression;
  private final ColumnStrategy strategy;

  /**
   * Creates a SqlColumnDeclaration.
   */
  public DremioSqlColumnDeclaration(SqlParserPos pos, SqlColumnPolicyPair columnPolicyPair,
                                    SqlDataTypeSpec dataType, SqlNode expression) {
    super(pos);
    this.name = columnPolicyPair.getName();
    this.dataType = dataType;
    this.expression = expression;
    this.policy = columnPolicyPair.getPolicy();
    this.strategy = ColumnStrategy.NULLABLE;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    if (expression == null) {
      return ImmutableList.of(new SqlColumnPolicyPair(pos, name, policy), dataType);
    } else {
      return ImmutableList.of(new SqlColumnPolicyPair(pos, name, policy), dataType, expression);
    }
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, 0, 0);
    dataType.unparse(writer, 0, 0);
    if (dataType.getNullable() != null && !dataType.getNullable()) {
      writer.keyword("NOT NULL");
    }
    if (expression != null) {
      switch (strategy) {
        case VIRTUAL:
        case STORED:
          writer.keyword("AS");
          exp(writer);
          writer.keyword(strategy.name());
          break;
        case DEFAULT:
          writer.keyword("DEFAULT");
          exp(writer);
          break;
        default:
          throw new AssertionError("unexpected: " + strategy);
      }
    }

    if (policy != null) {
      policy.unparse(writer, leftPrec, rightPrec);
    }
  }

  private void exp(SqlWriter writer) {
    if (writer.isAlwaysUseParentheses()) {
      expression.unparse(writer, 0, 0);
    } else {
      writer.sep("(");
      expression.unparse(writer, 0, 0);
      if (policy != null) {
        policy.unparse(writer, 0, 0);
      }
      writer.sep(")");
    }
  }

  public SqlIdentifier getName() {
    return name;
  }

  public SqlDataTypeSpec getDataType() {
    return dataType;
  }

  public SqlNode getExpression() {
    return expression;
  }

  public ColumnStrategy getStrategy() {
    return strategy;
  }

  public SqlPolicy getPolicy() {
    return policy;
  }
}

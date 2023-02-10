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

import com.google.common.collect.ImmutableList;

/**
 * SqlColumnPolicyPair
 */
public class SqlColumnPolicyPair extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
    new SqlSpecialOperator("COLUMN_POLICY_PAIR", SqlKind.OTHER) {
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        if(operands.length == 1) {
            return new SqlColumnPolicyPair(pos, (SqlIdentifier) operands[0], null);
          } else if(operands.length ==2) {
            return new SqlColumnPolicyPair(pos, (SqlIdentifier) operands[0], (SqlPolicy) operands[1]);
          }
        throw new IllegalArgumentException("SqlSpecialOperator.createCall() has to get 1 or 2 operand(s)!");
      }
    };

  private final SqlIdentifier name;
  private final SqlPolicy policy;

  /**
   * Creates a SqlColumnPolicyPair.
   */
  public SqlColumnPolicyPair(SqlParserPos pos, SqlIdentifier name, SqlPolicy policy) {
    super(pos);
    this.name = name;
    this.policy = policy;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    if (policy == null) {
      return ImmutableList.of(name);
    }
    return ImmutableList.of(name, policy);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, 0, 0);
    if (policy != null) {
      policy.unparse(writer, leftPrec, rightPrec);
    }
  }

  public SqlIdentifier getName() {
    return name;
  }

  public SqlPolicy getPolicy() {
    return policy;
  }

  @Override
  public String toString() {
    if (policy == null) {
      return name.toString();
    }
    return super.toString();
  }
}

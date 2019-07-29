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
import org.apache.calcite.util.ImmutableNullableList;

/**
 * SQL node tree for <code>FORGET TABLE table_identifier </code>
 */
public class SqlRefreshTable extends SqlSystemCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("REFRESH_TABLE", SqlKind.OTHER) {
        @Override public SqlCall createCall(SqlLiteral functionQualifier,
            SqlParserPos pos, SqlNode... operands) {
          return new SqlRefreshTable(pos,
            (SqlIdentifier) operands[0],
            (SqlLiteral) operands[1],
            (SqlLiteral) operands[2],
            (SqlLiteral) operands[3]);
        }
      };

  private SqlIdentifier table;
  private SqlLiteral deleteUnavail;
  private SqlLiteral forceUp;
  private SqlLiteral promotion;

  /** Creates a SqlForgetTable. */
  public SqlRefreshTable(SqlParserPos pos, SqlIdentifier table, SqlLiteral deleteUnavail, SqlLiteral forceUp, SqlLiteral promotion) {
    super(pos);
    this.table = table;
    this.deleteUnavail = deleteUnavail;
    this.forceUp = forceUp;
    this.promotion = promotion;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER");
    writer.keyword("TABLE");
    table.unparse(writer, leftPrec, rightPrec);
    writer.keyword("REFRESH");
    writer.keyword("METADATA");

    if (deleteUnavail.getValue() != null) {
      if (deleteUnavail.booleanValue()) {
        writer.keyword("DELETE");
        writer.keyword("WHEN");
        writer.keyword("MISSING");
      } else {
        writer.keyword("MAINTAIN");
        writer.keyword("WHEN");
        writer.keyword("MISSING");
      }
    }

    if (forceUp.getValue() != null) {
      if (forceUp.booleanValue()) {
        writer.keyword("FORCE");
        writer.keyword("UPDATE");
      } else {
        writer.keyword("LAZY");
        writer.keyword("UPDATE");
      }
    }

    if (promotion.getValue() != null) {
      if (promotion.booleanValue()) {
        writer.keyword("AUTO");
        writer.keyword("PROMOTION");
      } else {
        writer.keyword("AVOID");
        writer.keyword("PROMOTION");
      }
    }
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
      case 0:
        table = (SqlIdentifier) operand;
        break;
      case 1:
        deleteUnavail = (SqlLiteral) operand;
        break;
      case 2:
        forceUp = (SqlLiteral) operand;
        break;
      case 3:
        promotion = (SqlLiteral) operand;
        break;
      default:
        throw new AssertionError(i);
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.<SqlNode>of(table, deleteUnavail, forceUp, promotion);
  }

  public SqlIdentifier getTable() { return table; }
  public SqlLiteral getDeleteUnavail() { return deleteUnavail; }
  public SqlLiteral getForceUpdate() { return forceUp; }
  public SqlLiteral getPromotion() { return promotion; }

}


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

import com.dremio.service.namespace.NamespaceKey;
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

/** SQL node tree for <code>FORGET TABLE table_identifier </code> */
public class SqlForgetTable extends SqlSystemCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("FORGET_TABLE", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return new SqlForgetTable(pos, (SqlIdentifier) operands[0]);
        }
      };

  private SqlIdentifier table;

  /** Creates a SqlForgetTable. */
  public SqlForgetTable(SqlParserPos pos, SqlIdentifier table) {
    super(pos);
    this.table = table;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER");
    writer.keyword("TABLE");
    table.unparse(writer, leftPrec, rightPrec);
    writer.keyword("FORGET");
    writer.keyword("METADATA");
  }

  @Override
  public void setOperand(int i, SqlNode operand) {
    switch (i) {
      case 0:
        table = (SqlIdentifier) operand;
        break;
      default:
        throw new AssertionError(i);
    }
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.<SqlNode>of(table);
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(table.names);
  }

  public SqlIdentifier getTable() {
    return table;
  }
}

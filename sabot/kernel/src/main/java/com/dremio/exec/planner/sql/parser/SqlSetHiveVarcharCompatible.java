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

import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

public class SqlSetHiveVarcharCompatible extends SqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SET_HIVE_VARCHAR_COMPATIBLE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlSetHiveVarcharCompatible(pos, (SqlIdentifier) operands[0], (SqlLiteral) operands[1]);
    }
  };

  private SqlIdentifier tableName;
  private boolean isHiveVarcharCompatible;

  public SqlSetHiveVarcharCompatible(SqlParserPos pos, SqlIdentifier tableName, SqlLiteral isHiveVarcharCompatible) {
    this(pos, tableName, isHiveVarcharCompatible.booleanValue());
  }

  public SqlSetHiveVarcharCompatible(SqlParserPos pos, SqlIdentifier tableName, boolean isHiveVarcharCompatible) {
    super(pos);
    this.tableName = tableName;
    this.isHiveVarcharCompatible = isHiveVarcharCompatible;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(
        tableName,
        SqlLiteral.createBoolean(isHiveVarcharCompatible, SqlParserPos.ZERO)
    );
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER");
    writer.keyword("TABLE");
    tableName.unparse(writer, leftPrec, rightPrec);
    if (isHiveVarcharCompatible) {
      writer.keyword("ENABLE");
    } else {
      writer.keyword("DISABLE");
    }
    writer.keyword("HIVE");
    writer.keyword("VARCHAR");
    writer.keyword("COMPATIBILITY");
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(tableName.names);
  }

  public boolean isCompatible() {
    return isHiveVarcharCompatible;
  }

}

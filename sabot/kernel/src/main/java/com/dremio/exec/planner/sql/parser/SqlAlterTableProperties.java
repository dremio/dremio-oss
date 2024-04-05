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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Parse tree node for a ALTER TABLE ... SET|UNSET TBLPROPERTIES statement. */
public class SqlAlterTableProperties extends SqlAlterTable {

  public enum Mode {
    SET,
    UNSET
  }

  public static final SqlSpecialOperator ALTER_TABLE_PROPERTIES_OPERATOR =
      new SqlSpecialOperator("ALTER_TABLE_PROPERTIES_OPERATOR", SqlKind.ALTER_TABLE) {

        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 4,
              "SqlAlterTableProperties.createCall() " + "has to get 4 operands!");

          return new SqlAlterTableProperties(
              pos,
              (SqlIdentifier) operands[0],
              (SqlLiteral) operands[1],
              (SqlNodeList) operands[2],
              (SqlNodeList) operands[3]);
        }
      };

  private final SqlLiteral mode;
  private final SqlNodeList tablePropertyNameList;
  private final SqlNodeList tablePropertyValueList;

  public SqlAlterTableProperties(
      SqlParserPos pos,
      SqlIdentifier tableName,
      SqlLiteral mode,
      SqlNodeList tablePropertyNameList,
      SqlNodeList tablePropertyValueList) {
    super(pos, tableName);
    this.mode = Preconditions.checkNotNull(mode);
    this.tablePropertyNameList = tablePropertyNameList;
    this.tablePropertyValueList = tablePropertyValueList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    writer.keyword(mode.toValue());
    writer.keyword("TBLPROPERTIES");
    if (tablePropertyNameList != null) {
      writer.keyword("(");
      for (int i = 0; i < tablePropertyNameList.size(); i++) {
        if (i > 0) {
          writer.keyword(",");
        }
        tablePropertyNameList.get(i).unparse(writer, leftPrec, rightPrec);
        if (tablePropertyValueList != null && tablePropertyValueList.size() > i) {
          writer.keyword("=");
          tablePropertyValueList.get(i).unparse(writer, leftPrec, rightPrec);
        }
      }
      writer.keyword(")");
    }
  }

  @Override
  public SqlOperator getOperator() {
    return ALTER_TABLE_PROPERTIES_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(tblName, mode, tablePropertyNameList, tablePropertyValueList);
  }

  public Mode getMode() {
    return mode.symbolValue(Mode.class);
  }

  public List<String> getTablePropertyNameList() {
    return tablePropertyNameList.getList().stream()
        .map(x -> ((SqlLiteral) x).toValue())
        .collect(Collectors.toList());
  }

  public List<String> getTablePropertyValueList() {
    return tablePropertyValueList.getList().stream()
        .map(x -> ((SqlLiteral) x).toValue())
        .collect(Collectors.toList());
  }
}

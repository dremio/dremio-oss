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

import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SqlAlterTableSortOrder extends SqlAlterTable {

  public static final SqlSpecialOperator ALTER_SORT_ORDER =
    new SqlSpecialOperator("ALTER_SORT_ORDER", SqlKind.ALTER_TABLE) {

      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        Preconditions.checkArgument(operands.length == 2, "SqlAlterTableSortOrder.createCall()" +
          "has to get 2 operands!");
        return new SqlAlterTableSortOrder(
          pos,
          (SqlIdentifier) operands[0],
          (SqlNodeList) operands[1]
        );
      }
    };


  private final SqlNodeList sortList;

  public SqlAlterTableSortOrder(SqlParserPos pos, SqlIdentifier tblName, SqlNodeList sortList) {
    super(pos, tblName);
    this.sortList = sortList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    writer.keyword("LOCALSORT");
    writer.keyword("BY");
    SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, sortList);
  }

  @Override
  public SqlOperator getOperator() {
    return ALTER_SORT_ORDER;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(tblName, sortList);
  }

  public List<String> getSortList() {
    return sortList.getList().stream().map(SqlNode::toString).collect(Collectors.toList());
  }

}

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

import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Collections;
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

public class SqlAlterTableClusterKey extends SqlAlterTable {

  public static final SqlSpecialOperator ALTER_CLUSTER_KEY =
      new SqlSpecialOperator("ALTER_CLUSTER_KEY", SqlKind.ALTER_TABLE) {

        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 2,
              "SqlAlterTableClusterKey.createCall()" + "has to get 2 operands!");
          return new SqlAlterTableClusterKey(
              pos, (SqlIdentifier) operands[0], (SqlNodeList) operands[1]);
        }
      };

  private final SqlNodeList clusterKeyList;

  public SqlAlterTableClusterKey(
      SqlParserPos pos, SqlIdentifier tblName, SqlNodeList clusterKeyList) {
    super(pos, tblName);
    this.clusterKeyList = clusterKeyList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    if (clusterKeyList.size() == 0) {
      writer.keyword("DROP");
      writer.keyword("CLUSTERING");
      writer.keyword("KEY");
    } else {
      writer.keyword("CLUSTER");
      writer.keyword("BY");
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, clusterKeyList);
    }
  }

  @Override
  public SqlOperator getOperator() {
    return ALTER_CLUSTER_KEY;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(tblName, clusterKeyList);
  }

  public List<String> getClusterKeyList() {
    if (clusterKeyList == null || clusterKeyList == SqlNodeList.EMPTY) {
      return Collections.EMPTY_LIST;
    } else {
      return clusterKeyList.getList().stream().map(SqlNode::toString).collect(Collectors.toList());
    }
  }
}

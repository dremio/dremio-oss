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
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SqlInsertTable extends SqlCall implements DataAdditionCmdCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("INSERT", SqlKind.INSERT) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 3, "SqlInsertTable.createCall() has to get 3 operands!");
      return new SqlInsertTable(
        pos,
        (SqlIdentifier) operands[0],
        operands[1],
        (SqlNodeList) operands[2]);
    }
  };

  private final SqlIdentifier tblName;
  private final SqlNode query;
  private final SqlNodeList insertFields;

  public SqlInsertTable(
    SqlParserPos pos,
    SqlIdentifier tblName,
    SqlNode query,
    SqlNodeList insertFields) {
    super(pos);
    this.tblName = tblName;
    this.query = query;
    this.insertFields = insertFields;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(tblName, query, insertFields);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("INSERT");
    writer.keyword("INTO");
    tblName.unparse(writer, leftPrec, rightPrec);
    if (insertFields.size() > 0) {
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, insertFields);
    }
    query.unparse(writer, leftPrec, rightPrec);
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(tblName.names);
  }

  @Override
  public List<String> getPartitionColumns(DremioTable dremioTable) {
    Preconditions.checkNotNull(dremioTable);
    List<String> columnNames =  dremioTable.getDatasetConfig().getReadDefinition().getPartitionColumnsList();
    return columnNames != null ? columnNames : Lists.newArrayList();
  }

  @Override
  public List<String> getSortColumns() {
    return Lists.newArrayList();
  }

  @Override
  public List<String> getDistributionColumns() {
    return Lists.newArrayList();
  }

  @Override
  public PartitionDistributionStrategy getPartitionDistributionStrategy() {
    return PartitionDistributionStrategy.UNSPECIFIED;
  }

  @Override
  public boolean isSingleWriter() {
    return false;
  }

  @Override
  public List<String> getFieldNames() {
    for (SqlNode fieldNode : insertFields.getList()) {
      if (!(fieldNode instanceof SqlIdentifier)) {
        throw SqlExceptionHelper.parseError("Column type specified", this.toSqlString(new SqlDialect(SqlDialect.EMPTY_CONTEXT)).getSql(),
            fieldNode.getParserPosition()).buildSilently();
      }
    }
    return insertFields.getList().stream().map(SqlNode::toString).collect(Collectors.toList());
  }

  public SqlNode getQuery() {
    return query;
  }

  public SqlIdentifier getTblName() {
    return tblName;
  }
}

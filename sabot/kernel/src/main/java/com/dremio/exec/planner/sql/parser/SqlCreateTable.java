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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SqlCreateTable extends SqlCreateEmptyTable {

  public static final SqlSpecialOperator CREATE_TABLE_OPERATOR = new SqlSpecialOperator("CREATE_TABLE", SqlKind.CREATE_TABLE) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 16, "SqlCreateTable.createCall() has to get 16 operands!");
      return new SqlCreateTable(
        pos,
        (SqlIdentifier) operands[0],
        (SqlNodeList) operands[1],
        ((SqlLiteral) operands[9]).booleanValue(),
        ((SqlLiteral) operands[2]).symbolValue(PartitionDistributionStrategy.class),
        (SqlNodeList) operands[3],
        (SqlNodeList) operands[4],
        (SqlNode) operands[5],
        (SqlLiteral) operands[6],
        (SqlNodeList) operands[7],
        (SqlNodeList) operands[8],
        (SqlPolicy) operands[10],
        operands[15],
        (SqlNodeList) operands[11],
        (SqlNodeList) operands[12],
        ((SqlLiteral) operands[13]).symbolValue(ReferenceType.class),
        (SqlIdentifier) operands[14]);
    }
  };

  private final SqlNode query;

  public SqlCreateTable(
      SqlParserPos pos,
      SqlIdentifier tblName,
      SqlNodeList fieldList,
      boolean ifNotExists,
      PartitionDistributionStrategy partitionDistributionStrategy,
      SqlNodeList partitionColumns,
      SqlNodeList formatOptions,
      SqlNode location,
      SqlLiteral singleWriter,
      SqlNodeList sortFieldList,
      SqlNodeList distributionColumns,
      SqlPolicy policy,
      SqlNode query,
      SqlNodeList  tablePropertyNameList,
      SqlNodeList  tablePropertyValueList,
      ReferenceType refType,
      SqlIdentifier refValue) {
    super(pos, tblName, fieldList, ifNotExists, partitionDistributionStrategy, partitionColumns, formatOptions, location, singleWriter,
      sortFieldList, distributionColumns, policy, tablePropertyNameList, tablePropertyValueList, refType, refValue);
    this.query = query;
  }

  @Override
  public SqlOperator getOperator() {
    return CREATE_TABLE_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.addAll(super.getOperandList());
    ops.add(query);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AS");
    query.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SqlNode getQuery() {
    return query;
  }

}

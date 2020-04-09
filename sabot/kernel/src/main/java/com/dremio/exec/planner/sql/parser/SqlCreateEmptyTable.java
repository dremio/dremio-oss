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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SqlCreateEmptyTable extends SqlCall implements DataAdditionCmdCall {

  public static final SqlSpecialOperator CREATE_EMPTY_TABLE_OPERATOR = new SqlSpecialOperator("CREATE_EMPTY_TABLE", SqlKind.OTHER_DDL) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 8, "SqlCreateEmptyTable.createCall() " +
        "has to get 8 operands!");

      if (((SqlNodeList) operands[1]).getList().size() == 0) {
        throw UserException.parseError().message("Columns/Fields not specified for table.").buildSilently();
      }

      for (SqlNode sqlNode : ((SqlNodeList) operands[1]).getList()) { // check all columns have datatype declaration
        if (!(sqlNode instanceof SqlColumnDeclaration)) {
          throw UserException.parseError().message("Datatype not specified for some columns.").buildSilently();
        }
      }

      return new SqlCreateEmptyTable(
        pos,
        (SqlIdentifier) operands[0],
        (SqlNodeList) operands[1],
        ((SqlLiteral) operands[2]).symbolValue(PartitionDistributionStrategy.class),
        (SqlNodeList) operands[3],
        (SqlNodeList) operands[4],
        (SqlLiteral) operands[5],
        (SqlNodeList) operands[6],
        (SqlNodeList) operands[7]);
    }
  };

  protected final SqlIdentifier tblName;
  protected final SqlNodeList fieldList;
  protected final PartitionDistributionStrategy partitionDistributionStrategy;
  protected final SqlNodeList partitionColumns;
  protected final SqlNodeList sortColumns;
  protected final SqlNodeList distributionColumns;
  protected final SqlNodeList formatOptions;
  protected final SqlLiteral singleWriter;

  public SqlCreateEmptyTable(
    SqlParserPos pos,
    SqlIdentifier tblName,
    SqlNodeList fieldList,
    PartitionDistributionStrategy partitionDistributionStrategy,
    SqlNodeList partitionColumns,
    SqlNodeList formatOptions,
    SqlLiteral singleWriter,
    SqlNodeList sortFieldList,
    SqlNodeList distributionColumns) {
    super(pos);
    this.tblName = tblName;
    this.fieldList = fieldList;
    this.partitionDistributionStrategy = partitionDistributionStrategy;
    this.partitionColumns = partitionColumns;
    this.formatOptions = formatOptions;
    this.singleWriter = singleWriter;
    this.sortColumns = sortFieldList;
    this.distributionColumns = distributionColumns;
  }

  @Override
  public SqlOperator getOperator() {
    return CREATE_EMPTY_TABLE_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(tblName);
    ops.add(fieldList);
    ops.add(SqlLiteral.createSymbol(partitionDistributionStrategy, SqlParserPos.ZERO));
    ops.add(partitionColumns);
    ops.add(formatOptions);
    ops.add(singleWriter);
    ops.add(sortColumns);
    ops.add(distributionColumns);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    tblName.unparse(writer, leftPrec, rightPrec);
    if (fieldList.size() > 0) {
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, fieldList);
    }
    if (partitionColumns.size() > 0) {
      switch (partitionDistributionStrategy) {
        case UNSPECIFIED:
          break;
        case HASH:
          writer.keyword("HASH");
          break;
        case ROUND_ROBIN:
          writer.keyword("ROUNDROBIN");
          break;
        case STRIPED:
          writer.keyword("STRIPED");
          break;
      }
      writer.keyword("PARTITION");
      writer.keyword("BY");
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, partitionColumns);
    }
    if(distributionColumns.size() > 0) {
      writer.keyword("DISTRIBUTE");
      writer.keyword("BY");
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, distributionColumns);
    }
    if(sortColumns.size() > 0) {
      writer.keyword("LOCALSORT");
      writer.keyword("BY");
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, sortColumns);
    }
    if (formatOptions.size() > 0) {
      writer.keyword("STORE");
      writer.keyword("AS");
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, formatOptions);
    }
    if (singleWriter.booleanValue()) {
      writer.keyword("WITH");
      writer.keyword("SINGLE");
      writer.keyword("WRITER");
    }
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(tblName.names);
  }

  public List<String> getFieldNames() {
    List<String> columnNames = Lists.newArrayList();
    for (SqlNode node : fieldList.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }

  @Override
  public SqlNode getQuery() {
    return null;
  }

  public List<String> getSortColumns() {
    List<String> columnNames = Lists.newArrayList();
    for(SqlNode node : sortColumns.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }

  public List<String> getDistributionColumns() {
    List<String> columnNames = Lists.newArrayList();
    for(SqlNode node : distributionColumns.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }

  public List<String> getPartitionColumns(DatasetCatalog datasetCatalog, NamespaceKey key) {
    List<String> columnNames = Lists.newArrayList();
    for(SqlNode node : partitionColumns.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }

  public SqlNodeList getFieldList() {
    return fieldList;
  }

  public SqlNodeList getFormatOptions() {
    return formatOptions;
  }

  public boolean isSingleWriter() {
    return singleWriter.booleanValue();
  }

  public PartitionDistributionStrategy getPartitionDistributionStrategy() {
    return partitionDistributionStrategy;
  }

}

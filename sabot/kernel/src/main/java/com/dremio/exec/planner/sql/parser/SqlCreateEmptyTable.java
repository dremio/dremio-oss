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
import java.util.Set;
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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SqlCreateEmptyTable extends SqlCall implements DataAdditionCmdCall {
  private static final SqlLiteral sqlLiteralNull = SqlLiteral.createNull(SqlParserPos.ZERO);


  public static final SqlSpecialOperator CREATE_EMPTY_TABLE_OPERATOR = new SqlSpecialOperator("CREATE_EMPTY_TABLE", SqlKind.OTHER_DDL) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 15, "SqlCreateEmptyTable.createCall() " +
              "has to get 15 operands!");


      if (((SqlNodeList) operands[1]).getList().size() == 0) {
        throw UserException.parseError().message("Columns/Fields not specified for table.").buildSilently();
      }

      for (SqlNode sqlNode : ((SqlNodeList) operands[1]).getList()) { // check all columns have datatype declaration
        if (!(sqlNode instanceof DremioSqlColumnDeclaration)) {
          throw UserException.parseError().message("Datatype not specified for some columns.").buildSilently();
        }
      }

      return new SqlCreateEmptyTable(
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
        (SqlNodeList) operands[11],
        (SqlNodeList) operands[12],
        ((SqlLiteral) operands[13]).symbolValue(ReferenceType.class),
        (SqlIdentifier) operands[14]);
    }
  };

  protected final SqlIdentifier tblName;
  protected final SqlNodeList fieldList;
  protected final PartitionDistributionStrategy partitionDistributionStrategy;
  protected final SqlNodeList partitionTransforms;
  protected final SqlNodeList sortColumns;
  protected final SqlNodeList distributionColumns;
  protected final SqlNodeList formatOptions;
  protected final SqlNode location;
  protected final SqlPolicy policy;
  protected final SqlLiteral singleWriter;
  protected final boolean ifNotExists;
  protected final SqlNodeList tablePropertyNameList;
  protected final SqlNodeList tablePropertyValueList;
  protected final ReferenceType refType;
  protected SqlIdentifier refValue;

  public SqlCreateEmptyTable(
    SqlParserPos pos,
    SqlIdentifier tblName,
    SqlNodeList fieldList,
    boolean ifNotExists,
    PartitionDistributionStrategy partitionDistributionStrategy,
    SqlNodeList partitionTransforms,
    SqlNodeList formatOptions,
    SqlNode location,
    SqlLiteral singleWriter,
    SqlNodeList sortFieldList,
    SqlNodeList distributionColumns,
    SqlPolicy policy,
    SqlNodeList  tablePropertyNameList,
    SqlNodeList  tablePropertyValueList,
    ReferenceType refType,
    SqlIdentifier refValue) {
    super(pos);
    this.tblName = tblName;
    this.fieldList = fieldList;
    this.partitionDistributionStrategy = partitionDistributionStrategy;
    this.partitionTransforms = partitionTransforms;
    this.formatOptions = formatOptions;
    this.location = location;
    this.singleWriter = singleWriter;
    this.sortColumns = sortFieldList;
    this.distributionColumns = distributionColumns;
    this.ifNotExists = ifNotExists;
    this.policy = policy;
    this.tablePropertyNameList = tablePropertyNameList;
    this.tablePropertyValueList = tablePropertyValueList;
    this.refType = refType;
    this.refValue = refValue;
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
    ops.add(partitionTransforms);
    ops.add(formatOptions);
    ops.add(location);
    ops.add(singleWriter);
    ops.add(sortColumns);
    ops.add(distributionColumns);
    ops.add(SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO));
    ops.add(policy);
    ops.add(tablePropertyNameList);
    ops.add(tablePropertyValueList);
    if (refType == null) {
      ops.add(sqlLiteralNull);
    } else {
      ops.add(SqlLiteral.createSymbol(getRefType(), SqlParserPos.ZERO));
    }
    ops.add(refValue);
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

    if (refType != null && refValue != null) {
      writer.keyword("AT");
      writer.keyword(refType.toString());
      refValue.unparse(writer, leftPrec, rightPrec);
    }

    if (partitionTransforms.size() > 0) {
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
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, partitionTransforms);
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
    if (location != null) {
      writer.keyword("LOCATION");
      location.unparse(writer, leftPrec, rightPrec);
    }
    if (singleWriter.booleanValue()) {
      writer.keyword("WITH");
      writer.keyword("SINGLE");
      writer.keyword("WRITER");
    }
    if (policy != null) {
      writer.keyword("ROW");
      writer.keyword("ACCESS");
      writer.keyword("POLICY");
      policy.unparse(writer, leftPrec, rightPrec);
    }
    if(tablePropertyNameList != null && tablePropertyNameList.size() > 0) {
      writer.keyword("TBLPROPERTIES");
      writer.keyword("(");
      for (int i = 0; i < tablePropertyNameList.size(); i++) {
        if (i > 0) {
          writer.keyword(",");
        }
        tablePropertyNameList.get(i).unparse(writer, leftPrec, rightPrec);
        tablePropertyValueList.get(i).unparse(writer, leftPrec, rightPrec);
      }
      writer.keyword(")");
    }
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(tblName.names);
  }

  @Override
  public List<String> getFieldNames() {
    List<String> columnNames = Lists.newArrayList();
    for (SqlNode node : fieldList.getList()) {
      if (node instanceof SqlColumnPolicyPair) {
        columnNames.add(((SqlColumnPolicyPair) node).getName().toString());
      } else {
        columnNames.add(((DremioSqlColumnDeclaration) node).getName().toString());
      }
    }
    return columnNames;
  }

  @Override
  public SqlNode getQuery() {
    return null;
  }

  @Override
  public List<String> getSortColumns() {
    List<String> columnNames = Lists.newArrayList();
    for(SqlNode node : sortColumns.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }

  @Override
  public List<String> getDistributionColumns() {
    List<String> columnNames = Lists.newArrayList();
    for(SqlNode node : distributionColumns.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }

  public SqlPolicy getPolicy() {
    return policy;
  }


  @Override
  public List<String> getPartitionColumns(DremioTable dremioTable /* param is unused in this implementation*/) {
    List<String> columnNames = Lists.newArrayList();
    for (SqlNode node : partitionTransforms.getList()) {
      SqlPartitionTransform sqlPartitionTransform = (SqlPartitionTransform) node;
      // TODO: this is temporary until end to end is wired up, just ignore the actual transform
      // we need to work out what is going to happen with existing calls that expect a list of
      // column names - do those get converted to some form that understands partition specs?
      columnNames.add(sqlPartitionTransform.getColumnName().toString());
    }
    return columnNames;
  }

  @Override
  public List<PartitionTransform> getPartitionTransforms(DremioTable dremioTable) {
    return getPartitionTransformsFromSqlNodeList(partitionTransforms);
  }

  public static List<PartitionTransform> getPartitionTransformsFromSqlNodeList(final SqlNodeList partitionTransforms) {
    return partitionTransforms.getList().stream()
      .map(n -> (SqlPartitionTransform) n)
      .map(PartitionTransform::from)
      .collect(Collectors.toList());
  }

  public List<String> getTablePropertyNameList() {
    return tablePropertyNameList.getList().stream().map(x -> ((SqlLiteral)x).toValue()).collect(Collectors.toList());
  }

  public List<String> getTablePropertyValueList() {
    return tablePropertyValueList.getList().stream().map(x -> ((SqlLiteral)x).toValue()).collect(Collectors.toList());
  }

  public SqlNodeList getFieldList() {
    return fieldList;
  }

  public SqlNodeList getFormatOptions() {
    return formatOptions;
  }

  @Override
  public String getLocation() {
    if (location != null && location instanceof SqlLiteral) {
      return ((SqlLiteral)location).toValue();
    }
    return null;
  }

  public boolean getIfNotExists(){
    return ifNotExists;
  }

  @Override
  public boolean isSingleWriter() {
    return singleWriter.booleanValue();
  }

  @Override
  public PartitionDistributionStrategy getPartitionDistributionStrategy(
    SqlHandlerConfig config, List<String> partitionFieldNames, Set<String> fieldNames) {
    return partitionDistributionStrategy;
  }

  public ReferenceType getRefType() {
    return refType;
  }

  public SqlIdentifier getRefValue() {
    return refValue;
  }
}

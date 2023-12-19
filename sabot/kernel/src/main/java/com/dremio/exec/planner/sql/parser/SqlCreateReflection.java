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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SqlCreateReflection extends SqlSystemCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("ADD_LAYOUT", SqlKind.OTHER_DDL) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 12, "SqlCreateReflection.createCall() has to get 12 operands!");
      return new SqlCreateReflection(
          pos,
          (SqlIdentifier) operands[0],
          operands[1],
          (SqlNodeList) operands[2],
          (SqlNodeList) operands[3],
          (SqlNodeList) operands[4],
          (SqlNodeList) operands[5],
          (SqlNodeList) operands[6],
          (SqlNodeList) operands[7],
          (SqlLiteral) operands[8],
          ((SqlLiteral) operands[9]).symbolValue(PartitionDistributionStrategy.class),
          (SqlIdentifier) operands[10],
          (SqlTableVersionSpec) operands[11]
          );
    }
  };

  private final SqlIdentifier tblName;
  private final SqlNode isRaw;
  private final SqlNodeList displayList;
  private final SqlNodeList dimensionList;
  private final SqlNodeList measureList;
  private final SqlNodeList distributionList;
  private final SqlNodeList partitionList;
  private final SqlNodeList sortList;
  private final SqlLiteral arrowCachingEnabled;
  private final PartitionDistributionStrategy partitionDistributionStrategy;
  private final SqlIdentifier name;
  private final SqlTableVersionSpec tableVersionSpec;

  private SqlCreateReflection(SqlParserPos pos,
                              SqlIdentifier tblName,
                              SqlNode isRaw,
                              SqlNodeList displayList,
                              SqlNodeList dimensionList,
                              SqlNodeList measureList,
                              SqlNodeList distributionList,
                              SqlNodeList partitionList,
                              SqlNodeList sortList,
                              SqlLiteral arrowCachingEnabled,
                              PartitionDistributionStrategy partitionDistributionStrategy,
                              SqlIdentifier name,
                              SqlTableVersionSpec tableVersionSpec
                              ) {
    super(pos);
    this.tblName = tblName;
    this.isRaw = isRaw;
    this.displayList = MoreObjects.firstNonNull(displayList, SqlNodeList.EMPTY);
    this.dimensionList = MoreObjects.firstNonNull(dimensionList, SqlNodeList.EMPTY);
    this.measureList = MoreObjects.firstNonNull(measureList, SqlNodeList.EMPTY);
    this.distributionList = distributionList;
    this.partitionList = partitionList;
    this.sortList = sortList;
    this.arrowCachingEnabled = arrowCachingEnabled;
    this.partitionDistributionStrategy = partitionDistributionStrategy;
    this.name = name;
    this.tableVersionSpec = tableVersionSpec;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> operands = new ArrayList<>();

    operands.addAll(ImmutableList.of(tblName, isRaw, displayList, dimensionList, measureList, distributionList, partitionList,
        sortList, arrowCachingEnabled, SqlLiteral.createSymbol(partitionDistributionStrategy, SqlParserPos.ZERO)));

    operands.add(name);
    operands.add(tableVersionSpec);
    return operands;
  }

  public SqlIdentifier getTblName() {
    return tblName;
  }

  public boolean isRaw(){
    return ((SqlLiteral) this.isRaw).booleanValue();
  }

  public List<String> getDisplayList() {
    return toStrings(displayList);
  }

  public SqlIdentifier getName() {
    return name;
  }

  public List<NameAndGranularity> getDimensionList() {
    return toNameAndGranularity(dimensionList);
  }

  public List<NameAndMeasures> getMeasureList() {
    return toNameAndMeasures(measureList);
  }

  public List<String> getDistributionList() {
    return toStrings(distributionList);
  }

  public SqlNodeList getPartitionList() {
    return partitionList;
  }

  public List<String> getSortList() {
    return toStrings(sortList);
  }

  public Boolean getArrowCachingEnabled() { return this.arrowCachingEnabled.booleanValue(); }

  public PartitionDistributionStrategy getPartitionDistributionStrategy() {
    return partitionDistributionStrategy;
  }

  public SqlTableVersionSpec getSqlTableVersionSpec() {
    return tableVersionSpec;
  }

  private List<NameAndMeasures> toNameAndMeasures(SqlNodeList list){
    if(list == null){
      return ImmutableList.of();
    }
    List<NameAndMeasures> columnNames = Lists.newArrayList();
    for(SqlNode node : list.getList()) {
      IdentifierWithMeasures ident = (IdentifierWithMeasures) node;
      NameAndMeasures value = new NameAndMeasures(ident.getSimple(), ident.getMeasureTypes());
      columnNames.add(value);
    }
    return columnNames;
  }

  private List<NameAndGranularity> toNameAndGranularity(SqlNodeList list){
    if(list == null){
      return ImmutableList.of();
    }
    List<NameAndGranularity> columnNames = Lists.newArrayList();
    for(SqlNode node : list.getList()) {
      IdentifierWithGranularity ident = (IdentifierWithGranularity) node;
      NameAndGranularity value = new NameAndGranularity(ident.getSimple(), ident.getByDay() ? Granularity.BY_DAY : Granularity.NORMAL);
      columnNames.add(value);

    }
    return columnNames;
  }

  private List<String> toStrings(SqlNodeList list){
    if(list == null){
      return ImmutableList.of();
    }
    List<String> columnNames = Lists.newArrayList();
    for(SqlNode node : list.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }

  public static SqlCreateReflection createAggregation(SqlParserPos pos, SqlIdentifier tblName, SqlNodeList dimensionList,
                                                      SqlNodeList measureList, SqlNodeList distributionList,
                                                      SqlNodeList partitionList, SqlNodeList sortList, SqlLiteral arrowCachingEnabled,
                                                      PartitionDistributionStrategy partitionDistributionStrategy, SqlIdentifier name,
                                                      SqlTableVersionSpec tableVersionSpec) {
    return new SqlCreateReflection(pos, tblName, SqlLiteral.createBoolean(false, SqlParserPos.ZERO), null, dimensionList,
        measureList, distributionList, partitionList, sortList, arrowCachingEnabled, partitionDistributionStrategy, name, tableVersionSpec);
  }

  public static SqlCreateReflection createRaw(SqlParserPos pos, SqlIdentifier tblName, SqlNodeList displayList,
                                              SqlNodeList distributionList, SqlNodeList partitionList,
                                              SqlNodeList sortList, SqlLiteral arrowCachingEnabled,
                                              PartitionDistributionStrategy partitionDistributionStrategy,
                                              SqlIdentifier name,
                                              SqlTableVersionSpec tableVersionSpec) {
    return new SqlCreateReflection(pos, tblName, SqlLiteral.createBoolean(true, SqlParserPos.ZERO), displayList, null, null,
        distributionList, partitionList, sortList, arrowCachingEnabled, partitionDistributionStrategy, name, tableVersionSpec);
  }

  public static class NameAndGranularity {
    private final String name;
    private final Granularity granularity;

    public NameAndGranularity(String name, Granularity granularity) {
      super();
      this.name = name;
      this.granularity = granularity;
    }
    public String getName() {
      return name;
    }
    public Granularity getGranularity() {
      return granularity;
    }

  }

  public static class NameAndMeasures {
    private final String name;
    private final List<MeasureType> measureTypes;

    public NameAndMeasures(String name, List<MeasureType> measureTypes) {
      super();
      this.name = name;
      this.measureTypes = measureTypes;
    }
    public String getName() {
      return name;
    }
    public List<MeasureType> getMeasureTypes() {
      return measureTypes;
    }

  }

  /**
   * Type of Measure for Sql parsing purposes.
   */
  public static enum MeasureType {
    UNKNOWN("invalid"),
    MIN("MIN"),
    MAX("MAX"),
    SUM("SUM"),
    COUNT("COUNT"),
    APPROX_COUNT_DISTINCT("APPROXIMATE COUNT DISTINCT");

    final String sqlName;

    MeasureType(String sqlName){
      this.sqlName = sqlName;
    }

    @Override
    public String toString() {
      return sqlName;
    }
  }

  public static enum Granularity{
    NORMAL, BY_DAY
  }
}

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
package com.dremio.exec.catalog;

import java.util.List;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

/**
 * A DremioTable that has been localized to a particular DremioCatalogReader and RelDataTypeFactory
 * for the purposes of a particular planning session.
 */
public class DremioPrepareTable implements RelOptTable, PreparingTable, SqlValidatorTable {

  private final Supplier<RelDataType> rowType;
  private final DremioTable table;
  private final DremioCatalogReader catalog;

  public DremioPrepareTable(
      final DremioCatalogReader catalog,
      final RelDataTypeFactory dataTypeFactory,
      final DremioTable table) {
    super();
    this.catalog = catalog;
    this.rowType = new Supplier<RelDataType>(){
      @Override
      public RelDataType get() {
        return table.getRowType(dataTypeFactory);
      }};
    this.table = table;
  }

  @Override
  public List<String> getQualifiedName() {
    return table.getPath().getPathComponents();
  }

  @Override
  public double getRowCount() {
    return table.getStatistic().getRowCount();
  }

  @Override
  public RelDataType getRowType() {
    return rowType.get();
  }

  public Schema.TableType getJdbcTableType() {
    return table.getJdbcTableType();
  }

  @Override
  public RelNode toRel(ToRelContext paramToRelContext) {
    return table.toRel(paramToRelContext, this);
  }

  @Override
  public RelOptSchema getRelOptSchema() {
    return catalog;
  }

  @Override
  public List<RelCollation> getCollationList() {
    return ImmutableList.of();
  }

  @Override
  public RelDistribution getDistribution() {
    return RelDistributionTraitDef.INSTANCE.getDefault();
  }

  @Override
  public boolean isKey(ImmutableBitSet paramImmutableBitSet) {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> paramClass) {
    if(paramClass == DremioPrepareTable.class) {
      return paramClass.cast(this);
    } else if(paramClass == DremioTable.class
        || paramClass == table.getClass()
        || paramClass == Table.class) {
      return paramClass.cast(table);
    } else {
      return null;
    }
  }

  @Override
  public Expression getExpression(@SuppressWarnings("rawtypes") Class paramClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RelOptTable extend(List<RelDataTypeField> paramList) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SqlMonotonicity getMonotonicity(String paramString) {
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  @Override
  public SqlAccessType getAllowedAccess() {
    return SqlAccessType.ALL;
  }

  @Override
  public boolean supportsModality(SqlModality paramSqlModality) {
    return SqlModality.RELATION == paramSqlModality;
  }

  @Override
  public List<ColumnStrategy> getColumnStrategies() {
    return ImmutableList.of();
  }

  @Override
  public List<RelReferentialConstraint> getReferentialConstraints() {
    return ImmutableList.of();
  }

  @Override
  public boolean columnHasDefaultValue(RelDataType rowType, int ordinal, InitializerContext initializerContext) {
    return false;
  }

  public DremioTable getTable() {
    return table;
  }

}

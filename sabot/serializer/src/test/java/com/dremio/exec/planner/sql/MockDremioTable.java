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
package com.dremio.exec.planner.sql;

import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;

/** An mocked version of DremioTable that takes a NamespaceKey and RelDataType for injection. */
public class MockDremioTable implements DremioTable {
  private final NamespaceKey key;
  private final RelDataType relDataType;
  private final BatchSchema batchSchema;

  protected MockDremioTable(NamespaceKey key, RelDataType relDataType, BatchSchema batchSchema) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(relDataType);
    Preconditions.checkNotNull(batchSchema);

    this.key = key;
    this.relDataType = relDataType;
    this.batchSchema = batchSchema;
  }

  /**
   * Canonical path of the table. Note that this may be different than what was requested (both in
   * casing and components) depending on the behavior of the underlying source.
   *
   * @return
   */
  @Override
  public NamespaceKey getPath() {
    return this.key;
  }

  /**
   * Provide the version of the dataset, if available. Otherwise, return -1.
   *
   * @return
   */
  @Override
  public String getVersion() {
    return null;
  }

  /**
   * The BatchSchema for the dataset. For the exception of old dot file views, this returns correct
   * schema according to sampling/metadata of the underlying system.
   *
   * @return BatchSchema for the dataset.
   */
  @Override
  public BatchSchema getSchema() {
    return this.batchSchema;
  }

  @Override
  public DatasetConfig getDatasetConfig() {
    throw new IllegalStateException();
  }

  /**
   * Converts this table into a {@link RelNode relational expression}.
   *
   * @param context
   * @param relOptTable
   */
  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    return LogicalTableScan.create(context.getCluster(), relOptTable, ImmutableList.of());
  }

  /**
   * Returns this table's row type.
   *
   * <p>This is a struct type whose fields describe the names and types of the columns in this
   * table.
   *
   * <p>The implementer must use the type factory provided. This ensures that the type is converted
   * into a canonical form; other equal types in the same query will use the same object.
   *
   * @param typeFactory Type factory with which to create the type
   * @return Row type
   */
  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return this.relDataType;
  }

  /** Returns a provider of statistics about this table. */
  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  /** Type of table. */
  @Override
  public TableType getJdbcTableType() {
    return TableType.TABLE;
  }

  public static MockDremioTable create(NamespaceKey key, RelDataType relDataType) {
    return new MockDremioTable(
        key, relDataType, CalciteArrowHelper.fromCalciteRowType(relDataType));
  }
}

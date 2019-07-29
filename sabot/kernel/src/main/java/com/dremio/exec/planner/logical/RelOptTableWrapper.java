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
package com.dremio.exec.planner.logical;

import java.util.List;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.util.ImmutableBitSet;

public class RelOptTableWrapper implements RelOptTable {

  private final List<String> qualifiedName;
  private final RelOptTable relOptTable;

  public RelOptTableWrapper(List<String> qualifiedName, RelOptTable relOptTable) {
    this.qualifiedName = qualifiedName;
    this.relOptTable = relOptTable;
  }

  public RelOptTable getRelOptTable() {
    return relOptTable;
  }

  @Override
  public List<String> getQualifiedName() {
    return qualifiedName;
  }

  @Override
  public double getRowCount() {
    return relOptTable.getRowCount();
  }

  @Override
  public RelDataType getRowType() {
    return relOptTable.getRowType();
  }

  @Override
  public RelOptSchema getRelOptSchema() {
    return relOptTable.getRelOptSchema();
  }

  @Override
  public RelNode toRel(ToRelContext context) {
    return relOptTable.toRel(context);
  }

  @Override
  public List<RelCollation> getCollationList() {
    return relOptTable.getCollationList();
  }

  @Override
  public RelDistribution getDistribution() {
    return relOptTable.getDistribution();
  }

  @Override
  public boolean isKey(ImmutableBitSet columns) {
    return relOptTable.isKey(columns);
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    return relOptTable.unwrap(clazz);
  }

  @Override
  public Expression getExpression(Class clazz) {
    return relOptTable.getExpression(clazz);
  }

  @Override
  public RelOptTable extend(List<RelDataTypeField> extendedFields) {
    return relOptTable.extend(extendedFields);
  }

  @Override
  public List<ColumnStrategy> getColumnStrategies() {
    return relOptTable.getColumnStrategies();
  }

  @Override
  public List<RelReferentialConstraint> getReferentialConstraints() {
    return relOptTable.getReferentialConstraints();
  }
}

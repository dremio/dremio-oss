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
package com.dremio.exec.planner.common;

import static com.dremio.exec.planner.OptimizeOutputSchema.getRelDataType;

import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.sql.handlers.query.OptimizeOptions;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

/** Base class for 'OPTIMIZE TABLE' */
public abstract class TableOptimizeRelBase extends SingleRel {
  private final RelOptTable table;

  private final CreateTableEntry createTableEntry;

  private final OptimizeOptions optimizeOptions;

  protected TableOptimizeRelBase(
      Convention convention,
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelOptTable table,
      CreateTableEntry createTableEntry,
      OptimizeOptions optimizeOptions) {
    super(cluster, traitSet, input);
    assert getConvention() == convention;
    this.createTableEntry = createTableEntry;
    this.table = table;
    this.optimizeOptions =
        Preconditions.checkNotNull(optimizeOptions, "Optimize options can't be null!");
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (input.getTable() != null) {
      pw.item("table", input.getTable().getQualifiedName());
    }
    pw.item("operation", "OPTIMIZE");
    pw.item("columns", deriveRowType().getFieldNames());
    return pw;
  }

  @Override
  protected RelDataType deriveRowType() {
    return getRelDataType(getCluster().getTypeFactory(), optimizeOptions.isOptimizeManifestsOnly());
  }

  public CreateTableEntry getCreateTableEntry() {
    return this.createTableEntry;
  }

  @Override
  public RelOptTable getTable() {
    return table;
  }

  public OptimizeOptions getOptimizeOptions() {
    return optimizeOptions;
  }
}

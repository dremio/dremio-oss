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

import static com.dremio.exec.planner.VacuumOutputSchema.getRelDataType;
import static com.dremio.exec.planner.sql.handlers.SqlHandlerUtil.getTimestampFromMillis;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.google.common.base.Preconditions;

/**
 * Base class for 'VACUUM' query.
 */
public class VacuumTableRelBase extends SingleRel {
  private final RelOptTable table;
  private final CreateTableEntry createTableEntry;
  private final VacuumOptions vacuumOptions;

  protected VacuumTableRelBase(Convention convention,
                               RelOptCluster cluster,
                               RelTraitSet traitSet,
                               RelNode input,
                               RelOptTable table,
                               CreateTableEntry createTableEntry,
                               VacuumOptions vacuumOptions) {
    super(cluster, traitSet, input);
    assert getConvention() == convention;
    this.table = table;
    this.createTableEntry = createTableEntry;
    this.vacuumOptions = Preconditions.checkNotNull(vacuumOptions, "Vacuum option can't be null!");
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (table != null) {
      pw.item("table", table.getQualifiedName());
    }
    pw.item("older_than", getTimestampFromMillis(vacuumOptions.getOlderThanInMillis()));
    pw.item("retain_last", vacuumOptions.getRetainLast());
    return pw;
  }

  @Override
  protected RelDataType deriveRowType() {
    return getRelDataType(getCluster().getTypeFactory());
  }

  @Override
  public RelOptTable getTable() {
    return table;
  }

  public CreateTableEntry getCreateTableEntry() {
    return createTableEntry;
  }

  public VacuumOptions getVacuumOptions() {
    return vacuumOptions;
  }
}

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
package com.dremio.exec.calcite.logical;

import com.dremio.exec.planner.common.TableOptimizeRelBase;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.sql.handlers.query.OptimizeOptions;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/** Crel for OPTIMIZE query. */
public class TableOptimizeCrel extends TableOptimizeRelBase {

  public TableOptimizeCrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelOptTable table,
      CreateTableEntry createTableEntry,
      OptimizeOptions optimizeOptions) {
    super(Convention.NONE, cluster, traitSet, input, table, createTableEntry, optimizeOptions);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TableOptimizeCrel(
        getCluster(),
        traitSet,
        sole(inputs),
        getTable(),
        getCreateTableEntry(),
        getOptimizeOptions());
  }

  public TableOptimizeCrel createWith(CreateTableEntry createTableEntry) {
    return new TableOptimizeCrel(
        getCluster(),
        getTraitSet(),
        getInput(),
        getTable(),
        createTableEntry,
        getOptimizeOptions());
  }

  public RelNode createWith(OptimizeOptions options) {
    return new TableOptimizeCrel(
        getCluster(), traitSet, getInput(), getTable(), getCreateTableEntry(), options);
  }
}

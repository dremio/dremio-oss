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
package com.dremio.exec.planner.physical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.VacuumTableRel;
import com.dremio.exec.store.dfs.FileSystemPlugin;

/**
 * Generate physical plan for VACUUM TABLE with file systems.
 */
public class FileSystemVacuumTablePrule extends VacuumTablePruleBase {

  public FileSystemVacuumTablePrule(OptimizerRulesContext context) {
    super(RelOptHelper.some(VacuumTableRel.class, Rel.LOGICAL, RelOptHelper.any(RelNode.class)),
      "Prel.FileSystemVacuumTablePrule", context);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.<VacuumTableRel>rel(0).getCreateTableEntry().getPlugin() instanceof FileSystemPlugin;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final VacuumTableRel vacuumRel = call.rel(0);
    call.transformTo(getPhysicalPlan(vacuumRel, ((DremioPrepareTable) vacuumRel.getTable()).getTable().getDataset()));
  }
}

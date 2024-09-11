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

import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.VacuumPlanGenerator;
import com.dremio.exec.planner.VacuumTableExpireSnapshotsPlanGenerator;
import com.dremio.exec.planner.VacuumTableRemoveOrphansPlanGenerator;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;
import com.dremio.exec.planner.logical.VacuumTableRel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.iceberg.Table;

/** A base physical plan generator for VACUUM */
public abstract class VacuumTablePruleBase extends Prule {

  private final OptimizerRulesContext context;

  public VacuumTablePruleBase(
      RelOptRuleOperand operand, String description, OptimizerRulesContext context) {
    super(operand, description);
    this.context = context;
  }

  public Prel getPhysicalPlan(VacuumTableRel vacuumTableRel) {
    TableMetadata tableMetadata =
        ((DremioPrepareTable) vacuumTableRel.getTable()).getTable().getDataset();
    StoragePluginId internalStoragePlugin =
        TableFunctionUtil.getInternalTablePluginId(tableMetadata);
    StoragePluginId storagePluginId = tableMetadata.getStoragePluginId();

    Table icebergTable = IcebergUtils.getIcebergTable(vacuumTableRel.getCreateTableEntry());
    IcebergCostEstimates icebergCostEstimates = new IcebergCostEstimates(icebergTable);

    VacuumPlanGenerator planBuilder;
    if (vacuumTableRel.getVacuumOptions().isRemoveOrphans()) {
      planBuilder =
          new VacuumTableRemoveOrphansPlanGenerator(
              vacuumTableRel.getCluster(),
              vacuumTableRel.getTraitSet().plus(Prel.PHYSICAL),
              ImmutableList.copyOf(tableMetadata.getSplits()),
              icebergCostEstimates,
              vacuumTableRel.getVacuumOptions(),
              internalStoragePlugin,
              storagePluginId,
              tableMetadata.getUser(),
              vacuumTableRel.getCreateTableEntry(),
              icebergTable.location());
    } else {
      planBuilder =
          new VacuumTableExpireSnapshotsPlanGenerator(
              vacuumTableRel.getCluster(),
              vacuumTableRel.getTraitSet().plus(Prel.PHYSICAL),
              ImmutableList.copyOf(tableMetadata.getSplits()),
              icebergCostEstimates,
              vacuumTableRel.getVacuumOptions(),
              internalStoragePlugin,
              storagePluginId,
              tableMetadata.getUser(),
              icebergTable.location());
    }

    return planBuilder.buildPlan();
  }
}

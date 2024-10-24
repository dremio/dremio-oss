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

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.VacuumCatalogRemoveOrphansPlanGenerator;
import com.dremio.exec.planner.VacuumPlanGenerator;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.VacuumCatalogDrel;
import org.apache.calcite.plan.RelOptRuleCall;

/** Generate physical plan for VACUUM CATALOG with file systems. */
public class VacuumCatalogPrule extends Prule {

  private final OptimizerRulesContext context;

  public VacuumCatalogPrule(OptimizerRulesContext optimizerContext) {
    super(RelOptHelper.any(VacuumCatalogDrel.class), "Prel.VacuumCatalogPrule");
    this.context = optimizerContext;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final VacuumCatalogDrel vacuumRel = call.rel(0);
    call.transformTo(getPhysicalPlan(vacuumRel));
  }

  public Prel getPhysicalPlan(VacuumCatalogDrel vacuumCatalogDRel) {
    StoragePluginId storagePluginId = vacuumCatalogDRel.getStoragePluginId();
    VacuumOptions vacuumOptions = vacuumCatalogDRel.getVacuumOptions();

    // Setup high values until we have a better way to estimate the cost for Catalog tables.
    IcebergCostEstimates costEstimates = vacuumCatalogDRel.getCostEstimates();
    VacuumPlanGenerator planGenerator;
    planGenerator =
        new VacuumCatalogRemoveOrphansPlanGenerator(
            vacuumCatalogDRel.getCluster(),
            vacuumCatalogDRel.getTraitSet().plus(Prel.PHYSICAL),
            vacuumOptions,
            storagePluginId,
            costEstimates,
            vacuumCatalogDRel.getUser(),
            vacuumCatalogDRel.getFsScheme(),
            vacuumCatalogDRel.getSchemeVariate(),
            context);

    return planGenerator.buildPlan();
  }
}

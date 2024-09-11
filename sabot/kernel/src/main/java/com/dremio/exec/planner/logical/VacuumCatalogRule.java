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

import com.dremio.exec.calcite.logical.VacuumCatalogCrel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/** Planner rule, Applicable for VACUUM CATALOG command only. */
public class VacuumCatalogRule extends RelOptRule {

  public static final RelOptRule INSTANCE = new VacuumCatalogRule();

  private VacuumCatalogRule() {
    super(RelOptHelper.any(VacuumCatalogCrel.class, Convention.NONE), "VacuumCatalogRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final VacuumCatalogCrel vacuumCatalogCrel = call.rel(0);
    call.transformTo(
        new VacuumCatalogDrel(
            vacuumCatalogCrel.getCluster(),
            vacuumCatalogCrel.getTraitSet().plus(Rel.LOGICAL),
            vacuumCatalogCrel.getStoragePluginId(),
            vacuumCatalogCrel.getUser(),
            vacuumCatalogCrel.getSourceName(),
            vacuumCatalogCrel.getCostEstimates(),
            vacuumCatalogCrel.getVacuumOptions(),
            vacuumCatalogCrel.getFsScheme(),
            vacuumCatalogCrel.getSchemeVariate()));
  }
}

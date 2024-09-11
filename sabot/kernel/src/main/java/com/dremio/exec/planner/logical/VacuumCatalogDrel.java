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

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.common.VacuumCatalogRelBase;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/** Dremio rel node for 'VACUUM CATALOG' query. */
public class VacuumCatalogDrel extends VacuumCatalogRelBase implements Rel {

  public VacuumCatalogDrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      StoragePluginId storagePluginId,
      String user,
      String sourceName,
      IcebergCostEstimates icebergCostEstimates,
      VacuumOptions vacuumOptions,
      String fsScheme,
      String schemeVariate) {
    super(
        LOGICAL,
        cluster,
        traitSet,
        storagePluginId,
        user,
        sourceName,
        icebergCostEstimates,
        vacuumOptions,
        fsScheme,
        schemeVariate);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new VacuumCatalogDrel(
        getCluster(),
        traitSet,
        getStoragePluginId(),
        getUser(),
        getSourceName(),
        getCostEstimates(),
        getVacuumOptions(),
        getFsScheme(),
        getSchemeVariate());
  }
}

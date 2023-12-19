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

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.common.VacuumCatalogRelBase;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;

/**
 * Calcite rel for 'VACUUM CATALOG' query.
 */
public class VacuumCatalogCrel extends VacuumCatalogRelBase {

  private boolean isDefaultRetentionUsed = false;

  public VacuumCatalogCrel(RelOptCluster cluster,
                           RelTraitSet traitSet,
                           StoragePluginId storagePluginId,
                           String user,
                           String sourceName,
                           IcebergCostEstimates icebergCostEstimates,
                           VacuumOptions vacuumOptions) {
    super(Convention.NONE, cluster, traitSet, storagePluginId, user, sourceName, icebergCostEstimates, vacuumOptions);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new VacuumCatalogCrel(getCluster(), traitSet, getStoragePluginId(), getUser(), getSourceName(), getCostEstimates(), getVacuumOptions());
  }

  public RelNode createWith(StoragePluginId storagePluginId, VacuumOptions vacuumOptions, IcebergCostEstimates costEstimates, String user, boolean isDefaultRetentionUsed) {
    VacuumCatalogCrel newCrel = new VacuumCatalogCrel(getCluster(), traitSet, storagePluginId, user, getSourceName(), costEstimates, vacuumOptions);
    newCrel.setDefaultRetentionUsed(isDefaultRetentionUsed);
    return newCrel;
  }

  private void setDefaultRetentionUsed(boolean defaultRetentionUsed) {
    isDefaultRetentionUsed = defaultRetentionUsed;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    if (isDefaultRetentionUsed) {
      pw.item("dremio_default_retention", Boolean.TRUE);
    }

    return pw;
  }
}

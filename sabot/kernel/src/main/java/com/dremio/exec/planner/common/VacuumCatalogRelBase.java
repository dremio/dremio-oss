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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.VacuumOutputSchema;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;

/**
 * Base class for 'VACUUM CATALOG' query.
 */
public class VacuumCatalogRelBase extends AbstractRelNode {
  private final VacuumOptions vacuumOptions;
  private final IcebergCostEstimates costEstimates;
  private final StoragePluginId storagePluginId;
  private final String user;
  private final String sourceName;

  protected VacuumCatalogRelBase(Convention convention,
                                 RelOptCluster cluster,
                                 RelTraitSet traitSet,
                                 StoragePluginId storagePluginId,
                                 String user,
                                 String sourceName,
                                 IcebergCostEstimates costEstimates,
                                 VacuumOptions vacuumOptions) {
    super(cluster, traitSet);
    assert getConvention() == convention;
    this.storagePluginId = storagePluginId;
    this.user = user;
    this.sourceName = sourceName;
    this.vacuumOptions = vacuumOptions;
    this.costEstimates = costEstimates;
  }

  @Override
  protected RelDataType deriveRowType() {
    return VacuumOutputSchema.getCatalogOutputRelDataType(getCluster().getTypeFactory());
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner,
                                    RelMetadataQuery mq) {
    // by default, assume cost is proportional to number of rows
    // LARGE_FILE_COUNT is used as a placeholder value. This will be overriden by the actual estimate.
    double dRows =  costEstimates != null ? costEstimates.getEstimatedRows() : DremioCost.LARGE_FILE_COUNT;
    double dCpu = dRows + 1; // ensure non-zero cost
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  public VacuumOptions getVacuumOptions() {
    return this.vacuumOptions;
  }

  public StoragePluginId getStoragePluginId() {
    return this.storagePluginId;
  }

  public String getUser() {
    return this.user;
  }

  public String getSourceName() {
    return this.sourceName;
  }

  public IcebergCostEstimates getCostEstimates() {
    return this.costEstimates;
  }
}

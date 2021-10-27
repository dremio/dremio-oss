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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.physical.PrelUtil;

/**
 * Base class for logical and physical Screen implemented in Dremio
 */
public abstract class ScreenRelBase extends SingleRel {

  public ScreenRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
    assert input.getConvention() == convention;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner, relMetadataQuery).multiplyBy(.1);
    }
    // by default, assume cost is proportional to number of rows
    double rowCount = relMetadataQuery.getRowCount(this);
    Factory costFactory = (Factory)planner.getCostFactory();
    return costFactory.makeCost(rowCount, rowCount, 0, 0).multiplyBy(0.1);
  }

}

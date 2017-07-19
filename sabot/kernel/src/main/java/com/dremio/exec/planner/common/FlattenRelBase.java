/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.curator.shaded.com.google.common.base.Preconditions;

import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.physical.PrelUtil;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

/**
 * Flatten rel base (can be any convention)
 */
public abstract class FlattenRelBase extends SingleRel {

  protected final List<RexInputRef> toFlatten;
  protected final int numProjectsPushed;

  public FlattenRelBase(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexInputRef> toFlatten, int numProjectsPushed) {
    super(cluster, traits, child);
    Preconditions.checkArgument(!toFlatten.isEmpty(), "Must have at least one flatten input.");
    this.toFlatten = toFlatten;
    this.numProjectsPushed = numProjectsPushed;
  }

  public int getNumProjectsPushed() {
    return numProjectsPushed;
  }

  public List<RexInputRef> getToFlatten() {
    return toFlatten;
  }

  @Override
  protected RelDataType deriveRowType() {
    return super.deriveRowType();
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    // We expect for flattens output to be expanding. Use a constant to expand the data.
    return mq.getRowCount(input) * toFlatten.size() * PrelUtil.getPlannerSettings(getCluster().getPlanner()).getFlattenExpansionAmount();
  }

  public Set<Integer> getFlattenedIndices(){
    return FluentIterable.from(getToFlatten()).transform(new Function<RexInputRef, Integer>(){
      @Override
      public Integer apply(RexInputRef input) {
        return input.getIndex();
      }}).toSet();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    // cost is proportional to the number of rows and number of columns being projected
    double rowCount = this.estimateRowCount(mq);
    double cpuCost = DremioCost.PROJECT_CPU_COST * rowCount;

    Factory costFactory = (Factory)planner.getCostFactory();

    if (numProjectsPushed > 0) {
      return costFactory.makeCost(rowCount, cpuCost, 0, 0).multiplyBy(1/numProjectsPushed);
    } else {
      return costFactory.makeCost(rowCount, cpuCost, 0, 0);
    }
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("flattenField", this.toFlatten);
  }

}

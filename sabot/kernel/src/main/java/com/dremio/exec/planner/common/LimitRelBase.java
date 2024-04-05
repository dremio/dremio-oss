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

import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.physical.PrelUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

/** Base class for logical and physical Limits implemented in Dremio */
public abstract class LimitRelBase extends Sort {
  private boolean pushDown; // whether limit has been pushed past its child.

  // Limit is special in that when it's pushed down, the original LIMIT still remains.
  // Once the limit is pushed down, this flag will be TRUE for the original LIMIT
  // and be FALSE for the pushed down LIMIT.
  // This flag will prevent optimization rules to fire in a loop.

  public LimitRelBase(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch) {
    this(cluster, traitSet, child, offset, fetch, false);
  }

  public LimitRelBase(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode offset,
      RexNode fetch,
      boolean pushDown) {
    super(cluster, traitSet.plus(RelCollations.EMPTY), child, RelCollations.EMPTY, offset, fetch);
    this.traitSet = traitSet;
    this.pushDown = pushDown;
  }

  public RexNode getOffset() {
    return this.offset;
  }

  public RexNode getFetch() {
    return this.fetch;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    double numRows = mq.getRowCount(this);
    double cpuCost = DremioCost.COMPARE_CPU_COST * numRows;
    Factory costFactory = (Factory) planner.getCostFactory();
    return costFactory.makeCost(numRows, cpuCost, 0, 0);
  }

  @Override
  public RelCollation getCollation() {
    return RelCollations.EMPTY;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    int off = offset != null ? RexLiteral.intValue(offset) : 0;

    if (fetch == null) {
      return mq.getRowCount(getInput()) - off;
    } else {
      int f = RexLiteral.intValue(fetch);
      return off + f;
    }
  }

  public boolean isPushDown() {
    return this.pushDown;
  }
}

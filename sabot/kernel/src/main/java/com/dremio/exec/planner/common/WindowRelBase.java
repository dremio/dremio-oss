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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

public class WindowRelBase extends Window {

  /**
   * This factor is needed to make sure that Window is considered more costly than a similar project
   */
  private static final double WINDOW_COST_FACTOR = 2;

  public WindowRelBase(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      List<RexLiteral> constants,
      RelDataType rowType,
      List<Group> windows) {
    super(
        cluster,
        traits,
        child,
        constants,
        MoreRelOptUtil.uniqifyFieldName(rowType, cluster.getTypeFactory()),
        windows);
  }

  protected static RelTraitSet adjustTraits(
      RelOptCluster cluster, RelNode input, List<Group> groups, RelTraitSet traits) {
    // At first glance, Dremio window operator does not preserve collation
    return traits.replaceIfs(RelCollationTraitDef.INSTANCE, ImmutableList::of);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelOptCost cost = super.computeSelfCost(planner, mq);
    return cost.multiplyBy(WINDOW_COST_FACTOR);
  }
}

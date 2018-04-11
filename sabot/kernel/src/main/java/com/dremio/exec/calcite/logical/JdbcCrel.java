/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.exec.planner.logical.LogicalPlanImplementor;
import com.dremio.exec.planner.logical.Rel;

import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.CopyWithCluster.CopyToCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.MoreRelOptUtil;

/**
 * Jdbc rel node on top of jdbc subtree during calcite's planning (convention NONE).
 */
public class JdbcCrel extends SingleRel implements CopyToCluster, Rel {

  private StoragePluginId pluginId;

  public JdbcCrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    this(cluster, traitSet, input, null);
  }

  public JdbcCrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, StoragePluginId pluginId) {
    super(cluster, traitSet, input);
    this.pluginId = pluginId;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new JdbcCrel(getCluster(), traitSet, inputs.iterator().next(), pluginId);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // Make the cost inversely proportional to the height of the rel tree under this node.  We want to maximize the
    // height of the tree because we want to pushdown as much as possible to the jdbc source.  The main problem here is the
    // Projects.  ProjectRelBase.computeSelfCost() generally returns a cost of "tiny" (which is 1).  So, even if we
    // match LogicalProject to two different options with the same cost (one with Project on top of JdbcRel, and another with
    // JdbcProject below JdbcRel), we may choose the one with Project on top of JdbcRel.  This is because if the costs
    // are the same, calcite will choose the first plan option it generated.

    // Compute the height of the tree.
    int minDepth = MoreRelOptUtil.getDepth(input);
    if (minDepth <= 0) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    return planner.getCostFactory().makeCost(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE).multiplyBy(1.0/minDepth);
  }

  @Override
  public RelNode copyWith(CopyWithCluster copier) {
    final RelNode input = getInput().accept(copier);
    return new JdbcCrel(
      copier.getCluster(),
      getTraitSet(),
      input,
      pluginId
    );
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {
    throw new UnsupportedOperationException();
  }

  public StoragePluginId getPluginId() {
    return this.pluginId;
  }
}

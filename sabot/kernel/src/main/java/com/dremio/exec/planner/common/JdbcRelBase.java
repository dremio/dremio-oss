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
package com.dremio.exec.planner.common;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.base.Preconditions;

/**
 * For no-op relnodes for jdbc (e.g. jdbc drel, prel, and intermediate nodes).  Just points to the subtree.
 */
public abstract class JdbcRelBase extends AbstractRelNode implements ContainerRel {
  protected final RelNode jdbcSubTree;

  public JdbcRelBase(RelOptCluster cluster, RelTraitSet traitSet, RelNode jdbcSubTree) {
    super(cluster, traitSet);
    this.jdbcSubTree = Preconditions.checkNotNull(jdbcSubTree);
    this.rowType = jdbcSubTree.getRowType();
  }

  @Override
  public RelNode getSubTree() {
    return jdbcSubTree;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return copy(getTraitSet(), getInputs());
  }

  @Override
  protected RelDataType deriveRowType() {
    return jdbcSubTree.getRowType();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).input("jdbc pushdown:", jdbcSubTree);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCost(jdbcSubTree, mq);
  }

}

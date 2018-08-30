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
package com.dremio.reflection.rules;

import java.util.List;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.planner.common.ContainerRel;

/**
 * A pointer in a RelNode tree that indicates that the replacement can be swapped in that point in the tree
 */
public class ReplacementPointer extends SingleRel implements ContainerRel {
  /**
   * the alternative tree that can be substituted in
   */
  private final RelNode replacement;
  /**
   * the portion of the original tree that is replaced by replacement
   */
  private final RelNode equivalent;

  public ReplacementPointer(RelNode replacement, RelNode equiv) {
    this(replacement, equiv, equiv.getTraitSet());
  }

  private ReplacementPointer(RelNode replacement, RelNode equiv, RelTraitSet traitSet) {
    super(equiv.getCluster(), traitSet, equiv);
    this.replacement = replacement;
    this.rowType = equiv.getRowType();
    this.equivalent = equiv;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery md) {
    return equivalent.estimateRowCount(md);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery md) {
    return planner.getCostFactory().makeZeroCost();
  }

  @Override
  public ReplacementPointer copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ReplacementPointer(replacement, inputs.get(0), traitSet);
  }

  public RelNode getReplacement() {
    return replacement;
  }

  @Override
  public RelNode getSubTree() {
    return input;
  }
}

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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.planner.common.UnionRelBase;

/**
 * Union implemented in Dremio.
 */
public class UnionRel extends UnionRelBase implements Rel {
  /** Creates a UnionRel. */
  public UnionRel(RelOptCluster cluster, RelTraitSet traits,
      List<RelNode> inputs, boolean all, boolean checkCompatibility) throws InvalidRelException {
    super(cluster, traits, inputs, all, checkCompatibility);
  }

  @Override
  public UnionRel copy(RelTraitSet traitSet, List<RelNode> inputs,
      boolean all) {
    try {
      return new UnionRel(getCluster(), traitSet, inputs, all,
          false /* don't check compatibility during copy */);
    } catch (InvalidRelException e) {
      throw new AssertionError(e) ;
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // divide cost by two to ensure cheaper than EnumerableRel
    return super.computeSelfCost(planner, mq).multiplyBy(.5);
  }
}

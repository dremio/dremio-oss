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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.planner.common.JoinRelBase;

/**
 * Logical Join implemented in Dremio.
 */
public class JoinRel extends JoinRelBase implements Rel {

  /** Creates a JoinRel.
   * We do not throw InvalidRelException in Logical planning phase. It's up to the post-logical planning check or physical planning
   * to detect the unsupported join type, and throw exception.
   * */
  private JoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType)  {
    super(cluster, traits, left, right, condition, joinType);
    assert traits.contains(Rel.LOGICAL);
  }

  public static JoinRel create(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) {
    final RelTraitSet traits = adjustTraits(traitSet);

    return new JoinRel(cluster, traits, left, right, condition, joinType);
  }

  @Override
  public JoinRel copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new JoinRel(getCluster(), traitSet, left, right, condition, joinType);
  }
}

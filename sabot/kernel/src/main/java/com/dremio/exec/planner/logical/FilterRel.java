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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.planner.common.FilterRelBase;


public class FilterRel extends FilterRelBase implements Rel {

  private final boolean alreadyPushedDown;

  public FilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    this(cluster, traits, child, condition, false);
  }

  public FilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition, boolean alreadyPushedDown) {
    super(LOGICAL, cluster, traits, child, condition);
    this.alreadyPushedDown = alreadyPushedDown;
  }

  public boolean isAlreadyPushedDown() {
    return alreadyPushedDown;
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new FilterRel(getCluster(), traitSet, input, condition, alreadyPushedDown);
  }

  public static FilterRel create(RelNode child, RexNode condition) {
    return new FilterRel(child.getCluster(), child.getTraitSet(), child, condition)  ;
  }

  @Override
  public boolean canHaveContains() {
    return true;
  }
}

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
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.util.ImmutableBitSet;

public class CorrelateRel extends Correlate implements Rel {

  public CorrelateRel(
    RelOptCluster cluster,
    RelTraitSet traits,
    RelNode left,
    RelNode right,
    CorrelationId correlationId,
    ImmutableBitSet requiredColumns,
    SemiJoinType joinType) {
    super(cluster, traits, left, right, correlationId, requiredColumns, joinType);
    assert getConvention() == LOGICAL;
  }

  @Override
  public Correlate copy(RelTraitSet traitSet,
    RelNode left, RelNode right, CorrelationId correlationId,
    ImmutableBitSet requiredColumns, SemiJoinType joinType) {
    return new CorrelateRel(getCluster(), traitSet, left, right,
      correlationId, requiredColumns, joinType);
  }

  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

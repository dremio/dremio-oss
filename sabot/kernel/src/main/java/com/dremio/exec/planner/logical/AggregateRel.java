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

import com.dremio.exec.planner.common.AggregateRelBase;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

/** Aggregation implemented in Dremio. */
public class AggregateRel extends AggregateRelBase implements Rel {

  /** Creates a AggregateRel. */
  private AggregateRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls)
      throws InvalidRelException {
    super(cluster, traits, child, groupSet, groupSets, aggCalls);
  }

  public static AggregateRel create(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls)
      throws InvalidRelException {
    final RelTraitSet adjustedTraits = adjustTraits(traits);
    return new AggregateRel(cluster, adjustedTraits, child, groupSet, groupSets, aggCalls);
  }

  @Override
  public Aggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    try {
      return AggregateRel.create(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
    for (AggregateCall aggCall : getAggCallList()) {
      // For avg, stddev_pop, stddev_samp, var_pop and var_samp, the ReduceAggregatesRule is
      // supposed
      // to convert them to use sum and count. Here, we make the cost of the original functions high
      // enough such that the planner does not choose them and instead chooses the rewritten
      // functions.
      if (aggCall.getAggregation().getKind() == SqlKind.AVG
          || aggCall.getAggregation().getKind() == SqlKind.STDDEV_SAMP
          || aggCall.getAggregation().getKind() == SqlKind.STDDEV_POP
          || aggCall.getAggregation().getKind() == SqlKind.VAR_POP
          || aggCall.getAggregation().getKind() == SqlKind.VAR_SAMP) {
        return planner.getCostFactory().makeHugeCost();
      }
    }

    final double rowCount = relMetadataQuery.getRowCount(this);
    final double childRowCount = relMetadataQuery.getRowCount(this.getInput());
    // Aggregates with more aggregate functions cost a bit more
    float multiplier = 1f + aggCalls.size() * 0.125f;
    return ((Factory) planner.getCostFactory())
        .makeCost(rowCount, childRowCount * multiplier * DremioCost.FUNC_CPU_COST, 0, 0);
  }
}

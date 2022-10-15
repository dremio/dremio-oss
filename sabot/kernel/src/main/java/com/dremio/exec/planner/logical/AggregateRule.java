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

import static com.dremio.exec.planner.common.MoreRelOptUtil.containsUnsupportedDistinctCall;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

/**
 * Rule that converts an {@link LogicalAggregate} to a {@link AggregateRel}, implemented by a Dremio "segment" operation
 * followed by a "collapseaggregate" operation.
 */
public class AggregateRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new AggregateRule();
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private AggregateRule() {
    super(RelOptHelper.some(LogicalAggregate.class, Convention.NONE, RelOptHelper.any(RelNode.class)), "AggregateRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalAggregate aggregate = (LogicalAggregate) call.rel(0);
    final RelNode input = call.rel(1);

    if (containsUnsupportedDistinctCall(aggregate) || ProjectableSqlAggFunctions.isProjectableAggregate(aggregate)) {
      // currently, don't use this rule if any of the aggregates contains DISTINCT or projectable agg calls
      // (LIST_AGG with distinct is allowed)
      return;
    }

    final RelTraitSet traits = aggregate.getTraitSet().plus(Rel.LOGICAL);
    final RelNode convertedInput = convert(input, input.getTraitSet().plus(Rel.LOGICAL).simplify());
    try {
      call.transformTo(AggregateRel.create(aggregate.getCluster(), traits, convertedInput,
          aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList()));
    } catch (InvalidRelException e) {
      // Do nothing. Planning might not succeed, but that's okay.
      tracer.debug("Cannot create aggregate node", e);
    }
  }
}

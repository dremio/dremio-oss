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
package com.dremio.exec.planner.physical;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;


public class FilterNLJMergeRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new FilterNLJMergeRule();

  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private FilterNLJMergeRule() {
    super(operand(FilterPrel.class, operand(NestedLoopJoinPrel.class, any())), "NLJMergeRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    NestedLoopJoinPrel join = call.rel(1);
    return join.getJoinType() == JoinRelType.INNER &&
        PrelUtil.getPlannerSettings(call.getPlanner()).getOptions().getOption(NestedLoopJoinPrel.VECTORIZED);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FilterPrel filter = call.rel(0);
    NestedLoopJoinPrel join = call.rel(1);

    if ((join.getProjectedFields() == null) || join.getProjectedFields().cardinality() == join.getInputRowType().getFieldCount()) {
      call.transformTo(NestedLoopJoinPrel.create(join.getCluster(), join.getTraitSet(), join.getLeft(), join.getRight(), join.getJoinType(), RelOptUtil.andJoinFilters(join.getCluster().getRexBuilder(), join.getCondition(), filter.getCondition()), join.getProjectedFields()));
    } else {
      // Current filter condition is written based on projected fields on join. In order to push this filter down we need to rewrite filter condition
      final ImmutableBitSet topProjectedColumns = RelOptUtil.InputFinder.bits(filter.getCondition());
      final ImmutableBitSet bottomProjectedColumns = join.getProjectedFields();

      Mapping mapping = Mappings.create(MappingType.SURJECTION, join.getRowType().getFieldCount(), join.getInputRowType().getFieldCount());
      for (Ord<Integer> ord : Ord.zip(bottomProjectedColumns)) {
        if (topProjectedColumns.get(ord.i)) {
          mapping.set(ord.i, ord.e);
        }
      }

      RexShuttle shuttle = new RexPermuteInputsShuttle(mapping);
      RexNode updatedCondition = shuttle.apply(filter.getCondition());

      call.transformTo(NestedLoopJoinPrel.create(join.getCluster(), join.getTraitSet(), join.getLeft(), join.getRight(), join.getJoinType(), RelOptUtil.andJoinFilters(join.getCluster().getRexBuilder(), join.getCondition(), updatedCondition), join.getProjectedFields()));
    }
  }
}

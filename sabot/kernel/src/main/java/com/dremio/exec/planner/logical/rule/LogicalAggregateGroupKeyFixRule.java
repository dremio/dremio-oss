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
package com.dremio.exec.planner.logical.rule;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilder.GroupKey;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.google.common.collect.ImmutableList;

/**
 * Rule to rewrite aggregate so that the group keys are always first in the input, e.g.
 * groupSet = {0,2}
 * will become
 * groupSet = {0,1}
 * with an appropriate project added below
 */
public class LogicalAggregateGroupKeyFixRule {
  public static final RelOptRule RULE = new RelOptRule(
      RelOptHelper.any(LogicalAggregate.class, RelNode.class),
      "LogicalAggregateGroupKeyFix") {

    @Override
    public boolean matches(RelOptRuleCall call) {
      LogicalAggregate agg = call.rel(0);
      if (!(agg.getGroupType() == Group.SIMPLE)) {
        return false;
      }
      // only need to apply the rule if the group set does not contain continuous values starting with 0
      if (agg.getGroupSet().equals(ImmutableBitSet.range(agg.getGroupSet().cardinality()))) {
        return false;
      }
      return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalAggregate agg = call.rel(0);
      Mapping inputMapping = Mappings.create(MappingType.BIJECTION, agg.getInput().getRowType().getFieldCount(), agg.getInput().getRowType().getFieldCount());
      for (Ord<Integer> groupKey : Ord.zip(agg.getGroupSet())) {
        int source = groupKey.e;
        int target = groupKey.i;
        inputMapping.set(source, target);
      }

      int target = agg.getGroupCount();
      for (int i = 0; i < agg.getInput().getRowType().getFieldCount(); i++) {
        if (!agg.getGroupSet().get(i)) {
          inputMapping.set(i, target++);
        }
      }

      RelBuilder relBuilder = call.builder();
      relBuilder.push(agg.getInput());
      relBuilder.project(relBuilder.fields(Mappings.invert(inputMapping)));
      GroupKey groupKey = relBuilder.groupKey(Mappings.apply(inputMapping, agg.getGroupSet()), null);
      List<AggCall> newAggCallList = new ArrayList<>();
      for (AggregateCall aggCall : agg.getAggCallList()) {
        final ImmutableList<RexNode> args =
          relBuilder.fields(
            Mappings.apply2(inputMapping, aggCall.getArgList()));
        RelBuilder.AggCall newAggCall =
          relBuilder.aggregateCall(aggCall.getAggregation(), args)
            .distinct(aggCall.isDistinct())
            .approximate(aggCall.isApproximate())
            .sort(relBuilder.fields(aggCall.collation))
            .as(aggCall.name);
        newAggCallList.add(newAggCall);
      }
      relBuilder.aggregate(groupKey, newAggCallList);
      call.transformTo(relBuilder.build());
    }
  };

}

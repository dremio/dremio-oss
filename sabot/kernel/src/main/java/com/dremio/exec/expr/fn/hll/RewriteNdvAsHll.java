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
package com.dremio.exec.expr.fn.hll;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.RelOptHelper;

/**
 * Rule the converts an NDV expression as HLL Aggregate + HLL_DECODE project.
 */
public class RewriteNdvAsHll extends RelOptRule {

  public static final RelOptRule INSTANCE = new RewriteNdvAsHll();

  private RewriteNdvAsHll() {
    super(RelOptHelper.some(LogicalAggregate.class, Convention.NONE, RelOptHelper.any(RelNode.class)), DremioRelFactories.CALCITE_LOGICAL_BUILDER, "RewriteNDVAsHll");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalAggregate agg = call.rel(0);
    final RelDataTypeFactory typeFactory = agg.getCluster().getTypeFactory();

    List<AggregateCall> calls = new ArrayList<>();
    Set<Integer> hllApplications = new HashSet<>();

    int i = agg.getGroupCount();
    for (AggregateCall c : agg.getAggCallList()) {

      final int location = i;
      i++;

      if(!"NDV".equals(c.getAggregation().getName())) {
        calls.add(c);
        continue;
      }

      hllApplications.add(location);
      calls.add(AggregateCall.create(HyperLogLog.HLL, false, c.getArgList(), -1, typeFactory.createSqlType(SqlTypeName.VARBINARY, HyperLogLog.HLL_VARBINARY_SIZE), c.getName()));
    }

    if(hllApplications.isEmpty()) {
      return;
    }

    final RelBuilder builder = relBuilderFactory.create(agg.getCluster(), null);
    builder.push(agg.getInput());
    builder.aggregate(builder.groupKey(agg.getGroupSet().toArray()), calls);

    // add the hll application project.
    final List<RexNode> nodes = new ArrayList<>();
    for(int field = 0; field < agg.getRowType().getFieldCount(); field++) {
      if(!hllApplications.contains(field)) {
        nodes.add(builder.field(field));
        continue;
      }

      nodes.add(builder.call(HyperLogLog.HLL_DECODE, builder.field(field)));
    }
    builder.project(nodes);
    call.transformTo(builder.build());
  }

}

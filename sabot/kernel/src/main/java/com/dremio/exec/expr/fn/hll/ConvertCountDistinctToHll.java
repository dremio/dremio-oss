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
package com.dremio.exec.expr.fn.hll;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilder;

import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.dremio.exec.store.NamespaceTable;

/**
 * Converts CountDistinct to HLL if the underlying table allows such.
 */
public class ConvertCountDistinctToHll extends RelOptRule {

  public static final RelOptRule INSTANCE = new ConvertCountDistinctToHll();

  private ConvertCountDistinctToHll() {
    super(RelOptHelper.some(LogicalAggregate.class, Convention.NONE, RelOptHelper.any(RelNode.class)), DremioRelFactories.CALCITE_LOGICAL_BUILDER, "ConvertCountDistinctToHll");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalAggregate agg = call.rel(0);
    final RelNode input = agg.getInput();

    boolean distinctReplaced = false;
    List<AggregateCall> calls = new ArrayList<>();

    RelMetadataQuery query = null;

    final Boolean[] memo = new Boolean[agg.getInput().getRowType().getFieldCount()];
    for (AggregateCall c : agg.getAggCallList()) {
      final boolean candidate = c.isDistinct() && c.getArgList().size() == 1 && "COUNT".equals(c.getAggregation().getName());

      if(!candidate) {
        calls.add(c);
        continue;
      }

      final int inputOrdinal = c.getArgList().get(0);
      boolean allowed = false;
      if(memo[inputOrdinal] != null) {
        allowed = memo[inputOrdinal];
      } else {
        if(query == null) {
          query = agg.getCluster().getMetadataQuery();
        }

        Set<RelColumnOrigin> origins = query.getColumnOrigins(input, inputOrdinal);

        // see if any column origin allowed a transformation.
        for(RelColumnOrigin o : origins) {
          RelOptTable table = o.getOriginTable();
          NamespaceTable namespaceTable = table.unwrap(NamespaceTable.class);
          if(namespaceTable == null) {
            // unable to decide, no way to transform.
            return;
          }

          if(namespaceTable.isApproximateStatsAllowed()) {
            allowed = true;
          }
        }

        memo[inputOrdinal] = allowed;

      }


      if(allowed) {
        calls.add(AggregateCall.create(DremioSqlOperatorTable.NDV, false, c.getArgList(), -1, c.getType(), c.getName()));
        distinctReplaced = true;
      } else {
        calls.add(c);
      }

    }

    if(!distinctReplaced) {
      return;
    }

    final RelBuilder builder = relBuilderFactory.create(agg.getCluster(), null);
    builder.push(agg.getInput());
    builder.aggregate(builder.groupKey(agg.getGroupSet().toArray()), calls);
    call.transformTo(builder.build());
  }

}

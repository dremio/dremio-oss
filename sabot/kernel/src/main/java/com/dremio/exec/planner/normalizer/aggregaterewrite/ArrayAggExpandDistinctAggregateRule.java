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

package com.dremio.exec.planner.normalizer.aggregaterewrite;

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_AGG;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.sql.SqlKind;

import com.dremio.exec.planner.logical.RelOptHelper;

public final class ArrayAggExpandDistinctAggregateRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new ArrayAggExpandDistinctAggregateRule(
    AggregateExpandDistinctAggregatesRule.Config.JOIN);
  private final AggregateExpandDistinctAggregatesRule rule;

  private ArrayAggExpandDistinctAggregateRule(AggregateExpandDistinctAggregatesRule.Config config) {
    super(RelOptHelper.some(LogicalAggregate.class, Convention.NONE, RelOptHelper.any(RelNode.class)),
      "ArrayAggExpandDistinctAggregateRule");
    rule = config.toRule();
  }

  private boolean matches(AggregateCall call) {
    if (!call.isDistinct()) {
      return false;
    }
    return call.getAggregation().getKind() == SqlKind.LISTAGG && ARRAY_AGG.getName().equals(call.getAggregation().getName());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalAggregate aggregate = call.rel(0);
    if (aggregate.getAggCallList().stream().anyMatch(this::matches)) {
      rule.onMatch(call);
    }
  }
}

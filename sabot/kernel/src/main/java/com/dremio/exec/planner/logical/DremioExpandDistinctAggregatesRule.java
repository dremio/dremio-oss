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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_AGG;

import java.util.Objects;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.sql.SqlKind;

/**
 * This rule uses AggregateExpandDistinctAggregatesRule in Calcite.
 * This overrides matches function to prevent expanding distincts
 * when they all appear in LISTAGG.
 * <p>
 * In principle, AggregateExpandDistinctAggregatesRule works for
 * LISTAGG as well. However, as of right now, LISTAGG performs
 * DISTINCT operation in Arrow Vector (FixedListVarcharVector).
 * <a href="https://docs.google.com/document/d/1WrjAFO2O0Td_jZeIFY0BDkFUgwHjclbywYren9lzvuI/edit?disco=AAAAYD1IRxw">
 *   This was an explicit design decision for LISTAGG</a>.
 *
 */
public class DremioExpandDistinctAggregatesRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DremioExpandDistinctAggregatesRule(AggregateExpandDistinctAggregatesRule.Config.JOIN);
  private final AggregateExpandDistinctAggregatesRule rule;

  private DremioExpandDistinctAggregatesRule(AggregateExpandDistinctAggregatesRule.Config config) {
    super(RelOptHelper.some(LogicalAggregate.class, Convention.NONE, RelOptHelper.any(RelNode.class)), "DremioExpandDistinctAggregatesRule");
    rule = config.toRule();
  }

  private boolean matches(AggregateCall call) {
    if (!call.isDistinct()) {
      return false;
    }
    if (call.getAggregation().getKind() == SqlKind.LISTAGG) {
      // Both ARRAY_AGG and LISTAGG have same SQLKind. We would like
      // AggregateExpandDistinctAggregatesRule on ARRAY_AGG but not LISTAGG
      return Objects.equals(call.getAggregation().getName(), ARRAY_AGG.getName());
    }
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalAggregate aggregate = call.rel(0);
    if (aggregate.getAggCallList().stream().anyMatch(this::matches)) {
      rule.onMatch(call);
    }
  }
}

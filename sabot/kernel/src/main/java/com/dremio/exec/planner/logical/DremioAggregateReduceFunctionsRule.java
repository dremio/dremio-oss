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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 *
 */
public final class DremioAggregateReduceFunctionsRule extends AggregateReduceFunctionsRule {

  public static final DremioAggregateReduceFunctionsRule INSTANCE =
          new DremioAggregateReduceFunctionsRule(operand(LogicalAggregate.class, any()), true,
                  RelFactories.LOGICAL_BUILDER);

  private DremioAggregateReduceFunctionsRule(RelOptRuleOperand operand, boolean reduceSum,
                                             RelBuilderFactory relBuilderFactory) {
    super(operand, reduceSum, relBuilderFactory);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate a = call.rel(0);
    if (ProjectableSqlAggFunctions.isProjectableAggregate(a)) {
      return;
    }
    super.onMatch(call);
  }
}

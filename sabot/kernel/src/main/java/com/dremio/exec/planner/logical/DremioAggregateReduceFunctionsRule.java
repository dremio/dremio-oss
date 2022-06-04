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

import java.util.EnumSet;
import java.util.Set;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableSet;

/**
 *
 */
public class DremioAggregateReduceFunctionsRule extends AggregateReduceFunctionsRule {

  public static final Set<SqlKind> DEFAULT_FUNCTIONS_TO_REDUCE_NO_SUM =
    ImmutableSet.<SqlKind>builder()
      .addAll(SqlKind.AVG_AGG_FUNCTIONS)
      .build();

  public static final DremioAggregateReduceFunctionsRule INSTANCE =
          new DremioAggregateReduceFunctionsRule(LogicalAggregate.class,
                  RelFactories.LOGICAL_BUILDER, EnumSet.copyOf(Config.DEFAULT_FUNCTIONS_TO_REDUCE));

  public static final DremioAggregateReduceFunctionsRule NO_REDUCE_SUM =
    new DremioAggregateReduceFunctionsRule(AggregateRel.class,
      DremioRelFactories.LOGICAL_BUILDER, EnumSet.copyOf(DEFAULT_FUNCTIONS_TO_REDUCE_NO_SUM));

  public DremioAggregateReduceFunctionsRule(Class<? extends Aggregate> aggregateClass,
                                            RelBuilderFactory relBuilderFactory, EnumSet<SqlKind> functionsToReduce) {
    super( aggregateClass, relBuilderFactory, functionsToReduce);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate a = call.rel(0);
    if (ProjectableSqlAggFunctions.isProjectableAggregate(a)) {
      return;
    }
    super.onMatch(call);
  }

  public static class ForGroupingSets extends DremioAggregateReduceFunctionsRule {
    public ForGroupingSets(Class<? extends Aggregate> aggregateClass, RelBuilderFactory relBuilderFactory, EnumSet<SqlKind> functionsToReduce) {
      super(aggregateClass, relBuilderFactory, functionsToReduce);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      Aggregate agg = call.rel(0);
      return agg.getGroupSets().size() > 1;
    }
  }
}

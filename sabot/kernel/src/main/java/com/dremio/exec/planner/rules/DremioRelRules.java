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
package com.dremio.exec.planner.rules;

import java.util.EnumSet;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;

import com.dremio.exec.planner.logical.DremioAggregateReduceFunctionsRule;
import com.dremio.exec.planner.logical.DremioRelFactories;

public interface DremioRelRules {
  RelOptRule JOIN_SUB_QUERY_TO_CORRELATE =
    JoinSubQueryToCorrelate.Config.DEFAULT.toRule();

  DremioAggregateReduceFunctionsRule REDUCE_FUNCTIONS_FOR_GROUP_SETS =
    new DremioAggregateReduceFunctionsRule.ForGroupingSets(LogicalAggregate.class,
      DremioRelFactories.CALCITE_LOGICAL_BUILDER, EnumSet.copyOf(AggregateReduceFunctionsRule.Config.DEFAULT_FUNCTIONS_TO_REDUCE));
}

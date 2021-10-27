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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql2rel.CorrelationReferenceFinder;

import com.dremio.service.Pointer;

/**
 * Planner rule that pushes predicates from a Filter into the Join below, but not if the filter contains
 * correlated variables.
 */
public final class SimpleFilterJoinRule extends FilterJoinRule {

  public static final FilterJoinRule CALCITE_INSTANCE = new SimpleFilterJoinRule(
    SimpleFilterJoinRule.Config.EMPTY
      .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .withOperandSupplier(b0 ->
        b0.operand(LogicalFilter.class).oneInput(b1 ->
          b1.operand(LogicalJoin.class).anyInputs()))
      .withDescription("SimpleFilterJoinRuleCrel:filter")
      .as(SimpleFilterJoinRule.Config.class)
      .withSmart(true)
      .withPredicate(FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM)
      .as(SimpleFilterJoinRule.Config.class));

  public static final FilterJoinRule LOGICAL_INSTANCE = new SimpleFilterJoinRule(
    SimpleFilterJoinRule.Config.EMPTY
      .withRelBuilderFactory(DremioRelFactories.LOGICAL_BUILDER)
      .withOperandSupplier(b0 ->
        b0.operand(Filter.class).oneInput(b1 ->
          b1.operand(Join.class).anyInputs()))
      .withDescription("SimpleFilterJoinRuleDrel:filter")
      .as(SimpleFilterJoinRule.Config.class)
      .withSmart(true)
      .withPredicate(FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM)
      .as(SimpleFilterJoinRule.Config.class));

  private SimpleFilterJoinRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    Join join = call.rel(1);

    final Pointer<Boolean> containsCorrelation = new Pointer<>(false);
    class FinderImpl extends CorrelationReferenceFinder {
      @Override
      protected RexNode handle(RexFieldAccess fieldAccess) {
        containsCorrelation.value = true;
        return fieldAccess;
      }
    }

    filter.accept(new FinderImpl());
    if (containsCorrelation.value) {
      return;
    }

    perform(call, filter, join);
  }
}

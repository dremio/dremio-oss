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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.FilterJoinRule;

/**
 * Combined rule of FilterJoinRule and DremioJoinPushTransitivePredicatesRule.
 * The DremioJoinPushTransitivePredicatesRule was added for the purpose of providing transitive
 * filter pushdown without getting stuck into infinite loop due to transitive filter rule pushing filter
 * down and other reduce rules reducing filters which can end up firing transitive-reduce rules back and forth.
 * DremioJoinPushTransitivePredicatesRule addresses this infinite loop issue by providing a cache for the sub-tree
 * we see and the filters we are pushing down, but this cache was not efficient enough since if a similar sub-plan
 * with the same filters appear somewhere else in the tree, we will not apply the transitive filter in that case
 * since the cache will already contain the key-value pair (DX-27497). In other cases, the reduce rules were reducing
 * some RelNodes which was resulting in creation of NestedLoopJoins instead of HashJoins for some customers (CS-8112).
 * To take care of all these issues, we split the Pre-Logical pushdown phase into 2 steps:
 * 1) Apply normal pre-logical pushdown phase along with composite rules, but with added reduce rules (these rules
 * are disabled by default but can be enabled individually). This will push down all the filters.
 * 2) Pull the filters up from the Joins and then apply pre-logical phase again. The reason this filter pull-up step is
 * done after step 1 is for the case when the filters are already above the Joins, the filter pull-up will infer no
 * filters and we won't be able to fire the transitive rule.
 * With this change, we will only fire the composite rule once per Filter-Join pair after the filter pull-up stage, which
 * solves the infinite loop problem. The reduce rules are also configurable now, which solves the slow NLJ's problem.
 */
public abstract class CompositeFilterJoinRule extends FilterJoinRule {
  public static final CompositeFilterJoinRule TOP_FILTER = new WithFilter();
  public static final CompositeFilterJoinRule NO_TOP_FILTER = new NoFilter();

  private CompositeFilterJoinRule(RelOptRuleOperand operand, String desc) {
    super(operand, desc, true, DremioRelFactories.CALCITE_LOGICAL_BUILDER,
      FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM);
  }

  private static class NoFilter extends CompositeFilterJoinRule {
    private NoFilter() {
      super(RelOptRule.operand(LogicalJoin.class, RelOptRule.any()),
        "CompositeFilterJoinRule:no-filter");
    }

    @Override
    protected RelNode doMatch(RelOptRuleCall call) {
      Join join = call.rel(0);
      TransformCollectingCall c = new TransformCollectingCall(call.getPlanner(), this.getOperand(), new RelNode[]{join}, null);
      perform(c, null, join);
      if (c.outcome.isEmpty()) {
        return null;
      }
      return c.outcome.get(0);
    }
  }

  private static class WithFilter extends CompositeFilterJoinRule {
    private WithFilter() {
      super(RelOptRule.operand(LogicalFilter.class, RelOptRule.operand(LogicalJoin.class, RelOptRule.any())),
        "CompositeFilterJoinRule:filter");
    }

    @Override
    protected RelNode doMatch(RelOptRuleCall call) {
      Filter filter = call.rel(0);
      Join join = call.rel(1);
      TransformCollectingCall c = new TransformCollectingCall(call.getPlanner(), this.getOperand(), new RelNode[]{filter, join}, null);
      perform(c, filter, join);
      if (c.outcome.isEmpty()) {
        return null;
      }
      return c.outcome.get(0);
    }
  }

  protected abstract RelNode doMatch(RelOptRuleCall call);

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode result = doMatch(call);
    if (result == null) {
      return;
    }
    call.transformTo(result.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(LogicalJoin join) {
        DremioJoinPushTransitivePredicatesRule instance = new DremioJoinPushTransitivePredicatesRule();
        TransformCollectingCall c2 = new TransformCollectingCall(call.getPlanner(), instance.getOperand(), new RelNode[]{join}, null);
        instance.onMatch(c2);
        if (c2.outcome.isEmpty()) {
          return join;
        } else {
          return c2.outcome.get(0);
        }
      }
    }));
  }

  static class TransformCollectingCall extends RelOptRuleCall {
    final List<RelNode> outcome = new ArrayList<>();

    public TransformCollectingCall(RelOptPlanner planner, RelOptRuleOperand operand, RelNode[] rels,
                                   Map<RelNode, List<RelNode>> nodeInputs) {
      super(planner, operand, rels, nodeInputs);
    }

    @Override
    public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv) {
      outcome.add(rel);
    }
  }
}

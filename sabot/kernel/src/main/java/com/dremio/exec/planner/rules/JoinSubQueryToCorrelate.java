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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;

import com.dremio.exec.planner.sql.handlers.RexSubQueryUtils;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

/**
 * Rewrites a join condition with a sub-query to a Correlate with filter on the right side.
 * The upstream rule does not handle left joins.
 *
 * @see org.apache.calcite.rel.rules.CoreRules#JOIN_SUB_QUERY_TO_CORRELATE
 */
public class JoinSubQueryToCorrelate extends RelRule<JoinSubQueryToCorrelate.Config> {

  /**
   * Creates a RelRule.
   *
   * @param config
   */
  protected JoinSubQueryToCorrelate(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelNode left = join.getLeft();
    RelNode right = join.getRight();
    CorrelationId correlationId = findOrCreateCorrelateId(join.getCluster(), join);
    RelBuilder builder = call.builder();
    RexBuilder rexBuilder = builder.getRexBuilder();

    RexNode newCondition = rewrite(join, correlationId);

    call.transformTo(builder
      .push(left)
      .push(right)
      .filter(newCondition)
      .join(join.getJoinType(),
        rexBuilder.makeLiteral(true),
        ImmutableSet.of(correlationId))
      .build());
  }

  private static CorrelationId findOrCreateCorrelateId(RelOptCluster relOptCluster, Join join) {
    CorrelationId id = Iterables.getOnlyElement(join.getVariablesSet(), null);
    if(id == null) {
      return relOptCluster.createCorrel();
    } else {
      return id;
    }
  }

  private static RexNode rewrite(Join join, CorrelationId correlationId) {
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    RelNode left = join.getLeft();

    RexNode baseNode = rexBuilder.makeCorrel(left.getRowType(), correlationId);

    int leftSize = left.getRowType().getFieldCount();

    return join.getCondition().accept(new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        if(inputRef.getIndex() < leftSize) {
          return rexBuilder.makeFieldAccess(baseNode, inputRef.getIndex());
        } else {
          return rexBuilder.makeInputRef(inputRef.getType(), inputRef.getIndex() - leftSize);
        }
      }
    });
  }

  public interface Config extends RelRule.Config{
    Config DEFAULT = RelRule.Config.EMPTY
      .withOperandSupplier(os ->
        os.operand(Join.class)
          .predicate(join -> RexSubQueryUtils.containsSubQuery(join)
            && !join.getJoinType().generatesNullsOnLeft())
          .anyInputs())
      .as(Config.class);

    @Override default RelOptRule toRule() {
      return new JoinSubQueryToCorrelate(this);
    }
  }
}

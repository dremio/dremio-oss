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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;

/**
 * Rule that converts a {@link org.apache.calcite.rel.logical.LogicalJoin} to a
 * {@link JoinRel}, which is implemented by Dremio "join" operation.
 */
public class JoinRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new JoinRule(DremioRelFactories.LOGICAL_BUILDER);

  private final RelBuilderFactory factory;

  private JoinRule(RelBuilderFactory factory) {
    super(RelOptHelper.any(LogicalJoin.class, Convention.NONE), "JoinRule");
    this.factory = factory;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final RelNode left = join.getLeft();
    final RelNode right = join.getRight();
    final RelNode convertedLeft = convertIfNecessary(left);
    final RelNode convertedRight = convertIfNecessary(right);
    final RelBuilder builder = factory.create(join.getCluster(), null);

    builder.pushAll(ImmutableList.of(convertedLeft, convertedRight));
    builder.join(join.getJoinType(), join.getCondition());

    final RelNode newJoin = builder.build();
    if(newJoin != null) {
      call.transformTo(newJoin);
    }
  }

  /**
   * Converts the tree to the proper convention
   * @param node the node to convert
   * @return the converted node or possibly the original if no conversion necessary
   */
  private RelNode convertIfNecessary(RelNode node) {
    return convert(node, node.getTraitSet().plus(Rel.LOGICAL).simplify());
  }
}

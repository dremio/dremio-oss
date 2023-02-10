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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Rule that normalize Dremio {@code JoinRel} into a proper equi-join with an extra filter on top if necessary/possible,
 * ready to be converted into a {@code com.dremio.exec.planner.physical.JoinPrel} instance.
 */
public class JoinNormalizationRule extends RelOptRule {
  public static final JoinNormalizationRule INSTANCE = new JoinNormalizationRule(DremioRelFactories.LOGICAL_BUILDER);

  private final RelBuilderFactory factory;

  private JoinNormalizationRule(RelBuilderFactory factory) {
    super(RelOptHelper.any(JoinRel.class), "JoinNormalizationRule");
    this.factory = factory;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);

    final RelNode newJoin = normalize(join);

    if (newJoin != join) {
      call.transformTo(newJoin);
    }
  }

  /**
   * Normalize the join relation operator
   *
   * Normalize the join operator so that conditions are fully push down
   * and if possible, remaining condition are extracted in a separate filter
   *
   * @param rel
   * @return a new tree of operators containing the normalized join, or {@code join} if
   * already normalized
   */
  public RelNode normalize(Join join) {
    final RelBuilder builder = factory.create(join.getCluster(), null);

    RelNode newJoin = RelOptUtil.pushDownJoinConditions(join, builder);
    // If the join is the same, reset to the original join so we can bail out later
    if (newJoin instanceof Join) {
      final RexNode newCondition = ((Join) newJoin).getCondition();
      if (join.getCondition().toString().equals(newCondition.toString())) {
        newJoin = join;
      }
    }


    // newJoin might be a join, or might be a join below a project.
    // Need to visit the tree to find the first join and extract the remaining
    // condition in a separate filter
    newJoin = newJoin.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(RelNode other) {
        if (!(other instanceof Join)) {
          return super.visit(other);
        }

        Join join = (Join) other;

        return getNewJoinCondition(builder, join);

      }
    });

    return newJoin;
  }
  /**
   * Attempt to create a new join with a canonicalized join expression, and a possible filter
   * on top
   * @param builder
   * @param join
   * @return a new join tree (or same as original argument if no change)
   */
  private RelNode getNewJoinCondition(RelBuilder builder, Join join) {
    final List<Integer> leftKeys = Lists.newArrayList();
    final List<Integer> rightKeys = Lists.newArrayList();
    final List<Boolean> filterNulls = Lists.newArrayList();

    final RexNode remaining = RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(), join.getCondition(), leftKeys, rightKeys, filterNulls);
    final boolean hasEquiJoins = leftKeys.size() == rightKeys.size() && leftKeys.size() > 0 ;
    final JoinRelType joinType = join.getJoinType();

    // If join has no equi-join condition, do not transform
    if (!hasEquiJoins) {
      return join;
    }

    // Create a new partial condition for the equi-join
    final RexNode partialCondition = JoinFilterCanonicalizationRule.buildJoinCondition(
        builder.getRexBuilder(),
        join.getLeft().getRowType(),
        join.getRight().getRowType(),
        leftKeys,
        rightKeys,
        filterNulls);

    // We do not know how to add filter for non-INNER joins (see DRILL-1337)
    if (joinType != JoinRelType.INNER) {
      final RexNode newJoinCondition = RexUtil.composeConjunction(builder.getRexBuilder(), ImmutableList.of(partialCondition, remaining), false);
      if (RexUtil.eq(join.getCondition(), newJoinCondition)) {
        // Condition is the same, do not create a new rel
        return join;
      }
      builder.pushAll(ImmutableList.of(join.getLeft(), join.getRight()));
      builder.join(joinType, newJoinCondition);

      return builder.build();
    }

    // Check if join condition has changed if pure equi-join
    if (remaining.isAlwaysTrue() && RexUtil.eq(join.getCondition(), partialCondition)) {
      return join;
    }

    // Return the new join with a filter on top
    builder.pushAll(ImmutableList.of(join.getLeft(), join.getRight()));
    builder.join(joinType, partialCondition);
    builder.filter(remaining);
    return builder.build();
  }
}

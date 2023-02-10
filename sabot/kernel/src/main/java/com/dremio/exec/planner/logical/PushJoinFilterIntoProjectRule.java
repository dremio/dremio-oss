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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.dremio.exec.planner.common.MoreRelOptUtil;

/**
 * <pre>
 * Rule to push join filters that depend on only one side of join, past join into the project below, e.g if we have 10 fields below join, 5 on each side:
 * JoinRel(condition=[AND(=($1, $8), =($5, 10))]
 *    ... [left side: $0, $1, $2, $3, $4] ...
 *    ... [right side: $5, $6, $7, $8, $9] ...
 *
 * will be written as:
 *
 * JoinRel(condition=[AND(=($1, $9), =($5, $11))]
 *    ProjectRel($0, $1, $2, $3, $4, $5=[1])
 *       ...
 *    ProjectRel($0, $1, $2, $3, $4, $5=case when $0 = 10 then 1 else 0 end)
 *
 *    This rewrite allows the physical planning to use a HashJoin, since now all of the conditions are equi conditions
 *
 * </pre>
 */
public class PushJoinFilterIntoProjectRule extends RelOptRule {

  public static final RelOptRule INSTANCE = new PushJoinFilterIntoProjectRule(
    operand(JoinRel.class, any()), DremioRelFactories.LOGICAL_BUILDER);

  private final RelBuilderFactory factory;

  private PushJoinFilterIntoProjectRule(RelOptRuleOperand operand, RelBuilderFactory factory) {
    super(operand, "PushJoinFilterIntoProjectRule");
    this.factory = factory;
  }

  private static List<RexNode> shift(List<RexNode> conditions, int s, RelNode child) {
    return conditions.stream().map(c -> shift(c, s, child)).collect(Collectors.toList());
  }

  private static RexNode shift(RexNode rexNode, int s, RelNode child) {
    return rexNode.accept(new RexShiftShuttle(s, child.getRowType()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());

    if (joinInfo.isEqui()) {
      return;
    }

    // If there are no true equi-join conditions, there is no point in using this rule, as we still can't use HJ
    if (joinInfo.keys().size() == 0) {
      return;
    }

    RexNode remaining = joinInfo.getRemaining(join.getCluster().getRexBuilder());
    if (remaining.isAlwaysTrue()) {
      return;
    }

    List<RexNode> remainingConditions = RelOptUtil.conjunctions(remaining);
    List<RexNode> leftConditions = new ArrayList<>();
    List<RexNode> rightConditions = new ArrayList<>();


    for (RexNode rexNode : remainingConditions) {
      SingleSidedConditionFinder finder = new SingleSidedConditionFinder(join.getLeft().getRowType().getFieldCount());
      rexNode.accept(finder);
      // if any conditions have both left and right components, we can't convert to equi-join, so no point in continuing
      if (finder.hasRight && finder.hasLeft) {
        return;
      }
      if (finder.hasRight) {
        rightConditions.add(rexNode);
      } else {
        leftConditions.add(rexNode);
      }
    }

    RelBuilder relBuilder = factory.create(join.getCluster(), null);

    RelNode newJoin = createJoin(join, relBuilder, join.getLeft(), join.getRight(), joinInfo.leftKeys, joinInfo.rightKeys, leftConditions, rightConditions);
    call.transformTo(newJoin);
  }

  private RelNode createJoin(Join origJoin, RelBuilder relBuilder, RelNode left, RelNode right, List<Integer> leftKeys, List<Integer> rightKeys, List<RexNode> leftCond, List<RexNode> rightCond) {
    final RexBuilder rexBuilder = left.getCluster().getRexBuilder();
    final RexNode one = rexBuilder.makeBigintLiteral(BigDecimal.ONE);

    final List<RexNode> leftProjects = new ArrayList<>(MoreRelOptUtil.identityProjects(left.getRowType()));
    final List<RexNode> rightProjects = new ArrayList<>(MoreRelOptUtil.identityProjects(right.getRowType()));

    final List<Integer> newLeftKeys = new ArrayList<>(leftKeys);
    final List<Integer> newRightKeys = new ArrayList<>(rightKeys);

    int cnt = 0;

    if (!leftCond.isEmpty()) {
      newLeftKeys.add(leftProjects.size());
      newRightKeys.add(rightProjects.size());
      leftProjects.add(makeCase(rexBuilder, shift(leftCond, 0, left)));
      rightProjects.add(one);
      cnt++;
    }

    if (!rightCond.isEmpty()) {
      newLeftKeys.add(leftProjects.size());
      newRightKeys.add(rightProjects.size());
      rightProjects.add(makeCase(rexBuilder, shift(rightCond, -1 * left.getRowType().getFieldCount(), right)));
      leftProjects.add(one);
      cnt++;
    }

    RelNode newLeft = relBuilder.push(left).project(leftProjects).build();
    RelNode newRight = relBuilder.push(right).project(rightProjects).build();

    RexNode newEquiCondition = RelOptUtil.createEquiJoinCondition(newLeft, newLeftKeys, newRight, newRightKeys, left.getCluster().getRexBuilder());

    int finalCnt = cnt;
    List<RexNode> topProjects = MoreRelOptUtil.identityProjects(origJoin.getRowType())
      .stream().map(p -> RexUtil.shift(p, left.getRowType().getFieldCount(), finalCnt)).collect(Collectors.toList());

    return relBuilder
      .push(newLeft)
      .push(newRight)
      .join(origJoin.getJoinType(), newEquiCondition)
      .project(topProjects)
      .build();
  }

  private RexNode makeCase(RexBuilder builder, Iterable<RexNode> conditions) {
    return builder.makeCall(
      SqlStdOperatorTable.CASE,
      RexUtil.composeConjunction(builder, conditions, false),
      builder.makeBigintLiteral(BigDecimal.ONE), builder.makeBigintLiteral(BigDecimal.ZERO));
  }

  private static class RexShiftShuttle extends RexShuttle {
    private final int offset;
    private final RelDataType rowType;

    RexShiftShuttle(int offset, RelDataType rowType) {
      this.offset = offset;
      this.rowType = rowType;
    }

    @Override
    public RexNode visitInputRef(RexInputRef input) {
      int idx = input.getIndex() + offset;
      return new RexInputRef(idx, rowType.getFieldList().get(idx).getType());
    }
  }

  private static class SingleSidedConditionFinder extends RexVisitorImpl<Void> {
    private final int leftFieldCount;

    boolean hasLeft;
    boolean hasRight;

    SingleSidedConditionFinder(int leftFieldCount) {
      super(true);
      this.leftFieldCount = leftFieldCount;
    }

    @Override
    public Void visitInputRef(RexInputRef ref) {
      if (ref.getIndex() < leftFieldCount) {
        hasLeft = true;
      } else {
        hasRight = true;
      }
      return null;
    }
  }
}

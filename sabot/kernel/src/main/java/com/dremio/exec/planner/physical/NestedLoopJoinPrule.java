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
package com.dremio.exec.planner.physical;

import java.util.function.Consumer;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.work.foreman.UnsupportedRelOperatorException;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.JoinUtils.JoinCategory;
import com.google.common.collect.ImmutableList;


public class NestedLoopJoinPrule extends JoinPruleBase {
  public static final RelOptRule INSTANCE = new NestedLoopJoinPrule("Prel.NestedLoopJoinPrule", RelOptHelper.any(JoinRel.class));

  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private NestedLoopJoinPrule(String name, RelOptRuleOperand operand) {
    super(operand, name);
  }

  @Override
  protected boolean checkPreconditions(JoinRel join, RelNode left, RelNode right, PlannerSettings settings) {
    JoinRelType type = join.getJoinType();
    boolean vectorized = settings.getOptions().getOption(NestedLoopJoinPrel.VECTORIZED);
    switch(type) {
    case INNER:
      break;
    case LEFT:
    case RIGHT:
      if(!vectorized) {
        return false;
      }
      break;
    default:
      return false;
    }

    JoinCategory category = join.getJoinCategory();
    if (category == JoinCategory.EQUALITY && (settings.isHashJoinEnabled() || settings.isMergeJoinEnabled())) {
      return false;
    }

    if (settings.isNlJoinForScalarOnly()) {
      return JoinUtils.isScalarSubquery(left) || JoinUtils.isScalarSubquery(right);
    }

    return true;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return PrelUtil.getPlannerSettings(call.getPlanner()).isNestedLoopJoinEnabled();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    if (!settings.isNestedLoopJoinEnabled()) {
      return;
    }

    final JoinRel initialJoin = call.rel(0);
    if (!checkPreconditions(initialJoin, initialJoin.getLeft(), initialJoin.getRight(), settings)) {
      return;
    }

    JoinRel join = initialJoin;
    Consumer<RelNode> transform = a -> call.transformTo(a);

    // swap right joins since that is the only way to complete them.
    if(join.getJoinType() == JoinRelType.RIGHT) {
      RexNode swappedCondition = JoinUtils.getSwappedCondition(join);
      JoinRel swappedJoin = JoinRel.create(join.getCluster(), join.getTraitSet(), join.getRight(), join.getLeft(), swappedCondition, JoinRelType.LEFT,
        JoinUtils.projectSwap(join.getProjectedFields(), join.getLeft().getRowType().getFieldCount(),
          join.getRight().getRowType().getFieldCount()+join.getLeft().getRowType().getFieldCount()));
      RelNode projectMaybe = DremioRelFactories.LOGICAL_BUILDER.create(join.getCluster(), null).push(swappedJoin)
        .project(JoinUtils.createSwappedJoinExprsProjected(swappedJoin, join), ImmutableList.<String>of(), true).build();

      ProjectRel project = (ProjectRel) projectMaybe;

      if(!(project.getInput() instanceof JoinRel)) {
        tracer.debug("Post swap we don't have a ProjectRel on top of a JoinRel. Was actually a {}.", project.getInput().getClass().getName());
        return;
      }

      transform = input -> {
        RelTraitSet newTraits = input.getTraitSet().plus(Prel.PHYSICAL).plus(RelCollations.EMPTY); // we use an empty collation here since nlj doesn't maitain ordering.
        call.transformTo(new ProjectPrel(input.getCluster(), newTraits, input, project.getChildExps(), project.getRowType()));
      };
      join = (JoinRel) project.getInput(0);
    }

    final RelNode left = join.getLeft();
    final RelNode right = join.getRight();
    final RexNode joinCondition = join.getCondition();

    JoinRelType joinType = join.getJoinType();
    RelNode convertedLeft = convert(left, Prel.PHYSICAL);

    // see DX-17835 to understand why we do this kind of chaining (instead of building the traitset all at once)
    RelNode convertedRight = convert(right, Prel.PHYSICAL, DistributionTrait.BROADCAST);


    final NestedLoopJoinPrel newJoin = NestedLoopJoinPrel.create(join.getCluster(), convertedLeft.getTraitSet().plus(RelCollations.EMPTY), convertedLeft, convertedRight, joinType, joinCondition, join.getProjectedFields());
    final boolean vectorized = PrelUtil.getPlannerSettings(call.getPlanner()).getOptions().getOption(NestedLoopJoinPrel.VECTORIZED);

    if (joinCondition.isAlwaysTrue()) {
      transform.accept(newJoin);
      return;
    }

    if (!vectorized) {
      switch(joinType) {
      case INNER:
        // generate a join with a filter on top.
        transform.accept(new FilterPrel(join.getCluster(), convertedLeft.getTraitSet(), newJoin, joinCondition));
        return;
      default:
        // not supported.
        return;
      }
    }

    switch (joinType) {
    case INNER:
    case LEFT:
      transform.accept(newJoin);
      break;
    case RIGHT:
      break;
    default:
      break;

    }
  }

  @Override
  protected void createBroadcastPlan(
      final RelOptRuleCall call,
      final JoinRel join,
      final RexNode joinCondition,
      final RelNode incomingLeft,
      final RelNode incomingRight,
      final RelCollation collationLeft,
      final RelCollation collationRight) throws InvalidRelException {
    throw new UnsupportedOperationException("Not used.");
  }

  @Override
  protected void createDistBothPlan(RelOptRuleCall call, JoinRel join,
                                    RelNode left, RelNode right,
                                    RelCollation collationLeft, RelCollation collationRight,
                                    DistributionTrait hashLeftPartition, DistributionTrait hashRightPartition) throws UnsupportedRelOperatorException {
    throw new UnsupportedOperationException("Not used.");
  }


  /**
   * Convert to a set of new traits based on overriding the existing traits of the provided node
   *
   * TODO: evaluate if we should move this to Prule for other uses.
   *
   * See DX-17835 for further details of why this was introduced.
   *
   * @param rel The rel node to start with (and use as a basis for traits)
   * @param traits The ordered list of traits to convert to.
   * @return The converted node that incldues application of all the traits.
   */
  private static RelNode convert(RelNode rel, RelTrait... traits) {
    for (RelTrait t : traits) {
      rel = convert(rel, rel.getTraitSet().plus(t));
    }
    return rel;
  }
}

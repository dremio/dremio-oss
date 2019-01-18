/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.dremio.service.Pointer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Rule that converts a {@link org.apache.calcite.rel.logical.LogicalJoin} to a {@link JoinRel}, which is implemented by Dremio "join" operation.
 * Rename this to JoinRule once we are comfortable that fix for DX-11205 is ok
 */
public abstract class JoinRule extends RelOptRule {
  public static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinRule.class);

  public static final RelOptRule TO_CREL = new JoinRuleCrel();
  public static final RelOptRule TO_DREL = new JoinRuleDrel();
  public static final RelOptRule FROM_DREL = new JoinRuleFromDrel();

  private final RelBuilderFactory factory;

  private JoinRule(RelBuilderFactory factory) {
    super(RelOptHelper.any(LogicalJoin.class, Convention.NONE), "JoinRule");
    this.factory = factory;
  }

  private JoinRule(RelOptRuleOperand operand, RelBuilderFactory factory) {
    super(operand, "JoinRule");
    this.factory = factory;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final RelNode left = join.getLeft();
    final RelNode right = join.getRight();
    final RelNode convertedLeft = convertIfNecessary(left);
    final RelNode convertedRight = convertIfNecessary(right);
    org.apache.calcite.tools.RelBuilder builder = factory.create(join.getCluster(), null);
    RelNode newJoin = canonicalizeJoinCondition(builder, join.getJoinType(), join.getCondition(), convertedLeft, convertedRight);
    if(newJoin != null) {
      call.transformTo(newJoin);
    }
  }

  /**
   * Converts the tree to the proper convention if required by the rule implementation
   * @param node the node to convert
   * @return the converted node or possibly the original if no conversion necessary
   */
  protected abstract RelNode convertIfNecessary(RelNode node);

  /**
   * Attempt to create the new join condition from the equi-join conditions and the remaining conditions
   * @param builder
   * @param leftKeys
   * @param rightKeys
   * @param condition
   * @param partialCondition
   * @param remaining
   * @param joinType
   * @param newJoinCondition Pointer that will hold the resulting newJoinCondition RexNode
   * @return true if the remaining condition needs to be handled in a separate filter rel
   */
  protected abstract boolean getNewJoinCondition(org.apache.calcite.tools.RelBuilder builder,
                                                 List<Integer> leftKeys,
                                                 List<Integer> rightKeys,
                                                 RexNode condition,
                                                 RexNode partialCondition,
                                                 RexNode remaining,
                                                 JoinRelType joinType,
                                                 Pointer<RexNode> newJoinCondition);

  private static class JoinRuleDrel extends JoinRule {

    private JoinRuleDrel() {
      super(DremioRelFactories.LOGICAL_BUILDER);
    }

    @Override
    protected RelNode convertIfNecessary(RelNode node) {
      return convert(node, node.getTraitSet().plus(Rel.LOGICAL).simplify());
    }

    @Override
    protected boolean getNewJoinCondition(RelBuilder builder, List<Integer> leftKeys, List<Integer> rightKeys, RexNode condition, RexNode partialCondition, RexNode remaining, JoinRelType joinType, Pointer<RexNode> newJoinCondition) {
      newJoinCondition.value = RexUtil.composeConjunction(builder.getRexBuilder(), ImmutableList.of(partialCondition, remaining), false);
      return false;
    }
  }


  private static class JoinRuleCrel extends JoinRule {

    private JoinRuleCrel() {
      super(DremioRelFactories.CALCITE_LOGICAL_BUILDER);
    }

    @Override
    protected RelNode convertIfNecessary(RelNode node) {
      return node;
    }

    @Override
    protected boolean getNewJoinCondition(RelBuilder builder, List<Integer> leftKeys, List<Integer> rightKeys, RexNode condition, RexNode partialCondition, RexNode remaining, JoinRelType joinType, Pointer<RexNode> newJoinCondition) {
      // DRILL-1337: We can only pull up a non-equivjoin filter for INNER join.
      // For OUTER join, pulling up a non-eqivjoin filter will lead to incorrectly discarding qualified rows.
      boolean hasEquiJoins = leftKeys.size() == rightKeys.size() && leftKeys.size() > 0 ;
      boolean createFilterWithRemaining;
      if(!hasEquiJoins || joinType != JoinRelType.INNER) {
        newJoinCondition.value = RexUtil.composeConjunction(builder.getRexBuilder(), ImmutableList.of(partialCondition, remaining), false);
        createFilterWithRemaining = false;
      } else {
        newJoinCondition.value = partialCondition;
        createFilterWithRemaining = true;
      }
      if(RexUtil.eq(newJoinCondition.value, condition)) {
        newJoinCondition.value = null;
        return false;
      }
      return createFilterWithRemaining;
    }
  }

  private static class JoinRuleFromDrel extends JoinRule {

    private JoinRuleFromDrel() {
      super(RelOptHelper.any(JoinRel.class), DremioRelFactories.LOGICAL_BUILDER);
    }

    @Override
    protected RelNode convertIfNecessary(RelNode node) {
      return node;
    }

    @Override
    protected boolean getNewJoinCondition(RelBuilder builder, List<Integer> leftKeys, List<Integer> rightKeys, RexNode condition, RexNode partialCondition, RexNode remaining, JoinRelType joinType, Pointer<RexNode> newJoinCondition) {
      boolean hasEquiJoins = leftKeys.size() == rightKeys.size() && leftKeys.size() > 0 ;
      if(!hasEquiJoins || joinType != JoinRelType.INNER) {
        newJoinCondition.value = RexUtil.composeConjunction(builder.getRexBuilder(), ImmutableList.of(partialCondition, remaining), false);
        return false;
      }
      newJoinCondition.value = partialCondition;
      return true;
    }
  }

  /**
   * Canonicalize Join Condition, converting EQUALS/Null combination to IS NOT DISTINCT FROM form.
   *
   * @param builder
   * @param joinType
   * @param joinCondition
   * @param left
   * @param right
   * @return
   */
  private RelNode canonicalizeJoinCondition(
      org.apache.calcite.tools.RelBuilder builder,
      JoinRelType joinType,
      RexNode joinCondition,
      RelNode left,
      RelNode right) {
    final List<Integer> leftKeys = Lists.newArrayList();
    final List<Integer> rightKeys = Lists.newArrayList();
    final List<Boolean> filterNulls = Lists.newArrayList();

    try {
      RexNode remaining = RelOptUtil.splitJoinCondition(left, right, joinCondition, leftKeys, rightKeys, filterNulls);

      final RexNode partialCondition = buildJoinCondition(left.getRowType(), right.getRowType(), leftKeys, rightKeys, filterNulls, builder.getRexBuilder());
      final RexNode newJoinCondition;
      final boolean createFilterWithRemaining;

      if(remaining.isAlwaysTrue()) {
        newJoinCondition = partialCondition;
        createFilterWithRemaining = false;

      } else {
        Pointer<RexNode> newJoinConditionPointer = new Pointer<>();
        createFilterWithRemaining = getNewJoinCondition(builder, leftKeys, rightKeys, joinCondition, partialCondition, remaining, joinType, newJoinConditionPointer);
        newJoinCondition = newJoinConditionPointer.value;
      }

      // terminate if the same as previously
      if(newJoinCondition == null) {
        return null;
      }

      builder.pushAll(ImmutableList.of(left, right));
      builder.join(joinType, newJoinCondition);

      if (createFilterWithRemaining) {
        // If the join involves equijoins and non-equijoins, then we can process the non-equijoins through
        // a filter right after the join
        builder.filter(remaining);
      }

      return builder.build();
    } catch (RuntimeException e) {
      logger.debug("Failure while attempting to recalculate join condition.", e);
      return null;
    }
  }

  /**
   * Builds the euality join condition with only simple inputs
   * @param leftRowType
   * @param rightRowType
   * @param leftKeys
   * @param rightKeys
   * @param filterNulls
   * @param builder
   * @return the equi-join condition
   */
  private RexNode buildJoinCondition(RelDataType leftRowType, RelDataType rightRowType, List<Integer> leftKeys, List<Integer> rightKeys, List<Boolean> filterNulls, RexBuilder builder) {
    final List<RexNode> equijoinList = Lists.newArrayList();
    final int numLeftFields = leftRowType.getFieldCount();
    final List<RelDataTypeField> leftTypes = leftRowType.getFieldList();
    final List<RelDataTypeField> rightTypes = rightRowType.getFieldList();

    for (int i=0; i < leftKeys.size(); i++) {
      int leftKeyOrdinal = leftKeys.get(i);
      int rightKeyOrdinal = rightKeys.get(i);

      SqlBinaryOperator operator = filterNulls.get(i) ? SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
      RexNode leftInput = builder.makeInputRef(leftTypes.get(leftKeyOrdinal).getType(), leftKeyOrdinal);
      RexNode rightInput = builder.makeInputRef(rightTypes.get(rightKeyOrdinal).getType(), rightKeyOrdinal + numLeftFields);
      equijoinList.add(builder.makeCall(operator, leftInput, rightInput));
    }

    return RexUtil.composeConjunction(builder, equijoinList, false);
  }
}

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

import com.dremio.exec.planner.sql.ConsistentTypeUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Arrays;
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
import org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Rule that canonicalize a {@code LogicalJoin} filter condition by converting EQUALS/NULL
 * combination into IS NOT DISTINCT FROM.
 */
public class JoinFilterCanonicalizationRule extends RelOptRule {
  public static final RelOptRule INSTANCE =
      new JoinFilterCanonicalizationRule(
          RelOptHelper.any(LogicalJoin.class, Convention.NONE),
          DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  private final RelBuilderFactory factory;

  private JoinFilterCanonicalizationRule(RelOptRuleOperand operand, RelBuilderFactory factory) {
    super(operand, "JoinFilterCanonicalizationRule");
    this.factory = factory;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final RelNode left = join.getLeft();
    final RelNode right = join.getRight();
    RelBuilder builder = factory.create(join.getCluster(), null);
    RelNode newJoin =
        canonicalizeJoinCondition(builder, join.getJoinType(), join.getCondition(), left, right);
    if (newJoin != null) {
      call.transformTo(newJoin);
    }
  }

  /**
   * Create a join operator with a canonicalized version of {@code joinCondition}
   *
   * @param builder
   * @param joinType
   * @param joinCondition
   * @param left
   * @param right
   * @return the new join operator, or {@code null} if {@code joinCondition} hasn't changed.
   */
  private RelNode canonicalizeJoinCondition(
      RelBuilder builder,
      JoinRelType joinType,
      RexNode joinCondition,
      RelNode left,
      RelNode right) {
    final List<Integer> leftKeys = Lists.newArrayList();
    final List<Integer> rightKeys = Lists.newArrayList();
    final List<Boolean> filterNulls = Lists.newArrayList();

    final RexNode remaining =
        RelOptUtil.splitJoinCondition(left, right, joinCondition, leftKeys, rightKeys, filterNulls);

    // Create a normalized join condition
    final RexNode newPartialJoinCondition =
        buildJoinCondition(
            builder.getRexBuilder(),
            left.getRowType(),
            right.getRowType(),
            leftKeys,
            rightKeys,
            filterNulls);
    // Add the remaining filter condition
    final RexNode newJoinCondition =
        RexUtil.composeConjunction(
            builder.getRexBuilder(), Arrays.asList(newPartialJoinCondition, remaining));

    // terminate if the same condition as previously
    if (RexUtil.eq(joinCondition, newJoinCondition)) {
      return null;
    }

    builder.pushAll(ImmutableList.of(left, right));
    builder.join(joinType, newJoinCondition);

    return builder.build();
  }

  /**
   * Build a join condition based on the left/right keys
   *
   * @param leftRowType
   * @param rightRowType
   * @param leftKeys
   * @param rightKeys
   * @param filterNulls
   * @param builder
   * @return a conjunction of equi-join conditions
   */
  static RexNode buildJoinCondition(
      RexBuilder builder,
      RelDataType leftRowType,
      RelDataType rightRowType,
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      List<Boolean> filterNulls) {
    final List<RexNode> equijoinList = Lists.newArrayList();
    final int numLeftFields = leftRowType.getFieldCount();
    final List<RelDataTypeField> leftTypes = leftRowType.getFieldList();
    final List<RelDataTypeField> rightTypes = rightRowType.getFieldList();

    for (int i = 0; i < leftKeys.size(); i++) {
      int leftKeyOrdinal = leftKeys.get(i);
      int rightKeyOrdinal = rightKeys.get(i);

      SqlBinaryOperator operator =
          filterNulls.get(i)
              ? SqlStdOperatorTable.EQUALS
              : SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
      RexNode leftInput =
          builder.makeInputRef(leftTypes.get(leftKeyOrdinal).getType(), leftKeyOrdinal);
      RexNode rightInput =
          builder.makeInputRef(
              rightTypes.get(rightKeyOrdinal).getType(), rightKeyOrdinal + numLeftFields);

      List<RelDataType> types = ImmutableList.of(leftInput.getType(), rightInput.getType());
      if (ConsistentTypeUtil.allExactNumeric(types) && ConsistentTypeUtil.anyDecimal(types)) {
        equijoinList.add(builder.makeCall(operator, leftInput, rightInput));
      } else {
        RelDataType consistentType =
            ConsistentTypeUtil.consistentType(
                builder.getTypeFactory(), Consistency.LEAST_RESTRICTIVE, types);
        if (consistentType != null) {
          equijoinList.add(
              builder.makeCall(
                  operator,
                  builder.ensureType(consistentType, leftInput, true),
                  builder.ensureType(consistentType, rightInput, true)));
        } else {
          equijoinList.add(builder.makeCall(operator, leftInput, rightInput));
        }
      }
    }

    return RexUtil.composeConjunction(builder, equijoinList, false);
  }
}

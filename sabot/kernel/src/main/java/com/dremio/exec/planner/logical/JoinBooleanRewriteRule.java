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

import com.dremio.exec.planner.common.MoreRelOptUtil;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Triple;

public class JoinBooleanRewriteRule extends RelRule<JoinBooleanRewriteRule.Config> {
  public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

  /**
   * This rule rewrites boolean based equi-join condition to integer based condition with case
   * statement added below.
   *
   * @param config
   */
  protected JoinBooleanRewriteRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final JoinRel join = call.rel(0);
    final List<Triple<Integer, Integer, Boolean>> booleanConditions = new ArrayList<>();
    final List<Triple<Integer, Integer, Boolean>> nonBooleanConditions = new ArrayList<>();
    final RelDataType leftRowType = join.getLeft().getRowType();
    final RelDataType rightRowType = join.getRight().getRowType();
    final RexBuilder rb = join.getCluster().getRexBuilder();
    final List<Integer> leftKeys = new ArrayList<>();
    final List<Integer> rightKeys = new ArrayList<>();
    final List<Boolean> filterNulls = new ArrayList<>();

    // split join condition
    RexNode remaining =
        RelOptUtil.splitJoinCondition(
            join.getLeft(), join.getRight(), join.getCondition(), leftKeys, rightKeys, filterNulls);
    if (remaining.isAlwaysTrue()) {
      remaining = rb.makeLiteral(true);
    }

    // get boolean and non-boolean equi-conditions
    for (int i = 0; i < leftKeys.size(); i++) {
      final Integer leftKey = leftKeys.get(i);
      final Integer rightKey = rightKeys.get(i);
      final Boolean filterNull = filterNulls.get(i);
      if (isBoolean(leftRowType.getFieldList().get(leftKey).getType())
          || isBoolean(rightRowType.getFieldList().get(rightKey).getType())) {
        booleanConditions.add(Triple.of(leftKey, rightKey, filterNull));
      } else {
        nonBooleanConditions.add(Triple.of(leftKey, rightKey, filterNull));
      }
    }

    // no need to rewrite if boolean list is empty
    if (booleanConditions.isEmpty()) {
      return;
    }

    // add casted projects on left and right inputs
    final List<RexNode> leftProjects =
        new ArrayList<>(MoreRelOptUtil.identityProjects(join.getLeft().getRowType()));
    final List<RexNode> rightProjects =
        new ArrayList<>(MoreRelOptUtil.identityProjects(join.getRight().getRowType()));
    final RelDataType intType =
        join.getCluster()
            .getTypeFactory()
            .createTypeWithNullability(
                join.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
    booleanConditions.forEach(
        t -> {
          RexNode leftExpr = makeCase(join.getLeft().getRowType(), t.getLeft(), rb, intType);
          RexNode rightExpr = makeCase(join.getRight().getRowType(), t.getMiddle(), rb, intType);
          leftProjects.add(leftExpr);
          rightProjects.add(rightExpr);
        });

    RelBuilder relBuilder = call.builder();
    relBuilder.push(join.getLeft()).project(leftProjects);
    RelNode newLeft = relBuilder.peek();
    relBuilder.push(join.getRight()).project(rightProjects);
    RelNode newRight = relBuilder.peek();

    // merge non-boolean and rewritten boolean conditions
    final List<Triple<Integer, Integer, Boolean>> updatedEquiConditions = new ArrayList<>();
    nonBooleanConditions.forEach(
        t -> updatedEquiConditions.add(Triple.of(t.getLeft(), t.getMiddle(), t.getRight())));
    for (int i = 0; i < booleanConditions.size(); i++) {
      updatedEquiConditions.add(
          Triple.of(
              i + join.getLeft().getRowType().getFieldCount(),
              i + join.getRight().getRowType().getFieldCount(),
              booleanConditions.get(i).getRight()));
    }

    // construct a new join with rewritten join condition
    final RexNode equiJoinCondition =
        MoreRelOptUtil.createEquiJoinCondition(newLeft, newRight, updatedEquiConditions, rb);
    final RexNode shiftedRemaining =
        RexUtil.shift(
            remaining, join.getLeft().getRowType().getFieldCount(), booleanConditions.size());
    List<RexNode> conjunctions = new ArrayList<>();
    conjunctions.addAll(RelOptUtil.conjunctions(equiJoinCondition));
    conjunctions.addAll(RelOptUtil.conjunctions(shiftedRemaining));
    relBuilder.join(join.getJoinType(), RexUtil.composeConjunction(rb, conjunctions, false));

    // add project to preserve original RowType
    List<RexNode> topProjects =
        MoreRelOptUtil.identityProjects(join.getRowType()).stream()
            .map(
                p ->
                    RexUtil.shift(
                        p, join.getLeft().getRowType().getFieldCount(), booleanConditions.size()))
            .collect(Collectors.toList());

    relBuilder.project(topProjects);
    call.transformTo(relBuilder.build());
  }

  private static RexNode makeCase(RelDataType type, int field, RexBuilder rb, RelDataType outType) {
    return rb.makeCall(
        SqlStdOperatorTable.CASE,
        rb.makeCall(
            SqlStdOperatorTable.EQUALS,
            rb.makeInputRef(type.getFieldList().get(field).getType(), field),
            rb.makeLiteral(true)),
        rb.makeLiteral(BigDecimal.ONE, outType),
        rb.makeCall(
            SqlStdOperatorTable.EQUALS,
            rb.makeInputRef(type.getFieldList().get(field).getType(), field),
            rb.makeLiteral(false)),
        rb.makeLiteral(BigDecimal.ZERO, outType),
        rb.makeNullLiteral(outType));
  }

  private static boolean isBoolean(RelDataType type) {
    return SqlTypeName.BOOLEAN_TYPES.contains(type.getSqlTypeName());
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        EMPTY
            .withDescription("JoinBooleanRewriteRule")
            .withRelBuilderFactory(DremioRelFactories.LOGICAL_BUILDER)
            .withOperandSupplier(os1 -> os1.operand(JoinRel.class).anyInputs())
            .as(Config.class);

    @Override
    default JoinBooleanRewriteRule toRule() {
      return new JoinBooleanRewriteRule(this);
    }
  }
}

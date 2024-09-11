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
package com.dremio.exec.planner.normalizer;

import static com.google.common.collect.Iterables.concat;
import static org.apache.calcite.plan.RelOptRule.any;
import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.operandJ;
import static org.apache.calcite.plan.RelOptRule.some;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;

/**
 * This class is a copied and pasted from Calcite upstream. It's here until we rebase to upstream.
 * Once that happens we can just use the Calcite code and delete this.
 *
 * <p>DO NOT MODIFY THIS FILE
 */
public final class CalcitePruneEmptyRules {
  /**
   * Rule that converts a {@link Join} to empty if its left child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>Join(Empty, Scan(Dept), INNER) becomes Empty
   *   <li>Join(Empty, Scan(Dept), LEFT) becomes Empty
   *   <li>Join(Empty, Scan(Dept), SEMI) becomes Empty
   *   <li>Join(Empty, Scan(Dept), ANTI) becomes Empty
   * </ul>
   */
  public static final RelOptRule JOIN_LEFT_INSTANCE =
      new RelOptRule(
          operand(
              Join.class,
              some(
                  operandJ(Values.class, null, Values::isEmpty, none()),
                  operand(RelNode.class, any()))),
          "PruneEmptyJoin(left)") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          final Join join = call.rel(0);
          final RelNode right = call.rel(2);
          final RelBuilder relBuilder = call.builder();
          if (join.getJoinType().generatesNullsOnLeft()) {
            // If "emp" is empty, "select * from emp right join dept" will have
            // the same number of rows as "dept", and null values for the
            // columns from "emp". The left side of the join can be removed.
            call.transformTo(padWithNulls(relBuilder, right, join.getRowType(), true));
            return;
          }
          if (join.getJoinType() == JoinRelType.ANTI) {
            // In case of anti join: Join(X, Empty, ANTI) becomes X
            call.transformTo(join.getRight());
            return;
          }
          call.transformTo(relBuilder.push(join).empty().build());
        }
      };

  /**
   * Rule that converts a {@link Join} to empty if its right child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>Join(Scan(Emp), Empty, INNER) becomes Empty
   *   <li>Join(Scan(Emp), Empty, RIGHT) becomes Empty
   *   <li>Join(Scan(Emp), Empty, SEMI) becomes Empty
   *   <li>Join(Scan(Emp), Empty, ANTI) becomes Scan(Emp)
   * </ul>
   */
  public static final RelOptRule JOIN_RIGHT_INSTANCE =
      new RelOptRule(
          operand(
              Join.class,
              some(
                  operand(RelNode.class, any()),
                  operandJ(Values.class, null, Values::isEmpty, none()))),
          "PruneEmptyJoin(right)") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          final Join join = call.rel(0);
          final RelNode left = call.rel(1);
          final RelBuilder relBuilder = call.builder();
          if (join.getJoinType().generatesNullsOnRight()) {
            // If "dept" is empty, "select * from emp left join dept" will have
            // the same number of rows as "emp", and null values for the
            // columns from "dept". The right side of the join can be removed.
            call.transformTo(padWithNulls(relBuilder, left, join.getRowType(), false));
            return;
          }
          if (join.getJoinType() == JoinRelType.ANTI) {
            // In case of anti join: Join(X, Empty, ANTI) becomes X
            call.transformTo(join.getLeft());
            return;
          }
          call.transformTo(relBuilder.push(join).empty().build());
        }
      };

  /**
   * Rule that converts a {@link Correlate} to empty if its right child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>Correlate(Scan(Emp), Empty, INNER) becomes Empty
   *   <li>Correlate(Scan(Emp), Empty, SEMI) becomes Empty
   *   <li>Correlate(Scan(Emp), Empty, ANTI) becomes Scan(Emp)
   *   <li>Correlate(Scan(Emp), Empty, LEFT) becomes Project(Scan(Emp)) where the Project adds
   *       additional typed null columns to match the join type output.
   * </ul>
   */
  public static final RelOptRule CORRELATE_RIGHT_INSTANCE =
      new RelOptRule(
          operand(
              Correlate.class,
              some(
                  operand(RelNode.class, any()),
                  operandJ(Values.class, null, Values::isEmpty, none()))),
          "PruneEmptyCorrelate(right)") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          final Correlate corr = call.rel(0);
          final RelNode left = call.rel(1);
          RelBuilder b = call.builder();
          final RelNode newRel;
          switch (corr.getJoinType()) {
            case LEFT:
              newRel = padWithNulls(b, left, corr.getRowType(), false);
              break;
            case INNER:
            case SEMI:
              newRel = b.push(corr).empty().build();
              break;
            case ANTI:
              newRel = left;
              break;
            default:
              throw new IllegalStateException("Correlate does not support " + corr.getJoinType());
          }
          call.transformTo(newRel);
        }
      };

  private static RelNode padWithNulls(
      RelBuilder builder, RelNode input, RelDataType resultType, boolean leftPadding) {
    int padding = resultType.getFieldCount() - input.getRowType().getFieldCount();
    List<RexLiteral> nullLiterals = Collections.nCopies(padding, builder.literal(null));
    builder.push(input);
    if (leftPadding) {
      builder.project(concat(nullLiterals, builder.fields()));
    } else {
      builder.project(concat(builder.fields(), nullLiterals));
    }
    return builder.convert(resultType, true).build();
  }
}

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

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.google.common.collect.ImmutableList;

/**
 * Rewrite RexCall for known simple operator(<, >, <=, >=, =, !=) so inputs are in a fixed order.
 */
public final class SimpleExpressionCanonicalizer {

  private SimpleExpressionCanonicalizer() {}

  /**
   * Convert a RexNode to NNF
   * @param rexNode RexNode
   * @param rexBuilder RexBuilder
   * @return NNF
   */
  public static RexNode toNnf(RexNode rexNode, RexBuilder rexBuilder) {
    return rexNode.accept(new RexShuttle() {
      @Override
      public RexNode visitCall(RexCall call) {
        switch (call.getKind()) {
          case AND:
          case OR: {
            List<RexNode> childNodesNnf = MoreRelOptUtil.conDisjunctions(call)
              .stream()
              .map(node -> toNnf(node, rexBuilder))
              .collect(Collectors.toList());
            return MoreRelOptUtil.composeConDisjunction(rexBuilder, childNodesNnf, false,
              call.getKind());
          }

          case NOT: {
            RexNode nodeBelowNOT = call.getOperands().get(0);
            switch (nodeBelowNOT.getKind()) {
              case AND: {
                List<RexNode> childNodesNnf = RelOptUtil.conjunctions(nodeBelowNOT)
                  .stream()
                  .map(node -> toNnf(rexBuilder.makeCall(NOT, node), rexBuilder))
                  .collect(Collectors.toList());
                return RexUtil.composeDisjunction(rexBuilder, childNodesNnf, false);
              }
              case OR: {
                List<RexNode> childNodesNnf = RelOptUtil.disjunctions(nodeBelowNOT)
                  .stream()
                  .map(node -> toNnf(rexBuilder.makeCall(NOT, node), rexBuilder))
                  .collect(Collectors.toList());
                return RexUtil.composeConjunction(rexBuilder, childNodesNnf, false);
              }
              default:
                return super.visitCall(call);
            }
          }

          default:
            return super.visitCall(call);
        }
      }
    });
  }

  /**
   * Canonicalize leaf RexNodes by the inputBitSet of both sides, in the order that
   *  1) input refs are on the left and literals are on the right
   *  2) if both sides are input refs, then left are the one with smaller bitSets
   * @param rexNode RexNode to canonicalize
   * @param rexBuilder RexBuilder
   * @return canonicalized RexNode
   */
  public static RexNode canonicalizeExpression(RexNode rexNode, RexBuilder rexBuilder) {
    return rexNode.accept(new RexShuttle(){
      @Override
      public RexNode visitCall(RexCall call) {
      switch (call.getKind()) {
        case LESS_THAN:
        case GREATER_THAN:
        case LESS_THAN_OR_EQUAL:
        case GREATER_THAN_OR_EQUAL:
        case EQUALS:
        case NOT_EQUALS: {
          RexNode leftOp = call.getOperands().get(0);
          RexNode rightOp = call.getOperands().get(1);
          ImmutableBitSet leftBitSet = RelOptUtil.InputFinder.analyze(leftOp).build();
          ImmutableBitSet rightBitSet = RelOptUtil.InputFinder.analyze(rightOp).build();

          // Make input refs left and literals right
          if (leftBitSet.isEmpty() && !rightBitSet.isEmpty()) {
            return rexBuilder.makeCall(mirrorOperation(call.getKind()),
              ImmutableList.of(rightOp, leftOp));
          }

          // Both sides are input refs, make them a fixed or der
          if (!leftBitSet.isEmpty() && !rightBitSet.isEmpty()){
            if (leftBitSet.compareTo(rightBitSet) > 0) {
              return rexBuilder.makeCall(mirrorOperation(call.getKind()),
                ImmutableList.of(rightOp, leftOp));
            } else {
              return call;
            }
          }

          return call;
        }
        default:
          return super.visitCall(call);
      }
      }
    });
  }

  /**
   * Rewrite b > a to a < b
   * @param sqlKind kind of op
   * @return mirrored op
   */
  private static SqlOperator mirrorOperation(SqlKind sqlKind) {
    switch (sqlKind) {
      case LESS_THAN:
        return SqlStdOperatorTable.GREATER_THAN;
      case GREATER_THAN:
        return SqlStdOperatorTable.LESS_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
      case GREATER_THAN_OR_EQUAL:
        return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      case EQUALS:
        return SqlStdOperatorTable.EQUALS;
      case NOT_EQUALS:
        return SqlStdOperatorTable.NOT_EQUALS;
      default:
        throw new UnsupportedOperationException();
    }
  }
}

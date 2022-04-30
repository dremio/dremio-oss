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
package com.dremio.exec.planner.physical.rule.computation;

import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.google.common.collect.ImmutableList;

public class JoinComputationExtractor {
  public static ExtractedComputation extractedComputation(
    RelDataType joinRowType,
    RexNode joinCondition,
    RexNode extraCondition,
    RelNode leftNode,
    RelNode rightNode
  ) {
    RelOptCluster cluster = leftNode.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

    RelDataType leftType = leftNode.getRowType();
    int leftCount = leftType.getFieldCount();
    RelDataType rightType = rightNode.getRowType();
    int rightCount = rightType.getFieldCount();

    JoinSideDetector joinSideDetector = new JoinSideDetector(leftNode.getRowType().getFieldCount(),
      rightNode.getRowType().getFieldCount());
    ExpressionIdentifier.LeftRightAndComputationIndex expressions =
      ExpressionIdentifier.identifyNodesToPush(joinSideDetector, extraCondition);
    RexNode newJoinCondition = rewriteCondition(rexBuilder, joinCondition,
        expressions.rexNodeToComputationIndex, leftCount, expressions.leftComputations.size());
    RexNode newExtraCondition = rewriteCondition(rexBuilder, extraCondition,
      expressions.rexNodeToComputationIndex, leftCount, expressions.leftComputations.size());

    if (expressions.leftComputations.isEmpty() && expressions.rightComputations.isEmpty()) {
      return null;
    }

    List<RexNode> leftExprs = ImmutableList.<RexNode>builder()
      .addAll(MoreRelOptUtil.identityProjects(leftType))
      .addAll(expressions.leftComputations)
      .build();
    RelDataType leftProjectType = RexUtil.createStructType(typeFactory, leftExprs,
      null, SqlValidatorUtil.F_SUGGESTER);
    RelNode newLeft = expressions.leftComputations.isEmpty()
      ? leftNode
      : ProjectPrel.create(cluster, leftNode.getTraitSet(), leftNode, leftExprs, leftProjectType);


    List<RexNode> rightExprs = ImmutableList.<RexNode>builder()
      .addAll(MoreRelOptUtil.identityProjects(rightType))
      .addAll(expressions.rightComputations)
      .build();

    RelDataType rightProjectType = RexUtil.createStructType(typeFactory, rightExprs,
      null, SqlValidatorUtil.F_SUGGESTER);

    RelNode newRight = rightCount == rightExprs.size()
      ? rightNode
      : ProjectPrel.create(cluster, rightNode.getTraitSet(), rightNode, rightExprs, rightProjectType);

    List<RexNode> topProject = ImmutableList.<RexNode>builder()
      .addAll(MoreRelOptUtil.identityProjects(joinRowType, ImmutableBitSet.range(leftCount)))
      .addAll(identityProjectsWithShifting(joinRowType,
        ImmutableBitSet.range(leftCount, leftCount + rightCount),
        expressions.leftComputations.size()))
      .build();

    return new ExtractedComputation(newJoinCondition, newExtraCondition, newLeft, newRight, topProject);
  }

  private static RexNode rewriteCondition(RexBuilder rexBuilder, RexNode rexNode,
    Map<String, Integer> rexToComputationOffset, int leftOriginalCount, int shiftRightBy) {
    return rexNode.accept(new RexShuttle (){


      @Override public RexNode visitCall(RexCall call){
        if (rexToComputationOffset.containsKey(call.toString())) {
          return rexBuilder.makeInputRef(call.type, rexToComputationOffset.get(call.toString()));
        } else {
          return super.visitCall(call);
        }
      }

      @Override public RexNode visitInputRef(RexInputRef inputRef){
        if (inputRef.getIndex() < leftOriginalCount) {
          return inputRef;
        } else {
          return rexBuilder.makeInputRef(inputRef.getType(), inputRef.getIndex() + shiftRightBy);
        }
      }
    });
  }

  private static List<RexNode> identityProjectsWithShifting(RelDataType relDataType,
    ImmutableBitSet columns, int shift) {
    List<RexNode> projectedColumns = MoreRelOptUtil.identityProjects(relDataType, columns);
    return ImmutableList.<RexNode>builder()
      .addAll(RexUtil.shift(projectedColumns, shift))
      .build();
  }

  public static class ExtractedComputation {
    final RexNode joinCondition;
    final RexNode extraCondition;
    final RelNode left;
    final RelNode right;
    final List<RexNode> topProject;

    public ExtractedComputation(RexNode joinCondition,
                                RexNode extraCondition,
                                RelNode left, RelNode right,
                                List<RexNode> topProject) {
      this.joinCondition = joinCondition;
      this.extraCondition = extraCondition;
      this.left = left;
      this.right = right;
      this.topProject = topProject;
    }
  }
}

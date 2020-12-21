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

package com.dremio.sabot.op.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.planner.common.JoinRelBase;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.resolver.TypeCastRules;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.common.hashtable.Comparator;


public class JoinUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinUtils.class);

  public enum JoinCategory {
    EQUALITY,  // equality join
    INEQUALITY,  // inequality join: <>, <, >
    CARTESIAN   // no join condition
  }
  public static List<RexNode> createSwappedJoinExprsProjected(
    JoinRelBase newJoin,
    JoinRelBase origJoin) {
    final List<RelDataTypeField> newJoinFields =
      newJoin.getInputRowType().getFieldList();
    final RexBuilder rexBuilder = newJoin.getCluster().getRexBuilder();
    final List<RexNode> exps = new ArrayList<>();
    final int nFields = origJoin.getRight().getRowType().getFieldCount();

    List<Integer> projected;
    ImmutableBitSet projectedSet;
    if (newJoin.getProjectedFields() == null) {
      projectedSet = ImmutableBitSet.range(newJoin.getRowType().getFieldCount());
    }
    else {
      projectedSet = newJoin.getProjectedFields();
    }
    projected = projectedSet.asList();

    for (int i = 0; i < newJoinFields.size(); i++) {
      final int source = (i + nFields) % newJoinFields.size();
      if (projectedSet.get(source)) {
        RelDataTypeField field = newJoinFields.get(source);
        exps.add(rexBuilder.makeInputRef(field.getType(), projected.indexOf(source)));
      }
    }
    return exps;
  }

  public static RelDataType rowTypeFromProjected(RelNode left, RelNode right, RelDataType curRowType, ImmutableBitSet projected, RelDataTypeFactory typeFactory) {
    int leftSize = left.getRowType().getFieldCount();
    int rightSize = right.getRowType().getFieldCount();
    List<RelDataTypeField> fields = curRowType.getFieldList();
    if (projected.asSet().size() != fields.size()) {;
      List<RelDataType> dataTypes = new ArrayList<>();
      List<String> fieldNames = new ArrayList<>();
      for (int i = 0; i < fields.size(); i++) {
        if (projected.get(i)) {
          RelDataTypeField field = fields.get(i);
          dataTypes.add(field.getType());
          fieldNames.add(field.getName());
        }
      }
      return typeFactory.createStructType(dataTypes, fieldNames);
    }
    return curRowType;
  }

  // Given a Join RelNode, swap its left condition with right condition
  public static RexNode getSwappedCondition(RelNode relNode) {
    Join joinRel = (Join) relNode;
    int numFieldLeft = joinRel.getLeft().getRowType().getFieldCount();
    int numFieldRight = joinRel.getRight().getRowType().getFieldCount();

    int[] adjustments = new int[numFieldLeft + numFieldRight];
    Arrays.fill(adjustments, 0, numFieldLeft, numFieldRight);
    Arrays.fill(adjustments, numFieldLeft, numFieldLeft + numFieldRight, -numFieldLeft);

    return joinRel.getCondition().accept(
      new RelOptUtil.RexInputConverter(
        joinRel.getCluster().getRexBuilder(),
        joinRel.getCluster().getTypeFactory().createJoinType(joinRel.getLeft().getRowType(), joinRel.getRight().getRowType()).getFieldList(),
        joinRel.getCluster().getTypeFactory().createJoinType(joinRel.getRight().getRowType(), joinRel.getLeft().getRowType()).getFieldList(),
        adjustments
      )
    );
  }

  public static ImmutableBitSet projectSwap(ImmutableBitSet projectedFields, int numFieldLeft, int size) {
    if (null == projectedFields) {
      return null;
    }
    int[] adjustments = new int[size];
    Arrays.fill(adjustments, 0, numFieldLeft, size-numFieldLeft);
    Arrays.fill(adjustments, numFieldLeft, numFieldLeft + size-numFieldLeft, -numFieldLeft);
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (int i : projectedFields.asList()) {
      builder.set(adjustments[i]+i);
    }
    return builder.build();
  }

  public static ImmutableBitSet projectAll(int size) {
    return ImmutableBitSet.builder().set(0, size).build();
  }

  // Check the comparator is supported in join condition. Note that a similar check is also
  // done in JoinPrel; however we have to repeat it here because a physical plan
  // may be submitted directly to Dremio.
  public static Comparator checkAndReturnSupportedJoinComparator(JoinCondition condition) {
    switch(condition.getRelationship().toUpperCase()) {
      case "EQUALS":
      case "==": /* older json plans still have '==' */
        return Comparator.EQUALS;
      case "IS_NOT_DISTINCT_FROM":
        return Comparator.IS_NOT_DISTINCT_FROM;
    }

    throw UserException.unsupportedError()
        .message("Invalid comparator supplied to this join: ", condition.getRelationship())
        .build(logger);
  }

    /**
     * Check if the given RelNode contains any Cartesian join.
     * Return true if find one. Otherwise, return false.
     *
     * @param relNode   the RelNode to be inspected.
     * @param leftKeys  a list used for the left input into the join which has
     *                  equi-join keys. It can be empty or not (but not null),
     *                  this method will clear this list before using it.
     * @param rightKeys a list used for the right input into the join which has
     *                  equi-join keys. It can be empty or not (but not null),
     *                  this method will clear this list before using it.
     * @param filterNulls   The join key positions for which null values will not
     *                      match. null values only match for the "is not distinct
     *                      from" condition.
     * @return          Return true if the given relNode contains Cartesian join.
     *                  Otherwise, return false
     */
  public static boolean checkCartesianJoin(RelNode relNode, List<Integer> leftKeys, List<Integer> rightKeys, List<Boolean> filterNulls) {
    if (relNode instanceof Join) {
      leftKeys.clear();
      rightKeys.clear();

      Join joinRel = (Join) relNode;
      RelNode left = joinRel.getLeft();
      RelNode right = joinRel.getRight();

      RexNode remaining = RelOptUtil.splitJoinCondition(left, right, joinRel.getCondition(), leftKeys, rightKeys, filterNulls);
      if(joinRel.getJoinType() == JoinRelType.INNER) {
        if(leftKeys.isEmpty() || rightKeys.isEmpty()) {
          return true;
        }
      } else {
        if(!remaining.isAlwaysTrue() || leftKeys.isEmpty() || rightKeys.isEmpty()) {
          return true;
        }
      }
    }

    for (RelNode child : relNode.getInputs()) {
      if(checkCartesianJoin(child, leftKeys, rightKeys, filterNulls)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Checks if implicit cast is allowed between the two input types of the join condition. Currently we allow
   * implicit casts in join condition only between numeric types and varchar/varbinary types.
   * @param input1
   * @param input2
   * @return true if implicit cast is allowed false otherwise
   */
  private static boolean allowImplicitCast(MinorType input1, MinorType input2) {
    // allow implicit cast if both the input types are numeric
    if (TypeCastRules.isNumericType(input1) && TypeCastRules.isNumericType(input2)) {
      return true;
    }

    // allow implicit cast if input types are date/ timestamp
    if ((input1 == MinorType.DATE || input1 == MinorType.TIMESTAMP) &&
        (input2 == MinorType.DATE || input2 == MinorType.TIMESTAMP)) {
      return true;
    }

    // allow implicit cast if both the input types are varbinary/ varchar
    if ((input1 == MinorType.VARCHAR || input1 == MinorType.VARBINARY) &&
        (input2 == MinorType.VARCHAR || input2 == MinorType.VARBINARY)) {
      return true;
    }

    return false;
  }

  /**
   * Utility method used by joins to add implicit casts on one of the sides of the join condition in case the two
   * expressions have different types.
   * @param leftExpressions array of expressions from left input into the join
   * @param leftBatch left input record batch
   * @param rightExpressions array of expressions from right input into the join
   * @param rightBatch right input record batch
   * @param producer class producer
   * @param optionManager option manager to any specific options
   */
  public static void addLeastRestrictiveCasts(LogicalExpression[] leftExpressions, VectorAccessible leftBatch,
                                              LogicalExpression[] rightExpressions, VectorAccessible rightBatch,
                                              ClassProducer producer, OptionManager optionManager) {
    assert rightExpressions.length == leftExpressions.length;

    for (int i = 0; i < rightExpressions.length; i++) {
      LogicalExpression rightExpression = rightExpressions[i];
      LogicalExpression leftExpression = leftExpressions[i];
      MinorType rightType = rightExpression.getCompleteType().toMinorType();
      MinorType leftType = leftExpression.getCompleteType().toMinorType();

      if (rightType == MinorType.UNION || leftType == MinorType.UNION) {
        continue;
      }
      if (rightType != leftType) {

        if (!allowImplicitCast(rightType, leftType)) {
          throw new UnsupportedOperationException(String.format("Join only supports implicit casts between " +
              "1. Numeric data\n 2. Varchar, Varbinary data 3. Date, Timestamp data " +
              "Left type: %s, Right type: %s. Add explicit casts to avoid this error", leftType, rightType));
        }

        // We need to add a cast to one of the expressions
        List<MinorType> types = new LinkedList<>();
        types.add(rightType);
        types.add(leftType);
        MinorType result = TypeCastRules.getLeastRestrictiveType(types);

        if (result == null) {
          throw new RuntimeException(String.format("Join conditions cannot be compared failing left " +
                  "expression:" + " %s failing right expression: %s", leftExpression.getCompleteType().toString(),
              rightExpression.getCompleteType().toString()));
        } else if (result != rightType && result != leftType) {
          // cast both to common type.
          CompleteType resultType = CompleteType.fromMinorType(result);
          LogicalExpression castExpr = producer.addImplicitCast(rightExpression, resultType);
          rightExpressions[i] = producer.materialize(castExpr, rightBatch);
          LogicalExpression castExprLeft = producer.addImplicitCast(leftExpression, resultType);
          leftExpressions[i] = producer.materialize(castExprLeft, leftBatch);
        } else if (result != rightType) {
          // Add a cast expression on top of the right expression
          LogicalExpression castExpr = producer.addImplicitCast(rightExpression, leftExpression.getCompleteType());
          // Store the newly casted expression
          rightExpressions[i] = producer.materialize(castExpr, rightBatch);
        } else if (result != leftType) {
          // Add a cast expression on top of the left expression
          LogicalExpression castExpr = producer.addImplicitCast(leftExpression, rightExpression.getCompleteType());
          // store the newly casted expression
          leftExpressions[i] = producer.materialize(castExpr, leftBatch);
        }
      }
    }
  }

  /**
   * Utility method to check if a subquery (represented by its root RelNode) is provably scalar. Currently
   * only aggregates with no group-by are considered scalar. In the future, this method should be generalized
   * to include more cases and reconciled with Calcite's notion of scalar.
   * @param root The root RelNode to be examined
   * @return True if the root rel or its descendant is scalar, False otherwise
   */
  public static boolean isScalarSubquery(RelNode root) {
    AggregateRel agg = null;
    RelNode currentrel = root;
    while (agg == null && currentrel != null) {
      if (currentrel instanceof AggregateRel) {
        agg = (AggregateRel)currentrel;
      } else if (currentrel instanceof RelSubset) {
        currentrel = ((RelSubset)currentrel).getBest() ;
      } else if (currentrel.getInputs().size() == 1) {
        // If the rel is not an aggregate or RelSubset, but is a single-input rel (could be Project,
        // Filter, Sort etc.), check its input
        currentrel = currentrel.getInput(0);
      } else {
        break;
      }
    }

    if (agg != null) {
      if (agg.getGroupSet().isEmpty()) {
        return true;
      }
    }
    return false;
  }

}

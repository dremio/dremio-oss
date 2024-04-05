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
package com.dremio.exec.planner.logical.rewrite.sargable;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.commons.lang3.function.TriFunction;

/**
 *
 *
 * <pre>
 * A shift transformer that transforms a logical expression to an equal expression by shifting the change in
 * the opposite direction to the right hand side.
 * Examples:
 * DATEDIFF(col1, '2021-01-15') = 5                          => col1 = '2021-01-20'
 * DATE_ADD(col1, 5) = '2021-01-15'                          => col1 = '2021-01-10'
 * DATE_SUB(col1, 5) = '2021-01-15'                          => col1 = '2021-01-20'
 * DATE_DIFF(col1, 5) = '2021-01-15'                         => col1 = '2021-01-20'
 * DATE_DIFF(col1, CAST(1 AS INTERVAL MONTH)) = '2021-01-15' => col1 = '2021-02-15'
 * DATE_DIFF(col1, '2021-01-15') = 5                         => col1 = '2021-01-20'
 * DATE_DIFF(DATE '2021-01-15', col1) = 5                    => col1 = '2021-01-10'
 * </pre>
 */
public class ShiftTransformer implements Transformer {

  protected final RelOptCluster relOptCluster;
  protected final RexBuilder rexBuilder;

  private final StandardForm standardForm;
  private final SqlOperator transformedOp;
  private final boolean swapped;

  public ShiftTransformer(
      RelOptCluster relOptCluster, StandardForm standardForm, SqlOperator transformedOp) {
    this.relOptCluster = relOptCluster;
    this.rexBuilder = relOptCluster.getRexBuilder();
    this.standardForm = standardForm;
    this.transformedOp = transformedOp;
    this.swapped = standardForm.isSwapped();
  }

  @Override
  public RexNode transform() {
    switch (standardForm.getTransformingOp().kind) {
      case EQUALS:
        return transformEquals();
      case GREATER_THAN:
        return transformGreaterThan();
      case GREATER_THAN_OR_EQUAL:
        return transformGreaterThanOrEqual();
      case LESS_THAN:
        return transformLessThan();
      case LESS_THAN_OR_EQUAL:
        return transformLessThanOrEqual();
      case NOT_EQUALS:
        return transformNotEquals();
      default:
        return null;
    }
  }

  public RexNode transformEquals() {
    return SARGableRexUtils.eq(getColumn(), shiftExpr(), rexBuilder);
  }

  public RexNode transformNotEquals() {
    return SARGableRexUtils.neq(getColumn(), shiftExpr(), rexBuilder);
  }

  public RexNode transformGreaterThan() {
    if (isSwapped()) {
      return SARGableRexUtils.lt(getColumn(), shiftExpr(), rexBuilder);
    } else {
      return SARGableRexUtils.gt(getColumn(), shiftExpr(), rexBuilder);
    }
  }

  public RexNode transformGreaterThanOrEqual() {
    if (isSwapped()) {
      return SARGableRexUtils.lte(getColumn(), shiftExpr(), rexBuilder);
    } else {
      return SARGableRexUtils.gte(getColumn(), shiftExpr(), rexBuilder);
    }
  }

  public RexNode transformLessThan() {
    if (isSwapped()) {
      return SARGableRexUtils.gt(getColumn(), shiftExpr(), rexBuilder);
    } else {
      return SARGableRexUtils.lt(getColumn(), shiftExpr(), rexBuilder);
    }
  }

  public RexNode transformLessThanOrEqual() {
    if (isSwapped()) {
      return SARGableRexUtils.gte(getColumn(), shiftExpr(), rexBuilder);
    } else {
      return SARGableRexUtils.lte(getColumn(), shiftExpr(), rexBuilder);
    }
  }

  private RexNode shiftExpr() {
    RexNode right;
    switch (getRhsParamCount()) {
      case ARG1:
        if (getReturnType() != null) {
          right =
              rexBuilder.makeCall(
                  getReturnType(), getTransformedOp(), ImmutableList.of(getRhsNode()));
        } else {
          right = rexBuilder.makeCall(getTransformedOp(), ImmutableList.of(getRhsNode()));
        }
        break;
      case ARG2:
        if (getReturnType() != null) {
          right =
              rexBuilder.makeCall(
                  getReturnType(),
                  getTransformedOp(),
                  ImmutableList.of(getRhsNode(), getRhsParam()));
        } else {
          right =
              rexBuilder.makeCall(
                  getTransformedOp(), ImmutableList.of(getRhsNode(), getRhsParam()));
        }
        break;
      case ARG3:
        if (getReturnType() != null) {
          right =
              rexBuilder.makeCall(
                  getReturnType(),
                  getTransformedOp(),
                  getTransformationFunction().apply(getRhsNode(), getRhsParam(), getRhsParam2()));
        } else {
          right =
              rexBuilder.makeCall(
                  getTransformedOp(),
                  getTransformationFunction().apply(getRhsNode(), getRhsParam(), getRhsParam2()));
        }
        break;
      default:
        right = null;
    }

    RexExecutor rexExecutor = relOptCluster.getPlanner().getExecutor();
    if (rexExecutor != null) {
      final List<RexNode> reducedValues = new ArrayList<>();
      final List<RexNode> constExpNode = new ArrayList<>();
      constExpNode.add(right);
      rexExecutor.reduce(rexBuilder, constExpNode, reducedValues);
      constExpNode.clear();
      constExpNode.addAll(reducedValues);
      reducedValues.clear();
      // DATE_SUB('2023-02-01':VARCHAR(10), 1:INTERVAL MONTH)
      // => CAST(2023-01-01 00:00:00:TIMESTAMP(3)):VARCHAR(10) NOT NULL
      // => '2023-01-01':VARCHAR(10)
      rexExecutor.reduce(rexBuilder, constExpNode, reducedValues);
      if (reducedValues.get(0) != null) {
        return reducedValues.get(0);
      }
    }
    return right;
  }

  RexNode getColumn() {
    return getLhsCall().operands.get(0);
  }

  RexCall getLhsCall() {
    return standardForm.getLhsCall();
  }

  RexNode getLhsParam() {
    return getLhsCall().operands.get(1);
  }

  RexNode getRhsNode() {
    return standardForm.getRhsNode();
  }

  RelDataType getReturnType() {
    return null;
  }

  SqlOperator getTransformedOp() {
    return transformedOp;
  }

  RexNode getRhsParam() {
    return getLhsCall().operands.get(1);
  }

  RexNode getRhsParam2() {
    return null;
  }

  Args getRhsParamCount() {
    return getRhsParam() == null ? Args.ARG1 : getRhsParam2() == null ? Args.ARG2 : Args.ARG3;
  }

  boolean isSwapped() {
    return swapped;
  }

  TriFunction<RexNode, RexNode, RexNode, ImmutableList<RexNode>> getTransformationFunction() {
    return null;
  }
}

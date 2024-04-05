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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

/**
 *
 *
 * <pre>
 * A range transformer that transform a logical expression to an equal expression involving a range.
 * Examples:
 * // Truncate to begin:
 * DATE_TRUNC('month',col1) = DATE '2014-02-01' => col1 >= DATE '2014-02-01' and col1 < DATE '2014-03-01'
 * YEAR(col1) = 2023                            => col1 >= '2023-01-01 00:00:00' and col1 < '2024-01-01 00:00:00'
 * EXTRACT(YEAR FROM col1) = 2023               => col1 >= '2023-01-01 00:00:00' and col1 < '2024-01-01 00:00:00'
 * DATE_PART('year', col1) = 2023               => col1 >= '2023-01-01 00:00:00' and col1 < '2024-01-01 00:00:00'
 * // Truncate to end:
 * LAST_DAY(col1) = DATE '2023-02-28'           => col1 >= DATE '2014-02-01' and col1 < DATE '2014-03-01'
 * </pre>
 */
public class RangeTransformer implements Transformer {

  protected final RelOptCluster relOptCluster;
  protected final RexBuilder rexBuilder;

  private final StandardForm standardForm;
  private final SqlOperator transformedOp;
  private final boolean swapped;

  // True if the RHS equals to the beginning of an interval
  // In the case of the RHS equals the ending of an interval, you need to rewrite the RHS
  // to equal the beginning of the interval
  protected boolean isRhsEqBegin;
  // True if the function truncate a date/time to the beginning of an interval
  private final boolean isTruncToBegin;

  RangeTransformer(
      RelOptCluster relOptCluster, StandardForm standardForm, SqlOperator sqlOperator) {
    this(relOptCluster, standardForm, sqlOperator, true);
  }

  RangeTransformer(
      RelOptCluster relOptCluster,
      StandardForm standardForm,
      SqlOperator transformedOp,
      boolean isTruncToBegin) {
    this.relOptCluster = relOptCluster;
    this.rexBuilder = relOptCluster.getRexBuilder();
    this.standardForm = standardForm;
    this.transformedOp = transformedOp;
    this.swapped = standardForm.isSwapped();
    this.isRhsEqBegin = true;
    this.isTruncToBegin = isTruncToBegin;
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

  public void setRhsEqBegin(boolean rhsEqBegin) {
    isRhsEqBegin = rhsEqBegin;
  }

  public RexNode transformEquals() {
    RexDateRange range = getRange();
    if (range != null) {
      if (isRhsEqBegin) {
        return SARGableRexUtils.and(
            SARGableRexUtils.gte(getColumn(), range.begin, rexBuilder),
            SARGableRexUtils.lt(getColumn(), range.end, rexBuilder),
            rexBuilder);
      } else {
        return rexBuilder.makeLiteral(false);
      }
    }
    return SARGableRexUtils.eq(getLhsCall(), getRhsNode(), rexBuilder);
  }

  public RexNode transformNotEquals() {
    RexDateRange range = getRange();
    if (range != null) {
      if (isRhsEqBegin) {
        return SARGableRexUtils.or(
            SARGableRexUtils.lt(getColumn(), range.begin, rexBuilder),
            SARGableRexUtils.gte(getColumn(), range.end, rexBuilder),
            rexBuilder);
      } else {
        return rexBuilder.makeLiteral(true);
      }
    }
    return SARGableRexUtils.neq(getLhsCall(), getRhsNode(), rexBuilder);
  }

  public RexNode transformGreaterThan() {
    RexDateRange range = getRange();
    if (range != null) {
      if (isRhsEqBegin) {
        if (isSwapped()) {
          return SARGableRexUtils.lt(getColumn(), range.begin, rexBuilder);
        }
        return SARGableRexUtils.gte(getColumn(), range.end, rexBuilder);
      } else {
        if (isTruncToBegin) {
          if (isSwapped()) {
            return SARGableRexUtils.lt(
                getColumn(), range.end, rexBuilder); // e.g. '2023-02-15' > DATE_TRUNC('MONTH',col)
          }
          return SARGableRexUtils.gte(
              getColumn(), range.end, rexBuilder); // e.g. DATE_TRUNC('MONTH',col) > '2023-02-15'
        } else {
          if (isSwapped()) {
            return SARGableRexUtils.lt(getColumn(), range.begin, rexBuilder);
          }
          return SARGableRexUtils.gte(getColumn(), range.begin, rexBuilder);
        }
      }
    } else {
      if (isSwapped()) {
        return SARGableRexUtils.lt(getLhsCall(), getRhsNode(), rexBuilder);
      } else {
        return SARGableRexUtils.gt(getLhsCall(), getRhsNode(), rexBuilder);
      }
    }
  }

  public RexNode transformGreaterThanOrEqual() {
    RexDateRange range = getRange();
    if (range != null) {
      if (isRhsEqBegin) {
        if (isSwapped()) {
          return SARGableRexUtils.lt(getColumn(), range.end, rexBuilder);
        }
        return SARGableRexUtils.gte(getColumn(), range.begin, rexBuilder);
      } else {
        if (isTruncToBegin) {
          if (isSwapped()) {
            return SARGableRexUtils.lt(getColumn(), range.end, rexBuilder);
          }
          return SARGableRexUtils.gte(getColumn(), range.end, rexBuilder);
        } else {
          if (isSwapped()) {
            return SARGableRexUtils.lt(getColumn(), range.begin, rexBuilder);
          }
          return SARGableRexUtils.gte(getColumn(), range.begin, rexBuilder);
        }
      }
    } else {
      if (isSwapped()) {
        return SARGableRexUtils.lte(getLhsCall(), getRhsNode(), rexBuilder);
      } else {
        return SARGableRexUtils.gte(getLhsCall(), getRhsNode(), rexBuilder);
      }
    }
  }

  public RexNode transformLessThan() {
    RexDateRange range = getRange();
    if (range != null) {
      if (isRhsEqBegin) {
        if (isSwapped()) {
          return SARGableRexUtils.gte(getColumn(), range.end, rexBuilder);
        }
        return SARGableRexUtils.lt(getColumn(), range.begin, rexBuilder);
      } else {
        if (isTruncToBegin) {
          if (isSwapped()) {
            return SARGableRexUtils.gte(
                getColumn(), range.end, rexBuilder); // e.g. '2023-02-15' < DATE_TRUNC('MONTH',col)
          }
          return SARGableRexUtils.lt(
              getColumn(), range.end, rexBuilder); // e.g.  DATE_TRUNC('MONTH',col) < '2023-02-15'
        } else {
          if (isSwapped()) {
            return SARGableRexUtils.gte(getColumn(), range.begin, rexBuilder);
          }
          return SARGableRexUtils.lt(getColumn(), range.begin, rexBuilder);
        }
      }
    } else {
      if (isSwapped()) {
        return SARGableRexUtils.gt(getLhsCall(), getRhsNode(), rexBuilder);
      } else {
        return SARGableRexUtils.lt(getLhsCall(), getRhsNode(), rexBuilder);
      }
    }
  }

  public RexNode transformLessThanOrEqual() {
    RexDateRange range = getRange();
    if (range != null) {
      if (isRhsEqBegin) {
        if (isSwapped()) {
          return SARGableRexUtils.gte(getColumn(), range.begin, rexBuilder);
        }
        return SARGableRexUtils.lt(getColumn(), range.end, rexBuilder);
      } else {
        if (isTruncToBegin) {
          if (isSwapped()) {
            return SARGableRexUtils.gte(getColumn(), range.end, rexBuilder);
          }
          return SARGableRexUtils.lt(getColumn(), range.end, rexBuilder);
        } else {
          if (isSwapped()) {
            return SARGableRexUtils.gte(getColumn(), range.begin, rexBuilder);
          }
          return SARGableRexUtils.lt(getColumn(), range.begin, rexBuilder);
        }
      }
    } else {
      if (isSwapped()) {
        return SARGableRexUtils.gte(getLhsCall(), getRhsNode(), rexBuilder);
      } else {
        return SARGableRexUtils.lte(getLhsCall(), getRhsNode(), rexBuilder);
      }
    }
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

  public RexDateRange getRange() {
    final RexNode begin;
    final RexNode end;
    if (getTransformedOp() == null) {
      begin = getRhsNode();
      end = SARGableRexUtils.dateAdd(getRhsNode(), getRhsParam(), rexBuilder);
    } else {
      switch (getRhsParamCount()) {
        case ARG1:
          begin = rexBuilder.makeCall(getTransformedOp(), getRhsNode());
          end =
              rexBuilder.makeCall(
                  getTransformedOp(),
                  SARGableRexUtils.dateAdd(getRhsNode(), getRhsParam(), rexBuilder));
          break;
        case ARG2:
          begin = rexBuilder.makeCall(getTransformedOp(), getLhsParam(), getRhsNode());
          end =
              rexBuilder.makeCall(
                  getTransformedOp(),
                  getLhsParam(),
                  SARGableRexUtils.dateAdd(getRhsNode(), getRhsParam(), rexBuilder));
          break;
        default:
          begin = null;
          end = null;
          break;
      }
    }
    if (begin == null || end == null) {
      return null;
    }

    return getDateRange(begin, end);
  }

  protected RexDateRange getDateRange(RexNode begin, RexNode end) {
    RexExecutor rexExecutor = relOptCluster.getPlanner().getExecutor();
    if (rexExecutor != null) {
      final List<RexNode> reducedValues = new ArrayList<>();
      final List<RexNode> constExpNode = new ArrayList<>();
      constExpNode.add(begin);
      constExpNode.add(end);
      // Only check if rhsNode = begin, nothing related to isTruncToBegin
      constExpNode.add(SARGableRexUtils.eq(begin, getRhsNode(), rexBuilder));
      rexExecutor.reduce(rexBuilder, constExpNode, reducedValues);
      constExpNode.clear();
      constExpNode.addAll(reducedValues);
      reducedValues.clear();
      rexExecutor.reduce(rexBuilder, constExpNode, reducedValues);

      if (reducedValues.get(0) != null
          && reducedValues.get(1) != null
          && reducedValues.get(2) != null) {
        if (reducedValues.get(2) instanceof RexLiteral) {
          RexLiteral boolVal = (RexLiteral) reducedValues.get(2);
          isRhsEqBegin = (Boolean) boolVal.getValue();
        } else if (reducedValues.get(2) instanceof RexCall) {
          RexCall call = (RexCall) reducedValues.get(2);
          if (call.operands.get(0) instanceof RexLiteral) {
            RexLiteral boolVal = (RexLiteral) call.operands.get(0);
            isRhsEqBegin = (Boolean) boolVal.getValue();
          }
        }
        return new RexDateRange(reducedValues.get(0), reducedValues.get(1));
      }
    }

    return new RexDateRange(begin, end);
  }

  static class RexDateRange {
    private final RexNode begin;
    private final RexNode end;

    public RexDateRange(RexNode begin, RexNode end) {
      this.begin = begin;
      this.end = end;
    }

    public RexNode getBegin() {
      return begin;
    }

    public RexNode getEnd() {
      return end;
    }
  }
}

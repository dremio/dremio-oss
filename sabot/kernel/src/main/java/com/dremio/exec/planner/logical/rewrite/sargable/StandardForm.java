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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import com.dremio.exec.planner.common.MoreRelOptUtil;

/**
 * <pre>
 * Standard form of a SARGable logical filter
 *     lhsCall(column, lhsParam1[, lhsParam2]) < logicalOp > rhsNode
 * or swapped form:
 *     rhsNode < logicalOp > lhsCall(column, lhsParam1[, lhsParam2])
 * where logicalOp can be =, !=, >, >=, <, <=, in, not in, between, not between.
 * A standard form is called SARGable if it can be transformed to a logically equal form
 *     column < logicalOp1 > rhsCall(rhsNode1, rhsParam1[, rhsParam12]) [and/or]
 *     column < logicalOp2 > rhsCall(rhsNode2, rhsParam2[, rhsParam22]) ...
 * </pre>
 */
public class StandardForm {
  private final RexCall lhsCall;
  private final RexNode rhsNode;
  private final SqlOperator transformingOp;
  private final boolean swapped;

  StandardForm(RexCall lhsCall, RexNode rhsNode, SqlOperator transformingOp, boolean swapped) {
    this.lhsCall = lhsCall;
    this.rhsNode = rhsNode;
    this.transformingOp = transformingOp;
    this.swapped = swapped;
  }

  public RexCall getLhsCall() {
    return lhsCall;
  }

  public RexNode getRhsNode() {
    return rhsNode;
  }

  public SqlOperator getTransformingOp() {
    return transformingOp;
  }

  public boolean isSwapped() {
    return swapped;
  }

  public static StandardForm build(RexCall call) {
    RexNode lhs = call.operands.get(0);
    RexNode rhs = call.operands.get(1);
    SqlOperator operator = call.getOperator();
    if (lhs instanceof RexCall && rhs instanceof RexLiteral && containsOneRexInputRef((RexCall) lhs)) {
      return new StandardForm((RexCall) lhs, rhs, operator, false);
    } else if (lhs instanceof RexLiteral && rhs instanceof RexCall && containsOneRexInputRef((RexCall) rhs)) {
      return new StandardForm((RexCall) rhs, lhs, operator, true);
    } else if (call.getKind().equals(SqlKind.CASE)) {
      for (int i = 0; i < call.operands.size() - 1; i += 2) {
        if (call.operands.get(i) instanceof RexCall && call.operands.get(i + 1) instanceof RexCall) {
          RexCall cond = (RexCall) call.operands.get(i);
          if (!cond.getKind().equals(SqlKind.IS_NOT_NULL)) {
            return null;
          }
        }
      }
      return new StandardForm(call, null, operator, false);
    } else if (call.getKind().equals(SqlKind.AND) || call.getKind().equals(SqlKind.OR)) {
      return new StandardForm(call, null, operator, false);
    }
    return null;
  }

  private static boolean containsOneRexInputRef(RexCall call) {
    return MoreRelOptUtil.RexNodeCountVisitor.inputRefCount(call) == 1;
  }
}

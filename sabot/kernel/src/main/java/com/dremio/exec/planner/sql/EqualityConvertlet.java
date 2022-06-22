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
package com.dremio.exec.planner.sql;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;

import com.google.common.collect.Lists;

/**
 * Custom convertlet to handle equals, not equals, is distinct from, and is not distinct from for Decimal types
 * Specifically, this will preserve the scale of the input types, with the
 * possibility of not being able to preserve the precision if doing so
 * would result in precision higher than the MAX_PRECISION
 */
public class EqualityConvertlet implements SqlRexConvertlet {

  public static final EqualityConvertlet INSTANCE = new EqualityConvertlet();
  private static final int MAX_PRECISION = 38;

  private EqualityConvertlet() {
  }

  @Override
  public RexNode convertCall(SqlRexContext cx, SqlCall call) {
    return convertEquality(cx, call);
  }

  private static List<RexNode> convertExpressionList(SqlRexContext cx,
                                                     List<SqlNode> nodes) {
    final List<RexNode> exprs = Lists.newArrayList();
    for (SqlNode node : nodes) {
      exprs.add(cx.convertExpression(node));
    }
    if (exprs.size() > 1) {
      final SqlOperandTypeChecker.Consistency consistency = Consistency.LEAST_RESTRICTIVE;
      final List<RelDataType> types = RexUtil.types(exprs);
      final RelDataType type;
      if (ConsistentTypeUtil.allExactNumeric(types) && ConsistentTypeUtil.anyDecimal(types)) {
        // for mixed types, INT and BIGINT will be treated as DECIMAL(10,0) and DECIMAL(19,0) respectively
        type = ConsistentTypeUtil.consistentDecimalType(cx.getTypeFactory(), types);
      } else {
        // if there are no Decimal types or some of the types are non-exact, fall back to default Calcite
        // behavior which will convert to Double
        type = ConsistentTypeUtil.consistentType(cx.getTypeFactory(), consistency, types);
      }
      if (type != null) {
        final List<RexNode> oldExprs = Lists.newArrayList(exprs);
        exprs.clear();
        for (RexNode expr : oldExprs) {
          exprs.add(cx.getRexBuilder().ensureType(type, expr, true));
        }
      }
    }
    return exprs;
  }

  private static RexNode convertEquality(SqlRexContext cx, SqlCall call) {

    final List<SqlNode> operands = call.getOperandList();
    final RexBuilder rexBuilder = cx.getRexBuilder();
    final List<RexNode> exprs =
      convertExpressionList(cx, operands);
    SqlOperator op = call.getOperator();
    RelDataType type = rexBuilder.deriveReturnType(op, exprs);
    RexCall convertedCall = (RexCall) rexBuilder.makeCall(type, op, RexUtil.flatten(exprs, op));

    // The following is copied from Calcite's StdConvertletTable.convertEqualsOrNotEquals()
    final RexNode op0 = convertedCall.getOperands().get(0);
    final RexNode op1 = convertedCall.getOperands().get(1);
    final RexNode booleanOp;
    final RexNode integerOp;

    if (op0.getType().getSqlTypeName() == SqlTypeName.BOOLEAN
      && SqlTypeName.INT_TYPES.contains(op1.getType().getSqlTypeName())) {
      booleanOp = op0;
      integerOp = op1;
    } else if (SqlTypeName.INT_TYPES.contains(op0.getType().getSqlTypeName())
      && op1.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
      booleanOp = op1;
      integerOp = op0;
    } else {
      return convertedCall;
    }

    SqlOperator newOp = (op.getKind() == SqlKind.EQUALS || op.getKind() == SqlKind.NOT_EQUALS) ?
      SqlStdOperatorTable.EQUALS :
      SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;

    return rexBuilder.makeCall(call.getOperator(),
      booleanOp,
      rexBuilder.makeCall(
        SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(
          newOp,
          integerOp,
          rexBuilder.makeZeroLiteral(integerOp.getType())),
        rexBuilder.makeLiteral(false),
        rexBuilder.makeLiteral(true)));
  }
}

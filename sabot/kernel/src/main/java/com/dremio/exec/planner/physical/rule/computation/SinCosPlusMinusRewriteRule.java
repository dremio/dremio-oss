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

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import com.dremio.exec.planner.logical.RexRewriter;

/**
 * Rewrites expressions like cos(a - b) to a single call, cos_minus(a,b), to make it easier for the Expression pusher
 * detect if a and b are on separate sides of join, and thus the expression should be rewritten using trig identity
 */
class SinCosPlusMinusRewriteRule extends RexRewriter.RewriteRule {

  public SinCosPlusMinusRewriteRule(RexBuilder builder) {
    super(builder);
  }

  @Override
  public RexNode rewrite(RexCall call) {
    switch (call.getOperator().getName().toUpperCase()) {
      case "SIN": {
        RexNode child = call.getOperands().get(0);
        if (!(child instanceof RexCall)) {
          return null;
        }
        RexCall childCall = (RexCall) child;
        switch (child.getKind()) {
          case PLUS: {
            return builder.makeCall(SIN_PLUS,
              childCall.getOperands().get(0),
              childCall.getOperands().get(1));
          }
          case MINUS: {
            return builder.makeCall(SIN_MINUS,
              childCall.getOperands().get(0),
              childCall.getOperands().get(1));
          }
        }
      }
      case "COS": {
        RexNode child = call.getOperands().get(0);
        if (!(child instanceof RexCall)) {
          return null;
        }
        RexCall childCall = (RexCall) child;
        switch (child.getKind()) {
          case PLUS: {
            return builder.makeCall(COS_PLUS,
              childCall.getOperands().get(0),
              childCall.getOperands().get(1));
          }
          case MINUS: {
            return builder.makeCall(COS_MINUS,
              childCall.getOperands().get(0),
              childCall.getOperands().get(1));
          }
        }
      }
      default:
        return null;
    }
  }

  public static final SqlFunction SIN_PLUS =
    new SqlFunction(
      "SIN_PLUS",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.DOUBLE_NULLABLE,
      null,
      OperandTypes.NUMERIC,
      SqlFunctionCategory.NUMERIC);

  public static final SqlFunction SIN_MINUS =
    new SqlFunction(
      "SIN_MINUS",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.DOUBLE_NULLABLE,
      null,
      OperandTypes.NUMERIC,
      SqlFunctionCategory.NUMERIC);

  public static final SqlFunction COS_PLUS =
    new SqlFunction(
      "COS_PLUS",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.DOUBLE_NULLABLE,
      null,
      OperandTypes.NUMERIC,
      SqlFunctionCategory.NUMERIC);

  public static final SqlFunction COS_MINUS =
    new SqlFunction(
      "COS_MINUS",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.DOUBLE_NULLABLE,
      null,
      OperandTypes.NUMERIC,
      SqlFunctionCategory.NUMERIC);
}

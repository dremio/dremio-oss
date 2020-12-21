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
package com.dremio.exec.planner;

import static com.dremio.exec.planner.common.MoreRelOptUtil.isNegative;
import static com.dremio.exec.planner.common.MoreRelOptUtil.op;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.dremio.exec.planner.logical.RexRewriter;
import com.dremio.exec.planner.logical.RexRewriter.RewriteRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class ReduceTrigFunctionsRule extends RelOptRule {

  public static final ReduceTrigFunctionsRule INSTANCE = new ReduceTrigFunctionsRule();

  public ReduceTrigFunctionsRule() {
    super(operand(LogicalFilter.class, any()), "ReduceTrigFunctions");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = (LogicalFilter) call.rels[0];
    RexNode newCondition = RexRewriter.rewrite(filter.getCondition(), getRules(filter.getCluster().getRexBuilder()));
    if (newCondition != filter.getCondition()) {
      call.transformTo(LogicalFilter.create(filter.getInput(), newCondition));
    }
  }

  private List<RewriteRule> getRules(RexBuilder rexBuilder) {
    return ImmutableList.of(
      new SimpleArithmeticRule(rexBuilder),
      new InverseTrigRule(rexBuilder)
    );
  }

  private static class SimpleArithmeticRule extends RewriteRule {
    public SimpleArithmeticRule(RexBuilder builder) {
      super(builder);
    }

    @Override
    public RexNode rewrite(RexCall call) {
      if (!call.isA(SqlKind.COMPARISON)) {
        return null;
      }

      if (RexUtil.isConstant(call.getOperands().get(0)) && !RexUtil.isConstant(call.getOperands().get(1))) {
        RexNode inverted = RexUtil.invert(builder, call);
        if (!(inverted instanceof RexCall)) {
          return null;
        }
        call = (RexCall) inverted;
      }
      if (!(call.getOperands().get(0) instanceof RexCall)) {
        return null;
      }

      // no need to rewrite if we don't have a constant on one side
      if (!RexUtil.isConstant(call.getOperands().get(1))) {
        return null;
      }

      RexCall op0 = (RexCall) call.getOperands().get(0);
      RexNode op1 = call.getOperands().get(1);
      switch (op0.getKind()) {
        case TIMES:
          if (RexUtil.isConstant(op0.getOperands().get(0)) && !RexUtil.isConstant(op0.getOperands().get(1))) {
            RexNode reversed = builder.makeCall(op0.getOperator(), Lists.reverse(op0.operands));
            if (!(reversed instanceof RexCall)) {
              return null;
            }
            op0 = (RexCall) reversed;
          }
          RexNode rightNode = op0.getOperands().get(0);
          if (rightNode instanceof RexLiteral) {
            SqlOperator comparison = isNegative(((RexLiteral) rightNode)) ? op(call.getKind().reverse()) : call.getOperator();
            return builder.makeCall(comparison, op0.getOperands().get(0), builder.makeCall(SqlStdOperatorTable.DIVIDE, op1, op0.getOperands().get(1)));
          }
          break;
        case PLUS:
          if (RexUtil.isConstant(op0.getOperands().get(0)) && !RexUtil.isConstant(op0.getOperands().get(1))) {
            RexNode reversed = builder.makeCall(op0.getOperator(), Lists.reverse(op0.operands));
            if (!(reversed instanceof RexCall)) {
              return null;
            }
            op0 = (RexCall) reversed;
          }
          if (RexUtil.isConstant(op0.getOperands().get(1))) {
            return builder.makeCall(call.getOperator(), op0.getOperands().get(0), builder.makeCall(SqlStdOperatorTable.MINUS, op1, op0.getOperands().get(1)));
          }
          break;
        case MINUS:
          if (RexUtil.isConstant(op0.getOperands().get(0)) && !RexUtil.isConstant(op0.getOperands().get(1))) {
            RexNode reversed = builder.makeCall(op0.getOperator(), Lists.reverse(op0.operands));
            if (!(reversed instanceof RexCall)) {
              return null;
            }
            op0 = (RexCall) reversed;
          }
          if (RexUtil.isConstant(op0.getOperands().get(1))) {
            return builder.makeCall(call.getOperator(), op0.getOperands().get(0), builder.makeCall(SqlStdOperatorTable.PLUS, op1, op0.getOperands().get(1)));
          }
          break;
        default:
          break;
      }
      return null;
    }
  }

  private static class InverseTrigRule extends RewriteRule {

    public InverseTrigRule(RexBuilder builder) {
      super(builder);
    }

    @Override
    public RexNode rewrite(RexCall call) {
      if (!call.isA(SqlKind.COMPARISON)) {
        return null;
      }

      if (RexUtil.isConstant(call.getOperands().get(0)) && !RexUtil.isConstant(call.getOperands().get(1))) {
        RexNode inverted = RexUtil.invert(builder, call);
        if (!(inverted instanceof RexCall)) {
          return null;
        }
        call = (RexCall) inverted;
      }

      if (!(call.getOperands().get(0) instanceof RexCall)) {
        return null;
      }

      if (!call.getOperands().get(0).isA(SqlKind.OTHER_FUNCTION)) {
        return null;
      }

      RexCall functionCall = (RexCall) call.getOperands().get(0);
      String name = functionCall.getOperator().getName().toUpperCase();
      SqlOperator op;
      switch (name) {
        case "ASIN":
          op = SqlStdOperatorTable.SIN;
          break;
        case "ACOS":
          op = SqlStdOperatorTable.COS;
          break;
        case "ATAN":
          op = SqlStdOperatorTable.TAN;
          break;
        default:
          return null;
      }
      RexNode newCall = builder.makeCall(op, call.getOperands().get(1));
      return builder.makeCall(call.getOperator(), ((RexCall) call.getOperands().get(0)).getOperands().get(0), newCall);
    }
  }
}

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
package com.dremio.exec.planner.sql.handlers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.physical.FilterPrel;
import com.google.common.collect.ImmutableSet;

/**
 * A shuttle designed to convert
 * AND(exp_1<>literal_1, exp1<>literal_2, exp_2<>literal_3, exp_2<>literal_4,...) pattern to
 * AND(NOT(OR(exp_1=literal_1,exp_1=literal_2)),NOT(OR(exp_2=literal_3,exp_2=literal_4)),...).
 */
class AndToOrConverter extends StatelessRelShuttleImpl {

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof FilterPrel) {
      FilterPrel filter = (FilterPrel) other;
      AndToOrRexShuttle converter = new AndToOrRexShuttle(filter.getCluster().getRexBuilder());
      RexNode newCondition = converter.convert(filter.getCondition());
      if (converter.converted()) {
        return super.visit(filter.copy(filter.getTraitSet(), filter.getInput(), newCondition));
      }
    }
    return super.visit(other);
  }

  public static class AndToOrRexShuttle extends RexShuttle {

    private final RexBuilder rexBuilder;
    private boolean converted;
    private final Set<SqlTypeName> SUPPORTED_TYPES =
      ImmutableSet.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.DATE, SqlTypeName.TIME, SqlTypeName.TIMESTAMP, SqlTypeName.VARCHAR, SqlTypeName.VARBINARY);

    public AndToOrRexShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
      this.converted = false;
    }

    private boolean isTypeSuppored(SqlTypeName type) {
      return SUPPORTED_TYPES.contains(type);
    }

    public RexNode convert(RexNode rexNode) {
      converted = false;
      return rexNode.accept(this);
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (call.getKind() == SqlKind.AND) {
        final List<RexNode> operands = call.getOperands();
        Map<String, List<RexNode>> operandsByExpr = new HashMap<>();
        List<RexNode> extras = new ArrayList<>();
        for (RexNode rexNode : operands) {
          if (!(rexNode instanceof RexCall) || (rexNode.getKind() != SqlKind.NOT_EQUALS)) {
            extras.add(rexNode);
            continue;
          }

          RexCall opCall = (RexCall) rexNode;
          if (opCall.getOperands().size() != 2) {
            extras.add(rexNode);
            continue;
          }

          RexNode first = opCall.getOperands().get(0);
          RexNode second = opCall.getOperands().get(1);

          if ((first.getType().getSqlTypeName() != second.getType().getSqlTypeName()) || !isTypeSuppored(first.getType().getSqlTypeName())) {
            extras.add(rexNode);
            continue;
          }

          RexLiteral literal = null;
          RexNode other = null;

          if (first instanceof RexLiteral && !(second instanceof RexLiteral)) {
            literal = (RexLiteral) first;
            other = second;
          }

          if (second instanceof RexLiteral && !(first instanceof RexLiteral)) {
            literal = (RexLiteral) second;
            other = first;
          }

          if (literal == null) {
            extras.add(rexNode);
            continue;
          }

          String stringExpr = other.toString();

          RexNode newOperand = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, other, literal);
          if (!operandsByExpr.containsKey(stringExpr)) {
            operandsByExpr.put(stringExpr, new ArrayList<>());
          }
          operandsByExpr.get(stringExpr).add(newOperand);
        }

        if (extras.size() != operands.size()) {
          converted = true;
          List<RexNode> operandList = new ArrayList<>();
          for (String expr : operandsByExpr.keySet()) {
            if (operandsByExpr.get(expr).size() > 1) {
              operandList.add(rexBuilder.makeCall(SqlStdOperatorTable.NOT, rexBuilder.makeCall(SqlStdOperatorTable.OR, operandsByExpr.get(expr))));
            } else {
              operandList.add(rexBuilder.makeCall(SqlStdOperatorTable.NOT, operandsByExpr.get(expr)));
            }
          }
          operandList.addAll(extras);
          if (operandList.size() == 1) {
            return operandList.get(0);
          } else {
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, operandList);
          }
        }
      }
      return call;
    }

    public boolean converted() {
      return converted;
    }
  }
}

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

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** Flatten nested CASE expressions. */
public class FlattenCaseExpressionsVisitor extends StatelessRelShuttleImpl {
  private static final FlattenCaseExpressionsVisitor INSTANCE = new FlattenCaseExpressionsVisitor();

  public static RelNode simplify(RelNode relNode) {
    return relNode.accept(INSTANCE);
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    final RelNode inputRel = filter.getInput().accept(this);
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

    final CaseExpressionUnwrapper visitor = new CaseExpressionUnwrapper(rexBuilder);
    final RexNode condition = filter.getCondition().accept(visitor);
    return filter.copy(filter.getTraitSet(), inputRel, condition);
  }

  @Override
  public RelNode visit(LogicalProject project) {
    final RelNode inputRel = project.getInput().accept(this);
    final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    final List<RexNode> projects = new ArrayList<>();

    final CaseExpressionUnwrapper visitor = new CaseExpressionUnwrapper(rexBuilder);
    for (RexNode rexNode : project.getProjects()) {
      projects.add(rexNode.accept(visitor));
    }
    return project.copy(project.getTraitSet(), inputRel, projects, project.getRowType());
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    final RelNode left = join.getLeft().accept(this);
    final RelNode right = join.getRight().accept(this);
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    final CaseExpressionUnwrapper visitor = new CaseExpressionUnwrapper(rexBuilder);
    final RexNode condition = join.getCondition().accept(visitor);
    return join.copy(
        join.getTraitSet(), condition, left, right, join.getJoinType(), join.isSemiJoinDone());
  }

  public static final class CaseExpressionUnwrapper extends RexShuttle {
    private final RexBuilder rexBuilder;

    public CaseExpressionUnwrapper(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      boolean foundNestedCase = false;
      if (call.getOperator() == SqlStdOperatorTable.CASE) {
        /*
         * Flatten the children to see if we have nested case expressions
         * Case operands are always 2n + 1, and they are like:
         * (RexNode -> When expression
         * RexNode -> Then expression) repeats n times
         * RexNode -> Else expression
         */
        List<RexNode> operands = new ArrayList<>(call.getOperands());

        /*
         * Flatten all ELSE expressions. Anything nested under ELSE expression can be
         * pulled up to the parent case. e.g.
         *
         * CASE WHEN col1 = 'abc' THEN 0
         *      WHEN col1 = 'def' THEN 1
         *      ELSE (CASE WHEN col2 = 'ghi' THEN -1
         *                 ELSE (CASE WHEN col3 = 'jkl' THEN -2
         *                            ELSE -3))
         *
         * can be rewritten as:
         * CASE WHEN col1 = 'abc' THEN 0
         *      WHEN col1 = 'def' THEN 1
         *      WHEN col2 = 'ghi' THEN -1
         *      WHEN col3 = 'jkl' THEN -2
         *      ELSE -3
         */

        boolean unwrapped = true;
        while (unwrapped) { // Recursively unwrap the ELSE expression
          List<RexNode> elseOperators = new ArrayList<>();
          RexNode elseExpr = operands.get(operands.size() - 1);
          if (elseExpr instanceof RexCall) {
            RexCall elseCall = ((RexCall) elseExpr);
            if (elseCall.getOperator() == SqlStdOperatorTable.CASE) {
              foundNestedCase = true;
              elseOperators.addAll(elseCall.getOperands());
            }
          }
          if (elseOperators.isEmpty()) {
            unwrapped = false;
          } else {
            operands.remove(
                operands.size()
                    - 1); // Remove the ELSE expression and replace with the unwrapped one
            operands.addAll(elseOperators);
          }
        }
        if (foundNestedCase) {
          return rexBuilder.makeCall(call.getType(), SqlStdOperatorTable.CASE, operands);
        }
      }
      return super.visitCall(call);
    }
  }
}

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
package com.dremio.exec.planner.sql.evaluator;

import java.util.Map;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.dremio.exec.planner.logical.RelDataTypeEqualityUtil;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.dremio.sabot.exec.context.ContextInformation;
import com.google.common.collect.ImmutableMap;

public final class FunctionEvaluatorRule extends RelRule<RelRule.Config> {
  private static final Map<SqlOperator, FunctionEval> EVALUATORS = ImmutableMap.of(
    DremioSqlOperatorTable.IDENTITY, IdentityEvaluator.INSTANCE,
    DremioSqlOperatorTable.TYPEOF, TypeOfEvaluator.INSTANCE,
    SqlStdOperatorTable.CURRENT_TIME, CurrentTimeEvaluator.INSTANCE,
    SqlStdOperatorTable.CURRENT_DATE, CurrentDateEvaluator.INSTANCE,
    SqlStdOperatorTable.CURRENT_TIMESTAMP, CurrentTimestampEvaluator.INSTANCE,
    DremioSqlOperatorTable.CURRENT_TIME_UTC, CurrentTimeUtcEvaluator.INSTANCE,
    DremioSqlOperatorTable.CURRENT_DATE_UTC, CurrentDateUtcEvaluator.INSTANCE
  );

  private final ContextInformation contextInformation;

  public FunctionEvaluatorRule(ContextInformation contextInformation) {
    super(Config.EMPTY
      .withDescription("FunctionEvaluatorRule")
      .withOperandSupplier(op -> op.operand(RelNode.class).anyInputs()));
    this.contextInformation = contextInformation;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode relNode = call.rel(0);
    RelNode rewrittenRelNode = evaluate(relNode);
    if (relNode != rewrittenRelNode) {
      call.transformTo(rewrittenRelNode);
    }
  }

  public RelNode evaluate(RelNode relNode) {
    RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
    EvaluationContext evaluationContext = new EvaluationContext(rexBuilder, contextInformation);
    RexShuttle rexShuttle = new RexShuttleImpl(evaluationContext, EVALUATORS);
    return relNode.accept(rexShuttle);
  }

  private static final class RexShuttleImpl extends RexShuttle {
    private final EvaluationContext evaluationContext;
    private final Map<SqlOperator, FunctionEval> evals;

    private RexShuttleImpl(EvaluationContext evaluationContext, Map<SqlOperator, FunctionEval> evals) {
      this.evaluationContext = evaluationContext;
      this.evals = evals;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
      // Recurse to visit the operands
      RexCall visited = (RexCall) super.visitCall(call);
      SqlOperator operator = call.getOperator();
      FunctionEval functionEval = evals.get(operator);
      if (functionEval == null) {
        return visited;
      }

      RexNode evaluated = functionEval.evaluate(evaluationContext, visited);
      if (!RelDataTypeEqualityUtil.areEquals(call.getType(), evaluated.getType())) {
        throw new RuntimeException(
          "RexNode conversion resulted in type mismatch.\n" +
            "Original Type: " + call.getType() + " nullable: " + call.getType().isNullable() + "\n" +
            "Converted Type: " + evaluated.getType() + " nullable: " + evaluated.getType().isNullable());
      }
      return evaluated;
    }
  }
}

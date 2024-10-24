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

import static com.dremio.exec.planner.physical.PlannerSettings.USE_LEGACY_TYPEOF;

import com.dremio.exec.planner.logical.RelDataTypeEqualityComparer;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.ContextInformation;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;

/**
 * Utility class that given a Rex or Rel Node with Dynamic Functions will evaluate them into static
 * literals.
 */
public final class FunctionEvaluatorUtil {
  public static RelNode evaluateAll(
      RelNode relNode, ContextInformation contextInformation, OptionResolver optionResolver) {
    return evaluate(relNode, contextInformation, getEvalMap(FunctionEvalMaps.ALL, optionResolver));
  }

  public static RelNode evaluateDynamic(
      RelNode relNode, ContextInformation contextInformation, OptionResolver optionResolver) {
    return evaluate(
        relNode, contextInformation, getEvalMap(FunctionEvalMaps.DYNAMIC, optionResolver));
  }

  public static RelNode evaluateSystem(RelNode relNode, OptionResolver optionResolver) {
    return evaluate(relNode, null, getEvalMap(FunctionEvalMaps.SYSTEM, optionResolver));
  }

  public static RelNode evaluate(
      RelNode relNode,
      ContextInformation contextInformation,
      ImmutableMap<SqlOperator, FunctionEval> evals) {
    RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
    RexShuttle rexShuttle = createShuttle(rexBuilder, contextInformation, evals);
    return relNode.accept(rexShuttle);
  }

  public static RexNode evaluateAll(
      RexNode rexNode,
      RexBuilder rexBuilder,
      ContextInformation contextInformation,
      OptionResolver optionResolver) {
    return evaluate(
        rexNode, rexBuilder, contextInformation, getEvalMap(FunctionEvalMaps.ALL, optionResolver));
  }

  public static RexNode evaluateDynamic(
      RexNode rexNode,
      RexBuilder rexBuilder,
      ContextInformation contextInformation,
      OptionResolver optionResolver) {
    return evaluate(
        rexNode,
        rexBuilder,
        contextInformation,
        getEvalMap(FunctionEvalMaps.DYNAMIC, optionResolver));
  }

  public static RexNode evaluateSystem(
      RexNode rexNode, RexBuilder rexBuilder, OptionResolver optionResolver) {
    return evaluate(rexNode, rexBuilder, null, getEvalMap(FunctionEvalMaps.SYSTEM, optionResolver));
  }

  private static RexNode evaluate(
      RexNode rexNode,
      RexBuilder rexBuilder,
      ContextInformation contextInformation,
      ImmutableMap<SqlOperator, FunctionEval> evals) {
    RexShuttle rexShuttle = createShuttle(rexBuilder, contextInformation, evals);
    return rexNode.accept(rexShuttle);
  }

  private static RexShuttle createShuttle(
      RexBuilder rexBuilder,
      ContextInformation contextInformation,
      ImmutableMap<SqlOperator, FunctionEval> evals) {
    EvaluationContext evaluationContext = new EvaluationContext(rexBuilder, contextInformation);
    return new RexShuttleImpl(evaluationContext, evals);
  }

  private static ImmutableMap<SqlOperator, FunctionEval> getEvalMap(
      ImmutableMap<SqlOperator, FunctionEval> originalMap, OptionResolver optionResolver) {
    if ((optionResolver == null) || !optionResolver.getOption(USE_LEGACY_TYPEOF)) {
      return originalMap;
    }

    Map<SqlOperator, FunctionEval> newMap = new HashMap<>();
    for (SqlOperator operator : originalMap.keySet()) {
      if (operator != DremioSqlOperatorTable.TYPEOF) {
        newMap.put(operator, originalMap.get(operator));
      }
    }

    return ImmutableMap.copyOf(newMap);
  }

  private static final class RexShuttleImpl extends RexShuttle {
    private final EvaluationContext evaluationContext;
    private final Map<SqlOperator, FunctionEval> evals;

    private RexShuttleImpl(
        EvaluationContext evaluationContext, Map<SqlOperator, FunctionEval> evals) {
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

      if (!RelDataTypeEqualityComparer.areEquals(
          call.getType(),
          evaluated.getType(),
          RelDataTypeEqualityComparer.Options.builder()
              .withConsiderScale(false)
              .withConsiderPrecision(false)
              .withConsiderNullability(false)
              .build())) {
        throw new RuntimeException(
            "RexNode conversion resulted in type mismatch.\n"
                + "Original Type: "
                + call.getType()
                + " nullable: "
                + call.getType().isNullable()
                + "\n"
                + "Converted Type: "
                + evaluated.getType()
                + " nullable: "
                + evaluated.getType().isNullable());
      }

      return evaluated;
    }
  }
}

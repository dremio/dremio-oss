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
package com.dremio.exec.catalog.udf;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.sql.SqlConverter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class ScalarUserDefinedFunctionExpanderRule extends RelRule<RelRule.Config> {
  private final SqlConverter sqlConverter;

  public ScalarUserDefinedFunctionExpanderRule(SqlConverter sqlConverter) {
    super(Config.EMPTY
      .withDescription("ScalarUserDefinedFunctionExpanderRuleFilter")
      .withOperandSupplier(op1 ->
        op1.operand(RelNode.class).anyInputs()));
    this.sqlConverter = sqlConverter;
  }

  @Override public void onMatch(RelOptRuleCall relOptRuleCall) {
    RelNode rel = relOptRuleCall.rel(0);
    Pair<RelNode, CorrelationId> relAndCor = convert(rel);
    if (relAndCor.left == rel) {
      return;
    }

    RelNode transformedRel;
    if (!(rel instanceof Filter)) {
      // Don't need to do anything with the correlate id.
      transformedRel = relAndCor.left;
    } else {
      Filter filterRel = (Filter) rel;
      transformedRel = relOptRuleCall
        .builder()
        .push(filterRel.getInput())
        .filter(ImmutableList.of(relAndCor.right), ImmutableList.of(((Filter) relAndCor.left).getCondition()))
        .build();
    }

    relOptRuleCall.transformTo(transformedRel);
  }

  private Pair<RelNode, CorrelationId> convert(RelNode relNode) {
    RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();

    CorrelationId correlationId = relNode.getVariablesSet().isEmpty()
      ? relNode.getCluster().createCorrel()
      : Iterables.getOnlyElement(relNode.getVariablesSet());
    RexCorrelVariable rexCorrelVariable = relNode.getInputs().isEmpty() ? null : (RexCorrelVariable)rexBuilder.makeCorrel(
      relNode.getInput(0).getRowType(),
      correlationId);

    UdfExpander udfExpander = new UdfExpander(sqlConverter, rexBuilder, rexCorrelVariable);
    RelNode transformedRelNode = relNode.accept(udfExpander);
    return Pair.of(transformedRelNode, correlationId);
  }

  private static final class UdfExpander extends RexShuttle {
    private final SqlConverter sqlConverter;
    private final RexBuilder rexBuilder;
    private final RexCorrelVariable rexCorrelVariable;

    public UdfExpander(
      SqlConverter sqlConverter,
      RexBuilder rexBuilder,
      RexCorrelVariable rexCorrelVariable) {
      this.sqlConverter = sqlConverter;
      this.rexBuilder = rexBuilder;
      this.rexCorrelVariable = rexCorrelVariable;
    }

    @Override public RexNode visitCall(RexCall call) {
      SqlOperator operator = call.getOperator();

      // Preorder traversal to handle nested UDFs
      RexCall visitedCall = (RexCall) super.visitCall(call);
      if (!(operator instanceof SqlUserDefinedFunction)) {
        return visitedCall;
      }

      Function function = ((SqlUserDefinedFunction) operator).getFunction();
      if (!(function instanceof DremioScalarUserDefinedFunction)) {
        return visitedCall;
      }

      DremioScalarUserDefinedFunction dremioScalarUserDefinedFunction = (DremioScalarUserDefinedFunction) function;

      RexNode udfExpression = dremioScalarUserDefinedFunction.extractExpression(sqlConverter);
      RexNode rewrittenUdfExpression;
      if (!CorrelatedUdfDetector.hasCorrelatedUdf(udfExpression)) {
        rewrittenUdfExpression = ParameterizedQueryParameterReplacer.replaceParameters(
          udfExpression,
          function.getParameters(),
          visitedCall.getOperands(),
          rexBuilder);
      } else {
        RexInputRefToFieldAccess replacer = new RexInputRefToFieldAccess(
          rexBuilder,
          rexCorrelVariable);
        List<RexNode> rewrittenCorrelateOperands = visitedCall
          .getOperands()
          .stream()
          .map(operand -> operand.accept(replacer))
          .collect(Collectors.toList());
        rewrittenUdfExpression = RexArgumentReplacer.replaceArguments(
          udfExpression,
          function.getParameters(),
          rewrittenCorrelateOperands,
          visitedCall.getOperands(),
          rexCorrelVariable.id,
          rexBuilder);
      }

      RexNode castedNode = rexBuilder.makeCast(
        call.getType(),
        rewrittenUdfExpression,
        true);

      return castedNode;
    }
  }

  private static final class RexInputRefToFieldAccess extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final RexNode rexCorrelVariable;

    public RexInputRefToFieldAccess(RexBuilder rexBuilder, RexNode rexCorrelVariable) {
      this.rexBuilder = rexBuilder;
      this.rexCorrelVariable = rexCorrelVariable;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      return rexBuilder.makeFieldAccess(rexCorrelVariable, inputRef.getIndex());
    }
  }

  private static final class RexArgumentReplacer extends RexShuttle {
    private final RelShuttle correlateRelReplacer;
    private final RelShuttle refIndexRelReplacer;
    private final RexShuttle correlateRexReplacer;
    private final RexShuttle refIndexRexReplacer;
    private final CorrelationId correlationId;

    private RexArgumentReplacer(
      RelShuttle correlateRelReplacer,
      RelShuttle refIndexRelReplacer,
      RexShuttle correlateRexReplacer,
      RexShuttle refIndexRexReplacer,
      CorrelationId correlationId) {
      this.correlateRelReplacer = correlateRelReplacer;
      this.refIndexRelReplacer = refIndexRelReplacer;
      this.correlateRexReplacer = correlateRexReplacer;
      this.refIndexRexReplacer = refIndexRexReplacer;
      this.correlationId = correlationId;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      // For a subquery we want to rewrite the RelNode using correlates:
      RelNode rewrittenRelNode = subQuery.rel.accept(correlateRelReplacer);
      boolean relRewritten = rewrittenRelNode != subQuery.rel;

      // And the operands with ref indexes:
      List<RexNode> rewrittenOperands = subQuery
        .getOperands()
        .stream()
        .map(operand -> operand.accept(refIndexRexReplacer))
        .collect(ImmutableList.toImmutableList());

      // This is because the operands are in relation to the outer query
      // And the RelNode is in relation to the inner query

      // This is more clear in the case of IN vs EXISTS:
      // IN($0, {
      //  LogicalProject(DEPTNO=[$6])
      //  ScanCrel(table=[cp.scott."EMP.json"], columns=[`EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `DEPTNO`, `COMM`], splits=[1])
      //})
      //
      // EXISTS({
      //  LogicalFilter(condition=[=($6, $cor1.DEPTNO)])
      //  ScanCrel(table=[cp.scott."EMP.json"], columns=[`EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `DEPTNO`, `COMM`], splits=[1])
      //})


      CorrelationId rewrittenCorrelateId = relRewritten ? correlationId : null;
      // TODO: add RexSubQuery.clone(CorrelationId) so we don't need this switch case
      SqlKind kind = subQuery.op.kind;
      switch (kind) {
      case SCALAR_QUERY:
        return RexSubQuery.scalar(rewrittenRelNode, rewrittenCorrelateId);

      case EXISTS:
        return RexSubQuery.exists(rewrittenRelNode, rewrittenCorrelateId);

      case IN:
        return RexSubQuery.in(rewrittenRelNode, rewrittenOperands, rewrittenCorrelateId);

      case SOME:
        return RexSubQuery.some(rewrittenRelNode, rewrittenOperands, (SqlQuantifyOperator) subQuery.op, rewrittenCorrelateId);

      default:
        throw new UnsupportedOperationException("Can not support kind: " + kind);
      }
    }

    @Override
    public RexNode visitCall(final RexCall call) {
      RexNode visitedCall = super.visitCall(call);
      // For regular calls we replace with ref indexes
      return visitedCall.accept(refIndexRexReplacer);
    }

    public static RexNode replaceArguments(
      RexNode rexNode,
      List<FunctionParameter> functionParameters,
      List<RexNode> correlateReplacements,
      List<RexNode> refIndexReplacements,
      CorrelationId correlationId,
      RexBuilder rexBuilder) {
      RelShuttle correlateRelReplacer = ParameterizedQueryParameterReplacer.createRelParameterReplacer(functionParameters, correlateReplacements, rexBuilder);
      RelShuttle refIndexRelReplacer = ParameterizedQueryParameterReplacer.createRelParameterReplacer(functionParameters, refIndexReplacements, rexBuilder);
      RexShuttle correlateRexReplacer = ParameterizedQueryParameterReplacer.createRexParameterReplacer(functionParameters, correlateReplacements, rexBuilder);
      RexShuttle refIndexRexReplacer = ParameterizedQueryParameterReplacer.createRexParameterReplacer(functionParameters, refIndexReplacements, rexBuilder);
      RexArgumentReplacer compositeReplacer = new RexArgumentReplacer(
        correlateRelReplacer,
        refIndexRelReplacer,
        correlateRexReplacer,
        refIndexRexReplacer,
        correlationId);
      return rexNode.accept(compositeReplacer);
    }
  }
}

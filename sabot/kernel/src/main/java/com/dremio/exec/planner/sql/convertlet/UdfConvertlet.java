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
package com.dremio.exec.planner.sql.convertlet;

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.IDENTITY;

import com.dremio.exec.catalog.udf.CorrelatedUdfDetector;
import com.dremio.exec.catalog.udf.DremioScalarUserDefinedFunction;
import com.dremio.exec.catalog.udf.ParameterizedQueryParameterReplacer;
import com.dremio.exec.ops.UserDefinedFunctionExpander;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
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

public final class UdfConvertlet implements FunctionConvertlet {
  private final UserDefinedFunctionExpander expander;

  public UdfConvertlet(UserDefinedFunctionExpander expander) {
    this.expander = expander;
  }

  @Override
  public boolean matches(RexCall call) {
    SqlOperator operator = call.getOperator();
    if (!(operator instanceof SqlUserDefinedFunction)) {
      return false;
    }

    Function function = ((SqlUserDefinedFunction) operator).getFunction();
    return function instanceof DremioScalarUserDefinedFunction;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    SqlOperator operator = call.getOperator();
    Function function = ((SqlUserDefinedFunction) operator).getFunction();
    DremioScalarUserDefinedFunction dremioScalarUserDefinedFunction =
        (DremioScalarUserDefinedFunction) function;
    RexBuilder rexBuilder = cx.getRexBuilder();

    RexNode udfExpression = expander.expandScalar(dremioScalarUserDefinedFunction);
    if (!CorrelatedUdfDetector.hasCorrelatedUdf(udfExpression)) {
      udfExpression =
          ParameterizedQueryParameterReplacer.replaceParameters(
              udfExpression, function.getParameters(), call.getOperands(), rexBuilder);
    } else {
      RexCorrelVariable rexCorrelVariable = cx.getRexCorrelVariable();
      RexInputRefToFieldAccess replacer =
          new RexInputRefToFieldAccess(rexBuilder, rexCorrelVariable);
      List<RexNode> rewrittenCorrelateOperands =
          call.getOperands().stream()
              .map(operand -> operand.accept(replacer))
              .collect(Collectors.toList());
      udfExpression =
          RexArgumentReplacer.replaceArguments(
              udfExpression,
              function.getParameters(),
              rewrittenCorrelateOperands,
              call.getOperands(),
              rexCorrelVariable.id,
              rexBuilder);
    }

    // A user might have picked a whacky return type like VARCHAR for integer multiplication
    RelDataType udfSignatureType = call.getType();
    // Arrow types don't have a notion of nullability, so we might have to force the calcite type to
    // match
    udfExpression = rexBuilder.makeCast(udfSignatureType, udfExpression, true);

    // A UDF might return a literal, so we need to make it a call to match the function conversion
    // type system
    if (!(udfExpression instanceof RexCall)) {
      udfExpression = rexBuilder.makeCall(IDENTITY, udfExpression);
    }

    return (RexCall) udfExpression;
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
      List<RexNode> rewrittenOperands =
          subQuery.getOperands().stream()
              .map(operand -> operand.accept(refIndexRexReplacer))
              .collect(ImmutableList.toImmutableList());

      // This is because the operands are in relation to the outer query
      // And the RelNode is in relation to the inner query

      // This is more clear in the case of IN vs EXISTS:
      // IN($0, {
      //  LogicalProject(DEPTNO=[$6])
      //  ScanCrel(table=[cp.scott."EMP.json"], columns=[`EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`,
      // `SAL`, `DEPTNO`, `COMM`], splits=[1])
      // })
      //
      // EXISTS({
      //  LogicalFilter(condition=[=($6, $cor1.DEPTNO)])
      //  ScanCrel(table=[cp.scott."EMP.json"], columns=[`EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`,
      // `SAL`, `DEPTNO`, `COMM`], splits=[1])
      // })

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
          return RexSubQuery.some(
              rewrittenRelNode,
              rewrittenOperands,
              (SqlQuantifyOperator) subQuery.op,
              rewrittenCorrelateId);

        case ARRAY_QUERY_CONSTRUCTOR:
          return RexSubQuery.array(rewrittenRelNode, rewrittenCorrelateId);

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
      RelShuttle correlateRelReplacer =
          ParameterizedQueryParameterReplacer.createRelParameterReplacer(
              functionParameters, correlateReplacements, rexBuilder);
      RelShuttle refIndexRelReplacer =
          ParameterizedQueryParameterReplacer.createRelParameterReplacer(
              functionParameters, refIndexReplacements, rexBuilder);
      RexShuttle correlateRexReplacer =
          ParameterizedQueryParameterReplacer.createRexParameterReplacer(
              functionParameters, correlateReplacements, rexBuilder);
      RexShuttle refIndexRexReplacer =
          ParameterizedQueryParameterReplacer.createRexParameterReplacer(
              functionParameters, refIndexReplacements, rexBuilder);
      RexArgumentReplacer compositeReplacer =
          new RexArgumentReplacer(
              correlateRelReplacer,
              refIndexRelReplacer,
              correlateRexReplacer,
              refIndexRexReplacer,
              correlationId);
      return rexNode.accept(compositeReplacer);
    }
  }
}

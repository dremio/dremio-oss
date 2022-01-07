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
package com.dremio.exec.expr;

import java.util.Set;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.collect.Sets;

public class CachableExpressionSplit {

  public NamedExpression getCachedSplitNamedExpression() {
    return cachedSplitNamedExpression;
  }

  private final NamedExpression cachedSplitNamedExpression;

  // Set of splits on which this split depends
  private final Set<String> dependsOnSplitsInCachedVersion = Sets.newHashSet();

  // true, if the output of this split, is the output of the original expression
  private final boolean isOriginalExpression;

  private int totalReadersOfOutput = 0;

  // Should the llvm build be optimised if the split evaluated in gandiva
  private final boolean optimize;

  private final SupportedEngines.Engine executionEngine;

  // The number of extra if expressions created due to this split
  private final int numExtraIfExprs;

  // ValueVectorReadExpression representing the output of this split
  private final TypedFieldId typedFieldIdCachedVersion;

  public CachableExpressionSplit(NamedExpression namedExpression, boolean isOriginalExpression, OperatorContext operatorContext,
                                 SupportedEngines.Engine executionEngine, SplitDependencyTracker helper, int numExtraIfExprs, TypedFieldId fieldId) {
    LogicalExpression expression = CodeGenerationContextRemover.removeCodeGenContext(namedExpression.getExpr());
    this.cachedSplitNamedExpression = new NamedExpression(expression, namedExpression.getRef());
    this.executionEngine = executionEngine;
    this.isOriginalExpression = isOriginalExpression;
    this.dependsOnSplitsInCachedVersion.addAll(helper.getNamesOfDependencies());
    this.numExtraIfExprs = numExtraIfExprs;
    this.optimize = operatorContext.getOptions().getOption(ExecConstants.GANDIVA_OPTIMIZE)
      && (executionEngine.equals(SupportedEngines.Engine.GANDIVA) && expression.accept(new ExpressionWorkEstimator(), null) < operatorContext.getOptions()
      .getOption(ExecConstants.EXPR_COMPLEXITY_NO_OPTIMIZE_THRESHOLD));
    this.typedFieldIdCachedVersion = fieldId;
  }

  public CachableExpressionSplit(CachableExpressionSplit cachableExpressionSplit) {
    this.cachedSplitNamedExpression = new NamedExpression(cachableExpressionSplit.getCachedSplitNamedExpression().getExpr(), cachableExpressionSplit.getCachedSplitNamedExpression().getRef());
    this.executionEngine = cachableExpressionSplit.getExecutionEngine();
    this.isOriginalExpression = cachableExpressionSplit.isOriginalExpression();
    this.dependsOnSplitsInCachedVersion.addAll(cachableExpressionSplit.getDependsOnSplitsInCachedVersion());
    this.optimize = cachableExpressionSplit.getOptimize();
    this.numExtraIfExprs = cachableExpressionSplit.getOverheadDueToExtraIfs();
    this.typedFieldIdCachedVersion = cachableExpressionSplit.getTypedFieldIdCachedVersion();
    this.totalReadersOfOutput = cachableExpressionSplit.getTotalReadersOfOutput();
  }

  public CachableExpressionSplit(ExpressionSplit expressionSplit) {
    this.cachedSplitNamedExpression = new NamedExpression(expressionSplit.getNamedExpression().getExpr(), expressionSplit.getNamedExpression().getRef());
    this.executionEngine = expressionSplit.getExecutionEngine();
    this.isOriginalExpression = expressionSplit.isOriginalExpression();
    this.dependsOnSplitsInCachedVersion.addAll(expressionSplit.getDependsOnSplits());
    this.optimize = expressionSplit.getOptimize();
    this.numExtraIfExprs = expressionSplit.getOverheadDueToExtraIfs();
    this.typedFieldIdCachedVersion = expressionSplit.getTypedFieldId();
    this.totalReadersOfOutput = expressionSplit.getTotalReadersOfOutput();
  }

  // Estimates the cost of evaluating an expression
  // Can enhance this later to provide additional weight for specific Gandiva/Java functions
  // TODO: Improve the definition of work
  // For now, functions and if-expr contribute 1 to the work
  class ExpressionWorkEstimator extends AbstractExprVisitor<Double, Void, RuntimeException> {
    @Override
    public Double visitFunctionHolderExpression(FunctionHolderExpression holder, Void value) throws RuntimeException {
      double result = 1.0;

      for (LogicalExpression arg : holder.args) {
        result += arg.accept(this, null);
      }

      return result;
    }

    @Override
    public Double visitIfExpression(IfExpression ifExpr, Void value) throws RuntimeException {
      double result = 1.0;

      result += ifExpr.ifCondition.condition.accept(this, null);
      result += ifExpr.ifCondition.expression.accept(this, null);
      result += ifExpr.elseExpression.accept(this, null);

      return result;
    }

    @Override
    public Double visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
      double result = op.args.size() - 1;

      for (LogicalExpression arg : op.args) {
        result += arg.accept(this, null);
      }

      return result;
    }

    @Override
    public Double visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
      return 0.0;
    }

    @Override
    public Double visitCaseExpression(CaseExpression caseExpression, Void value) throws RuntimeException {
      double result = 1.0;

      for (LogicalExpression e : caseExpression) {
        result += e.accept(this, value);
      }
      return result;
    }
  }


  public Set<String> getDependsOnSplitsInCachedVersion() {
    return dependsOnSplitsInCachedVersion;
  }

  boolean isOriginalExpression() {
    return isOriginalExpression;
  }

  public boolean getOptimize() {
    return optimize;
  }

  public SupportedEngines.Engine getExecutionEngine() {
    return executionEngine;
  }

  int getOverheadDueToExtraIfs() {
    return numExtraIfExprs;
  }

  // increment the readers of this split
  void incrementReaders() {
    this.totalReadersOfOutput++;
  }

  public int getTotalReadersOfOutput() {
    return totalReadersOfOutput;
  }

  public String getOutputName() {
    return cachedSplitNamedExpression.getRef().getAsUnescapedPath();
  }

  public TypedFieldId getTypedFieldIdCachedVersion() {
    return typedFieldIdCachedVersion;
  }

}

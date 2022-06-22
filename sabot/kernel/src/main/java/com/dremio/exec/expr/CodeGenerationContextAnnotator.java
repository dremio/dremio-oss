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

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.exec.record.TypedFieldId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Visitor that replaces all nodes in the tree with Code Generation Context nodes.
 * These nodes carry the runtime context needed to select the code generation to use.
 */
public class CodeGenerationContextAnnotator extends AbstractExprVisitor<CodeGenContext, Void,
  RuntimeException> {
  // TODO(ramesh): This is a temporary kludge until the Gandiva patch supporting very large case statements
  // As this is temporary and will be removed soon (DX-34386 or DX-29559), no support key for this
  private static final int VERY_LARGE_CASE_THRESHOLD = 400;

  private boolean expHasComplexField = false;
  private boolean expHasVeryLargeCase = false;

  @Override
  public CodeGenContext visitFunctionHolderExpression(FunctionHolderExpression expr, Void
    value) {
    List<LogicalExpression> newArgs = Lists.newArrayList();
    for (LogicalExpression arg : expr.args) {
      CodeGenContext newArg = arg.accept(this, value);
      newArgs.add(newArg);
    }
    FunctionHolderExpression result = expr.copy(newArgs);
    CodeGenContext contextExpr = new CodeGenContext(result);
    for (LogicalExpression arg : result.args) {
      CodeGenContext context = (CodeGenContext) arg;
      // if expression and arg are not the same execution engine type then
      // we have mixed mode engine, remove sub expression support to indicate that.
      if (!context.getExecutionEngineForSubExpression().equals(contextExpr.getExecutionEngineForSubExpression())) {
        contextExpr.markSubExprIsMixed();
        return contextExpr;
      }
    }
    return contextExpr;
  }

  @Override
  public CodeGenContext visitBooleanOperator(BooleanOperator operator, Void value) {
    List<LogicalExpression> newArgs = Lists.newArrayList();
    for (LogicalExpression arg : operator.args) {
      newArgs.add(arg.accept(this, value));
    }
    BooleanOperator result = new BooleanOperator(operator.getName(), newArgs);
    CodeGenContext boolExpr = new CodeGenContext(result);
    for (LogicalExpression arg : result.args) {
      CodeGenContext context = (CodeGenContext) arg;
      // if expression and arg are not the same execution engine type then
      // we have mixed mode engine, remove sub expression support to indicate that.
      if (!context.getExecutionEngineForSubExpression().equals(boolExpr.getExecutionEngineForSubExpression())) {
        boolExpr.markSubExprIsMixed();
        return boolExpr;
      }
    }
    return boolExpr;
  }

  @Override
  public CodeGenContext visitIfExpression(IfExpression ifExpression, Void value) {
    IfExpression.IfCondition condition = ifExpression.ifCondition;
    CodeGenContext conditionExpression = condition.condition.accept(this, value);
    CodeGenContext thenExpression = condition.expression.accept(this, value);
    CodeGenContext elseExpression = ifExpression.elseExpression.accept(this, value);
    IfExpression.IfCondition newCondition = new IfExpression.IfCondition(conditionExpression,
      thenExpression);
    IfExpression result = IfExpression.newBuilder().setIfCondition(newCondition).setElse
      (elseExpression).build();
    CodeGenContext ifExpr = new CodeGenContext(result);
    if (conditionExpression.isMixedModeExecution() || thenExpression.isMixedModeExecution() ||
      elseExpression.isMixedModeExecution()) {
      ifExpr.markSubExprIsMixed();
    }
    return ifExpr;
  }

  @Override
  public CodeGenContext visitCaseExpression(CaseExpression caseExpression, Void value) throws RuntimeException {
    boolean isMixedMode = false;
    List<CaseExpression.CaseConditionNode> caseConditions = new ArrayList<>();
    for (CaseExpression.CaseConditionNode conditionNode : caseExpression.caseConditions) {
      LogicalExpression whenExpr = conditionNode.whenExpr.accept(this, value);
      LogicalExpression thenExpr = conditionNode.thenExpr.accept(this, value);
      CaseExpression.CaseConditionNode newConditionExpression = new CaseExpression.CaseConditionNode(whenExpr, thenExpr);
      caseConditions.add(newConditionExpression);
      if (((CodeGenContext) newConditionExpression.whenExpr).isMixedModeExecution() || ((CodeGenContext)
        newConditionExpression.thenExpr).isMixedModeExecution()) {
        isMixedMode = true;
      }
    }
    CodeGenContext elseExpr = caseExpression.elseExpr.accept(this, value);
    CaseExpression result = CaseExpression.newBuilder().setCaseConditions(caseConditions).setElseExpr(elseExpr).build();
    CodeGenContext caseExpr = new CodeGenContext(result);
    if (isMixedMode || elseExpr.isMixedModeExecution()) {
      caseExpr.markSubExprIsMixed();
    }
    if (caseExpression.caseConditions.size() > VERY_LARGE_CASE_THRESHOLD) {
      expHasVeryLargeCase = true;
    }
    return caseExpr;
  }

  @Override
  public CodeGenContext visitUnknown(LogicalExpression expression, Void value) {

    if (expression instanceof ValueVectorReadExpression) {
      expHasComplexField = expHasComplexField || isComplexField((ValueVectorReadExpression) expression);
    }
    // assert that the tree does not already have context nodes in it.
    Preconditions.checkArgument(!(expression instanceof CodeGenContext));
    return new CodeGenContext(expression);
  }

  private boolean isComplexField(ValueVectorReadExpression e) {
    TypedFieldId fieldId = e.getTypedFieldId();
    CompleteType type = fieldId.getIntermediateType() != null ? fieldId.getIntermediateType()
      : fieldId.getFinalType();

    boolean isComplexRead = fieldId.getFieldIds().length > 1;
    return isComplexRead || type.isComplex();
  }

  public boolean isExpHasComplexField() {
    return expHasComplexField;
  }

  public boolean isExpHasVeryLargeCase() {
    return expHasVeryLargeCase;
  }
}

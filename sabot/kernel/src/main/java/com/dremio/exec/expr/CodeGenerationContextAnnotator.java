/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.List;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Visitor that replaces all nodes in the tree with Code Generation Context nodes.
 * These nodes carry the runtime context needed to select the code generation to use.
 */
public class CodeGenerationContextAnnotator extends AbstractExprVisitor<CodeGenContext, Void,
  RuntimeException> {

  @Override
  public CodeGenContext visitFunctionHolderExpression(FunctionHolderExpression expr, Void
    value) {
    List<LogicalExpression> newArgs = Lists.newArrayList();
    for (LogicalExpression arg : expr.args) {
      newArgs.add(arg.accept(this, value));
    }
    LogicalExpression result = expr.copy(newArgs);
    CodeGenContext contextExpr = new CodeGenContext(result);
    return contextExpr;
  }

  @Override
  public CodeGenContext visitBooleanOperator(BooleanOperator operator, Void value) {
    List<LogicalExpression> newArgs = Lists.newArrayList();
    for (LogicalExpression arg : operator.args) {
      newArgs.add(arg.accept(this, value));
    }
    LogicalExpression result = new BooleanOperator(operator.getName(), newArgs);
    return new CodeGenContext(result);
  }

  @Override
  public CodeGenContext visitIfExpression(IfExpression ifExpression, Void value) {
    IfExpression.IfCondition condition = ifExpression.ifCondition;
    LogicalExpression conditionExpression = condition.condition.accept(this, value);
    LogicalExpression thenExpression = condition.expression.accept(this, value);
    LogicalExpression elseExpression = ifExpression.elseExpression.accept(this, value);
    IfExpression.IfCondition newCondition = new IfExpression.IfCondition(conditionExpression,
      thenExpression);
    IfExpression result = IfExpression.newBuilder().setIfCondition(newCondition).setElse
      (elseExpression).build();
    return new CodeGenContext(result);
  }

  @Override
  public CodeGenContext visitUnknown(LogicalExpression expression, Void value) {
    // assert that the tree does not already have context nodes in it.
    Preconditions.checkArgument(!(expression instanceof CodeGenContext));
    return new CodeGenContext(expression);
  }
}

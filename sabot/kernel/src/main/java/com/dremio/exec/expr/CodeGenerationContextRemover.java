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
import com.google.common.collect.Lists;

/**
 * Used to remove all of the code generation context nodes. By now the decision has been made on the
 * execution engine(Java/Gandiva) and this can be safely removed.
 *
 * Basically iterates over all the nodes and returns the child node held by the context nodes.
 */
public class CodeGenerationContextRemover extends AbstractExprVisitor<LogicalExpression, Void, RuntimeException> {

   public static LogicalExpression removeCodeGenContext(LogicalExpression expr) {
     CodeGenerationContextRemover contextRemover = new CodeGenerationContextRemover();
     return expr.accept(contextRemover, null);
   }

   @Override
   public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression expr, Void
     value) {
    List<LogicalExpression> newArgs = Lists.newArrayList();
    for (LogicalExpression arg : expr.args) {
      newArgs.add(arg.accept(this, value));
    }
    return expr.copy(newArgs);
  }

  @Override
  public LogicalExpression visitBooleanOperator(BooleanOperator operator, Void value) {
    List<LogicalExpression> newArgs = Lists.newArrayList();
    for (LogicalExpression arg : operator.args) {
      newArgs.add(arg.accept(this, value));
    }
    LogicalExpression result = new BooleanOperator(operator.getName(), newArgs);
    return result;
  }

  @Override
  public LogicalExpression visitIfExpression(IfExpression ifExpression, Void value) {
    IfExpression.IfCondition condition = ifExpression.ifCondition;
    LogicalExpression conditionExpression = condition.condition.accept(this, value);
    LogicalExpression thenExpression = condition.expression.accept(this, value);
    LogicalExpression elseExpression = ifExpression.elseExpression.accept(this, value);
    IfExpression.IfCondition newCondition = new IfExpression.IfCondition(conditionExpression,
      thenExpression);
    return IfExpression.newBuilder().setIfCondition(newCondition).setElse(elseExpression).build();
  }

  @Override
  public LogicalExpression visitUnknown(LogicalExpression expression, Void value) {
    return expression;
  }

}

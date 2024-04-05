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
package com.dremio.sabot.op.llvm;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.visitors.AbstractExprVisitor;

/**
 * Approximately estimates the cost evaluating an expression by calculating the number of internal
 * nodes in it.
 */
// Can enhance this later to provide additional weight for specific Gandiva/Java functions
// TODO: Improve the definition of work
// For now, functions, if-expr and case statements contribute 1 to the work
public class ExpressionWorkEstimator extends AbstractExprVisitor<Double, Void, RuntimeException> {
  @Override
  public Double visitFunctionHolderExpression(FunctionHolderExpression holder, Void value)
      throws RuntimeException {
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
  public Double visitCaseExpression(CaseExpression caseExpression, Void value)
      throws RuntimeException {
    double result = 1.0;

    for (LogicalExpression e : caseExpression) {
      result += e.accept(this, value);
    }
    return result;
  }
}

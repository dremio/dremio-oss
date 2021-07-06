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

package com.dremio.common.expression.visitors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.IfExpression.IfCondition;
import com.dremio.common.expression.LogicalExpression;
import com.google.common.collect.Lists;

public class ConditionalExprOptimizer extends AbstractExprVisitor<LogicalExpression, Void, RuntimeException> {

  public static ConditionalExprOptimizer INSTANCE = new ConditionalExprOptimizer();

  @Override
  public LogicalExpression visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {

    List<LogicalExpression> newArgs = Lists.newArrayList();

    newArgs.addAll(op.args);

    Collections.sort(newArgs, costComparator);

    return new BooleanOperator(op.getName(), newArgs);
  }


  @Override
  public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression holder, Void value) throws RuntimeException {
    List<LogicalExpression> args = Lists.newArrayList();
    for (int i = 0; i < holder.args.size(); ++i) {
      LogicalExpression newExpr = holder.args.get(i).accept(this, value);
      assert newExpr != null;
      args.add(newExpr);
    }

    //replace with a new function call, since its argument could be changed.

    return holder.copy(args);
  }

  @Override
  public LogicalExpression visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    return e;
  }


  @Override
  public LogicalExpression visitIfExpression(IfExpression ifExpr, Void value) throws RuntimeException{
    LogicalExpression newElseExpr = ifExpr.elseExpression.accept(this, value);
    IfCondition conditions = ifExpr.ifCondition;

    LogicalExpression newCondition = conditions.condition.accept(this, value);
    LogicalExpression newExpr = conditions.expression.accept(this, value);
    conditions = new IfExpression.IfCondition(newCondition, newExpr);

    return IfExpression.newBuilder().setElse(newElseExpr).setIfCondition(conditions).build();
  }

  @Override
  public LogicalExpression visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    throw new UnsupportedOperationException("FunctionCall is not expected here. "
        + "It should have been converted to FunctionHolderExpression in materialization");
  }

  @Override
  public LogicalExpression visitCastExpression(CastExpression cast, Void value) throws RuntimeException {
    throw new UnsupportedOperationException("CastExpression is not expected here. "
        + "It should have been converted to FunctionHolderExpression in materialization");
  }


  @Override
  public LogicalExpression visitConvertExpression(ConvertExpression cast, Void value) throws RuntimeException {
    throw new UnsupportedOperationException("ConvertExpression is not expected here. "
        + "It should have been converted to FunctionHolderExpression in materialization");
  }

  @Override
  public LogicalExpression visitCaseExpression(CaseExpression caseExpression, Void value) throws RuntimeException {
    List<CaseExpression.CaseConditionNode> newConditions = new ArrayList<>();
    LogicalExpression newElseExpr = caseExpression.elseExpr.accept(this, value);

    for (CaseExpression.CaseConditionNode conditionNode : caseExpression.caseConditions) {
      LogicalExpression newWhen = conditionNode.whenExpr.accept(this, value);
      LogicalExpression newThen = conditionNode.thenExpr.accept(this, value);
      CaseExpression.CaseConditionNode condition = new CaseExpression.CaseConditionNode(newWhen, newThen);
      newConditions.add(condition);
    }
    return CaseExpression.newBuilder().setCaseConditions(newConditions).setElseExpr(newElseExpr).build();
  }

  private static Comparator<LogicalExpression> costComparator = new Comparator<LogicalExpression> () {
    @Override
    public int compare(LogicalExpression e1, LogicalExpression e2) {
      return Integer.compare(e1.getCumulativeCost(), e2.getCumulativeCost());
    }
  };



}

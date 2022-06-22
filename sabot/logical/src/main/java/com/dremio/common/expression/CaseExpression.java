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
package com.dremio.common.expression;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;

import com.dremio.common.expression.visitors.ExprVisitor;
import com.google.common.collect.ImmutableList;

public class CaseExpression extends LogicalExpressionBase {
  public final List<CaseConditionNode> caseConditions;
  public final LogicalExpression elseExpr;
  public final CompleteType outputType;
  private final int sizeOfChildren;

  public CaseExpression(List<CaseConditionNode> caseConditions, LogicalExpression elseExpr, CompleteType outputType) {
    if (caseConditions == null) {
      caseConditions = Collections.emptyList();
    } else {
      if (!(caseConditions instanceof ImmutableList)) {
        caseConditions = ImmutableList.copyOf(caseConditions);
      }
    }
    this.caseConditions = caseConditions;
    this.elseExpr = elseExpr;
    this.outputType = outputType;
    this.sizeOfChildren = caseConditions.size()*2 + 1;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitCaseExpression(this, value);
  }

  @Override
  public int getSizeOfChildren() {
    return sizeOfChildren;
  }

  @Override
  @Nonnull
  public Iterator<LogicalExpression> iterator() {
    return new Iterator<LogicalExpression>() {
      private int currentExprIdx = 0;

      @Override
      public boolean hasNext() {
        return currentExprIdx < sizeOfChildren;
      }

      @Override
      public LogicalExpression next() {
        if (currentExprIdx < sizeOfChildren - 1) {
          final boolean isWhen = currentExprIdx % 2 == 0;
          final CaseConditionNode node = caseConditions.get(currentExprIdx / 2);
          currentExprIdx++;
          return (isWhen) ? node.whenExpr : node.thenExpr;
        } else if (currentExprIdx == sizeOfChildren - 1) {
          currentExprIdx++;
          return elseExpr;
        }
        return null;
      }
    };
  }

  @Override
  public CompleteType getCompleteType() {
    if (outputType != null) {
      return outputType;
    }

    CompleteType type = elseExpr.getCompleteType();
    for (CaseConditionNode e : caseConditions) {
      type = type.merge(e.thenExpr.getCompleteType());
    }
    return type;
  }

  @Override
  public int getCumulativeCost() {
    // return the average cost of operands for a boolean "and" | "or"
    int cost = this.getSelfCost();

    int i = 0;
    for (LogicalExpression e : this) {
      cost += e.getCumulativeCost();
      i++;
    }

    return cost / i;
  }

  public static class CaseConditionNode {
    public final LogicalExpression whenExpr;
    public final LogicalExpression thenExpr;

    public CaseConditionNode(LogicalExpression whenExpr, LogicalExpression thenExpr) {
      this.whenExpr = whenExpr;
      this.thenExpr = thenExpr;
    }
  }

  public static class Builder {
    private List<CaseConditionNode> caseConditions;
    private LogicalExpression elseExpr;
    private CompleteType outputType;

    public Builder setCaseConditions(List<CaseConditionNode> caseConditions) {
      this.caseConditions = caseConditions;
      return this;
    }

    public Builder setElseExpr(LogicalExpression elseExpr) {
      this.elseExpr = elseExpr;
      return this;
    }

    public Builder setOutputType(CompleteType outputType) {
      this.outputType = outputType;
      return this;
    }

    public CaseExpression build() {
      return new CaseExpression(caseConditions, elseExpr, outputType);
    }
  }

  public static Builder newBuilder() {
    return new CaseExpression.Builder();
  }
}

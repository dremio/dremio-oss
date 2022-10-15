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

import java.util.Iterator;

import com.dremio.common.expression.visitors.ExprVisitor;
import com.google.common.base.Preconditions;

public final class IfExpression extends LogicalExpressionBase {
  public static final boolean ALLOW_MIXED_DECIMALS = true;

  public final IfCondition ifCondition;
  public final LogicalExpression elseExpression;
  public final CompleteType outputType;

  private IfExpression(IfCondition conditions, LogicalExpression elseExpression, CompleteType outputType) {
    this.ifCondition = conditions;
    this.elseExpression = elseExpression;
    this.outputType = outputType;
  }

  public static class IfCondition{
    public final LogicalExpression condition;
    public final LogicalExpression expression;

    public IfCondition(LogicalExpression condition, LogicalExpression expression) {
      //logger.debug("Generating IfCondition {}, {}", condition, expression);

      this.condition = condition;
      this.expression = expression;
    }

  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitIfExpression(this, value);
  }

  public static class Builder {
    IfCondition conditions;
    private LogicalExpression elseExpression;
    private CompleteType outputType;

    public Builder setElse(LogicalExpression elseExpression) {
      this.elseExpression = elseExpression;
            return this;
    }

    public Builder setIfCondition(IfCondition conditions) {
      this.conditions = conditions;
      return this;
    }

    public Builder setOutputType(CompleteType outputType) {
      this.outputType = outputType;
      return this;
    }

    public IfExpression build(){
      Preconditions.checkNotNull(conditions);
      return new IfExpression(conditions, elseExpression, outputType);
    }

  }

  @Override
  public CompleteType getCompleteType() {
    if (outputType != null) {
      return outputType;
    }

    return ifCondition.expression.getCompleteType().merge(elseExpression.getCompleteType(), ALLOW_MIXED_DECIMALS);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return new Iterator<LogicalExpression>() {
      private int currentExprIdx = 0;
      @Override
      public boolean hasNext() {
        return currentExprIdx < 3;
      }

      @Override
      public LogicalExpression next() {
        switch (currentExprIdx++) {
          case 0:
            return ifCondition.condition;
          case 1:
            return ifCondition.expression;
          case 2:
            return elseExpression;
        }
        return null;
      }
    };
  }

  @Override
  public int getSizeOfChildren() {
    return 3;
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

    return (int) (cost / i) ;
  }

}

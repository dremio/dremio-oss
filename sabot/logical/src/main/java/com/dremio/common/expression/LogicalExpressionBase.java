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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "type" })
public abstract class LogicalExpressionBase implements LogicalExpression {

  // was pushed in 3.0 have to retain this so that serde does not break.
  private final EvaluationType evaluationType;

  protected LogicalExpressionBase() {
    super();
    this.evaluationType = new EvaluationType();
    evaluationType.addEvaluationType(EvaluationType.ExecutionType.JAVA);
  }

  protected void i(StringBuilder sb, int indent) {
    for (int i = 0; i < indent; i++) {
      sb.append("  ");
    }
  }

  @Override
  public CompleteType getCompleteType() {
    throw new UnsupportedOperationException(String.format("The type of %s doesn't currently support LogicalExpression.getCompleteType().", this.getClass().getName()));
  }

  @Override
  @JsonIgnore
  public int getSelfCost() {
    return 0;
  }

  @Override
  @JsonIgnore
  public int getCumulativeCost() {
    int cost = this.getSelfCost();

    for (LogicalExpression e : this) {
      cost += e.getCumulativeCost();
    }

    return cost;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    ExpressionStringBuilder esb = new ExpressionStringBuilder();
    this.accept(esb, sb);
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!this.getClass().equals(obj)) {
      return false;
    }
    return this.toString().equals(obj.toString());
  }
}

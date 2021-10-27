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

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.EvaluationType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.visitors.ExprVisitor;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;

public class HoldingContainerExpression implements LogicalExpression{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HoldingContainerExpression.class);

  final HoldingContainer container;
  // was pushed in 3.0 have to retain this so that serde does not break.
  private final EvaluationType evaluationType;

  public HoldingContainerExpression(HoldingContainer container) {
    this.container = container;
    this.evaluationType = new EvaluationType();
    evaluationType.addEvaluationType(EvaluationType.ExecutionType.JAVA);
  }

  @Override
  public CompleteType getCompleteType() {
    return container.getCompleteType();
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }


  public HoldingContainer getContainer() {
    return container;
  }

  @Override
  public int getSelfCost() {
    return 0;  // TODO
  }

  @Override
  public int getCumulativeCost() {
    return 0; // TODO
  }

}

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

import java.util.Collections;
import java.util.Iterator;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.EvaluationType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.visitors.ExprVisitor;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;

public class HoldingContainerExpression implements LogicalExpression{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HoldingContainerExpression.class);

  final HoldingContainer container;
  private final EvaluationType evaluationType;

  public HoldingContainerExpression(HoldingContainer container) {
    this.container = container;
    this.evaluationType = new EvaluationType();
    addEvaluationType(EvaluationType.ExecutionType.JAVA);
  }

  @Override
  public boolean isEvaluationTypeSupported(EvaluationType.ExecutionType executionType) {
    return evaluationType.isEvaluationTypeSupported(executionType);
  }

  @Override
  public void addEvaluationType(EvaluationType.ExecutionType executionType) {
    evaluationType.addEvaluationType(executionType);
  }

  @Override
  public EvaluationType getEvaluationType() {
    return evaluationType;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Collections.emptyIterator();
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

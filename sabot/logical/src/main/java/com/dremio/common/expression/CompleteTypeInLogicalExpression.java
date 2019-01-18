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
package com.dremio.common.expression;

import java.util.Iterator;

import com.dremio.common.expression.visitors.ExprVisitor;

/**
 * MajorTypeInLogicalExpression is a LogicalExpression, which wraps a given @{TypeProtos.MajorType}
 */
public class CompleteTypeInLogicalExpression implements LogicalExpression {
  private CompleteType type;
  private final EvaluationType evaluationType;

  public CompleteTypeInLogicalExpression(CompleteType type) {
    this.type = type;
    this.evaluationType = new EvaluationType();
    evaluationType.addEvaluationType(EvaluationType.ExecutionType.JAVA);  }

  @Override
  public CompleteType getCompleteType() {
    return type;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    throw new UnsupportedOperationException();
  }

  public int getSelfCost() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getCumulativeCost() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    throw new UnsupportedOperationException();
  }
}

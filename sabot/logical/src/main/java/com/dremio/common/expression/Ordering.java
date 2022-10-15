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
import com.google.common.collect.Iterators;

public class Ordering extends LogicalExpressionBase implements Iterable<LogicalExpression> {
  private final LogicalExpression field;
  private final String direction;
  private final String nullDirection;

  public Ordering(LogicalExpression field, String direction, String nullDirection) {
    this.field = field;
    this.direction = direction;
    this.nullDirection = nullDirection;
  }

  public LogicalExpression getField() {
    return field;
  }

  public String getDirection() {
    return direction;
  }

  public String getNullDirection() {
    return nullDirection;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E{
    return visitor.visitOrdering(this, value);
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.singletonIterator(field);
  }

  @Override
  public int getSizeOfChildren() {
    return 1;
  }

  @Override
  public CompleteType getCompleteType() {
    throw new UnsupportedOperationException(
      "No return type as FunctionCall is not a materialized expression");
  }

  @Override
  public String toString() {
    return "OrderExpression [field=" + field + ", direction=" + direction + ", nullDirection=" + nullDirection + "]";
  }
}

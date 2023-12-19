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

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.visitors.ExprVisitor;
import com.google.common.collect.ImmutableList;

public final class ArrayLiteralExpression extends LogicalExpressionBase {
  private final ImmutableList<LogicalExpression> items;

  public ArrayLiteralExpression(ImmutableList<LogicalExpression> items) {
    this.items = items;
  }

  public ImmutableList<LogicalExpression> getItems() {
    return items;
  }

  @Override
  public CompleteType getCompleteType() {
    List<Field> arrayItemFields = ImmutableList.of(items.get(0).getCompleteType().toField("array item"));
    CompleteType completeType = new CompleteType(
    ArrowType.List.INSTANCE,
    arrayItemFields);
    return completeType;
  }

  @Override
  public int getSizeOfChildren() {
    return items.size();
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitArrayLiteralExpression(this, value);
  }
}

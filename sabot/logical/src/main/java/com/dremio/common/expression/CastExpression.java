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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import com.dremio.common.expression.visitors.ExprVisitor;
import com.dremio.common.types.TypeProtos.MajorType;
import com.google.common.collect.Iterators;

public class CastExpression extends LogicalExpressionBase implements Iterable<LogicalExpression>{

  private final LogicalExpression input;
  private final MajorType type;

  public CastExpression(LogicalExpression input, MajorType type) {
    this.input = input;
    this.type = checkNotNull(type, "Type cannot be null");
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitCastExpression(this, value);
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.singletonIterator(input);
  }

  @Override
  public int getSizeOfChildren() {
    return 1;
  }

  public LogicalExpression getInput() {
    return input;
  }

  public MajorType retrieveMajorType(){
    return type;
  }

  @Override
  public CompleteType getCompleteType() {
    return CompleteType.fromMajorType(type);
  }

  @Override
  public String toString() {
    return "CastExpression [input=" + input + ", type=" + type + "]";
  }



}

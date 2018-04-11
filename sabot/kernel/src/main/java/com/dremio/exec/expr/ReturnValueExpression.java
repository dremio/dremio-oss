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

import java.util.Iterator;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.visitors.ExprVisitor;
import com.google.common.collect.Iterators;

public class ReturnValueExpression implements LogicalExpression {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReturnValueExpression.class);

  private LogicalExpression child;
  private boolean returnTrueOnOne;

  public ReturnValueExpression(LogicalExpression child) {
    this(child, true);
  }

  public ReturnValueExpression(LogicalExpression child, boolean returnTrueOnOne) {
    this.child = child;
    this.returnTrueOnOne = returnTrueOnOne;
  }

  public LogicalExpression getChild() {
    return child;
  }

  @Override
  public CompleteType getCompleteType() {
    return CompleteType.NULL;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.singletonIterator(child);
  }

  public boolean isReturnTrueOnOne() {
    return returnTrueOnOne;
  }

  public int getSelfCost() {
    throw new UnsupportedOperationException(String.format("The type of %s doesn't currently support LogicalExpression.getSelfCost().", this.getClass().getCanonicalName()));
  }

  public int getCumulativeCost() {
    throw new UnsupportedOperationException(String.format("The type of %s doesn't currently support LogicalExpression.getCumulativeCost().", this.getClass().getCanonicalName()));
  }

}

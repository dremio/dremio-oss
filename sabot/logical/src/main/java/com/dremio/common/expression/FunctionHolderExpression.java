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

import com.dremio.common.expression.fn.FunctionHolder;
import com.dremio.common.expression.visitors.ExprVisitor;
import com.google.common.collect.ImmutableList;

public abstract class FunctionHolderExpression extends LogicalExpressionBase {
  public final List<LogicalExpression> args;
  public final String nameUsed;

  public FunctionHolderExpression(String nameUsed, List<LogicalExpression> args) {
    if (args == null) {
      args = Collections.emptyList();
    } else {
      if (!(args instanceof ImmutableList)) {
        args = ImmutableList.copyOf(args);
      }
    }
    this.args = args;
    this.nameUsed = nameUsed;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return args.iterator();
  }

  @Override
  public int getSizeOfChildren() {
    return args.size();
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitFunctionHolderExpression(this, value);
  }

  /**
   * A function can have multiple names, it returns the function name used in the query
   * @return
   */
  public String getName() {
    return nameUsed;
  }

  /**
   * constant input expected for i'th argument?
   * @param i
   * @return
   */
  public abstract boolean argConstantOnly(int i);

  /**
   * @return aggregating function or not
   */
  public abstract boolean isAggregating();

  /**
   * is the function output non-deterministic?
   */
  public abstract boolean isRandom();

  /**
   * @ return a copy of FunctionHolderExpression, with passed in argument list.
   */
  public abstract FunctionHolderExpression copy(List<LogicalExpression> args);

  /** Return the underlying function implementation holder. */
  public abstract FunctionHolder getHolder();

  @Override
  public String toString() {
    return "FunctionHolderExpression [args=" + args + ", name=" + getName() + ", returnType=" + getCompleteType() + ", isRandom=" + isRandom() + "]";
  }



}

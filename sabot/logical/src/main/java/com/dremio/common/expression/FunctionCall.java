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

import com.dremio.common.expression.visitors.ExprVisitor;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class FunctionCall extends LogicalExpressionBase implements Iterable<LogicalExpression> {
  private final String name;
  public final List<LogicalExpression> args;

  public FunctionCall(String name, List<LogicalExpression> args) {
    this.name = name;
    if (args == null) {
      args = Collections.emptyList();
    } else {
      if (!(args instanceof ImmutableList)) {
        args = ImmutableList.copyOf(args);
      }
    }
    this.args = args;
  }

  public String getName() {
    return name;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitFunctionCall(this, value);
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
  public CompleteType getCompleteType() {
    throw new UnsupportedOperationException(
        "No return type as FunctionCall is not a materialized expression");
  }

  @Override
  public String toString() {
    final int maxLen = 10;
    return "FunctionCall [func="
        + name
        + ", args="
        + (args != null ? args.subList(0, Math.min(args.size(), maxLen)) : null)
        + "]";
  }
}

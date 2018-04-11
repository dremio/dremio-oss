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
import com.google.common.collect.Iterators;

public class TypedNullConstant extends LogicalExpressionBase {

    private final CompleteType type;

    public TypedNullConstant(CompleteType type) {
      this.type = type;
    }

    @Override
    public CompleteType getCompleteType() {
      return this.type;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitNullConstant(this, value);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }

}

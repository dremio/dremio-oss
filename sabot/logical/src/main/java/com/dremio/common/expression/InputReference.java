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

import com.dremio.common.expression.visitors.ExprVisitor;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * A form reference that also declares both the specific field to retrieve and which input ordinal this expression is
 * associated with, allowing a single expression tree that references multiple inputs.
 *
 * RexInputRef does this implicitly by the rules of how operators combine fields and using indices. Here we try to make
 * things a little bit more explicit.
 */
@JsonTypeName("input")
public class InputReference extends LogicalExpressionBase {

  private final int inputOrdinal;
  private final FieldReference reference;

  public InputReference(int i, SchemaPath sp) {
    this.inputOrdinal = i;
    this.reference = new FieldReference(sp);
  }

  @JsonCreator
  public InputReference(@JsonProperty("inputOrdinal") int inputOrdinal, @JsonProperty("reference") FieldReference reference) {
    this.inputOrdinal = inputOrdinal;
    this.reference = reference;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitInputReference(this, value);
  }

  public int getInputOrdinal() {
    return inputOrdinal;
  }

  public FieldReference getReference() {
    return reference;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Collections.<LogicalExpression>singleton(reference).iterator();
  }

  @Override
  public int getSizeOfChildren() {
    return 1;
  }

}

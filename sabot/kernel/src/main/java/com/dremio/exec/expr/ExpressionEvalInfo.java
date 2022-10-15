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
package com.dremio.exec.expr;

import java.util.Objects;

import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.LogicalExpression;

public class ExpressionEvalInfo {
  private final LogicalExpression exp;
  private final ClassGenerator.BlockCreateMode mode;
  private final boolean allowInnerMethods;
  private final FieldReference currentReference;

  public ExpressionEvalInfo(LogicalExpression exp, ClassGenerator.BlockCreateMode mode,
                            boolean allowInnerMethods, FieldReference currentReference) {
    this.exp = exp;
    this.mode = mode;
    this.allowInnerMethods = allowInnerMethods;
    this.currentReference = currentReference;
  }

  public LogicalExpression getExp() {
    return exp;
  }

  public ClassGenerator.BlockCreateMode getMode() {
    return mode;
  }

  public boolean isAllowInnerMethods() {
    return allowInnerMethods;
  }

  public FieldReference getCurrentReference() {
    return currentReference;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ExpressionEvalInfo that = (ExpressionEvalInfo) o;
    return allowInnerMethods == that.allowInnerMethods && mode == that.mode &&
      Objects.equals(currentReference, that.currentReference) &&
      this.exp.accept(new EqualityVisitor(), that.exp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, allowInnerMethods, currentReference) + exp.accept(new HashVisitor(), null);
  }

  @Override
  public String toString() {
    return "[expr= {" + exp + "} mode = {" + mode + "} " + allowInnerMethods + "]";
  }
}

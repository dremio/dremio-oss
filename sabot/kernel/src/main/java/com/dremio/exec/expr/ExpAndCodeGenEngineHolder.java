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

import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.logical.data.NamedExpression;
import java.util.Objects;

public class ExpAndCodeGenEngineHolder {

  private final NamedExpression namedExpression;
  // The below field   expressionSplitter is used once and we can make it null after that and also
  // it is not used in the equals method,
  // hence keeping it non final and mutable, but rest of the fields are and should be immutable
  private ExpressionSplitter expressionSplitter;

  private final SupportedEngines.CodeGenOption codeGenOption;

  public ExpressionSplitter getExpressionSplitter() {
    return expressionSplitter;
  }

  public NamedExpression getNamedExpression() {
    return namedExpression;
  }

  public SupportedEngines.CodeGenOption getCodeGenOption() {
    return codeGenOption;
  }

  public ExpAndCodeGenEngineHolder(
      NamedExpression namedExpression,
      SupportedEngines.CodeGenOption codeGenOption,
      ExpressionSplitter expressionSplitter) {
    // the expression we store in cache shouldn't be annotated with CodeGenContextInfo
    if (namedExpression.getExpr() instanceof CodeGenContext) {
      this.namedExpression =
          new NamedExpression(
              CodeGenerationContextRemover.removeCodeGenContext(namedExpression.getExpr()),
              namedExpression.getRef());
    } else {
      this.namedExpression = namedExpression;
    }
    this.codeGenOption = codeGenOption;
    this.expressionSplitter = expressionSplitter;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExpAndCodeGenEngineHolder that = (ExpAndCodeGenEngineHolder) o;
    return this.codeGenOption == that.codeGenOption
        && this.namedExpression
            .getExpr()
            .accept(new EqualityVisitor(), that.namedExpression.getExpr());
  }

  @Override
  public int hashCode() {
    return Objects.hash(codeGenOption)
        + this.namedExpression.getExpr().accept(new HashVisitor(), null);
  }

  public void setExpressionSplitter(ExpressionSplitter expressionSplitter) {
    this.expressionSplitter = expressionSplitter;
  }
}

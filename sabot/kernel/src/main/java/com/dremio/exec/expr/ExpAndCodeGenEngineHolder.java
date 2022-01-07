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

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.logical.data.NamedExpression;

public class ExpAndCodeGenEngineHolder {
  //The below fields (namedExpression and expressionSplitter) are used once and we can make it null after that and also these are not used in the equals method,
  // hence keeping it non final and mutable, but rest of the fields are and should be immutable
  private NamedExpression namedExpression;
  private ExpressionSplitter expressionSplitter;

  private final SupportedEngines.CodeGenOption codeGenOption;
  private final LogicalExpression originalExp;

  public ExpressionSplitter getExpressionSplitter() {
    return expressionSplitter;
  }

  public NamedExpression getNamedExpression() {
    return namedExpression;
  }

  public SupportedEngines.CodeGenOption getCodeGenOption() {
    return codeGenOption;
  }

  public ExpAndCodeGenEngineHolder(NamedExpression namedExpression, SupportedEngines.CodeGenOption codeGenOption, ExpressionSplitter expressionSplitter, LogicalExpression originalExp) {
    this.namedExpression = namedExpression;
    this.codeGenOption = codeGenOption;
    this.expressionSplitter = expressionSplitter;
    this.originalExp = originalExp;
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
    return this.codeGenOption == that.codeGenOption &&
      this.originalExp.accept(new EqualityVisitor(), that.originalExp);
  }

  public LogicalExpression getOriginalExp() {
    return originalExp;
  }


  @Override
  public int hashCode() {
    return Objects.hash(codeGenOption) + this.originalExp.accept(new HashVisitor(), null);
  }


  public void setNamedExpression(NamedExpression namedExpression) {
    this.namedExpression = namedExpression;
  }

  public void setExpressionSplitter(ExpressionSplitter expressionSplitter) {
    this.expressionSplitter = expressionSplitter;
  }
}

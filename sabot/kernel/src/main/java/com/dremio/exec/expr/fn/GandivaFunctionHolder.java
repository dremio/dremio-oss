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
package com.dremio.exec.expr.fn;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.sun.codemodel.JVar;

import java.util.List;

public class GandivaFunctionHolder extends AbstractFunctionHolder {
  private final CompleteType[] argTypes;

  private final CompleteType returnType;

  private final String name;

  public GandivaFunctionHolder(CompleteType[] argTypes, CompleteType returnType, String name) {
    this.argTypes = argTypes;
    this.returnType = returnType;
    this.name = name;
  }

  @Override
  public JVar[] renderStart(ClassGenerator<?> g, CompleteType resolvedOutput, ClassGenerator.HoldingContainer[] inputVariables, FunctionErrorContext errorContext) {
    throw new UnsupportedOperationException("Gandiva code generation is handled during build.");
  }

  @Override
  public ClassGenerator.HoldingContainer renderEnd(ClassGenerator<?> g, CompleteType resolvedOutput, ClassGenerator.HoldingContainer[] inputVariables, JVar[] workspaceJVars) {
    throw new UnsupportedOperationException("Gandiva code generation is handled during build.");
  }

  @Override
  public FunctionHolderExpression getExpr(String name, List<LogicalExpression> args) {
    return new GandivaFunctionHolderExpression(name, this, args);
  }

  @Override
  public CompleteType getParamType(int i) {
    return argTypes[i];
  }

  @Override
  public int getParamCount() {
    return argTypes.length;
  }

  // TODO: https://dremio.atlassian.net/browse/GDV-98
  // following will be done in task above.
  @Override
  public boolean checkPrecisionRange() {
    return false;
  }

  @Override
  public boolean isReturnTypeIndependent() {
    return false;
  }

  @Override
  public FunctionTemplate.NullHandling getNullHandling() {
    return null;
  }

  public CompleteType getReturnType() {
    return returnType;
  }
}

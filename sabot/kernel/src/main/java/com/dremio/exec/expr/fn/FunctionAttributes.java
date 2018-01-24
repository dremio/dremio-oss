/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionCostCategory;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionSyntax;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.fn.BaseFunctionHolder.ValueReference;
import com.dremio.exec.expr.fn.BaseFunctionHolder.WorkspaceReference;

/**
 * Attributes of a function
 * Those are used in code generation and optimization.
 */
public class FunctionAttributes {
  private final FunctionScope scope;
  private final NullHandling nullHandling;
  private final boolean isBinaryCommutative;
  private final boolean isDeterministic;
  private final boolean isDynamic;
  private final FunctionSyntax syntax;
  private final String[] registeredNames;
  private final ValueReference[] parameters;
  private final ValueReference returnValue;
  private final WorkspaceReference[] workspaceVars;
  private final FunctionCostCategory costCategory;
  private final OutputDerivation derivation;

  public FunctionAttributes(FunctionScope scope, NullHandling nullHandling, boolean isBinaryCommutative,
      boolean isDeterministic, boolean isDynamic, FunctionSyntax syntax, String[] registeredNames,
      ValueReference[] parameters, ValueReference returnValue, WorkspaceReference[] workspaceVars,
      FunctionCostCategory costCategory, OutputDerivation derivation) {
    super();
    this.scope = scope;
    this.nullHandling = nullHandling;
    this.isBinaryCommutative = isBinaryCommutative;
    this.isDeterministic = isDeterministic;
    this.isDynamic = isDynamic;
    this.syntax = syntax;
    this.registeredNames = registeredNames;
    this.parameters = parameters;
    this.returnValue = returnValue;
    this.workspaceVars = workspaceVars;
    this.costCategory = costCategory;
    this.derivation = derivation;
  }

  public FunctionScope getScope() {
    return scope;
  }

  public OutputDerivation getDerivation() {
    return derivation;
  }

  public NullHandling getNullHandling() {
    return nullHandling;
  }

  public boolean isBinaryCommutative() {
    return isBinaryCommutative;
  }

  @Deprecated
  public boolean isRandom() {
    return !isDeterministic;
  }

  public boolean isDeterministic() {
    return isDeterministic;
  }

  public boolean isDynamic() {
    return isDynamic;
  }

  public FunctionSyntax getSyntax() {
    return syntax;
  }

  public String[] getRegisteredNames() {
    return registeredNames;
  }

  public ValueReference[] getParameters() {
    return parameters;
  }

  public ValueReference getReturnValue() {
    return returnValue;
  }

  public WorkspaceReference[] getWorkspaceVars() {
    return workspaceVars;
  }

  public FunctionCostCategory getCostCategory() {
    return costCategory;
  }


}

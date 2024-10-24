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
package com.dremio.exec.expr.fn;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.sun.codemodel.JVar;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class GandivaFunctionHolder extends AbstractFunctionHolder {
  private final CompleteType[] argTypes;
  private final CompleteType returnType;
  private final String name;
  private final boolean returnTypeIndependent;

  public GandivaFunctionHolder(CompleteType[] argTypes, CompleteType returnType, String name) {
    this.argTypes = argTypes;
    this.returnType = returnType;
    this.name = name;
    // a function that processes decimals. we need to determine output return type.
    // it is based on input type.
    if (returnType.getType().getTypeID() == ArrowType.ArrowTypeID.Decimal) {
      this.returnTypeIndependent = false;
    } else {
      this.returnTypeIndependent = true;
    }
  }

  @Override
  public JVar[] renderStart(
      ClassGenerator<?> g,
      CompleteType resolvedOutput,
      ClassGenerator.HoldingContainer[] inputVariables,
      FunctionErrorContext errorContext) {
    throw new UnsupportedOperationException("Gandiva code generation is handled during build.");
  }

  @Override
  public ClassGenerator.HoldingContainer renderEnd(
      ClassGenerator<?> g,
      CompleteType resolvedOutput,
      ClassGenerator.HoldingContainer[] inputVariables,
      JVar[] workspaceJVars) {
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
    return returnTypeIndependent;
  }

  @Override
  public FunctionTemplate.NullHandling getNullHandling() {
    return null;
  }

  @Override
  public CompleteType getReturnType(List<LogicalExpression> args) {
    if (returnTypeIndependent) {
      return returnType;
    }

    OutputDerivation derivation;
    switch (name) {
      case "add":
        derivation = OutputDerivation.DECIMAL_ADD;
        break;
      case "subtract":
        derivation = OutputDerivation.DECIMAL_SUBTRACT;
        break;
      case "multiply":
        derivation = OutputDerivation.DECIMAL_MULTIPLY;
        break;

      case "divide":
        derivation = OutputDerivation.DECIMAL_DIVIDE;
        break;
      case "mod":
      case "modulo":
        derivation = OutputDerivation.DECIMAL_MOD;
        break;
      case "abs":
        derivation = OutputDerivation.DECIMAL_MAX;
        break;
      case "ceil":
      case "floor":
        derivation = OutputDerivation.DECIMAL_ZERO_SCALE;
        break;
      case "negative":
        derivation = OutputDerivation.DECIMAL_NEGATIVE;
        break;
      case "round":
        derivation =
            (args.size() == 1)
                ? OutputDerivation.DECIMAL_ZERO_SCALE_ROUND
                : OutputDerivation.DECIMAL_SET_SCALE_ROUND;
        break;
      case "trunc":
      case "truncate":
        derivation =
            (args.size() == 1)
                ? OutputDerivation.DECIMAL_ZERO_SCALE_TRUNCATE
                : OutputDerivation.DECIMAL_SET_SCALE_TRUNCATE;
        break;
      case "castDECIMAL":
      case "castDECIMALNullOnOverflow":
        derivation = OutputDerivation.DECIMAL_CAST;
        break;
      default:
        throw new UnsupportedOperationException("unknown decimal function " + this.name);
    }

    return derivation.getOutputType(CompleteType.DECIMAL, args);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName()
        + " [functionName="
        + name
        + ", returnType="
        + (returnTypeIndependent ? returnType : "arg dependent")
        + ", parameters="
        + Arrays.toString(argTypes)
        + "]";
  }
}

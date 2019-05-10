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

import static org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.OperationType;

import java.util.List;

import org.apache.arrow.gandiva.evaluator.DecimalTypeUtil;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.sun.codemodel.JVar;

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
    if (argTypes[0].getType().getTypeID() == ArrowType.ArrowTypeID.Decimal) {
      this.returnTypeIndependent = false;
    } else {
      this.returnTypeIndependent = true;
    }
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
    ArrowType.Decimal resultType;
    switch (name) {
      case "add" :
        resultType = DecimalTypeUtil.getResultTypeForOperation(
          OperationType.ADD,
          args.get(0).getCompleteType().getType(ArrowType.Decimal.class),
          args.get(1).getCompleteType().getType(ArrowType.Decimal.class));
        break;
      case "subtract" :
        resultType = DecimalTypeUtil.getResultTypeForOperation(
          OperationType.SUBTRACT,
          args.get(0).getCompleteType().getType(ArrowType.Decimal.class),
          args.get(1).getCompleteType().getType(ArrowType.Decimal.class));
        break;
      case "multiply" :
        resultType = DecimalTypeUtil.getResultTypeForOperation(
          OperationType.MULTIPLY,
          args.get(0).getCompleteType().getType(ArrowType.Decimal.class),
          args.get(1).getCompleteType().getType(ArrowType.Decimal.class));
        break;
      case "divide" :
        resultType = DecimalTypeUtil.getResultTypeForOperation(
          OperationType.DIVIDE,
          args.get(0).getCompleteType().getType(ArrowType.Decimal.class),
          args.get(1).getCompleteType().getType(ArrowType.Decimal.class));
        break;
      case "mod" :
        resultType = DecimalTypeUtil.getResultTypeForOperation(
          OperationType.MOD,
          args.get(0).getCompleteType().getType(ArrowType.Decimal.class),
          args.get(1).getCompleteType().getType(ArrowType.Decimal.class));
        break;
      default:
        throw new RuntimeException("Unsupported operation for decimal.");
    }
    return new CompleteType(resultType);
  }
}

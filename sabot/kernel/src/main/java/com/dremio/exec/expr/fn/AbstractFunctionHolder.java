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
import com.dremio.common.expression.fn.FunctionHolder;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.sun.codemodel.JVar;
import java.util.List;

public abstract class AbstractFunctionHolder implements FunctionHolder {

  public abstract JVar[] renderStart(
      ClassGenerator<?> g,
      CompleteType resolvedOutput,
      HoldingContainer[] inputVariables,
      FunctionErrorContext errorContext);

  public void renderMiddle(
      ClassGenerator<?> g,
      CompleteType resolvedOutput,
      HoldingContainer[] inputVariables,
      JVar[] workspaceJVars) {
    // default implementation is add no code
  }

  public abstract HoldingContainer renderEnd(
      ClassGenerator<?> g,
      CompleteType resolvedOutput,
      HoldingContainer[] inputVariables,
      JVar[] workspaceJVars);

  public boolean isNested() {
    return false;
  }

  public boolean isFieldReader(int i) {
    return false;
  }

  public abstract FunctionHolderExpression getExpr(String name, List<LogicalExpression> args);

  public abstract CompleteType getParamType(int i);

  public abstract int getParamCount();

  /*
   *  Does the function held here use a FunctionErrorContext injectable?
   */
  public boolean usesErrContext() {
    return false;
  }

  /*
   * Is this a decimal function.
   * @return true if yes
   */
  public abstract boolean checkPrecisionRange();

  /**
   * Does this function always return the same type, no matter the inputs?
   *
   * @return true if yes
   */
  public abstract boolean isReturnTypeIndependent();

  /** The final return type even if it is dependent on input; */
  public abstract CompleteType getReturnType(final List<LogicalExpression> args);

  /**
   * Returns how the method handles null inputs. For e.g. isNotNull would be NULL_NEVER, add(a+b)
   * would NULL IF NULL.
   *
   * @return the appropriate null handling for the method.
   */
  public abstract FunctionTemplate.NullHandling getNullHandling();
}

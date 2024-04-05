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

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.fn.FunctionHolder;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.dremio.exec.expr.fn.ComplexWriterFunctionHolder;
import com.google.common.base.Objects;
import java.util.List;

public class FunctionHolderExpr extends FunctionHolderExpression
    implements Iterable<LogicalExpression> {
  private BaseFunctionHolder holder;
  private SimpleFunction interpreter;

  public FunctionHolderExpr(
      String nameUsed, BaseFunctionHolder holder, List<LogicalExpression> args) {
    super(nameUsed, args);
    this.holder = holder;
  }

  @Override
  public CompleteType getCompleteType() {
    return holder.getReturnType(args);
  }

  @Override
  public FunctionHolder getHolder() {
    return holder;
  }

  @Override
  public boolean isAggregating() {
    return holder.isAggregating();
  }

  @Override
  public boolean isRandom() {
    return !holder.isDeterministic();
  }

  @Override
  public boolean argConstantOnly(int i) {
    return holder.isConstant(i);
  }

  public boolean isComplexWriterFuncHolder() {
    return holder instanceof ComplexWriterFunctionHolder;
  }

  @Override
  public int getSelfCost() {
    return holder.getCostCategory();
  }

  @Override
  public int getCumulativeCost() {
    int cost = this.getSelfCost();

    for (LogicalExpression arg : this.args) {
      cost += arg.getCumulativeCost();
    }

    return cost;
  }

  @Override
  public FunctionHolderExpr copy(List<LogicalExpression> args) {
    return new FunctionHolderExpr(this.nameUsed, this.holder, args);
  }

  public void setInterpreter(SimpleFunction interpreter) {
    this.interpreter = interpreter;
  }

  public SimpleFunction getInterpreter() {
    return this.interpreter;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof FunctionHolderExpr)) {
      return false;
    }
    FunctionHolderExpr castOther = (FunctionHolderExpr) other;
    return Objects.equal(holder, castOther.holder)
        && Objects.equal(interpreter, castOther.interpreter);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(holder, interpreter);
  }
}

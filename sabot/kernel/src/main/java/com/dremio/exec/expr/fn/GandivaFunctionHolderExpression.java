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

import java.util.Iterator;
import java.util.List;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.EvaluationType;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.fn.FunctionHolder;
import com.dremio.exec.expr.FunctionHolderExpr;
import com.dremio.exec.expr.annotations.FunctionTemplate;

public class GandivaFunctionHolderExpression extends FunctionHolderExpression implements
  Iterable<LogicalExpression> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionHolderExpr.class);
  private GandivaFunctionHolder holder;
  // was pushed in 3.0 have to retain this so that serde does not break.
  private final EvaluationType evaluationType;
  public GandivaFunctionHolderExpression(String nameUsed, GandivaFunctionHolder holder, List<LogicalExpression> args) {
      super(nameUsed, args);
      this.holder = holder;
    this.evaluationType = new EvaluationType();
    evaluationType.addEvaluationType(EvaluationType.ExecutionType.JAVA);
  }

  @Override
  public CompleteType getCompleteType() {
      return holder.getReturnType(args);
    }

  @Override
  public Iterator<LogicalExpression> iterator() {
      return args.iterator();
    }

  @Override
  public FunctionHolder getHolder() {
      return holder;
    }

  @Override
  public boolean isAggregating() {
    // TODO : https://dremio.atlassian.net/browse/GDV-98
    return false;
  }

  @Override
  public boolean argConstantOnly(int i) {
    // TODO : https://dremio.atlassian.net/browse/GDV-98
    return false;
  }

  @Override
  public boolean isRandom() {
      return false;
    }

  @Override
  public GandivaFunctionHolderExpression copy(List<LogicalExpression> args) {
    return new GandivaFunctionHolderExpression(this.nameUsed, this.holder, args);
  }

  @Override
  public int getSelfCost() {
    return FunctionTemplate.FunctionCostCategory.getDefault().getValue();
  }
}

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

import java.util.Iterator;
import java.util.List;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.fn.FunctionHolder;
import com.dremio.exec.expr.FunctionHolderExpr;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.fn.HiveFuncHolder;

public class HiveFuncHolderExpr extends FunctionHolderExpression implements Iterable<LogicalExpression>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionHolderExpr.class);
  private HiveFuncHolder holder;

  public HiveFuncHolderExpr(String nameUsed, HiveFuncHolder holder, List<LogicalExpression> args) {
    super(nameUsed, args);
    this.holder = holder;
  }

  @Override
  public CompleteType getCompleteType() {
    return holder.getReturnType();
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
    return holder.isAggregating();
  }

  @Override
  public boolean argConstantOnly(int i) {
    // looks like hive UDF has no notion of constant argument input
    return false;
  }

  @Override
  public boolean isRandom() {
    return holder.isRandom();
  }

  @Override
  public HiveFuncHolderExpr copy(List<LogicalExpression> args) {
    return new HiveFuncHolderExpr(this.nameUsed, this.holder, args);
  }

  @Override
  public int getSelfCost() {
    return FunctionTemplate.FunctionCostCategory.getDefault().getValue();
  }

}

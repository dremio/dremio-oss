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
package com.dremio.exec.physical.config;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("filter")
public class Filter extends AbstractSingle {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Filter.class);

  private final LogicalExpression expr;
  private final float selectivity;

  @JsonCreator
  public Filter(@JsonProperty("child") PhysicalOperator child, @JsonProperty("expr") LogicalExpression expr, @JsonProperty("selectivity") float selectivity) {
    super(child);
    this.expr = expr;
    this.selectivity = selectivity;
  }

  public LogicalExpression getExpr() {
    return expr;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitFilter(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new Filter(child, expr, selectivity);
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext context) {
    return child.getSchema(context).clone(SelectionVectorMode.TWO_BYTE);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.FILTER_VALUE;
  }


}

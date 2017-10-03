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

import java.util.List;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("project")
public class Project extends AbstractSingle{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Project.class);

  private final List<NamedExpression> exprs;

  @JsonCreator
  public Project(@JsonProperty("exprs") List<NamedExpression> exprs, @JsonProperty("child") PhysicalOperator child) {
    super(child);
    this.exprs = exprs;
  }

  /**
   * A strange current behavior here is that an empty exprs means convert to json for complex types.
   */
  public List<NamedExpression> getExprs() {
    return exprs;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitProject(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new Project(exprs, child);
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext context) {
    final BatchSchema childSchema = child.getSchema(context);
    return ExpressionTreeMaterializer.materializeFields(getExprs(), childSchema, context, true)
        .setSelectionVectorMode(childSchema.getSelectionVectorMode())
        .build();
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.PROJECT_VALUE;
  }
}

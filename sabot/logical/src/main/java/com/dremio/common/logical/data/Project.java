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
package com.dremio.common.logical.data;

import java.util.List;

import com.dremio.common.exceptions.ExpressionParsingException;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.visitors.LogicalVisitor;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;

@JsonTypeName("project")
public class Project extends SingleInputOperator {

  private final List<NamedExpression> selections;

  @JsonCreator
  public Project(@JsonProperty("projections") List<NamedExpression> selections) {
    this.selections = selections;
    if (selections == null || selections.size() == 0) {
      throw new ExpressionParsingException(
          "Project did not provide any projection selections.  At least one projection must be provided.");
    }
  }

  @JsonProperty("projections")
  public List<NamedExpression> getSelections() {
    return selections;
  }

  @Override
  public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitProject(this, value);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends AbstractSingleBuilder<Project, Builder> {

    private List<NamedExpression> exprs = Lists.newArrayList();

    public Builder addExpr(NamedExpression expr) {
      exprs.add(expr);
      return this;
    }

    public Builder addExpr(FieldReference ref, LogicalExpression expr) {
      exprs.add(new NamedExpression(expr, ref));
      return this;
    }

    @Override
    public Project internalBuild() {
      return new Project(exprs);
    }

  }

}

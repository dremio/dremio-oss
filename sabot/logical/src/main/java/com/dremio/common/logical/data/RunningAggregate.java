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
package com.dremio.common.logical.data;

import java.util.Iterator;
import java.util.List;

import com.dremio.common.expression.FieldReference;
import com.dremio.common.logical.data.visitors.LogicalVisitor;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;

@JsonTypeName("runningaggregate")
public class RunningAggregate extends SingleInputOperator{

  private final FieldReference within;
  private final List<NamedExpression> aggregations;

  @JsonCreator
  public RunningAggregate(@JsonProperty("within") FieldReference within,
                          @JsonProperty("aggregations") List<NamedExpression> aggregations) {
    super();
    this.within = within;
    this.aggregations = aggregations;
  }

  public FieldReference getWithin() {
    return within;
  }

  public List<NamedExpression> getAggregations() {
    return aggregations;
  }

    @Override
    public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
        return logicalVisitor.visitRunningAggregate(this, value);
    }

    @Override
    public Iterator<LogicalOperator> iterator() {
        return Iterators.singletonIterator(getInput());
    }



}

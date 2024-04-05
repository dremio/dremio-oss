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
package com.dremio.exec.physical.config;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;

@JsonTypeName("streaming-aggregate")
public class StreamingAggregate extends AbstractSingle {

  private final List<NamedExpression> groupByExprs;
  private final List<NamedExpression> aggrExprs;

  private final float cardinality;

  @JsonCreator
  public StreamingAggregate(
      @JsonProperty("props") OpProps props,
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("groupByExprs") List<NamedExpression> groupByExprs,
      @JsonProperty("aggrExprs") List<NamedExpression> aggrExprs,
      @JsonProperty("cardinality") float cardinality) {
    super(props, child);
    this.groupByExprs = groupByExprs;
    this.aggrExprs = aggrExprs;
    this.cardinality = cardinality;
  }

  public List<NamedExpression> getGroupByExprs() {
    return groupByExprs;
  }

  public List<NamedExpression> getAggrExprs() {
    return aggrExprs;
  }

  public float getCardinality() {
    return cardinality;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitStreamingAggregate(this, value);
  }

  @Override
  protected StreamingAggregate getNewWithChild(PhysicalOperator child) {
    return new StreamingAggregate(props, child, groupByExprs, aggrExprs, cardinality);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.STREAMING_AGGREGATE_VALUE;
  }
}

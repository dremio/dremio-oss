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

import java.util.List;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggSpillStats;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;

@JsonTypeName("hash-aggregate")
public class HashAggregate extends AbstractSingle {

  private final List<NamedExpression> groupByExprs;
  private final List<NamedExpression> aggrExprs;
  private final boolean vectorize;
  private final boolean useSpill;
  private final float cardinality;
  private final int hashTableBatchSize;

  /* testing related parameters */
  private VectorizedHashAggSpillStats spillStats;

  @JsonCreator
  public HashAggregate(
      @JsonProperty("props") OpProps props,
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("groupByExprs") List<NamedExpression> groupByExprs,
      @JsonProperty("aggrExprs") List<NamedExpression> aggrExprs,
      @JsonProperty("vectorize") boolean vectorize,
      @JsonProperty("useSpill") boolean useSpill,
      @JsonProperty("cardinality") float cardinality,
      @JsonProperty("hashTableBatchSize") int hashTableBatchSize
      ) {
    super(props, child);
    this.groupByExprs = groupByExprs;
    this.aggrExprs = aggrExprs;
    this.vectorize = vectorize;
    this.useSpill = useSpill;
    this.cardinality = cardinality;
    this.hashTableBatchSize = hashTableBatchSize;
  }

  // for testing only
  public HashAggregate(
    OpProps props,
    PhysicalOperator child,
    List<NamedExpression> groupByExprs,
    List<NamedExpression> aggrExprs,
    boolean vectorize,
    boolean useSpill,
    float cardinality) {
    this(props, child, groupByExprs, aggrExprs, vectorize, useSpill, cardinality, 3968);
  }

  public boolean isVectorize(){
    return vectorize;
  }

  public boolean isUseSpill(){
    return useSpill;
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
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitHashAggregate(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new HashAggregate(props, child, groupByExprs, aggrExprs, vectorize, useSpill, cardinality);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HASH_AGGREGATE_VALUE;
  }

  public int getHashTableBatchSize() {
    return hashTableBatchSize;
  }

  @VisibleForTesting
  public VectorizedHashAggSpillStats getSpillStats() {
    return spillStats;
  }

  @VisibleForTesting
  public void setSpillStats(final VectorizedHashAggSpillStats stats) {
    this.spillStats = stats;
  }
}

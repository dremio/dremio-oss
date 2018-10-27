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
package com.dremio.exec.physical.config;

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.MemoryCalcConsidered;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.options.TypeValidators.RangeDoubleValidator;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggSpillStats;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;

@Options
@JsonTypeName("hash-aggregate")
public class HashAggregate extends AbstractSingle implements MemoryCalcConsidered {

  public static final DoubleValidator AGG_FACTOR = new RangeDoubleValidator("planner.op.hashagg.factor", 0.0, 1000.0, 1.0d);
  public static final BooleanValidator AGG_BOUNDED = new BooleanValidator("planner.op.hashagg.bounded", true);

  public static final PositiveLongValidator LOWER_LIMIT = new PositiveLongValidator("planner.op.hashagg.low_limit", Long.MAX_VALUE, 300_000_000);
  public static final PositiveLongValidator UPPER_LIMIT = new PositiveLongValidator("planner.op.hashagg.limit", Long.MAX_VALUE, Long.MAX_VALUE);

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregate.class);

  private final List<NamedExpression> groupByExprs;
  private final List<NamedExpression> aggrExprs;
  private final boolean vectorize;
  private final float cardinality;

  /* testing related parameters */
  private VectorizedHashAggSpillStats spillStats;

  @JsonCreator
  public HashAggregate(@JsonProperty("child") PhysicalOperator child,
                       @JsonProperty("keys") List<NamedExpression> groupByExprs,
                       @JsonProperty("exprs") List<NamedExpression> aggrExprs,
                       @JsonProperty("vectorize") boolean vectorize,
                       @JsonProperty("cardinality") float cardinality) {
    super(child);
    this.groupByExprs = groupByExprs;
    this.aggrExprs = aggrExprs;
    this.cardinality = cardinality;
    this.vectorize = vectorize;
    this.spillStats = null;
  }

  public boolean isVectorize(){
    return vectorize;
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
    HashAggregate ha = new HashAggregate(child, groupByExprs, aggrExprs, vectorize, cardinality);
    ha.setMaxAllocation(this.getMaxAllocation());
    return ha;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HASH_AGGREGATE_VALUE;
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext context) {
    final BatchSchema childSchema = child.getSchema(context);
    List<NamedExpression> exprs = new ArrayList<>();
    exprs.addAll(groupByExprs);
    exprs.addAll(aggrExprs);
    return ExpressionTreeMaterializer.materializeFields(exprs, childSchema, context)
        .setSelectionVectorMode(SelectionVectorMode.NONE)
        .build();
  }

  @Override
  public double getMemoryFactor(OptionManager options) {
    return options.getOption(AGG_FACTOR);
  }

  @Override
  public boolean shouldBeMemoryBounded(OptionManager options) {
    return options.getOption(AGG_BOUNDED) && options.getOption(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR);
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

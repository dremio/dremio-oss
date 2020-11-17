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
package com.dremio.exec.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.holders.IntHolder;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.FunctionHolderExpr;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.options.TypeValidators.RangeDoubleValidator;
import com.google.common.collect.ImmutableList;

@Options
public class HashAggPrel extends AggPrelBase implements Prel{

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggPrel.class);

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.hashagg.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.hashagg.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);
  public static final LongValidator LOW_LIMIT = new PositiveLongValidator("planner.op.hashagg.low_limit_bytes", Long.MAX_VALUE, 300_000_000);
  public static final DoubleValidator FACTOR = new RangeDoubleValidator("planner.op.hashagg.factor", 0.0, 1000.0, 1.0d);
  public static final BooleanValidator BOUNDED = new BooleanValidator("planner.op.hashagg.bounded", true);


  private Boolean canVectorize;
  private Boolean canUseSpill;

  private HashAggPrel(RelOptCluster cluster,
                     RelTraitSet traits,
                     RelNode child,
                     boolean indicator,
                     ImmutableBitSet groupSet,
                     List<ImmutableBitSet> groupSets,
                     List<AggregateCall> aggCalls,
                     OperatorPhase phase) throws InvalidRelException {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls, phase);
  }

  public static HashAggPrel create(RelOptCluster cluster,
                     RelTraitSet traits,
                     RelNode child,
                     boolean indicator,
                     ImmutableBitSet groupSet,
                     List<ImmutableBitSet> groupSets,
                     List<AggregateCall> aggCalls,
                     OperatorPhase phase) throws InvalidRelException {
    final RelTraitSet adjustedTraits = AggPrelBase.adjustTraits(traits, child, groupSet);
    return new HashAggPrel(cluster, adjustedTraits, child, indicator, groupSet, groupSets, aggCalls, phase);
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    try {
      return HashAggPrel.create(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls,
          this.getOperatorPhase());
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    final RelNode child = this.getInput();
    double inputRows = mq.getRowCount(child);

    int numGroupByFields = this.getGroupCount();
    int numAggrFields = this.aggCalls.size();
    // cpu cost of hashing each grouping key
    double cpuCost = DremioCost.HASH_CPU_COST * numGroupByFields * inputRows;
    // add cpu cost for computing the aggregate functions
    cpuCost += DremioCost.FUNC_CPU_COST * numAggrFields * inputRows;
    double diskIOCost = 0; // assume in-memory for now until we enforce operator-level memory constraints

    // TODO: use distinct row count
    // + hash table template stuff
    double factor = PrelUtil.getPlannerSettings(planner).getOptions()
      .getOption(ExecConstants.HASH_AGG_TABLE_FACTOR_KEY).getFloatVal();
    long fieldWidth = PrelUtil.getPlannerSettings(planner).getOptions()
      .getOption(ExecConstants.AVERAGE_FIELD_WIDTH_KEY).getNumVal();

    // table + hashValues + links
    double memCost =
      (
        (fieldWidth * numGroupByFields) +
          IntHolder.WIDTH +
          IntHolder.WIDTH
      ) * inputRows * factor;

    Factory costFactory = (Factory) planner.getCostFactory();
    return costFactory.makeCost(inputRows, cpuCost, diskIOCost, 0 /* network cost */, memCost);
  }


  @Override public double estimateRowCount(RelMetadataQuery mq) {
    // Assume that each sort column has 90% of the value count.
    // Therefore one sort column has .10 * rowCount,
    // 2 sort columns give .19 * rowCount.
    // Zero sort columns yields 1 row (or 0 if the input is empty).
    final int groupCount = groupSet.cardinality();
    if (groupCount == 0) {
      return 1;
    } else {
      // don't use super.estimateRowCount(mq) to not apply on top of calcite
      // estimation for Aggregate. Directly get input rowcount
      double rowCount = mq.getRowCount(getInput());
      if(this.operPhase != OperatorPhase.PHASE_2of2) {
        rowCount *= 1.0 - Math.pow(.9, groupCount);
      }
      return rowCount;
    }
  }

  private boolean canVectorize(PhysicalPlanCreator creator, PhysicalOperator child){
    if(canVectorize == null){
      canVectorize = initialCanVectorize(creator, child);
    }
    return canVectorize;
  }

  private boolean canUseSpill(PhysicalPlanCreator creator, PhysicalOperator child){
    if(canUseSpill == null){
      canUseSpill = initialUseSpill(creator, child);
    }
    return canUseSpill;
  }

  private boolean initialUseSpill(PhysicalPlanCreator creator, PhysicalOperator child) {
    if (!canVectorize(creator, child)) {
      return false;
    }
    boolean useSpill = true;
    final BatchSchema childSchema = child.getProps().getSchema();
    for(NamedExpression ne : aggExprs) {
      final LogicalExpression expr = ExpressionTreeMaterializer.materializeAndCheckErrors(ne.getExpr(), childSchema, creator.getContext().getFunctionRegistry());
      if (expr != null && (expr instanceof FunctionHolderExpr)) {
        final String functionName = ((FunctionHolderExpr) expr).getName();
        final boolean isMinMaxFn = (functionName.equals("min") || functionName.equals("max"));
        final boolean isNDVFn = (functionName.equals("hll") || functionName.equals("hll_merge"));
        if (isNDVFn || (isMinMaxFn && expr.getCompleteType().isVariableWidthScalar())) {
          useSpill = false;
          break;
        }
      }
    }
    return useSpill;
  }

  private boolean initialCanVectorize(PhysicalPlanCreator creator, PhysicalOperator child){
    if(!creator.getContext().getOptions().getOption(ExecConstants.ENABLE_VECTORIZED_HASHAGG)){
      return false;
    }

    final BatchSchema childSchema = child.getProps().getSchema();

    for(NamedExpression ne : keys){
      // these should all be simple.

      LogicalExpression expr = ExpressionTreeMaterializer.materializeAndCheckErrors(ne.getExpr(), childSchema, creator.getContext().getFunctionRegistry());

      if(expr == null || !(expr instanceof ValueVectorReadExpression) ){
        return false;
      }

      switch(expr.getCompleteType().toMinorType()) {
        case BIGINT:
        case DATE:
        case FLOAT4:
        case FLOAT8:
        case INT:
        case INTERVALDAY:
        case INTERVALYEAR:
        case TIME:
        case TIMESTAMP:
        case VARBINARY:
        case VARCHAR:
        case DECIMAL:
        case BIT:
          continue;
        default:
          return false;
      }
    }

    final boolean enabledVarcharNdv = creator.getContext().getOptions().getOption(ExecConstants.ENABLE_VECTORIZED_NOSPILL_VARCHAR_NDV_ACCUMULATOR);

    for(NamedExpression ne : aggExprs){
      final LogicalExpression expr = ExpressionTreeMaterializer.materializeAndCheckErrors(ne.getExpr(), childSchema, creator.getContext().getFunctionRegistry());

      if(expr == null || !(expr instanceof FunctionHolderExpr) ){
        return false;
      }

      FunctionHolderExpr func = (FunctionHolderExpr) expr;
      ImmutableList<LogicalExpression> exprs = ImmutableList.copyOf(expr);

      // COUNT(1)
      if(func.getName().equals("count")){
        continue;
      }

      if((exprs.size() != 1 ||  !(exprs.get(0) instanceof ValueVectorReadExpression))){
        return false;
      }

      final CompleteType inputType = exprs.get(0).getCompleteType();

      switch(func.getName()){
      case "$sum0":
      case "sum":
        switch(inputType.toMinorType()){
          case BIGINT:
          case FLOAT4:
          case FLOAT8:
          case INT:
          case DECIMAL:
            continue;
        }

        return false;

      case "min":
      case "max":
        switch(inputType.toMinorType()){
        case BIGINT:
        case FLOAT4:
        case FLOAT8:
        case INT:
        case BIT:
        case DATE:
        case INTERVALDAY:
        case INTERVALYEAR:
        case TIME:
        case TIMESTAMP:
        case DECIMAL:
        case VARCHAR:
        case VARBINARY:
          if (!enabledVarcharNdv) {
            return false;
          }
          continue;
        }

        return false;

      case "hll":
      case "hll_merge":
        if (!enabledVarcharNdv) {
          return false;
        }
        continue;


      default:
        return false;
      }
    }

    return true;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    PhysicalOperator child = ((Prel) this.getInput()).getPhysicalOperator(creator);

    final BatchSchema childSchema = child.getProps().getSchema();
    List<NamedExpression> exprs = new ArrayList<>();
    exprs.addAll(keys);
    exprs.addAll(aggExprs);
    BatchSchema schema = ExpressionTreeMaterializer.materializeFields(exprs, childSchema, creator.getFunctionLookupContext())
        .setSelectionVectorMode(SelectionVectorMode.NONE)
        .build();

    boolean canVectorize = canVectorize(creator, child);
    boolean canSpill = canUseSpill(creator, child);

    int hashTableBatchSize = 0;
    long lowLimit = creator.getOptionManager().getOption(LOW_LIMIT);
    long reservation = creator.getOptionManager().getOption(RESERVE);
    if (canVectorize && canSpill) {
      HashAggMemoryEstimator estimator = HashAggMemoryEstimator.create(keys, aggExprs, schema, childSchema,
        creator.getFunctionLookupContext(), creator.getOptionManager());

      // reservation limit to allow for at-least one batch (two for caution).
      reservation = Long.max(reservation, estimator.getMemTotal());

      //safety check in case the lowlimit is set too low
      lowLimit = Long.max(lowLimit, estimator.getMemTotal() * 2);
      hashTableBatchSize = estimator.getHashTableBatchSize();
    }
    return new HashAggregate(
        creator.props(this, null, schema, reservation, LIMIT, lowLimit)
          .cloneWithBound(creator.getOptionManager().getOption(BOUNDED) && canSpill && canVectorize)
          .cloneWithMemoryFactor(creator.getOptionManager().getOption(FACTOR))
          .cloneWithMemoryExpensive(true)
        ,
        child,
        keys,
        aggExprs,
        canVectorize,
        canSpill,
        1.0f,
        hashTableBatchSize);
  }


  //options.getOption(AGG_BOUNDED) && options.getOption(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR);

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}

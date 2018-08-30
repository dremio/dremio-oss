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
package com.dremio.exec.planner.physical;

import java.io.IOException;
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
import com.google.common.collect.ImmutableList;

public class HashAggPrel extends AggPrelBase implements Prel{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggPrel.class);

  private Boolean canVectorize;

  public HashAggPrel(RelOptCluster cluster,
                     RelTraitSet traits,
                     RelNode child,
                     boolean indicator,
                     ImmutableBitSet groupSet,
                     List<ImmutableBitSet> groupSets,
                     List<AggregateCall> aggCalls,
                     OperatorPhase phase) throws InvalidRelException {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls, phase);
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    try {
      return new HashAggPrel(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls,
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


  private boolean canVectorize(PhysicalPlanCreator creator, PhysicalOperator child){
    if(canVectorize == null){
      canVectorize = initialCanVectorize(creator, child);
    }
    return canVectorize;
  }

  private boolean initialCanVectorize(PhysicalPlanCreator creator, PhysicalOperator child){
    if(!creator.getContext().getOptions().getOption(ExecConstants.ENABLE_VECTORIZED_HASHAGG)){
      return false;
    }

    final BatchSchema childSchema = child.getSchema(creator.getContext().getFunctionRegistry());

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
          continue;
        }

        return false;

      default:
        return false;
      }
    }

    return true;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    PhysicalOperator child = ((Prel) this.getInput()).getPhysicalOperator(creator);
    HashAggregate g = new HashAggregate(child, keys, aggExprs, canVectorize(creator, child), 1.0f);
    return creator.addMetadata(this, g);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}

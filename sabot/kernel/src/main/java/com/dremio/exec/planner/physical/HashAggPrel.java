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
package com.dremio.exec.planner.physical;

import java.io.IOException;
import java.util.List;
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
    return super.computeHashAggCost(planner, mq);
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

      switch(expr.getCompleteType().toMinorType()){
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
      case "min":
      case "max":
        switch(inputType.toMinorType()){
        case BIGINT:
        case FLOAT4:
        case FLOAT8:
        case INT:
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

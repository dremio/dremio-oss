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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.JoinUtils.JoinCategory;
import com.google.common.collect.Lists;

public class HashJoinPrel  extends JoinPrel {

  private boolean swapped = false;

  public HashJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
                      JoinRelType joinType) throws InvalidRelException {
    this(cluster, traits, left, right, condition, joinType, false);
  }

  public HashJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType, boolean swapped) throws InvalidRelException {
    super(cluster, traits, left, right, condition, joinType);
    this.swapped = swapped;
    joincategory = JoinUtils.getJoinCategory(left, right, condition, leftKeys, rightKeys, filterNulls);
  }

  @Override
  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    try {
      return new HashJoinPrel(this.getCluster(), traitSet, left, right, conditionExpr, joinType, this.swapped);
    }catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    if (joincategory == JoinCategory.CARTESIAN || joincategory == JoinCategory.INEQUALITY) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    return computeHashJoinCost(planner, mq);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    // Depending on whether the left/right is swapped for hash inner join, pass in different
    // combinations of parameters.
    if (! swapped) {
      return getHashJoinPop(creator, left, right, leftKeys, rightKeys);
    } else {
      return getHashJoinPop(creator, right, left, rightKeys, leftKeys);
    }
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  private PhysicalOperator getHashJoinPop(PhysicalPlanCreator creator, RelNode left, RelNode right,
                                          List<Integer> leftKeys, List<Integer> rightKeys) throws IOException{
    final List<String> fields = getRowType().getFieldNames();
    assert isUnique(fields);

    final List<String> leftFields = left.getRowType().getFieldNames();
    final List<String> rightFields = right.getRowType().getFieldNames();

    PhysicalOperator leftPop = ((Prel)left).getPhysicalOperator(creator);
    PhysicalOperator rightPop = ((Prel)right).getPhysicalOperator(creator);

    JoinRelType jtype = this.getJoinType();

    final List<JoinCondition> conditions = Lists.newArrayList();

    buildJoinConditions(conditions, leftFields, rightFields, leftKeys, rightKeys);

    final boolean vectorize = creator.getContext().getOptions().getOption(ExecConstants.ENABLE_VECTORIZED_HASHJOIN)
        && canVectorize(creator.getContext().getFunctionRegistry(), leftPop, rightPop, conditions);
    final HashJoinPOP hjoin = new HashJoinPOP(leftPop, rightPop, conditions, jtype, vectorize);
    return creator.addMetadata(this, hjoin);
  }

  private boolean canVectorize(FunctionLookupContext functionLookup, PhysicalOperator leftPop, PhysicalOperator rightPop, List<JoinCondition> conditions){
    BatchSchema left = leftPop.getSchema(functionLookup);
    BatchSchema right = rightPop.getSchema(functionLookup);

    // we can only vectorize if the join keys are of a safe type.
    for(JoinCondition c : conditions){
      LogicalExpression leftExpr = ExpressionTreeMaterializer.materializeAndCheckErrors(c.getLeft(), left, functionLookup);
      LogicalExpression rightExpr = ExpressionTreeMaterializer.materializeAndCheckErrors(c.getRight(), right, functionLookup);
      if(!isJoinable(leftExpr.getCompleteType()) || !isJoinable(rightExpr.getCompleteType())){
        return false;
      }

      // there are a few situations (namely values join) where vectorization is not possible because implicit casts are missing.
      if(!leftExpr.getCompleteType().equals(rightExpr.getCompleteType())){
        return false;
      }
    }

    return true;
  }

  private boolean isJoinable(CompleteType ct){
    switch(ct.toMinorType()){
    case BIGINT:
    case DATE:
    case FLOAT4:
    case FLOAT8:
    case INT:
    case INTERVAL:
    case TIME:
    case TIMESTAMP:
    case VARBINARY:
    case VARCHAR:
      return true;
    default:
      return false;
    }
  }

  public void setSwapped(boolean swapped) {
    this.swapped = swapped;
  }

  public boolean isSwapped() {
    return this.swapped;
  }

}

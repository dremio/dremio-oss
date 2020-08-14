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
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.planner.physical.filter.RuntimeFilterInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.options.TypeValidators.RangeDoubleValidator;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.JoinUtils.JoinCategory;
import com.google.common.collect.Lists;

@Options
public class HashJoinPrel extends JoinPrel {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.hashjoin.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.hashjoin.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  public static final DoubleValidator FACTOR = new RangeDoubleValidator("planner.op.hashjoin.factor", 0.0, 1000.0, 1.0d);
  public static final BooleanValidator BOUNDED = new BooleanValidator("planner.op.hashjoin.bounded", false);

  private final boolean swapped;
  private RuntimeFilterInfo runtimeFilterInfo;

  private HashJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
                       JoinRelType joinType, boolean swapped) {
    super(cluster, traits, left, right, condition, joinType, JoinUtils.projectAll(left.getRowType().getFieldCount()+right.getRowType().getFieldCount()));
    this.swapped = swapped;
  }

  private HashJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
                       JoinRelType joinType, boolean swapped, ImmutableBitSet projectedFields) {
    super(cluster, traits, left, right, condition, joinType, projectedFields);
    this.swapped = swapped;
  }

  private HashJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
                       JoinRelType joinType, boolean swapped, ImmutableBitSet projectedFields, RuntimeFilterInfo runtimeFilterInfo) {
    this(cluster, traits, left, right, condition, joinType, swapped, projectedFields);
    this.runtimeFilterInfo = runtimeFilterInfo;
  }

  public static HashJoinPrel create(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
                                    JoinRelType joinType, ImmutableBitSet projectedFields) {
    final RelTraitSet adjustedTraits = JoinPrel.adjustTraits(traits);
    return new HashJoinPrel(cluster, adjustedTraits, left, right, condition, joinType, false, projectedFields, null);
  }

  @Override
  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new HashJoinPrel(this.getCluster(), traitSet, left, right, conditionExpr, joinType, this.swapped, getProjectedFields(), this.runtimeFilterInfo);
  }

  @Override
  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone, ImmutableBitSet projectedFields) {
    return new HashJoinPrel(this.getCluster(), traitSet, left, right, conditionExpr, joinType, this.swapped, projectedFields, this.runtimeFilterInfo);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    if (joinCategory == JoinCategory.CARTESIAN || joinCategory == JoinCategory.INEQUALITY) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    return computeHashJoinCost(planner, mq);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return getHashJoinPop(creator);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  private PhysicalOperator getHashJoinPop(PhysicalPlanCreator creator) throws IOException {
    final List<String> fields = getRowType().getFieldNames();
    assert isUnique(fields);

    final RelNode currentLeft;
    final RelNode currentRight;
    final List<Integer> currentLeftKeys;
    List<Integer> currentRightKeys;

    final JoinRelType jtype;

    // Swapping left and side if necessary
    // Output is not swapped as the operator uses field names and not field indices
    // so it is not impacted by reordering
    if (this.swapped) {
      currentLeft = right;
      currentRight = left;
      currentLeftKeys = rightKeys;
      currentRightKeys = leftKeys;
      jtype = this.getJoinType().swap();
    } else {
      currentLeft = left;
      currentRight = right;
      currentLeftKeys = leftKeys;
      currentRightKeys = rightKeys;
      jtype = this.getJoinType();
    }

    final List<String> leftFields = currentLeft.getRowType().getFieldNames();
    final List<String> rightFields = currentRight.getRowType().getFieldNames();

    final PhysicalOperator leftPop = ((Prel)currentLeft).getPhysicalOperator(creator);
    final PhysicalOperator rightPop = ((Prel)currentRight).getPhysicalOperator(creator);

    final List<JoinCondition> conditions = Lists.newArrayList();

    buildJoinConditions(conditions, leftFields, rightFields, currentLeftKeys, currentRightKeys);

    final boolean vectorize = creator.getContext().getOptions().getOption(ExecConstants.ENABLE_VECTORIZED_HASHJOIN)
        && canVectorize(creator.getContext().getFunctionRegistry(), leftPop, rightPop, conditions);

    SchemaBuilder b = BatchSchema.newBuilder();
    for (Field f : rightPop.getProps().getSchema()) {
      b.addField(f);
    }
    for (Field f : leftPop.getProps().getSchema()) {
      b.addField(f);
    }
    BatchSchema schema = b.build();

    return new HashJoinPOP(
        creator
          .props(this, null, schema, RESERVE, LIMIT)
          .cloneWithBound(creator.getOptionManager().getOption(BOUNDED))
          .cloneWithMemoryFactor(creator.getOptionManager().getOption(FACTOR))
          .cloneWithMemoryExpensive(true),
        leftPop,
        rightPop,
        conditions,
        joinType,
        vectorize,
        runtimeFilterInfo
    );
  }

  private boolean canVectorize(FunctionLookupContext functionLookup, PhysicalOperator leftPop, PhysicalOperator rightPop, List<JoinCondition> conditions){
    BatchSchema left = leftPop.getProps().getSchema();
    BatchSchema right = rightPop.getProps().getSchema();

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

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
      .itemIf("swapped", swapped, swapped)
      .itemIf("runtimeFilter", runtimeFilterInfo, runtimeFilterInfo != null);
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
    case DECIMAL:
      return true;
    default:
      return false;
    }
  }

  public HashJoinPrel swap() {
    return new HashJoinPrel(getCluster(), traitSet, left, right, condition, joinType, !swapped, getProjectedFields());
  }

  public boolean isSwapped() {
    return this.swapped;
  }

  public RuntimeFilterInfo getRuntimeFilterInfo() {
    return runtimeFilterInfo;
  }

  public void setRuntimeFilterInfo(RuntimeFilterInfo runtimeFilterInfo) {
    this.runtimeFilterInfo = runtimeFilterInfo;
  }


}

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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.MergeJoinPOP;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.sabot.op.join.JoinUtils.JoinCategory;

@Options
public class MergeJoinPrel  extends JoinPrel {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.mergejoin.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.mergejoin.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  /** Creates a MergeJoinPrel. */
  private MergeJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) {
    super(cluster, traits, left, right, condition, joinType);
  }

  public static MergeJoinPrel create(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) {
    final RelTraitSet adjustedTraits = JoinPrel.adjustTraits(traits);
    return new MergeJoinPrel(cluster, adjustedTraits, left, right, condition, joinType);
  }

  @Override
  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new MergeJoinPrel(this.getCluster(), traitSet, left, right, conditionExpr, joinType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }
    if (joinCategory == JoinCategory.CARTESIAN || joinCategory == JoinCategory.INEQUALITY) {
      return ((Factory)planner.getCostFactory()).makeInfiniteCost();
    }
    double leftRowCount = mq.getRowCount(this.getLeft());
    double rightRowCount = mq.getRowCount(this.getRight());
    // cost of evaluating each leftkey=rightkey join condition
    double joinConditionCost = DremioCost.COMPARE_CPU_COST * this.getLeftKeys().size();
    double cpuCost = joinConditionCost * (leftRowCount + rightRowCount);
    Factory costFactory = (Factory)planner.getCostFactory();
    return costFactory.makeCost(leftRowCount + rightRowCount, cpuCost, 0, 0);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    final List<String> fields = getRowType().getFieldNames();
    assert isUnique(fields);

    final int leftCount = left.getRowType().getFieldCount();
    final List<String> leftFields = fields.subList(0, leftCount);
    final List<String> rightFields = fields.subList(leftCount, fields.size());

    PhysicalOperator leftPop = ((Prel)left).getPhysicalOperator(creator);
    PhysicalOperator rightPop = ((Prel)right).getPhysicalOperator(creator);

    JoinRelType jtype = this.getJoinType();

    final List<JoinCondition> conditions = buildJoinConditions(leftFields, rightFields, leftKeys, rightKeys);

    SchemaBuilder b = BatchSchema.newBuilder();
    for (Field f : rightPop.getProps().getSchema()) {
      b.addField(f);
    }
    for (Field f : leftPop.getProps().getSchema()) {
      b.addField(f);
    }
    BatchSchema schema = b.build();

    return new MergeJoinPOP(
        creator.props(this, null, schema, RESERVE, LIMIT),
        leftPop,
        rightPop,
        conditions,
        jtype
        );
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    // currently, MergeJoin operator is not handling incoming batch containing SV2 or SV4, so
    // it requires a SVRemover
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public RexNode getExtraCondition() {
    return null;
  }
}

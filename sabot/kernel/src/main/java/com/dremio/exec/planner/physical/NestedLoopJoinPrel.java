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

import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.NestedLoopJoinPOP;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.google.common.collect.Lists;

@Options
public class NestedLoopJoinPrel  extends JoinPrel {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.nlj.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.nlj.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);


  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NestedLoopJoinPrel.class);
  private NestedLoopJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
                      JoinRelType joinType) {
    super(cluster, traits, left, right, cluster.getRexBuilder().makeLiteral(true), joinType);
  }

  public static NestedLoopJoinPrel create(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
      JoinRelType joinType) {
    final RelTraitSet adjustedTraits = JoinPrel.adjustTraits(traits);
    return new NestedLoopJoinPrel(cluster, adjustedTraits, left, right, joinType);
  }

  @Override
  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new NestedLoopJoinPrel(this.getCluster(), traitSet, left, right, joinType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    double leftRowCount = mq.getRowCount(this.getLeft());
    double rightRowCount = mq.getRowCount(this.getRight());
    double nljFactor = PrelUtil.getSettings(getCluster()).getNestedLoopJoinFactor();

    // cpu cost of evaluating each leftkey=rightkey join condition
    double joinConditionCost = DremioCost.COMPARE_CPU_COST * this.getLeftKeys().size();

    double cpuCost = joinConditionCost * (leftRowCount * rightRowCount) * nljFactor;

    Factory costFactory = (Factory) planner.getCostFactory();
    return costFactory.makeCost(leftRowCount * rightRowCount, cpuCost, 0, 0, 0);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    final List<String> fields = getRowType().getFieldNames();
    assert isUnique(fields);

    final List<String> leftFields = left.getRowType().getFieldNames();
    final List<String> rightFields = right.getRowType().getFieldNames();

    PhysicalOperator leftPop = ((Prel)left).getPhysicalOperator(creator);
    PhysicalOperator rightPop = ((Prel)right).getPhysicalOperator(creator);

    List<JoinCondition> conditions = Lists.newArrayList();

    buildJoinConditions(conditions, leftFields, rightFields, leftKeys, rightKeys);
    if(!conditions.isEmpty()) {
      UserException.unsupportedError().message("NLJ doesn't support conditions yet. Conditions found %s.", conditions).build(logger);
    }

    SchemaBuilder b = BatchSchema.newBuilder();
    for (Field f : rightPop.getProps().getSchema()) {
      b.addField(f);
    }
    for (Field f : leftPop.getProps().getSchema()) {
      b.addField(f);
    }
    BatchSchema schema = b.build();
    return new NestedLoopJoinPOP(
        creator.props(this, null, schema, RESERVE, LIMIT),
        leftPop,
        rightPop);
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

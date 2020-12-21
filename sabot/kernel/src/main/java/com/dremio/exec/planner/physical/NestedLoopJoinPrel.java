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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;

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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.NestedLoopJoinPOP;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.sabot.op.join.JoinUtils;
import com.google.common.collect.Lists;

@Options
public class NestedLoopJoinPrel  extends JoinPrel {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.nlj.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.nlj.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);
  public static final LongValidator OUTPUT_COUNT = new PositiveLongValidator("planner.op.nlj.output_count", Long.MAX_VALUE, 1048576L);
  public static final BooleanValidator VECTORIZED = new BooleanValidator("planner.op.nlj.vectorized", true);

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NestedLoopJoinPrel.class);

  private final RexNode vectorExpression;

  private NestedLoopJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, JoinRelType joinType, RexNode condition, RexNode vectorExpression) {
    this(cluster, traits, left, right, joinType, condition, vectorExpression, JoinUtils.projectAll(left.getRowType().getFieldCount()+right.getRowType().getFieldCount()));
  }

  private NestedLoopJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, JoinRelType joinType, RexNode condition, RexNode vectorExpression, ImmutableBitSet projectedFields) {
    super(cluster, traits, left, right, condition, joinType, projectedFields);
    this.vectorExpression = vectorExpression;
  }


  public static NestedLoopJoinPrel create(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, JoinRelType joinType, RexNode condition) {
    final RelTraitSet adjustedTraits = JoinPrel.adjustTraits(traits);
    return new NestedLoopJoinPrel(cluster, adjustedTraits, left, right, joinType, condition, (RexNode) null);
  }

  public static NestedLoopJoinPrel create(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, JoinRelType joinType, RexNode condition, ImmutableBitSet projectedFields) {
    final RelTraitSet adjustedTraits = JoinPrel.adjustTraits(traits);
    return new NestedLoopJoinPrel(cluster, adjustedTraits, left, right, joinType, condition, null, projectedFields);
  }

  @Override
  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new NestedLoopJoinPrel(this.getCluster(), traitSet, left, right, joinType, conditionExpr, vectorExpression, getProjectedFields());
  }

  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone, RexNode vectorExpression) {
    return new NestedLoopJoinPrel(this.getCluster(), traitSet, left, right, joinType, conditionExpr, vectorExpression, getProjectedFields());
  }

  @Override
  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone, ImmutableBitSet projectedFields) {
    return new NestedLoopJoinPrel(this.getCluster(), traitSet, left, right, joinType, conditionExpr, null, projectedFields);
  }

  public Join copy(RexNode condition, RexNode vectorExpression) {
    return new NestedLoopJoinPrel(this.getCluster(), traitSet, left, right, joinType, condition, vectorExpression, getProjectedFields());
  }

  public Join copy(ImmutableBitSet projectedFields) {
    return new NestedLoopJoinPrel(this.getCluster(), traitSet, left, right, joinType, condition, vectorExpression, projectedFields);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    double leftRowCount = mq.getRowCount(this.getLeft());
    double rightRowCount = mq.getRowCount(this.getRight());
    double nljFactor = PrelUtil.getSettings(getCluster()).getNestedLoopJoinFactor();

    double cpuCost = (leftRowCount * rightRowCount) * nljFactor;

    if (vectorExpression != null) {
      cpuCost *= 0.05;
    }
    Factory costFactory = (Factory) planner.getCostFactory();
    return costFactory.makeCost(leftRowCount * rightRowCount, cpuCost, 0, 0, 0);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
      .itemIf("vectorCondition", vectorExpression, vectorExpression != null);
  }

  public RexNode getCombinedCondition() {
    if(vectorExpression == null){
      return getCondition();
    }
    return RexUtil.composeConjunction(getCluster().getRexBuilder(), Arrays.asList(vectorExpression, condition), false);
  }

  public boolean hasVectorExpression() {
    return vectorExpression != null;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    assert isUnique(getRowType().getFieldNames());

    final PhysicalOperator probePop = ((Prel)left).getPhysicalOperator(creator);
    final PhysicalOperator buildPop = ((Prel)right).getPhysicalOperator(creator);
    final PlannerSettings settings = PrelUtil.getSettings(getCluster());
    final boolean vectorized = settings.getOptions().getOption(VECTORIZED);

    LogicalExpression condition = null;
    LogicalExpression vectorCondition = null;
    if (!vectorized) {
      // the non-vectorized operation doesn't support conditions.
      List<JoinCondition> conditions = Lists.newArrayList();
      final List<String> leftFields = left.getRowType().getFieldNames();
      final List<String> rightFields = right.getRowType().getFieldNames();
      buildJoinConditions(conditions, leftFields, rightFields, leftKeys, rightKeys);
      if(!conditions.isEmpty()) {
        throw UserException.unsupportedError().message("NLJ doesn't support conditions yet. Conditions found %s.", conditions).build(logger);
      }
    } else {
      // vectorized supports conditions.
      final int leftCount = left.getRowType().getFieldCount();
      final int rightCount = leftCount + right.getRowType().getFieldCount();

      // map the fields to the correct input so that RexToExpr can generate appropriate InputReferences
      IntFunction<Optional<Integer>> fieldIndexToInput = i -> {
        if(i < leftCount) {
          return Optional.of(0);
        } else if (i < rightCount) {
          return Optional.of(1);
        } else {
          throw new IllegalArgumentException("Unable to handle input number: " + i);
        }
      };

      condition = RexToExpr.toExpr(
          new ParseContext(settings),
          getInputRowType(),
          getCluster().getRexBuilder(),
          getCondition(),
          true,
          fieldIndexToInput);
      if(vectorExpression != null) {
        vectorCondition = RexToExpr.toExpr(
            new ParseContext(settings),
            getInputRowType(),
            getCluster().getRexBuilder(),
            vectorExpression,
            true,
            fieldIndexToInput);
      }
    }

    // generate the schema for execution. Yes, for some reason we put build first then probe (opposite of Calcite)
    final SchemaBuilder b = BatchSchema.newBuilder();
    int leftSize = left.getRowType().getFieldCount(); // left
    int rightSize = right.getRowType().getFieldCount(); // right
    Set<Integer> buildProjected = new HashSet<>();
    Set<Integer> probeProjected = new HashSet<>();

    if (vectorized) {
      Map<String, Integer> fieldToInd = new HashMap<>();
      ImmutableBitSet projected = getProjectedFields();
      if (projected == null) {
        projected = JoinUtils.projectAll(leftSize + rightSize);
      }
      for (int ind : projected.asList()) {
        fieldToInd.put(getInputRowType().getFieldNames().get(ind), ind);
      }

      for (int i = 0; i < buildPop.getProps().getSchema().getFieldCount(); i++) {
        Field f = buildPop.getProps().getSchema().getFields().get(i);
        if (fieldToInd.containsKey(f.getName())) {
          b.addField(f);
          buildProjected.add(i);
        }
      }

      for (int i = 0; i < probePop.getProps().getSchema().getFieldCount(); i++) {
        Field f = probePop.getProps().getSchema().getFields().get(i);
        if (fieldToInd.containsKey(f.getName())) {
          b.addField(f);
          probeProjected.add(i);
        }
      }
    } else {
      for (Field f : buildPop.getProps().getSchema()) {
        b.addField(f);
      }
      for (Field f : probePop.getProps().getSchema()) {
        b.addField(f);
      }
    }

    final BatchSchema schema = b.build();
    return new NestedLoopJoinPOP(
        creator.props(this, null, schema, RESERVE, LIMIT),
        probePop,
        buildPop,
        joinType,
        condition,
        vectorized,
        vectorCondition,
        buildProjected,
        probeProjected);
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

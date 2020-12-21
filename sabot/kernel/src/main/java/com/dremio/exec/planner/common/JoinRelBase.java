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
package com.dremio.exec.planner.common;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.holders.IntHolder;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.cost.RelMdRowCount;
import com.dremio.exec.planner.logical.JoinNormalizationRule;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.JoinUtils.JoinCategory;
import com.dremio.service.Pointer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Base class for logical and physical Joins implemented in Dremio.
 */
public abstract class JoinRelBase extends Join {
  protected final List<Integer> leftKeys;
  protected final List<Integer> rightKeys;

  /**
   * The join key positions for which null values will not match. null values only match for the
   * "is not distinct from" condition.
   */
  protected final List<Boolean> filterNulls;

  /**
   * The remaining filter condition composed of non-equi expressions
   */
  protected final RexNode remaining;

  /**
   * Fields required by its consumer
   */
  protected ImmutableBitSet projectedFields;

  /**
   * This is rowType for incoming fields.
   * This will be only used when projectedFields property is not null.
   */
  protected RelDataType inputRowType;

  /**
   * Dremio join category
   */
  protected final JoinUtils.JoinCategory joinCategory;

  protected JoinRelBase(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
                        JoinRelType joinType, ImmutableBitSet projectedFields) {
    super(cluster, traits, left, right, condition, CorrelationId.setOf(Collections.emptySet()), joinType);
    leftKeys = Lists.newArrayList();
    rightKeys = Lists.newArrayList();
    filterNulls = Lists.newArrayList();

    remaining = RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys, filterNulls);
    joinCategory = getJoinCategory(condition, leftKeys, rightKeys, filterNulls, remaining);

    this.projectedFields = projectedFields;
    if (projectedFields != null) {
      List<RelDataType> fields = getRowType().getFieldList().stream().map(RelDataTypeField::getType).collect(Collectors.toList());
      List<String> names = ImmutableList.copyOf(getRowType().getFieldNames());
      inputRowType = cluster.getTypeFactory().createStructType(fields, names);
      rowType = JoinUtils.rowTypeFromProjected(left, right, getRowType(), projectedFields, cluster.getTypeFactory());
    }
  }

  public RelDataType getInputRowType() {
    if (inputRowType != null) {
      return inputRowType;
    }
    return getRowType();
  }

  protected static RelTraitSet adjustTraits(RelTraitSet traits) {
    // Join operators do not preserve collations
    return traits.replaceIfs(RelCollationTraitDef.INSTANCE, ImmutableList::of);
  }

  public ImmutableBitSet getProjectedFields() {
    return this.projectedFields;
  }

  @Override public boolean isValid(Litmus litmus, Context context) {
    if (condition != null) {
      if (condition.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
        return litmus.fail("condition must be boolean: {}",
          condition.getType());
      }
      // The input to the condition is a row type consisting of system
      // fields, left fields, and right fields. Very similar to the
      // output row type, except that fields have not yet been made due
      // due to outer joins.
      RexChecker checker =
        new RexChecker(
          getCluster().getTypeFactory().builder()
            .addAll(getSystemFieldList())
            .addAll(getLeft().getRowType().getFieldList())
            .addAll(getRight().getRowType().getFieldList())
            .build(),
          context, litmus);
      condition.accept(checker);
      if (checker.getFailureCount() > 0) {
        return litmus.fail(checker.getFailureCount()
          + " failures in condition " + condition);
      }
    }
    return litmus.succeed();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // normalize join first
    RelNode normalized = JoinNormalizationRule.INSTANCE.normalize(this);
    if (normalized != this) {
      // If normalized, return sum of all converted/generated rels
      final Pointer<RelOptCost> cost = new Pointer<>(planner.getCostFactory().makeZeroCost());
      normalized.accept(new RelShuttleImpl() {
        @Override
        public RelNode visit(RelNode other) {
          cost.value = cost.value.plus(mq.getNonCumulativeCost(other));
          if (!(other instanceof Join)) {
            return super.visit(other);
          }
          return other;
        }
      });
      return cost.value;
    }

    // Compute filter cost
    RelOptCost remainingFilterCost;
    if (remaining.isAlwaysTrue()) {
      remainingFilterCost = planner.getCostFactory().makeZeroCost();
    } else {
      // Similar to FilterRelBase
      double inputRows = Math.max(mq.getRowCount(getLeft()), mq.getRowCount(getRight()));
      double compNum = inputRows;
      double rowCompNum = this.getRowType().getFieldCount() * inputRows ;

      final List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);
      final int conjunctionsSize = conjunctions.size();
      for (int i = 0; i< conjunctionsSize; i++) {
        RexNode conjFilter = RexUtil.composeConjunction(this.getCluster().getRexBuilder(), conjunctions.subList(0, i + 1), false);
        compNum += RelMdUtil.estimateFilteredRows(this, conjFilter, mq);
      }

      double cpuCost = compNum * DremioCost.COMPARE_CPU_COST + rowCompNum * DremioCost.COPY_COST;
      Factory costFactory = (Factory)planner.getCostFactory();
      // Do not include input rows into the extra filter cost
      remainingFilterCost = costFactory.makeCost(0, cpuCost, 0, 0);
    }
    return remainingFilterCost.plus(doComputeSelfCost(planner, mq));
  }


  /**
   * Compute inner cost of the join, not taking into account remaining conditions
   * @param planner
   * @param relMetadataQuery
   * @return
   */
  private RelOptCost doComputeSelfCost(RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
    /*
     * for costing purpose, we don't use JoinUtils.getJoinCategory()
     * to get the join category. here we are more interested in knowing
     * if the join is equality v/s inequality/cartesian join and accordingly
     * compute the cost. JoinUtils.getJoinCategory() will split out
     * the equi-join components from join condition and can mis-categorize
     * an equality join for the purpose of computing plan cost.
     */
    if (leftKeys.isEmpty() && rightKeys.isEmpty()) {
      // Inequality/Cartesian join
      if (PrelUtil.getPlannerSettings(planner).isNestedLoopJoinEnabled()) {
        if (PrelUtil.getPlannerSettings(planner).isNlJoinForScalarOnly()) {
          if (hasScalarSubqueryInput()) {
            return computeLogicalJoinCost(planner, relMetadataQuery);
          }

          /*
           *  Why do we return non-infinite cost for CartesianJoin with non-scalar subquery, when LOPT planner is enabled?
           *   - We do not want to turn on the two Join permutation rule : PushJoinPastThroughJoin.LEFT, RIGHT.
           *   - As such, we may end up with filter on top of join, which will cause CanNotPlan in LogicalPlanning, if we
           *   return infinite cost.
           *   - Such filter on top of join might be pushed into JOIN, when LOPT planner is called.
           *   - Return non-infinite cost will give LOPT planner a chance to try to push the filters.
           */
          if (PrelUtil.getPlannerSettings(planner).isHepOptEnabled()) {
            return computeCartesianJoinCost(planner, relMetadataQuery);
          }

          /*
           * Make cost infinite (not supported)
           */
          return ((Factory)planner.getCostFactory()).makeInfiniteCost();
        }

        // If cartesian joins are allowed for non scalar inputs
        // return cost
        return computeLogicalJoinCost(planner, relMetadataQuery);

      }
      return ((Factory)planner.getCostFactory()).makeInfiniteCost();
    }

    return computeLogicalJoinCost(planner, relMetadataQuery);
  }

  /**
   * Copied for {@link RelMdRowCount#getRowCount(Join, RelMetadataQuery)}. We will be removing this
   * function usage in Dremio code in future: TODO: DX-12150
   *
   * @param mq
   * @return
   */
  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return RelMdRowCount.estimateRowCount(this, mq);
  }

  /**
   * Returns whether there are any elements in common between left and right.
   */
  private static <T> boolean intersects(List<T> left, List<T> right) {
    return new HashSet<>(left).removeAll(right);
  }

  protected boolean uniqueFieldNames(RelDataType rowType) {
    return isUnique(rowType.getFieldNames());
  }

  protected static <T> boolean isUnique(List<T> list) {
    return new HashSet<>(list).size() == list.size();
  }

  public List<Integer> getLeftKeys() {
    return this.leftKeys;
  }

  public List<Integer> getRightKeys() {
    return this.rightKeys;
  }

  public RexNode getRemaining() {
    return remaining;
  }

  public JoinUtils.JoinCategory getJoinCategory() {
    return joinCategory;
  }

  protected  RelOptCost computeCartesianJoinCost(RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
    final double probeRowCount = relMetadataQuery.getRowCount(this.getLeft());
    final double buildRowCount = relMetadataQuery.getRowCount(this.getRight());

    final Factory costFactory = (Factory) planner.getCostFactory();

    final double mulFactor = 10000; // This is a magic number,
                                    // just to make sure Cartesian Join is more expensive
                                    // than Non-Cartesian Join.

    final int keySize = 1 ;  // assume having 1 join key, when estimate join cost.
    final DremioCost cost = (DremioCost) computeHashJoinCostWithKeySize(planner, keySize, relMetadataQuery).multiplyBy(mulFactor);

    // Cartesian join row count will be product of two inputs. The other factors come from the above estimated DremioCost.
    return costFactory.makeCost(
        buildRowCount * probeRowCount,
        cost.getCpu(),
        cost.getIo(),
        cost.getNetwork(),
        cost.getMemory() );

  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    boolean projectAll = projectedFields == null
      || projectedFields.cardinality() == left.getRowType().getFieldCount() + right.getRowType().getFieldCount();
    return super.explainTerms(pw)
      .itemIf("projectedFields", projectedFields, !projectAll);
  }

  protected RelOptCost computeLogicalJoinCost(RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
    // During Logical Planning, although we don't care much about the actual physical join that will
    // be chosen, we do care about which table - bigger or smaller - is chosen as the right input
    // of the join since that is important at least for hash join and we don't currently have
    // hybrid-hash-join that can swap the inputs dynamically.  The Calcite planner's default cost of a join
    // is the same whether the bigger table is used as left input or right. In order to overcome that,
    // we will use the Hash Join cost as the logical cost such that cardinality of left and right inputs
    // is considered appropriately.
    return computeHashJoinCost(planner, relMetadataQuery);
  }

  protected RelOptCost computeHashJoinCost(RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
      return computeHashJoinCostWithKeySize(planner, this.getLeftKeys().size(), relMetadataQuery);
  }

  /**
   *
   * @param planner  : Optimization Planner.
   * @param keySize  : the # of join keys in join condition. Left key size should be equal to right key size.
   * @return         : RelOptCost
   */
  private RelOptCost computeHashJoinCostWithKeySize(RelOptPlanner planner, int keySize, RelMetadataQuery relMetadataQuery) {
    /**
     * DRILL-1023, DX-3859:  Need to make sure that join row count is calculated in a reasonable manner.  Calcite's default
     * implementation is leftRowCount * rightRowCount * discountBySelectivity, which is too large (cartesian join).
     * Since we do not support cartesian join, we should just take the maximum of the two join input row counts when
     * computing cost of the join.
     */
    double probeRowCount = relMetadataQuery.getRowCount(this.getLeft());
    double buildRowCount = relMetadataQuery.getRowCount(this.getRight());

    // cpu cost of hashing the join keys for the build side
    double cpuCostBuild = DremioCost.HASH_CPU_COST * keySize * buildRowCount;
    // cpu cost of hashing the join keys for the probe side
    double cpuCostProbe = DremioCost.HASH_CPU_COST * keySize * probeRowCount;
    // cpu cost associated with really large rows
    double cpuCostColCount = getRowType().getFieldCount() * Math.max(probeRowCount, buildRowCount);

    // cpu cost of evaluating each leftkey=rightkey join condition
    double joinConditionCost = DremioCost.COMPARE_CPU_COST * keySize;

    double factor = PrelUtil.getPlannerSettings(planner).getOptions()
        .getOption(ExecConstants.HASH_JOIN_TABLE_FACTOR_KEY).getFloatVal();
    long fieldWidth = PrelUtil.getPlannerSettings(planner).getOptions()
        .getOption(ExecConstants.AVERAGE_FIELD_WIDTH_KEY).getNumVal();

    // table + hashValues + links
    double memCost =
        (
            (fieldWidth * keySize) +
                IntHolder.WIDTH +
                IntHolder.WIDTH
        ) * buildRowCount * factor;

    double cpuCost = joinConditionCost * (probeRowCount) // probe size determine the join condition comparison cost
        + cpuCostBuild + cpuCostProbe + cpuCostColCount;

    Factory costFactory = (Factory) planner.getCostFactory();

    return costFactory.makeCost(buildRowCount + probeRowCount, cpuCost, 0, 0, memCost);
  }

  private boolean hasScalarSubqueryInput() {
    if (JoinUtils.isScalarSubquery(this.getLeft())
        || JoinUtils.isScalarSubquery(this.getRight())) {
      return true;
    }

    return false;
  }

  /**
   * Creates a copy of this join, overriding condition, system fields and
   * inputs.
   *
   * <p>General contract as {@link RelNode#copy}.
   *
   * @param traitSet        Traits
   * @param conditionExpr   Condition
   * @param left            Left input
   * @param right           Right input
   * @param joinType        Join type
   * @param semiJoinDone    Whether this join has been translated to a
   *                        semi-join
   * @Param projectedFields
   * @return Copy of this join
   */
  public abstract Join copy(RelTraitSet traitSet, RexNode conditionExpr,
                            RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone, ImmutableBitSet projectedFields);

  private static JoinCategory getJoinCategory(RexNode condition,
      List<Integer> leftKeys, List<Integer> rightKeys, List<Boolean> filterNulls, RexNode remaining) {
    if (condition.isAlwaysTrue()) {
      return JoinCategory.CARTESIAN;
    }

    if (!remaining.isAlwaysTrue() || (leftKeys.size() == 0 || rightKeys.size() == 0) ) {
      // for practical purposes these cases could be treated as inequality
      return JoinCategory.INEQUALITY;
    }
    return JoinCategory.EQUALITY;
  }
}

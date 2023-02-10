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

import java.util.List;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.common.JoinRelBase;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.work.foreman.UnsupportedRelOperatorException;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.op.join.JoinUtils.JoinCategory;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;

// abstract base class for the join physical rules
public abstract class JoinPruleBase extends Prule {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinPruleBase.class);

  protected JoinPruleBase(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  protected boolean checkPreconditions(JoinRel join, RelNode left, RelNode right,
      PlannerSettings settings) {
    JoinCategory category = join.getJoinCategory();
    if (category == JoinCategory.CARTESIAN) {
      return false;
    }
    if (category == JoinCategory.INEQUALITY) {
      return isInequalityHashJoinSupported(join, settings.getOptions());
    }
    return true;
  }

  public static boolean isInequalityHashJoinSupported(JoinRelBase join, OptionResolver options) {
    if (!options.getOption(PlannerSettings.EXTRA_CONDITIONS_HASHJOIN)) {
      return false;
    }

    if (!options.getOption(ExecConstants.ENABLE_VECTORIZED_HASHJOIN)) {
      // Inequality hashjoin with non-equi condition is only supported for vectorized hash joins
      return false;
    }

    final boolean hasInequalityExpression = join.getJoinCategory() == JoinCategory.INEQUALITY; // Must have an inequality expression
    final boolean hasEqualityExpression = join.getLeftKeys().size() != 0 && join.getRightKeys().size() != 0; // Must have an equality expression
    final boolean hasOuterJoin = join.getJoinType() != JoinRelType.INNER; // Must be an outer join

    if (!hasInequalityExpression || !hasEqualityExpression || !hasOuterJoin) {
      return false;
    }

    if (join.getRemaining().isAlwaysTrue()) {
      return false;
    }

    // We can only support inequality hash join for vectorized hash joins
    // and we can only vectorize if the join keys are of a safe type
    return keysAreJoinable(join.getLeft(), join.getLeftKeys()) && keysAreJoinable(join.getRight(), join.getRightKeys());
  }

  private static boolean keysAreJoinable(RelNode relNode, List<Integer> keys) {
    for (int key : keys) {
      RelDataTypeField field = relNode.getRowType().getFieldList().get(key);
      if (isNotJoinableType(field.getType().getSqlTypeName())) {
        return false;
      }
    }
    return true;
  }

  private static boolean isNotJoinableType(SqlTypeName sqlTypeName) {
    switch (sqlTypeName) {
      case BIGINT:
      case DATE:
      case FLOAT:
      case INTEGER:
      case TIME:
      case TIMESTAMP:
      case VARBINARY:
      case VARCHAR:
      case DECIMAL:
        // These types are joinable
        return false;
      default:
        return true;
    }
  }

  protected ImmutableList<DistributionField> getDistributionField(List<Integer> keys) {
    ImmutableList.Builder<DistributionField> distFields = ImmutableList.builder();

    for (int key : keys) {
      distFields.add(new DistributionField(key));
    }

    return distFields.build();
  }

  public static boolean checkBroadcastConditions(
    JoinRelType joinType,
    RelNode probe,
    RelNode build,
    double probeRowCount,
    double buildRowCount,
    boolean hasBroadcastHint) {
    // Probe = Left side, Build = Right side
    // Build node is the one that is being considered to be broadcasted.
    if (probe.getTraitSet().getTrait(DistributionTraitDef.INSTANCE).equals(DistributionTrait.SINGLETON)
      || (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT))
    {
      return false;
    }

    // Now that we have checked this join supports broadcast, we can go ahead and return true if the user specified
    // the BROADCAST hint without needing to check on the costing.
    if (hasBroadcastHint) {
      return true;
    }

    final int buildColumnCount = build.getRowType().getFieldCount();
    final double buildCellCount = buildRowCount * buildColumnCount;
    final PlannerSettings plannerSettings = PrelUtil.getSettings(probe.getCluster());
    if (buildRowCount >= plannerSettings.getBroadcastThreshold()) {
      return false;
    }

    // DX-3862:  For broadcast joins, the cost should not just consider the traits and join type.  If the broadcast table is small enough,
    // we shouldn't need to worry too much and allow broadcast join and see what the planner picks.
    double cellCountThreshold = plannerSettings.getOptions().getOption(PlannerSettings.BROADCAST_CELL_COUNT_THRESHOLD);
    if (buildCellCount > cellCountThreshold) {
      // DX-17913 : For cases when the table is too big due to large number of columns, we should not do the broadcast join.
      logger.debug("Won't do broadcast join if the size of the table is too big based of total number of cells (rows x columns)");
      return false;
    }
    if (buildRowCount <= plannerSettings.getOptions().getOption(PlannerSettings.BROADCAST_MIN_THRESHOLD)) {
      logger.debug("Enable broadcast plan? true (rightRowCount {} smaller than minimum broadcast threshold)", buildRowCount);
      return true;
    }

    final long maxWidthPerNode = plannerSettings.getMaxWidthPerNode();

    if (maxWidthPerNode <= 0) {
      logger.debug("No executors are available. Won't do broadcast join");
      return false;
    }

    // In this case, the broadcast table is big-ish.  So, we should check to see if it is reasonable to do broadcast.
    // The broadcasted table will be sent at most (numEndPoints * maxWidthPerNode) times, (rightRowCount) rows.  We add a
    // penalty to broadcast (broadcastFactor).
    final double broadcastFactor = plannerSettings.getBroadcastFactor();

    final int numEndPoints = plannerSettings.numEndPoints();
    final long maxWidthPerQuery = plannerSettings.getOptions().getOption(ExecConstants.MAX_WIDTH_GLOBAL);
    final long sliceTarget = plannerSettings.getSliceTarget();
    final double minFactor = Doubles.min(probeRowCount * 1.0 / sliceTarget, numEndPoints * maxWidthPerNode, maxWidthPerQuery);
    final boolean enableBroadCast = (minFactor * broadcastFactor * buildRowCount < probeRowCount);
    logger.debug("Enable broadcast plan? {} minFactor {} (numEndPoints {}, maxWidthPerNode {}, rightRowCount {}, broadcastFactor {}, leftRowCount {}, sliceTarget {}, maxWidthPerQuery {})",
        enableBroadCast, minFactor, numEndPoints, maxWidthPerNode, buildRowCount, broadcastFactor, probeRowCount, sliceTarget, maxWidthPerQuery);
    return enableBroadCast;
  }

  protected void createDistBothPlan(RelOptRuleCall call, JoinRel join,
      RelNode left, RelNode right,
      RelCollation collationLeft, RelCollation collationRight, boolean hashSingleKey) throws InvalidRelException, UnsupportedRelOperatorException {

    /* If join keys are  l1 = r1 and l2 = r2 and ... l_k = r_k, then consider the following options of plan:
     *   1) Plan1: distributed by (l1, l2, ..., l_k) for left side and by (r1, r2, ..., r_k) for right side.
     *   2) Plan2: distributed by l1 for left side, by r1 for right side.
     *   3) Plan3: distributed by l2 for left side, by r2 for right side.
     *   ...
     *      Plan_(k+1): distributed by l_k for left side, by r_k by right side.
     *
     *   Whether enumerate plan 2, .., Plan_(k+1) depends on option : hashSingleKey.
     */

    final ImmutableList<DistributionField> leftDistributionFields = getDistributionField(join.getLeftKeys());
    final ImmutableList<DistributionField> rightDistributionFields = getDistributionField(join.getRightKeys());

    DistributionTrait hashLeftPartition = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, leftDistributionFields);
    DistributionTrait hashRightPartition = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, rightDistributionFields);

    createDistBothPlan(call, join, left, right, collationLeft, collationRight, hashLeftPartition, hashRightPartition);

    assert (join.getLeftKeys().size() == join.getRightKeys().size());

    if (!hashSingleKey) {
      return;
    }

    int numJoinKeys = join.getLeftKeys().size();
    if (numJoinKeys > 1) {
      for (int i = 0; i < numJoinKeys; i++) {
        hashLeftPartition = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, leftDistributionFields.subList(i, i+1));
        hashRightPartition = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, rightDistributionFields.subList(i, i+1));

        createDistBothPlan(call, join, left, right, collationLeft, collationRight, hashLeftPartition, hashRightPartition);
      }
    }
  }


  // Create join plan with both left and right children hash distributed. If the physical join type
  // is MergeJoin, a collation must be provided for both left and right child and the plan will contain
  // sort converter if necessary to provide the collation.
  protected abstract void createDistBothPlan(RelOptRuleCall call, JoinRel join,
                                  RelNode left, RelNode right,
                                  RelCollation collationLeft, RelCollation collationRight,
                                  DistributionTrait hashLeftPartition, DistributionTrait hashRightPartition) throws InvalidRelException, UnsupportedRelOperatorException;

  // Create join plan with left child ANY distributed and right child BROADCAST distributed. If the physical join type
  // is MergeJoin, a collation must be provided for both left and right child and the plan will contain sort converter
  // if necessary to provide the collation.
  protected abstract void createBroadcastPlan(final RelOptRuleCall call, final JoinRel join,
                                              final RexNode joinCondition,
                                              final RelNode left, final RelNode right,
                                              final RelCollation collationLeft, final RelCollation collationRight) throws InvalidRelException;
}

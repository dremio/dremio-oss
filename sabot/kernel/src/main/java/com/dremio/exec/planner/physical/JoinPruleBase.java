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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.work.foreman.UnsupportedRelOperatorException;
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
    if (category == JoinCategory.CARTESIAN || category == JoinCategory.INEQUALITY) {
      return false;
    }
    return true;
  }

  protected ImmutableList<DistributionField> getDistributionField(List<Integer> keys) {
    ImmutableList.Builder<DistributionField> distFields = ImmutableList.builder();

    for (int key : keys) {
      distFields.add(new DistributionField(key));
    }

    return distFields.build();
  }

  protected boolean checkBroadcastConditions(RelOptPlanner planner, JoinRel join, RelNode left, RelNode right) {
    final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
    // Right node is the one that is being considered to be broadcasted..
    double targetRowCount = mq.getRowCount(right);
    int targetColumnCount = right.getRowType().getFieldCount();
    double targetCellCount = targetRowCount * targetColumnCount;
    double otherRowCount = mq.getRowCount(left);

    if (targetRowCount < PrelUtil.getSettings(join.getCluster()).getBroadcastThreshold()
        && ! left.getTraitSet().getTrait(DistributionTraitDef.INSTANCE).equals(DistributionTrait.SINGLETON)
        && (join.getJoinType() == JoinRelType.INNER || join.getJoinType() == JoinRelType.LEFT)) {
      // DX-3862:  For broadcast joins, the cost should not just consider the traits and join type.  If the broadcast table is small enough,
      // we shouldn't need to worry too much and allow broadcast join and see what the planner picks.
      final PlannerSettings plannerSettings = PrelUtil.getSettings(join.getCluster());
      double cellCountThreshold = plannerSettings.getOptions().getOption(PlannerSettings.BROADCAST_CELL_COUNT_THRESHOLD);
      if (targetCellCount > cellCountThreshold) {
        // DX-17913 : For cases when the table is too big due to large number of columns, we should not do the broadcast join.
        logger.debug("Won't do broadcast join if the size of the table is too big based of total number of cells (rows x columns)");
        return false;
      }
      if (targetRowCount <= plannerSettings.getOptions().getOption(PlannerSettings.BROADCAST_MIN_THRESHOLD)) {
        logger.debug("Enable broadcast plan? true (rightRowCount {} smaller than minimum broadcast threshold)", targetRowCount);
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
      final double minFactor = Doubles.min(otherRowCount * 1.0 / sliceTarget, numEndPoints * maxWidthPerNode, maxWidthPerQuery);
      final boolean enableBroadCast = (minFactor * broadcastFactor * targetRowCount < otherRowCount);
      logger.debug("Enable broadcast plan? {} minFactor {} (numEndPoints {}, maxWidthPerNode {}, rightRowCount {}, broadcastFactor {}, leftRowCount {}, sliceTarget {}, maxWidthPerQuery {})",
          enableBroadCast, minFactor, numEndPoints, maxWidthPerNode, targetRowCount, broadcastFactor, otherRowCount, sliceTarget, maxWidthPerQuery);
      return enableBroadCast;
    }

    return false;
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

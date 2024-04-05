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
package com.dremio.exec.planner;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.physical.PrelUtil;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.LoptJoinTree;
import org.apache.calcite.rel.rules.LoptMultiJoin;

/**
 * Version of {@link JoinCommuteRule} that only applies if the rowcount difference is greater than a
 * configurable threshold, or if rowcount is within threshold, but right side has more columns.
 * Having fewer columns on the right(build) side will have a lower memory footprint. This is similar
 * to the logic used in {@link
 * org.apache.calcite.rel.rules.DremioLoptOptimizeJoinRule#swapInputs(RelMetadataQuery,
 * LoptMultiJoin, LoptJoinTree, LoptJoinTree, boolean)}
 */
public class DremioJoinCommuteRule extends JoinCommuteRule {
  public static final RelOptRule INSTANCE = new DremioJoinCommuteRule();

  protected DremioJoinCommuteRule() {
    super(
        Config.DEFAULT
            .withRelBuilderFactory(DremioRelFactories.LOGICAL_BUILDER)
            .as(Config.class)
            .withOperandFor(JoinRel.class)
            .withSwapOuter(true));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelMetadataQuery mq = join.getCluster().getMetadataQuery();

    // Check if we need to force a broadcast exchange.
    MoreRelOptUtil.BroadcastHintCollector leftHintCollector =
        new MoreRelOptUtil.BroadcastHintCollector();
    MoreRelOptUtil.BroadcastHintCollector rightHintCollector =
        new MoreRelOptUtil.BroadcastHintCollector();
    join.getLeft().accept(leftHintCollector);
    boolean leftBroadcast = leftHintCollector.shouldBroadcast();
    join.getRight().accept(rightHintCollector);
    boolean rightBroadcast = rightHintCollector.shouldBroadcast();

    if (leftBroadcast && !rightBroadcast) {
      // Need to swap since we want to broadcast the left.
      return true;
    } else if (!leftBroadcast && rightBroadcast) {
      // Should not swap regardless of the row counts since we want to
      // broadcast the right.
      return false;
    }

    double leftCount = mq.getRowCount(join.getLeft());
    double rightCount = mq.getRowCount(join.getRight());
    double swapFactor = PrelUtil.getSettings(join.getCluster()).getHashJoinSwapMarginFactor();
    boolean leftSmaller = leftCount < (1 - swapFactor) * rightCount;
    if (leftSmaller) {
      return true;
    }
    boolean leftBigger = leftCount > (1 + swapFactor) * rightCount;
    if (leftBigger) {
      return false;
    }
    // left and right about the same. Swap if right has more columns, so less memory is needed on
    // build side
    // of hash join
    return join.getRight().getRowType().getFieldCount()
        > join.getLeft().getRowType().getFieldCount();
  }
}

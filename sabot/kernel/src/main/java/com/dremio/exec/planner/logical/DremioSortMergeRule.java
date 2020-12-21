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
package com.dremio.exec.planner.logical;

import java.math.BigDecimal;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

/**
 * Copied from HiveSortMergeRule.
 * This rule will merge two Sort operators.
 * <p>
 * It is applied when the top match is a pure limit operation (no sorting).
 * <p>
 * If the bottom operator is not synthetic and does not contain a limit,
 * we currently bail out. Thus, we avoid a lot of unnecessary limit operations
 * in the middle of the execution plan that could create performance regressions.
 */
public class DremioSortMergeRule extends RelOptRule {

  public static final DremioSortMergeRule INSTANCE = new DremioSortMergeRule();

  private DremioSortMergeRule() {
    super(RelOptRule.operand(LogicalSort.class, RelOptRule.operand(LogicalSort.class, any())),
      DremioRelFactories.CALCITE_LOGICAL_BUILDER,
      "DremioSortMergeRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Sort topSort = call.rel(0);
    final Sort bottomSort = call.rel(1);

    // If top operator is not a pure limit, we bail out
    if (!pureLimitRelNode(topSort)) {
      return false;
    }

    // If the bottom operator is not synthetic and it does not contain a limit,
    // we will bail out; we do not want to end up with limits all over the tree
    return limitRelNode(bottomSort);
  }

  public void onMatch(RelOptRuleCall call) {
    final Sort topSort = call.rel(0);
    final Sort bottomSort = call.rel(1);

    final RexNode newOffset;
    final RexNode newLimit;
    if (limitRelNode(bottomSort)) {
      final RexBuilder rexBuilder = topSort.getCluster().getRexBuilder();
      int topOffset = topSort.offset == null ? 0 : RexLiteral.intValue(topSort.offset);
      int topLimit = RexLiteral.intValue(topSort.fetch);
      int bottomOffset = bottomSort.offset == null ? 0 : RexLiteral.intValue(bottomSort.offset);
      int bottomLimit = RexLiteral.intValue(bottomSort.fetch);

      // Three different cases
      if (topOffset + topLimit <= bottomLimit) {
        // 1. Fully contained
        // topOffset + topLimit <= bottomLimit
        newOffset = bottomOffset + topOffset == 0 ? null :
          rexBuilder.makeExactLiteral(BigDecimal.valueOf(bottomOffset + topOffset));
        newLimit = topSort.fetch;
      } else if (topOffset < bottomLimit) {
        // 2. Partially contained
        // topOffset + topLimit > bottomLimit && topOffset < bottomLimit
        newOffset = bottomOffset + topOffset == 0 ? null :
          rexBuilder.makeExactLiteral(BigDecimal.valueOf(bottomOffset + topOffset));
        newLimit = rexBuilder.makeExactLiteral(BigDecimal.valueOf(bottomLimit - topOffset));
      } else {
        // 3. Outside
        // we need to create a new limit 0
        newOffset = null;
        newLimit = rexBuilder.makeExactLiteral(BigDecimal.valueOf(0));
      }
    } else {
      // Bottom operator does not contain offset/fetch
      newOffset = topSort.offset;
      newLimit = topSort.fetch;
    }

    final Sort newSort = bottomSort.copy(bottomSort.getTraitSet(),
      bottomSort.getInput(), bottomSort.collation, newOffset, newLimit);

    call.transformTo(newSort);
  }

  public static boolean pureLimitRelNode(RelNode rel) {
    return limitRelNode(rel) && !orderRelNode(rel);
  }

  public static boolean limitRelNode(RelNode rel) {
    return rel instanceof Sort && ((Sort) rel).fetch != null;
  }

  public static boolean orderRelNode(RelNode rel) {
    return rel instanceof Sort && !((Sort) rel).getCollation().getFieldCollations().isEmpty();
  }
}

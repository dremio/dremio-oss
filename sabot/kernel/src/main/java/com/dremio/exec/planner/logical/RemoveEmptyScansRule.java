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

import com.dremio.exec.calcite.logical.EmptyCrel;
import com.dremio.exec.planner.common.ScanRelBase;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/**
 * If the underlying table has zero splits, we use {@link EmptyCrel} as the logical {@link
 * org.apache.calcite.rel.RelNode} as opposed to other specific implementations of {@link
 * ScanRelBase}
 */
public class RemoveEmptyScansRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new RemoveEmptyScansRule();

  private RemoveEmptyScansRule() {
    super(RelOptHelper.any(ScanRelBase.class, Convention.NONE), "RemoveEmptyScansRuleRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ScanRelBase scan = call.rel(0);
    if (scan.getTableMetadata().getSplitCount() == 0) {
      call.transformTo(
          new EmptyCrel(
              scan.getCluster(),
              scan.getTraitSet(),
              scan.getTable(),
              scan.getPluginId(),
              scan.getTableMetadata(),
              scan.getProjectedColumns(),
              scan.getObservedRowcountAdjustment(),
              scan.getHints()));
    }
  }
}

/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import com.dremio.exec.calcite.logical.OldScanCrel;

/**
 * Converts OldScanCrel (a scan in Calcite's Convention.NONE) to LOGICAL scan, ScanRel.
 */
public final class OldScanRelRule extends RelOptRule {
  public static OldScanRelRule INSTANCE = new OldScanRelRule();

  private OldScanRelRule() {
    super(RelOptHelper.any(OldScanCrel.class, Convention.NONE), "ScanRelRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    OldScanCrel logicalScan = call.rel(0);
    // Plugins can be null for certain group scans (I believe Direct Group Scan is one of them)
    return logicalScan.getGroupScan().getStoragePluginConvention() == null
        || logicalScan.getGroupScan().getStoragePluginConvention() == Convention.NONE;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final OldScanCrel scanRel = call.rel(0);
    call.transformTo(
        new ScanRel(
            scanRel.getCluster(),
            scanRel.getTable(),
            scanRel.getTraitSet().plus(Rel.LOGICAL),
            scanRel.getRowType(),
            scanRel.getGroupScan(),
            scanRel.getLayoutInfo(),
            false,
            false,
            scanRel.getRowCountDiscount()
            ));
  }
}

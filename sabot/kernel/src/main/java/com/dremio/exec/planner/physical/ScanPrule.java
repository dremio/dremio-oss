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
package com.dremio.exec.planner.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;

import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.logical.ScanRel;
import com.dremio.exec.planner.logical.RelOptHelper;

public class ScanPrule extends Prule{
  public static final RelOptRule INSTANCE = new ScanPrule();

  public ScanPrule() {
    super(RelOptHelper.any(ScanRel.class), "Prel.ScanPrule");

  }
  @Override
  public void onMatch(RelOptRuleCall call) {
    final ScanRel scan = (ScanRel) call.rel(0);

    GroupScan groupScan = scan.getGroupScan();

    DistributionTrait partition =
        (groupScan.getMaxParallelizationWidth() > 1 || groupScan.getDistributionAffinity() == DistributionAffinity.HARD)
            ? DistributionTrait.ANY : DistributionTrait.SINGLETON;

    final RelTraitSet traits = scan.getTraitSet().plus(Prel.PHYSICAL).plus(partition);

    final OldScanPrelBase newScan =
        OldScanPrel.create(scan, traits, groupScan, scan.getRowType(), scan.getTable().getQualifiedName());

    call.transformTo(newScan);
  }

}

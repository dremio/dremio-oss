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
package com.dremio.exec.store.sys;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.google.common.collect.ImmutableList;

/**
 * Rule that converts System logical to physical scan
 */
public class SystemScanPrule extends RelOptRule {

  public static final RelOptRule INSTANCE = new SystemScanPrule();

  public SystemScanPrule() {
    super(RelOptHelper.any(SystemScanDrel.class), "SystemScanPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    SystemScanDrel logicalScan = call.rel(0);
    SystemScanPrel physicalScan = new SystemScanPrel(
        logicalScan.getCluster(),
        logicalScan.getTraitSet().replace(Prel.PHYSICAL),
        logicalScan.getTable(),
        logicalScan.getTableMetadata(),
        logicalScan.getProjectedColumns(),
        logicalScan.getObservedRowcountAdjustment(),
        ImmutableList.of());

    call.transformTo(physicalScan);
  }
}

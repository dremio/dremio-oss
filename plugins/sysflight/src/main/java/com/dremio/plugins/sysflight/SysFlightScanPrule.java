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
package com.dremio.plugins.sysflight;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.store.StoragePlugin;
import com.google.common.collect.ImmutableList;

/**
 * Rule that converts Flight logical to physical scan
 */
public class SysFlightScanPrule extends RelOptRule {

  private SysFlightStoragePlugin plugin;

  public SysFlightScanPrule(StoragePlugin plugin) {
    super(RelOptHelper.any(SysFlightScanDrel.class), "FlightScanPrule");
    this.plugin = (SysFlightStoragePlugin) plugin;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    SysFlightScanDrel logicalScan = call.rel(0);
    SysFlightScanPrel physicalScan = new SysFlightScanPrel(
        logicalScan.getCluster(),
        logicalScan.getTraitSet().replace(Prel.PHYSICAL),
        logicalScan.getTable(),
        logicalScan.getTableMetadata(),
        null,
        logicalScan.getProjectedColumns(),
        logicalScan.getObservedRowcountAdjustment(),
        plugin,
        ImmutableList.of()
        );

    call.transformTo(physicalScan);
  }
}

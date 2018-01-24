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
package com.dremio.exec.store.hbase;

import org.apache.calcite.plan.RelOptRule;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;

public class HBaseScanRule extends SourceLogicalConverter {

  public static final RelOptRule INSTANCE = new HBaseScanRule();

  private HBaseScanRule() {
    super(HBaseStoragePlugin.TYPE);

  }
  @Override
  public Rel convertScan(ScanCrel scan) {
    return new HBaseScanDrel(scan.getCluster(), scan.getTraitSet().plus(Rel.LOGICAL), scan.getTable(), scan.getPluginId(), scan.getTableMetadata(), scan.getProjectedColumns(), scan.getObservedRowcountAdjustment());
  }

}

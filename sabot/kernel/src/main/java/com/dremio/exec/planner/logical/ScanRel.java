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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.common.OldScanRelBase;
import com.dremio.exec.planner.sql.MaterializationDescriptor.LayoutInfo;
import com.google.common.base.Objects;

/**
 * GroupScan of a Dremio table.
 */
@Deprecated
public class ScanRel extends OldScanRelBase implements Rel {

  /****************************************
   * Members used in equals()/hashCode()!
   * If you add a new member that needs to be in equality, add them equals() and hashCode().
   ****************************************/
  private final boolean partitionFilterPushdown;
  private final boolean projectForFlattenPushedDown;
  private final LayoutInfo layoutInfo;



  /** Creates a ScanRel. */
  public ScanRel(final RelOptCluster cluster,
                      final RelOptTable relOptTable,
                      final RelTraitSet traits,
                      final RelDataType rowType,
                      final GroupScan groupScan,
                      final LayoutInfo layoutInfo,
                      boolean partitionFilterPushdown,
                      boolean projectForFlattenPushedDown,
                      double discountRowCount) {
    super(cluster, traits, relOptTable, rowType, groupScan, discountRowCount);
    this.partitionFilterPushdown = partitionFilterPushdown;
    this.projectForFlattenPushedDown = projectForFlattenPushedDown;
    this.layoutInfo = layoutInfo;
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {
    throw new UnsupportedOperationException();
  }

  public LayoutInfo getLayoutInfo(){
    return layoutInfo;
  }

  public List<String> getSortColumns() {
    if (layoutInfo == null) {
      return groupScan.getSortColumns();
    }
    return layoutInfo.getSortColumns();
  }

  public boolean partitionFilterPushdown() {
    return this.partitionFilterPushdown;
  }

  public boolean isProjectForFlattenPushedDown() {
    return this.projectForFlattenPushedDown;
  }

  /****************************************
   * Add members if they are needed for equality, do not auto-generate!
   ****************************************/
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScanRel that = (ScanRel) o;
    return partitionFilterPushdown == that.partitionFilterPushdown &&
        projectForFlattenPushedDown == that.projectForFlattenPushedDown &&
        Objects.equal(rowType, that.rowType) &&
        Objects.equal(groupScan, that.groupScan) &&
        Objects.equal(layoutInfo, that.layoutInfo);
  }

  /****************************************
   * Add members if they are needed for equality, do not auto-generate!
   ****************************************/
  @Override
  public int hashCode() {
    return Objects.hashCode(
        rowType,
        groupScan,
        partitionFilterPushdown,
        projectForFlattenPushedDown,
        layoutInfo);
  }
}

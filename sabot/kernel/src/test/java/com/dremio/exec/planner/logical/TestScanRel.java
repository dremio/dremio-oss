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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.google.common.collect.ImmutableList;

/**
 * Tests several aspects of {@link ScanRel}
 */
public class TestScanRel {

  @Mock
  private RelOptCluster cluster;
  @Mock
  private RelMetadataQuery metadataQuery;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private RelOptTable table;
  @Mock
  private RelDataType relType;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private GroupScan groupScan;



  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    // set up planner
    RelOptPlanner planner = mock(RelOptPlanner.class);
    when(cluster.getPlanner()).thenReturn(planner);
    when(planner.getCostFactory()).thenReturn(new DremioCost.Factory());
    // for some reason mockito cannot return accurate type leading to cast exception when deep stub is used so mock explicitly
    final Context context = mock(Context.class);
    when(planner.getContext()).thenReturn(context);

    final PlannerSettings settings = mock(PlannerSettings.class);
    when(context.unwrap(PlannerSettings.class)).thenReturn(settings);
    // set up table
    when(table.getRowType().getFieldCount()).thenReturn(10);
    // set up group scan
    when(groupScan.getColumns()).thenReturn(ImmutableList.of(SchemaPath.getSimplePath("a")));
    when(groupScan.getScanStats(any(PlannerSettings.class)).getRecordCount()).thenReturn(100L);

  }

  // make sure less scan cost factor yields less cost
  @Test
  public void testSmallerScanCostFactorReturnsSmallerCost() {
    // Mockito cannot mock RelTraitSet as it is final. This is just fine as we should never need this.
    final RelTraitSet traits = null;
    final ScanRel scanRel = new ScanRel(cluster, table, traits, relType, groupScan, null, false, false, 1);

    final RelOptPlanner planner = cluster.getPlanner();
    when(groupScan.getScanCostFactor()).thenReturn(ScanCostFactor.of(10));
    final RelOptCost first = scanRel.computeSelfCost(planner, metadataQuery);
    when(groupScan.getScanCostFactor()).thenReturn(ScanCostFactor.of(100));
    final RelOptCost second = scanRel.computeSelfCost(planner, metadataQuery);

    assertTrue("Lower scan factor must yield smaller cost", first.isLt(second));
  }


  /**
   * Make sure parquet is the cheapest across all factors until we find something faster :)
   *
   * Assuming all group scan implementations use built-in scan factors, this test guarantees that parquet scan will be
   * cheaper as compared to other scans, which is integral to acceleration.
   *
   * We, however, do not enforce use of built-in ScanCostFactor for extensibility reasons.
   *
   * Ideally, we should create parquet and non-parquet groupscans to test scan cost factor cheapness but writing this
   * test is substantially harder.
   */
  @Test
  public void testParquetScanFactorIsTheCheapest() {
    final ScanCostFactor forParquet = ScanCostFactor.PARQUET;
    final ScanCostFactor[] all = new ScanCostFactor[] {
        ScanCostFactor.MONGO,
        ScanCostFactor.ELASTIC,
        ScanCostFactor.EASY,
        ScanCostFactor.OTHER
    };

    for (final ScanCostFactor other : all) {
      assertTrue("parquet scan cost must be the cheapest", forParquet.getFactor() < other.getFactor());
    }
  }
}

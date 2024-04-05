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
package com.dremio.exec.store.iceberg;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.exec.ExecConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestIcebergScanPlanWithDeleteFiles extends BaseTestIcebergScanPlan {

  private static IcebergTestTables.Table orders;
  private static IcebergTestTables.Table ordersWithDeletes;

  @BeforeClass
  public static void initTables() {
    orders = IcebergTestTables.V2_ORDERS.get();
    ordersWithDeletes = IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
  }

  @AfterClass
  public static void cleanupTables() throws Exception {
    orders.close();
    ordersWithDeletes.close();
  }

  @Test
  public void testBasicPlan() throws Exception {
    try (AutoCloseable option =
        withSystemOption(ExecConstants.ENABLE_ICEBERG_MERGE_ON_READ_SCAN, true)) {
      String sql = String.format("SELECT order_id FROM %s", ordersWithDeletes.getTableName());
      testPlanMatchingPatterns(
          sql,
          new String[] {
            dataFileScanWithRowCount(9000),
            deleteFileAggWithRowCount(9),
            "HashJoin",
            dataManifestScanWithRowCount(9),
            dataManifestList(),
            deleteManifestScanWithRowCount(4),
            deleteManifestList()
          });
    }
  }

  @Test
  public void testPlanWithNoDeletes() throws Exception {
    try (AutoCloseable option =
        withSystemOption(ExecConstants.ENABLE_ICEBERG_MERGE_ON_READ_SCAN, true)) {
      String sql = String.format("SELECT order_id FROM %s", orders.getTableName());
      testPlanMatchingPatterns(
          sql,
          new String[] {
            dataFileScanWithRowCount(600), splitGenManifestScanWithRowCount(6), dataManifestList(),
          },
          deleteManifestContent());
    }
  }

  @Test
  public void testWithFlagDisabled() throws Exception {
    try (AutoCloseable option =
        withSystemOption(ExecConstants.ENABLE_ICEBERG_MERGE_ON_READ_SCAN, false)) {
      assertThatThrownBy(
              () ->
                  test(
                      "ALTER PDS %s REFRESH METADATA FORCE UPDATE",
                      ordersWithDeletes.getTableName()))
          .hasMessageContaining("Iceberg V2 tables with delete files are not supported.");
    }
  }
}

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

import com.dremio.common.AutoCloseables;
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
    AutoCloseables.close(orders, ordersWithDeletes);
  }

  @Test
  public void testBasicPlan() throws Exception {
    String sql = String.format("SELECT order_id FROM %s", ordersWithDeletes.getTableName());
    testPlanMatchingPatterns(
        sql,
        new String[] {
          filterWithRowCount(4500),
          "HashJoin",
          dataFileScanWithRowCount(9000),
          dataManifestScanWithRowCount(9),
          dataManifestList(),
          dataFileScanWithRowCount(9000),
          deleteManifestScanWithRowCount(4),
          deleteManifestList()
        });
  }

  @Test
  public void testPlanWithNoDeletes() throws Exception {
    String sql = String.format("SELECT order_id FROM %s", orders.getTableName());
    testPlanMatchingPatterns(
        sql,
        new String[] {
          dataFileScanWithRowCount(600), splitGenManifestScanWithRowCount(6), dataManifestList(),
        },
        deleteManifestContent());
  }
}

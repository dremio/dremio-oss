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
package com.dremio;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.util.TestTools;

public class TestCTASPartitionFilter extends PlanTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestCTASPartitionFilter.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @BeforeClass
  public static void setup() throws Exception {
    test("alter session set \"planner.slice_target\" = 1");
    test("use dfs_test");
  }

  @AfterClass
  public static void shutdown() throws Exception {
    test("alter session reset \"planner.slice_target\"");
  }

  private static void testExcludeFilter(String query, int expectedNumFiles,
      String excludedFilterPattern, int expectedRowCount) throws Exception {
    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "splits=\\[" + expectedNumFiles;
    testPlanMatchingPatterns(query, new String[]{numFilesPattern}, new String[]{excludedFilterPattern});
  }

  private static void testIncludeFilter(String query, int expectedNumFiles,
                                        String includedFilterPattern, int expectedRowCount) throws Exception {
    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "splits=\\[" + expectedNumFiles;
    testPlanMatchingPatterns(query, new String[]{numFilesPattern, includedFilterPattern}, new String[]{});
  }

  @Test
  @Ignore("path no longer consistent now that it includes partition directory")
  public void testDrill3965() throws Exception {
    test("create table orders_auto_partition HASH partition by(o_orderpriority) as select * from cp.\"tpch/orders.parquet\"");
    test("explain plan for select count(*) from \"orders_auto_partition/1_0_1.parquet\" where o_orderpriority = '5-LOW'");
  }

  @Test
  public void withDistribution() throws Exception {
    // need to rename dir0, dir1 else they conflict with dir0, dir1 when reading then new table
    test(String.format("create table orders_distribution HASH partition by (o_orderpriority) as select o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, dir0 as \"year\", dir1 as \"quarter\" from dfs.\"%s/multilevel/parquet\"", TEST_RES_PATH));
    String query = "select * from orders_distribution where o_orderpriority = '1-URGENT'";
    testExcludeFilter(query, 1, "Filter", 24);
  }

  @Test
  public void withoutDistribution() throws Exception {
    // need to rename dir0, dir1 else they conflict with dir0, dir1 when reading then new table
    test(String.format("create table orders_no_distribution partition by (o_orderpriority) as select o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, dir0 as \"year\", dir1 as \"quarter\" from dfs.\"%s/multilevel/parquet\"", TEST_RES_PATH));
    String query = "select * from orders_no_distribution where o_orderpriority = '1-URGENT'";
    testExcludeFilter(query, 2, "Filter", 24);
  }

  @Test
  public void testDRILL3410() throws Exception {
    // need to rename dir0, dir1 else they conflict with dir0, dir1 when reading then new table
    test(String.format("create table drill_3410 HASH partition by (o_orderpriority) as select o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, dir0 as \"year\", dir1 as \"quarter\" from dfs.\"%s/multilevel/parquet\"", TEST_RES_PATH));
    String query = "select * from drill_3410 where (o_orderpriority = '1-URGENT' and o_orderkey = 10) or (o_orderpriority = '2-HIGH' or o_orderkey = 11)";
    testIncludeFilter(query, 5, "Filter", 34);
  }


  @Ignore("DX-4214")
  @Test
  public void testDRILL3414() throws Exception {
    test(String.format("create table drill_3414 HASH partition by (x, y) as select dir0 as x, dir1 as y, columns from dfs.\"%s/multilevel/csv\"", TEST_RES_PATH));
    String query = ("select * from drill_3414 where (x=1994 or y='Q1') and (x=1995 or y='Q2' or columns[0] > 5000)");
    // learn schema
    test("select * from drill_3414");
    testIncludeFilter(query, 6, "Filter", 20);
  }

  @Ignore("DX-4214")
  @Test
  public void testDRILL3414_2() throws Exception {
    test(String.format("create table drill_3414_2 HASH partition by (x, y) as select dir0 as x, dir1 as y, columns from dfs.\"%s/multilevel/csv\"", TEST_RES_PATH));
    String query = ("select * from drill_3414_2 where (x=1994 or y='Q1') and (x=1995 or y='Q2' or columns[0] > 5000) or columns[0] < 3000");
    // learn schema
    test("select * from drill_3414_2");
    testIncludeFilter(query, 1, "Filter", 120);
  }
}

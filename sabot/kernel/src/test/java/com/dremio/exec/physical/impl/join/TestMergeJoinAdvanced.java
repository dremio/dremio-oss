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
package com.dremio.exec.physical.impl.join;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.FileUtils;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.resource.GroupResourceInformation;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestMergeJoinAdvanced extends BaseTestQuery {
  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(120, TimeUnit.SECONDS); // Longer timeout than usual.

  // Have to disable hash join to test merge join in this class
  @BeforeClass
  public static void disableHashJoin() throws Exception {
    test("alter session set \"planner.enable_hashjoin\" = false; alter session set \"planner.enable_mergejoin\" = true");
  }

  @AfterClass
  public static void enableHashJoin() throws Exception {
    test("alter session set \"planner.enable_hashjoin\" = true; alter session set \"planner.enable_mergejoin\" = false");
  }

  @Test
  public void testJoinWithDifferentTypesInCondition() throws Exception {
    String query = "select t1.full_name from cp.\"employee.json\" t1, cp.\"department.json\" t2 " +
        "where cast(t1.department_id as double) = t2.department_id and t1.employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery("alter session set \"planner.enable_hashjoin\" = true")
        .unOrdered()
        .baselineColumns("full_name")
        .baselineValues("Sheri Nowmer")
        .go();


    query = "select t1.bigint_col from cp.\"jsoninput/implicit_cast_join_1.json\" t1, cp.\"jsoninput/implicit_cast_join_1.json\" t2 " +
        " where t1.bigint_col = cast(t2.bigint_col as int) and" + // join condition with bigint and int
        " t1.double_col  = cast(t2.double_col as float) and" + // join condition with double and float
        " t1.bigint_col = cast(t2.bigint_col as double)"; // join condition with bigint and double

    testBuilder()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery("alter session set \"planner.enable_hashjoin\" = true")
        .unOrdered()
        .baselineColumns("bigint_col")
        .baselineValues(1l)
        .go();

    try (AutoCloseable c = disableUnlimitedSplitsSupportFlags()) {
      // date_dictionary.parquet file has implicit partition columns and the partition value is not same as
      // value in the file.
      query = "select count(*) col1 from " +
        "(select t1.date_opt from cp.\"parquet/date_dictionary.parquet\" t1, cp.\"parquet/timestamp_table.parquet\" t2 " +
        "where t1.date_opt = cast(t2.timestamp_col as date))"; // join condition contains date and timestamp
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(4l)
        .go();
    }
  }

  @Test
  @Ignore // TODO file JIRA to fix this
  public void testFix2967() throws Exception {
    setSessionOption(PlannerSettings.BROADCAST.getOptionName(), "false");
    setSessionOption(PlannerSettings.HASHJOIN.getOptionName(), "false");
    setSessionOption(ExecConstants.SLICE_TARGET, "1");
    setSessionOption(GroupResourceInformation.MAX_WIDTH_PER_NODE_KEY, "23");

    final String TEST_RES_PATH = TestTools.getWorkingPath() + "/src/test/resources";

    try {
      test("select * from dfs.\"%s/join/j1\" j1 left outer join dfs.\"%s/join/j2\" j2 on (j1.c_varchar = j2.c_varchar)",
        TEST_RES_PATH, TEST_RES_PATH);
    } finally {
      setSessionOption(PlannerSettings.BROADCAST.getOptionName(), String.valueOf(PlannerSettings.BROADCAST.getDefault
        ().getBoolVal()));
      setSessionOption(PlannerSettings.HASHJOIN.getOptionName(), String.valueOf(PlannerSettings.HASHJOIN.getDefault()
        .getBoolVal()));
      setSessionOption(ExecConstants.SLICE_TARGET, String.valueOf(ExecConstants.SLICE_TARGET_DEFAULT));
      resetSessionOption(GroupResourceInformation.MAX_WIDTH_PER_NODE_KEY);
    }
  }

  private static void generateData(final BufferedWriter leftWriter, final BufferedWriter rightWriter,
                             final long left, final long right) throws IOException {
    for (int i=0; i < left; ++i) {
      leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10000, i));
    }
    leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10001, 10001));
    leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10002, 10002));

    for (int i=0; i < right; ++i) {
      rightWriter.write(String.format("{ \"k1\" : %d , \"v1\": %d }", 10000, i));
    }
    rightWriter.write(String.format("{ \"k1\" : %d , \"v1\": %d }", 10004, 10004));
    rightWriter.write(String.format("{ \"k1\" : %d , \"v1\": %d }", 10005, 10005));
    rightWriter.write(String.format("{ \"k1\" : %d , \"v1\": %d }", 10006, 10006));

    leftWriter.close();
    rightWriter.close();
  }

  private void testMultipleBatchJoin(final long right, final long left,
                                     final String joinType, final long expected) throws Exception {
    final String leftSide = BaseTestQuery.getTempDir("merge-join-left.json");
    final String rightSide = BaseTestQuery.getTempDir("merge-join-right.json");
    final BufferedWriter leftWriter = new BufferedWriter(new FileWriter(new File(leftSide)));
    final BufferedWriter rightWriter = new BufferedWriter(new FileWriter(new File(rightSide)));
    generateData(leftWriter, rightWriter, left, right);
    final String query1 = String.format("select count(*) c1 from dfs.\"%s\" L %s join dfs.\"%s\" R on L.k=R.k1",
      leftSide, joinType, rightSide);
    testBuilder()
      .sqlQuery(query1)
      .optionSettingQueriesForTestQuery("alter session set \"planner.enable_hashjoin\" = false")
      .unOrdered()
      .baselineColumns("c1")
      .baselineValues(expected)
      .go();
  }

  @Test
  public void testMergeInnerJoinLargeRight() throws Exception {
    testMultipleBatchJoin(1000l, 5000l, "inner", 5000l * 1000l);
  }

  @Test
  public void testMergeLeftJoinLargeRight() throws Exception {
    testMultipleBatchJoin(1000l, 5000l, "left", 5000l * 1000l +2l);
  }

  @Test
  public void testMergeRightJoinLargeRight() throws Exception {
    testMultipleBatchJoin(1000l, 5000l, "right", 5000l*1000l +3l);
  }

  @Test
  public void testMergeInnerJoinLargeLeft() throws Exception {
    testMultipleBatchJoin(5000l, 1000l, "inner", 5000l*1000l);
  }

  @Test
  public void testMergeLeftJoinLargeLeft() throws Exception {
    testMultipleBatchJoin(5000l, 1000l, "left", 5000l*1000l + 2l);
  }

  @Test
  public void testMergeRightJoinLargeLeft() throws Exception {
    testMultipleBatchJoin(5000l, 1000l, "right", 5000l*1000l + 3l);
  }

  // Following tests can take some time.
  @Test
  @Ignore
  public void testMergeInnerJoinRandomized() throws Exception {
    final Random r = new Random();
    final long right = r.nextInt(10001) + 1l;
    final long left = r.nextInt(10001) + 1l;
    testMultipleBatchJoin(left, right, "inner", left*right);
  }

  @Test
  @Ignore
  public void testMergeLeftJoinRandomized() throws Exception {
    final Random r = new Random();
    final long right = r.nextInt(10001) + 1l;
    final long left = r.nextInt(10001) + 1l;
    testMultipleBatchJoin(left, right, "left", left*right + 2l);
  }

  @Test
  @Ignore
  public void testMergeRightJoinRandomized() throws Exception {
    final Random r = new Random();
    final long right = r.nextInt(10001) + 1l;
    final long left = r.nextInt(10001) + 1l;
    testMultipleBatchJoin(left, right, "right", left * right + 3l);
  }

  @Test
  public void testDrill4165() throws Exception {
    final String query1 = "select count(*) cnt from cp.\"tpch/lineitem.parquet\" l1, cp.\"tpch/lineitem.parquet\" l2 where l1.l_partkey = l2.l_partkey and l1.l_suppkey < 30 and l2.l_suppkey < 30";
    testBuilder()
      .sqlQuery(query1)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(202452l)
      .go();
  }

  @Test
  public void testDrill4196() throws Exception {
    final String leftSide = BaseTestQuery.getTempDir("merge-join-left.json");
    final String rightSide = BaseTestQuery.getTempDir("merge-join-right.json");
    final BufferedWriter leftWriter = new BufferedWriter(new FileWriter(new File(leftSide)));
    final BufferedWriter rightWriter = new BufferedWriter(new FileWriter(new File(rightSide)));

    // output batch is 32k, create 60k left batch
    leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 9999, 9999));
    for (int i=0; i < 6000; ++i) {
      leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10000, 10000));
    }
    leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10001, 10001));
    leftWriter.write(String.format("{ \"k\" : %d , \"v\": %d }", 10002, 10002));

    // Keep all values same. Jon will consume entire right side.
    for (int i=0; i < 800; ++i) {
      rightWriter.write(String.format("{ \"k1\" : %d , \"v1\": %d }", 10000, 10000));
    }

    leftWriter.close();
    rightWriter.close();

    final String query1 = String.format("select count(*) c1 from dfs.\"%s\" L %s join dfs.\"%s\" R on L.k=R.k1",
      leftSide, "inner", rightSide);
    testBuilder()
      .sqlQuery(query1)
      .optionSettingQueriesForTestQuery("alter session set \"planner.enable_hashjoin\" = false")
      .unOrdered()
      .baselineColumns("c1")
      .baselineValues(6000*800L)
      .go();
  }

  @Test
  public void testMergeJoinInnerEmptyBatch() throws Exception {
    String plan = Files.toString(FileUtils.getResourceAsFile("/join/merge_join_empty_batch.json"), Charsets.UTF_8).replace("${JOIN_TYPE}", "INNER");
    assertEquals(0, testPhysical(plan));
  }

  @Test
  public void testMergeJoinLeftEmptyBatch() throws Exception {
    final String plan = Files.toString(FileUtils.getResourceAsFile("/join/merge_join_empty_batch.json"), Charsets.UTF_8)
        .replace("${JOIN_TYPE}", "LEFT");
    assertEquals(50, testPhysical(plan));
  }

  @Test
  public void testMergeJoinRightEmptyBatch() throws Exception {
    final String plan = Files.toString(FileUtils.getResourceAsFile("/join/merge_join_empty_batch.json"), Charsets.UTF_8)
        .replace("${JOIN_TYPE}", "RIGHT");
    assertEquals(0, testPhysical(plan));
  }

  @Ignore("DX-12609: parquet file format has changed, test case to be upgraded")
  @Test
  public void testMergeJoinExprInCondition() throws Exception {
    final String plan = Files.toString(FileUtils.getResourceAsFile("/join/mergeJoinExpr.json"), Charsets.UTF_8)
        .replace("${JOIN_TYPE}", "RIGHT");
    assertEquals(10, testPhysical(plan));
  }


  @Ignore("DX-12609: parquet file format has changed, test case to be upgraded")
  @Test
  //the physical plan is obtained for the following SQL query:
  //  "select l.l_partkey, l.l_suppkey, ps.ps_partkey, ps.ps_suppkey "
  //      + " from cp.\"tpch/lineitem.parquet\" l join "
  //      + "      cp.\"tpch/partsupp.parquet\" ps"
  //      + " on l.l_partkey = ps.ps_partkey and "
  //      + "    l.l_suppkey = ps.ps_suppkey";
  public void testMergeJoinMultiKeys() throws Exception {
    assertEquals(60175, testPhysicalFromFile("join/mj_multi_condition.json"));
  }

  @Test
  @Ignore
  // The physical plan is obtained through sql:
  // alter session set \"planner.enable_hashjoin\"=false;
  // select * from cp.\"region.json\" t1, cp.\"region.json\" t2 where t1.non_exist = t2.non_exist2 ;
  public void testMergeJoinInnerNullKey() throws Exception {
    assertEquals(0, testPhysicalFromFile("join/merge_join_nullkey.json"));
  }

  @Test
  @Ignore("join on non-existent column")
  // The physical plan is obtained through sql:
  // alter session set \"planner.enable_hashjoin\"=false;
  // select * from cp.\"region.json\" t1 left outer join cp.\"region.json\" t2 on  t1.non_exist = t2.non_exist2 ;
  public void testMergeJoinLeftOuterNullKey() throws Exception {
    assertEquals(110, testPhysicalFromFile("/join/merge_join_nullkey.json"));
  }
}

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
package com.dremio.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.exec.hive.HiveTestBase;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.sabot.rpc.user.QueryDataBatch;

public class ITHivePartitionPruning extends HiveTestBase {

  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(100000, TimeUnit.SECONDS);

  // enable decimal data type
  @BeforeClass
  public static void enableDecimalDataType() throws Exception {
    test(String.format("alter session set \"%s\" = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  //Currently we do not have a good way to test plans so using a crude string comparison
  @Test
  public void testSimplePartitionFilter() throws Exception {
    final String query = "explain plan for select * from hive.\"default\".partition_pruning_test where c = 1";
    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse("Unexpected plan\n" + plan, plan.contains("Filter"));
  }

  /* Partition pruning is not supported for disjuncts that do not meet pruning criteria.
   * Will be enabled when we can do wild card comparison for partition pruning
   */
  @Test
  public void testDisjunctsPartitionFilter() throws Exception {
    final String query = "explain plan for select * from hive.\"default\".partition_pruning_test where (c = 1) or (d = 1)";
    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse("Unexpected plan\n" + plan, plan.contains("Filter"));
  }

  @Test
  public void testConjunctsPartitionFilter() throws Exception {
    final String query = "explain plan for select * from hive.\"default\".partition_pruning_test where c = 1 and d = 1";
    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse("Unexpected plan\n" + plan, plan.contains("Filter"));
  }

  @Test
  public void testComplexFilter() throws Exception {
    final String query = "explain plan for select * from hive.\"default\".partition_pruning_test where (c = 1 and d = 1) or (c = 2 and d = 3)";
    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse("Unexpected plan\n" + plan, plan.contains("Filter"));
  }

  @Test
  public void testRangeFilter() throws Exception {
    final String query = "explain plan for " +
        "select * from hive.\"default\".partition_pruning_test where " +
        "c > 1 and d > 1";

    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse("Unexpected plan\n" + plan, plan.contains("Filter"));
  }

  @Test
  public void testRangeFilterWithDisjunct() throws Exception {
    final String query = "explain plan for " +
        "select * from hive.\"default\".partition_pruning_test where " +
        "(c > 1 and d > 1) or (c < 2 and d < 2)";

    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse("Unexpected plan\n" + plan, plan.contains("Filter"));
  }

  /**
   * Tests pruning on table that has partitions columns of supported data types. Also tests whether Hive pruning code
   * is able to deserialize the partition values in string format to appropriate type holder.
   */
  @Test
  public void pruneDataTypeSupport() throws Exception {
    final String query = "EXPLAIN PLAN FOR " +
        "SELECT * FROM hive.readtest WHERE tinyint_part = 64";

    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse("Unexpected plan\n" + plan, plan.contains("Filter"));
  }

  @Test
  public void pruneDataTypeSupportNativeReaders() throws Exception {
    final String query = "EXPLAIN PLAN FOR " +
        "SELECT * FROM hive.readtest_parquet WHERE tinyint_part = 64";

    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse("Unexpected plan\n" + plan, plan.contains("Filter"));

    // Make sure the plan contains the Hive scan utilizing native parquet reader
    assertTrue(plan, plan.contains("mode=[NATIVE_PARQUET]"));
  }

  @Test // DRILL-3579
  public void selectFromPartitionedTableWithNullPartitions() throws Exception {
    final String query = "SELECT count(*) nullCount FROM hive.partition_pruning_test " +
        "WHERE c IS NULL OR d IS NULL OR e IS NULL";
    final String plan = getPlanInString("EXPLAIN PLAN FOR " + query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse("Unexpected plan\n" + plan, plan.contains("Filter"));

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("nullCount")
        .baselineValues(95L)
        .go();
  }

  @Test // DRILL-5032
  @Ignore("DX-5232")
  public void testPartitionColumnsCaching() throws Exception {
    final String query = "EXPLAIN PLAN FOR SELECT * FROM hive.partition_with_few_schemas";

    List<QueryDataBatch> queryDataBatches = testSqlWithResults(query);
    String resultString = getResultString(queryDataBatches, "|");

    // different for both partitions column strings from physical plan
    String columnString = "\"name\" : \"a\"";
    String secondColumnString = "\"name\" : \"a1\"";

    int columnIndex = resultString.indexOf(columnString);
    assertTrue(columnIndex >= 0);
    columnIndex = resultString.indexOf(columnString, columnIndex + 1);
    // checks that column added to physical plan only one time
    assertEquals(-1, columnIndex);

    int secondColumnIndex = resultString.indexOf(secondColumnString);
    assertTrue(secondColumnIndex >= 0);
    secondColumnIndex = resultString.indexOf(secondColumnString, secondColumnIndex + 1);
    // checks that column added to physical plan only one time
    assertEquals(-1, secondColumnIndex);
  }

  @Test
  public void selectFromPartitionedTableWithDecimalPartitionOverflow() throws Exception {
    final String selectStar = "SELECT * FROM hive.parquet_decimal_partition_overflow_ext";
    testBuilder()
        .sqlQuery(selectStar)
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(2202, new BigDecimal("123456789.120"))
        .baselineValues(234, new BigDecimal("123456789101.123"))
        .baselineValues(154, new BigDecimal("123456789101.123"))
        .baselineValues(202, null)
        .baselineValues(184, new BigDecimal("123456789102.123"))
        .baselineValues(184, new BigDecimal("123456789102.123"))
        .baselineValues(153, new BigDecimal("15.300"))
        .go();

    final String query = "SELECT count(*) nullCount FROM hive.parquet_decimal_partition_overflow_ext " +
        "WHERE col2 IS NULL";
    final String plan = getPlanInString("EXPLAIN PLAN FOR " + query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse("Unexpected plan\n" + plan, plan.contains("Filter"));

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("nullCount")
        .baselineValues(1L)
        .go();
  }

  @AfterClass
  public static void disableDecimalDataType() throws Exception {
    test(String.format("alter session set \"%s\" = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }
}

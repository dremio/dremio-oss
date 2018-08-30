/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.exec.hive.HiveTestBase;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.hive.HivePluginOptions;

public class TestHiveProjectPushDown extends HiveTestBase {

  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(200, TimeUnit.SECONDS);

  // enable decimal data type
  @BeforeClass
  public static void enableDecimalDataType() throws Exception {
    setSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, "true");
  }

  @AfterClass
  public static void disableDecimalDataType() throws Exception {
    setSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, "false");
  }

  private void testHelper(String query, int expectedRecordCount, String... expectedSubstrs)throws Exception {
    testPhysicalPlan(query, expectedSubstrs);

    int actualRecordCount = testSql(query);
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test
  public void testSingleColumnProject() throws Exception {
    String query = "SELECT \"value\" as v FROM hive.\"default\".kv";
    testHelper(query, 5, expectedColumnsString("value"));
  }

  @Test
  public void testMultipleColumnsProject() throws Exception {
    String query = "SELECT boolean_field as b_f, tinyint_field as ti_f FROM hive.\"default\".readtest";
    testHelper(query, 2, expectedColumnsString("boolean_field", "tinyint_field"));
  }

  @Test
  public void testPartitionColumnProject() throws Exception {
    String query = "SELECT double_part as dbl_p FROM hive.\"default\".readtest";
    testHelper(query, 2, expectedColumnsString("double_part"));
  }

  @Test
  public void testMultiplePartitionColumnsProject() throws Exception {
    String query = "SELECT double_part as dbl_p, decimal0_part as dec_p FROM hive.\"default\".readtest";
    testHelper(query, 2, expectedColumnsString("double_part", "decimal0_part"));
  }

  @Test
  public void testPartitionAndRegularColumnProjectColumn() throws Exception {
    String query = "SELECT boolean_field as b_f, tinyint_field as ti_f, " +
        "double_part as dbl_p, varchar_part as varchar_p FROM hive.\"default\".readtest";
    testHelper(query, 2, expectedColumnsString("boolean_field", "tinyint_field", "double_part", "varchar_part"));
  }

  @Test
  public void testHiveCountStar() throws Exception {
    String query = "SELECT count(*) as cnt FROM hive.\"default\".kv";
    testHelper(query, 1);
  }

  @Test
  public void projectPushDownOnHiveParquetTable() throws Exception {
    try {
      test(String.format("alter session set \"%s\" = true", HivePluginOptions.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      String query = "SELECT boolean_field, boolean_part, int_field, int_part FROM hive.readtest_parquet";
      testHelper(query, 2, expectedColumnsString("boolean_field", "boolean_part", "int_field", "int_part"), "NATIVE_PARQUET");
    } finally {
      test(String.format("alter session set \"%s\" = false", HivePluginOptions.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }
}

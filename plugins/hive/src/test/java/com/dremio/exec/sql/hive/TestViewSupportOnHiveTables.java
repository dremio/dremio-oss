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
package com.dremio.exec.sql.hive;

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.sql.TestBaseViewSupport;
import com.dremio.exec.store.hive.HiveTestDataGenerator;
import com.google.common.collect.ImmutableList;

public class TestViewSupportOnHiveTables extends TestBaseViewSupport {

  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(200, TimeUnit.SECONDS);

  private static final String TEMP_SCHEMA = "dfs_test";

  protected static HiveTestDataGenerator hiveTest;

  @BeforeClass
  public static void generateHive() throws Exception{
    hiveTest = HiveTestDataGenerator.getInstance();
    hiveTest.addHiveTestPlugin(getSabotContext().getStorage());
    test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
  }

  @Test
  public void viewWithStarInDef_StarInQuery() throws Exception{
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT * FROM hive.kv",
        "SELECT * FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 1",
        new String[] { "key", "value"},
        ImmutableList.of(new Object[] { 1, " key_1" })
    );
  }

  @Test
  public void viewWithStarInDef_SelectFieldsInQuery1() throws Exception{
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT * FROM hive.kv",
        "SELECT key, `value` FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 1",
        new String[] { "key", "value" },
        ImmutableList.of(new Object[] { 1, " key_1" })
    );
  }

  @Test
  public void viewWithStarInDef_SelectFieldsInQuery2() throws Exception{
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT * FROM hive.kv",
        "SELECT `value` FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 1",
        new String[] { "value" },
        ImmutableList.of(new Object[] { " key_1" })
    );
  }

  @Test
  public void viewWithSelectFieldsInDef_StarInQuery() throws Exception{
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT key, `value` FROM hive.kv",
        "SELECT * FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 1",
        new String[] { "key", "value" },
        ImmutableList.of(new Object[] { 1, " key_1" })
    );
  }

  @Test
  public void viewWithSelectFieldsInDef_SelectFieldsInQuery() throws Exception{
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT key, `value` FROM hive.kv",
        "SELECT key, `value` FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 1",
        new String[] { "key", "value" },
        ImmutableList.of(new Object[] { 1, " key_1" })
    );
  }

  @Test
  @Ignore("currently namespace doesn't store view/table type")
  public void testInfoSchemaWithHiveView() throws Exception {
    testBuilder()
        .optionSettingQueriesForTestQuery("USE hive.`default`")
        .sqlQuery("SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'hiveview'")
        .unOrdered()
        .baselineColumns("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION")
        .baselineValues("DRILL", "hive.default", "hiveview", "SELECT `kv`.`key`, `kv`.`value` FROM `default`.`kv`")
        .go();
  }

  @AfterClass
  public static void cleanupHiveTestData() throws Exception{
    if (hiveTest != null) {
      hiveTest.deleteHiveTestPlugin(getSabotContext().getStorage());
    }
  }
}

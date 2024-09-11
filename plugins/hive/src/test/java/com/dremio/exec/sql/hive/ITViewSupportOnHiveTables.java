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
package com.dremio.exec.sql.hive;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.config.DremioConfig;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.sql.TestBaseViewSupport;
import com.dremio.exec.store.hive.HiveTestDataGenerator;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.collect.ImmutableList;

public class ITViewSupportOnHiveTables extends TestBaseViewSupport {

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(100, TimeUnit.SECONDS);

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  private static final String TEMP_SCHEMA = "dfs_test";

  protected static HiveTestDataGenerator hiveTest;

  @Before
  public void before() {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
  }

  @BeforeClass
  public static void generateHive() throws Exception{
    hiveTest = HiveTestDataGenerator.getInstance();
    Objects.requireNonNull(hiveTest);
    hiveTest.addHiveTestPlugin(HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME, getCatalogService());
  }

  @AfterClass
  public static void cleanupHiveTestData() {
    if (hiveTest != null) {
      hiveTest.deleteHiveTestPlugin(HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME, getCatalogService());
    }
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
        "SELECT key, \"value\" FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 1",
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
        "SELECT \"value\" FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 1",
        new String[] { "value" },
        ImmutableList.of(new Object[] { " key_1" })
    );
  }

  @Test
  public void viewWithSelectFieldsInDef_StarInQuery() throws Exception{
    testViewHelper(
        TEMP_SCHEMA,
        null,
        "SELECT key, \"value\" FROM hive.kv",
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
        "SELECT key, \"value\" FROM hive.kv",
        "SELECT key, \"value\" FROM TEST_SCHEMA.TEST_VIEW_NAME LIMIT 1",
        new String[] { "key", "value" },
        ImmutableList.of(new Object[] { 1, " key_1" })
    );
  }

  @Test
  @Ignore("currently namespace doesn't store view/table type")
  public void testInfoSchemaWithHiveView() throws Exception {
    testBuilder()
        .optionSettingQueriesForTestQuery("USE hive.\"default\"")
        .sqlQuery("SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'hiveview'")
        .unOrdered()
        .baselineColumns("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION")
        .baselineValues("DRILL", "hive.default", "hiveview", "SELECT \"kv\".\"key\", \"kv\".\"value\" FROM \"default\".\"kv\"")
        .go();
  }

  @Test
  public void throwUnsupportedErrorWhenUserRequestsExistingHiveView() throws Exception{

    hiveTest.executeDDL("CREATE TABLE IF NOT EXISTS existing_table(a INT, b STRING)");
    hiveTest.executeDDL("INSERT INTO existing_table VALUES(1, 'a')");
    hiveTest.executeDDL("CREATE VIEW existing_view AS SELECT * FROM existing_table");
    try {
      assertThatThrownBy(() -> client.runQuery(UserBitShared.QueryType.SQL, "SELECT * FROM hive.\"default\".\"existing_view\""))
        .isInstanceOf(RpcException.class)
        .hasMessageContaining("Hive views are not supported");
    } finally {
      hiveTest.executeDDL("DROP VIEW existing_view");
      hiveTest.executeDDL("DROP TABLE existing_table");
    }
  }
}

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

import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static com.dremio.exec.store.hive.HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.hive.LazyDataGeneratingHiveTestBase;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.hive.HiveConfFactory;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.collect.ImmutableMap;

/**
 * Tests for Alter Table commands on Iceberg tables in Hive catalog.
 */
public class ITAlterTableOnHiveTables extends LazyDataGeneratingHiveTestBase {
  private static final String SCHEME = "file:///";
  private static String WAREHOUSE_LOCATION;

  @BeforeClass
  public static void setup() throws Exception {
    WAREHOUSE_LOCATION = dataGenerator.getWhDir() + "/";
    dataGenerator.updatePluginConfig(getCatalogService(),
      ImmutableMap.of(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, SCHEME + WAREHOUSE_LOCATION,
        HiveConfFactory.ENABLE_DML_TESTS_WITHOUT_LOCKING, "true"));
  }

  private static void setupTable(String tableName) throws Exception {
    runSQL("Create table " + HIVE_TEST_PLUGIN_NAME + "." + tableName + "(col1 int, col2 varchar, col3 double)");
  }

  @Test
  public void testAlterTableAddColumn() throws Exception {
    // TODO: DX-46976 - Enable these for MapR
    assumeNonMaprProfile();
    final String tableName = "addCol";
    try {
      setupTable(tableName);

      test("alter table " + HIVE_TEST_PLUGIN_NAME + "." + tableName + " add columns (col4  ARRAY(int))");
      assertThat(runDescribeQuery(tableName)).contains("col4|ARRAY");
    } finally {
      dataGenerator.executeDDL("DROP TABLE IF EXISTS " + tableName);
    }
  }

  @Test
  public void testAlterTableDropColumn() throws Exception {
    // TODO: DX-46976 - Enable these for MapR
    assumeNonMaprProfile();
    final String tableName = "dropCol";
    try {
      setupTable(tableName);

      assertThat(runDescribeQuery(tableName)).contains("col3");
      test("alter table " + HIVE_TEST_PLUGIN_NAME + "." + tableName + " DROP COLUMN col3");
      assertThat(runDescribeQuery(tableName)).doesNotContain("col3");
    } finally {
      dataGenerator.executeDDL("DROP TABLE IF EXISTS " + tableName);
    }
  }

  @Test
  public void testAlterTableChangeColumn() throws Exception {
    // TODO: DX-46976 - Enable these for MapR
    assumeNonMaprProfile();

    final String tableName = "changeCol";
    try {
      setupTable(tableName);

      assertThat(runDescribeQuery(tableName)).doesNotContain("col4");
      test("alter table " + HIVE_TEST_PLUGIN_NAME + "." + tableName + " CHANGE COLUMN col1 col4 bigint");
      assertThat(runDescribeQuery(tableName)).contains("col4|BIGINT");
    } finally {
      dataGenerator.executeDDL("DROP TABLE IF EXISTS " + tableName);
    }
  }

  String runDescribeQuery(String dirName) throws Exception {
    String query = String.format("describe %s.%s", HIVE_TEST_PLUGIN_NAME, dirName);
    List<QueryDataBatch> queryDataBatches = testRunAndReturn(UserBitShared.QueryType.SQL, query);
    return getResultString(queryDataBatches, "|", false);
  }
}

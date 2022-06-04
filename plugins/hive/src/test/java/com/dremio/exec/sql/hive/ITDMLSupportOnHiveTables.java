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

import static com.dremio.exec.store.hive.HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.hive.LazyDataGeneratingHiveTestBase;
import com.dremio.exec.store.hive.HiveConfFactory;
import com.google.common.collect.ImmutableMap;

public class ITDMLSupportOnHiveTables extends LazyDataGeneratingHiveTestBase {
  private static AutoCloseable enableIcebergDmlSupportFlags;
  private static final String SCHEME = "file:///";
  private static String WAREHOUSE_LOCATION;

  @BeforeClass
  public static void setup() throws Exception {
    enableIcebergDmlSupportFlags = enableIcebergDmlSupportFlag();
    WAREHOUSE_LOCATION = dataGenerator.getWhDir() + "/";
    dataGenerator.updatePluginConfig((getSabotContext().getCatalogService()),
      ImmutableMap.of(HiveConfFactory.DEFAULT_WAREHOUSE_LOCATION, SCHEME + WAREHOUSE_LOCATION,
        HiveConfFactory.ENABLE_DML_TESTS_WITHOUT_LOCKING, "true"));
  }

  @AfterClass
  public static void close() throws Exception {
    enableIcebergDmlSupportFlags.close();
  }

  @Test
  public void testCreateEmptyIcebergTable() throws Exception {
    final String tableName = "iceberg_test";

    try {
      testBuilder()
        .sqlQuery("Create table " + HIVE_TEST_PLUGIN_NAME + "." + tableName + "(n int)")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "Table created")
        .go();
    }
    finally {
      dataGenerator.executeDDL("DROP TABLE IF EXISTS " + tableName);
    }
  }

  @Test
  public void testCreateEmptyIcebergTableOnLocation() throws Exception {
    final String tableName = "iceberg_test_location";
    final String queryTableLocation = "default/location";
    try {
      testBuilder()
        .sqlQuery(String.format("Create table %s.%s(n int) LOCATION '%s'", HIVE_TEST_PLUGIN_NAME, tableName, queryTableLocation))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "Table created")
        .go();

      File tableFolder = new File(WAREHOUSE_LOCATION + queryTableLocation);
      assertTrue("Error in checking if the " + tableFolder.toString() + " exists", tableFolder.exists());
    }
    finally {
      dataGenerator.executeDDL("DROP TABLE IF EXISTS " + tableName);
    }
  }
}

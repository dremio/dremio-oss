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

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.hive.LazyDataGeneratingHiveTestBase;
import com.dremio.exec.store.hive.HiveConfFactory;
import com.google.common.collect.ImmutableMap;

/**
 * Tests for Schema Evolution scenarios on Iceberg tables in Hive catalog.
 */
public class ITHiveSchemaEvolution extends LazyDataGeneratingHiveTestBase {
  private static AutoCloseable enableIcebergDmlSupportFlags;
  private static final String SCHEME = "file:///";
  private static String WAREHOUSE_LOCATION;

  @BeforeClass
  public static void setup() throws Exception {
    enableIcebergDmlSupportFlags = enableIcebergDmlSupportFlag();
    WAREHOUSE_LOCATION = dataGenerator.getWhDir() + "/";
    dataGenerator.updatePluginConfig((getSabotContext().getCatalogService()),
      ImmutableMap.of(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, SCHEME + WAREHOUSE_LOCATION,
        HiveConfFactory.ENABLE_DML_TESTS_WITHOUT_LOCKING, "true"));
  }

  @AfterClass
  public static void close() throws Exception {
    enableIcebergDmlSupportFlags.close();
  }


  @Test
  public void testInsertAfterDropColumn() throws Exception {
    // TODO: DX-46976 - Enable these for MapR
    assumeNonMaprProfile();

    final String tableName  = "iceberg_test_testInsertAfterDropColumn";
    final String tableNameWithHiveCatalog = HIVE_TEST_PLUGIN_NAME + "." + tableName;
    try {
      setupInitialTable(tableNameWithHiveCatalog);
      dropColumn(tableNameWithHiveCatalog, "col1");
      runSQL(getInsertQuery(tableNameWithHiveCatalog , "(2)"));
      testBuilder()
        .sqlQuery(getSelectQuery(tableNameWithHiveCatalog))
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues(1)
        .baselineValues(2)
        .build()
        .run();
    }
    finally {
      dataGenerator.executeDDL(getDropTableQuery(tableName));
    }
  }

  @Test
  public void testInsertAfterRenameColumn() throws Exception {
    // TODO: DX-46976 - Enable these for MapR
    assumeNonMaprProfile();

    final String tableName = "iceberg_test_testInsertAfterRenameColumn";
    final String tableNameWithCatalog = HIVE_TEST_PLUGIN_NAME + "." + tableName;
    try {
      setupInitialTable(tableNameWithCatalog);
      renameColumn(tableNameWithCatalog, "col1", "col4", "int");
      String insertCommand2 = getInsertQuery( tableNameWithCatalog , "(2, 4)");
      runSQL(insertCommand2);
      testBuilder()
        .sqlQuery(getSelectQuery(tableNameWithCatalog))
        .unOrdered()
        .baselineColumns("col4", "col2")
        .baselineValues(1 ,1)
        .baselineValues(2, 4)
        .build()
        .run();
    }
    finally {
      dataGenerator.executeDDL(getDropTableQuery(tableName));
    }
  }

  @Test
  public void testCTASAfterAddColumn() throws Exception {
    // TODO: DX-46976 - Enable these for MapR
    assumeNonMaprProfile();

    final String tableName  = "iceberg_test_testInsertAfterAddColumn";
    final String newTableName  = tableName + "1";
    final String tableNameWithCatalog = HIVE_TEST_PLUGIN_NAME + "." + tableName;
    final String newTableNameWithCatalog  = HIVE_TEST_PLUGIN_NAME + "." + newTableName;
    try {
      setupInitialTable(tableNameWithCatalog);
      addColumns(tableNameWithCatalog, " (col4 int)");
      String insertCommand2 =  getInsertQuery( tableNameWithCatalog, "(2, 4, 6)");
      runSQL(insertCommand2);
      testBuilder()
        .sqlQuery(getSelectQuery(tableNameWithCatalog))
        .unOrdered()
        .baselineColumns("col1", "col2", "col4")
        .baselineValues(1 ,1, null)
        .baselineValues(2, 4, 6)
        .build()
        .run();
      testCTAS(tableNameWithCatalog,  newTableNameWithCatalog);
    }
    finally {
      dataGenerator.executeDDL(getDropTableQuery(tableName));
      dataGenerator.executeDDL(getDropTableQuery(newTableName));
    }
  }

  @Test
  public void testDropAddSameColumn() throws Exception {
    // TODO: DX-46976 - Enable these for MapR
    assumeNonMaprProfile();

    final String tableName = "iceberg_test_testDropAddSameColumn";
    final String tableNameWithCatalog = HIVE_TEST_PLUGIN_NAME + "." + tableName;
    try {
      setupInitialTable(tableNameWithCatalog);
      dropColumn(tableNameWithCatalog,  "col1");
      String insertCommand2 = getInsertQuery(tableNameWithCatalog ,"(2)");
      runSQL(insertCommand2);
      testBuilder()
        .sqlQuery(getSelectQuery(tableNameWithCatalog))
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues(1)
        .baselineValues(2)
        .build()
        .run();
      addColumns(tableNameWithCatalog, "(col1 int)");
      runSQL(getInsertQuery(tableNameWithCatalog,"(3, 4)"));
      testBuilder()
        .sqlQuery(getSelectQuery(tableNameWithCatalog))
        .unOrdered()
        .baselineColumns("col2", "col1")
        .baselineValues(1, null)
        .baselineValues(2, null)
        .baselineValues(3, 4)
        .build()
        .run();
    }
    finally {
      dataGenerator.executeDDL(getDropTableQuery(tableName));
    }
  }

  private void testCTAS(String oldTableName, String newTableName) throws Exception {
    runSQL(getCTASQuery(oldTableName, newTableName));
    testBuilder()
      .sqlQuery(getSelectQuery(oldTableName))
      .unOrdered()
      .sqlBaselineQuery(getSelectQuery(newTableName));
  }

  private void setupInitialTable(String tableNameWithHiveCatalog) throws Exception {
    String createCommandSql = getCreateTableQuery(tableNameWithHiveCatalog,
      " (col1 int, col2 int)");
    String insertCommand1 = getInsertQuery(tableNameWithHiveCatalog, "(1, 1)");
    runSQL(createCommandSql);
    runSQL(insertCommand1);
    testBuilder()
      .sqlQuery(getSelectQuery(tableNameWithHiveCatalog))
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(1, 1)
      .build()
      .run();
  }
}

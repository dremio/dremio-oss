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

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.exec.store.hive.HiveTestDataGenerator;

/**
 * Test new metadata refresh flow ORC input format
 */
public class ITHiveOrcMetadataRefresh extends ITHiveRefreshDatasetMetadataRefresh {

  @BeforeClass
  public static void generateHiveWithoutData() throws Exception {
      formatType = "ORC";
      ITHiveRefreshDatasetMetadataRefresh.generateHiveWithoutData();

      HiveTestDataGenerator dataGenerator = ITHiveRefreshDatasetMetadataRefresh.getDataGenerator();
      String table = "orctable_more_columns";
      String datatable = "CREATE TABLE IF NOT EXISTS " + table + " (col1 int, col2 int) STORED AS ORC";
      dataGenerator.executeDDL(datatable);
      String insert_datatable = "INSERT INTO " + table + " VALUES (1,2)";
      dataGenerator.executeDDL(insert_datatable);
      String exttable = table + "_ext";
      String ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS " + exttable +
        " (col1 int, col2 int, col3 int, col4 int)" + "STORED AS ORC LOCATION 'file://" + warehouseDir + "/" + table + "'";
      dataGenerator.executeDDL(ext_table);
  }

  @Test
  public void testOrcTableWithMoreColExt() throws Exception {
    String query = "SELECT col2, col3 FROM hive.orctable_more_columns_ext";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col2", "col3")
      .baselineValues(2, null)
      .go();
  }

  @Override
  @Test
  @Ignore // Test is not valid for ORC format
  public void testFailTableOptionQuery() throws Exception {
  }
}

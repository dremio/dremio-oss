/*
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
package com.dremio.exec.sql;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestAlterTableWithContext extends BaseTestQuery {

  @Test
  public void drop() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "dropcol0";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        test("USE " + testSchema);

        String query = String.format("ALTER TABLE %s DROP COLUMN col1", tableName);
        errorMsgTestHelper(query, "Column [col1] is not present in table [" + testSchema + ".dropcol0]");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void add() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "addcol7";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        test("USE " + testSchema);

        String query = String.format("ALTER TABLE %s ADD COLUMNS(col1 varchar, col2 int, col3 map)", tableName);
        errorMsgTestHelper(query, "Invalid column type [`map`] specified for column [col3]");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void change() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "changecol8";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as " +
            "SELECT n_regionkey from cp.\"tpch/nation.parquet\" where n_regionkey < 2 GROUP BY n_regionkey ",
          testSchema, tableName);
        test(createTableQuery);

        final String selectFromCreatedTable = String.format("select * from %s.%s", testSchema, tableName);
        testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .baselineColumns("n_regionkey")
          .baselineValues(0)
          .baselineValues(1)
          .build()
          .run();

        final String useSchemaQuery = "USE  " + testSchema;
        test(useSchemaQuery);

        Thread.sleep(1001);
        String changeColQuery = String.format("ALTER TABLE %s CHANGE n_RegiOnkey regionkey int", tableName);
        test(changeColQuery);

        testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .baselineColumns("regionkey")
          .baselineValues(0)
          .baselineValues(1)
          .build()
          .run();

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

}

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

public class TestAlterTableDropColumn extends BaseTestQuery {

  @Test
  public void badSql() {
    String[] queries = {
        "ALTER TABLE tbl DROP COLUMN",
        "ALTER TABLE DROP COLUMN col1"};
    for (String q : queries) {
      errorMsgTestHelper(q, "Failure parsing the query.");
    }
  }

  @Test
  public void dropNonExistingCol() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "dropcol1";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s DROP COLUMN col1", testSchema, tableName);
        errorMsgTestHelper(query, "Column [col1] is not present in table [" + testSchema + ".dropcol1]");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void noContextFail() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "dropcol01";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s DROP COLUMN col1", tableName);
        errorMsgTestHelper(query, "Table [dropcol01] not found");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void dropOnSingleColTable() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "dropcol2";
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


        Thread.sleep(1001);
        String alterQuery = String.format("ALTER TABLE %s.%s DROP N_REGIONKEY", testSchema, tableName);
        errorMsgTestHelper(alterQuery, "Cannot drop all columns of a table");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addAndDrop() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "dropcol3";
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


        Thread.sleep(1001);
        String addColQuery = String.format("ALTER TABLE %s.%s ADD COLUMNS(key int)", testSchema, tableName);
        test(addColQuery);

        testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .baselineColumns("n_regionkey", "key")
          .baselineValues(0, null)
          .baselineValues(1, null)
          .build()
          .run();

        Thread.sleep(1001);
        String dropColQuery = String.format("ALTER TABLE %s.%s DROP COLUMN n_Regionkey", testSchema, tableName);
        test(dropColQuery);

        testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .baselineColumns("key")
          .baselineValues((Object) null)
          .baselineValues((Object) null)
          .build()
          .run();

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void dropPartitionColumn() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "dropcol4";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s PARTITION BY (n_regionkey) as " +
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

        Thread.sleep(1001);
        String addColQuery = String.format("ALTER TABLE %s.%s ADD COLUMNS(key int)", testSchema, tableName);
        test(addColQuery);

        testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .baselineColumns("n_regionkey", "key")
          .baselineValues(0, null)
          .baselineValues(1, null)
          .build()
          .run();

        Thread.sleep(1001);
        String dropColQuery = String.format("ALTER TABLE %s.%s DROP COLUMN n_RegiOnkey", testSchema, tableName);
        errorMsgTestHelper(dropColQuery, "[n_regionkey] is a partition column. Partition spec change is not supported.");

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

}

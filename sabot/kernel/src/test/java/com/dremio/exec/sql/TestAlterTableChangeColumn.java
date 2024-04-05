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

import com.dremio.BaseTestQuery;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.UserExceptionAssert;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class TestAlterTableChangeColumn extends BaseTestQuery {

  @Test
  public void badSql() {
    String[] queries = {
      "ALTER TABLE tbl CHANGE ",
      "ALTER TABLE CHANGE col1 col2 varchar int",
      "ALTER TABLE %s.%s CHANGE COLUMN version commit_message varchar",
      "ALTER TABLE CHANGE col1 col2 varchar"
    };
    for (String q : queries) {
      UserExceptionAssert.assertThatThrownBy(() -> test(q))
          .hasErrorType(UserBitShared.DremioPBError.ErrorType.PARSE);
    }
  }

  @Test
  public void renameToExistingColumn() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "changecol1";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
                testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query =
            String.format(
                "ALTER TABLE %s.%s CHANGE CATALOG_NAME CATALOG_DESCRIPTION varchar",
                testSchema, tableName);
        errorMsgTestHelper(
            query,
            "Column [CATALOG_DESCRIPTION] already present in table ["
                + testSchema
                + ".changecol1]");

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void invalidType() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "changecol2";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
                testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query =
            String.format(
                "ALTER TABLE %s.%s CHANGE CATALOG_NAME commit_message2 varchare",
                testSchema, tableName);
        errorMsgTestHelper(query, "Invalid column type [`varchare`] specified");

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void invalidPromoteStringToInt() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "changecol3";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
                testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query =
            String.format(
                "ALTER TABLE %s.%s CHANGE CATALOG_NAME commit_message2 int", testSchema, tableName);
        errorMsgTestHelper(
            query, "Cannot change data type of column [CATALOG_NAME] from VARCHAR to INTEGER");

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void rename() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "changecol4";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as "
                    + "SELECT n_regionkey from cp.\"tpch/nation.parquet\" where n_regionkey < 2 GROUP BY n_regionkey ",
                testSchema, tableName);
        test(createTableQuery);

        final String selectFromCreatedTable =
            String.format("select * from %s.%s", testSchema, tableName);
        testBuilder()
            .sqlQuery(selectFromCreatedTable)
            .unOrdered()
            .baselineColumns("n_regionkey")
            .baselineValues(0)
            .baselineValues(1)
            .build()
            .run();

        Thread.sleep(1001);
        String renameQuery =
            String.format(
                "ALTER TABLE %s.%s CHANGE n_regionkey regionkey int", testSchema, tableName);
        test(renameQuery);

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

  @Test
  public void uppromote() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "changecol5";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as "
                    + "SELECT n_regionkey from cp.\"tpch/nation.parquet\" where n_regionkey < 2 GROUP BY n_regionkey ",
                testSchema, tableName);
        test(createTableQuery);

        final String selectFromCreatedTable =
            String.format("select * from %s.%s", testSchema, tableName);
        testBuilder()
            .sqlQuery(selectFromCreatedTable)
            .unOrdered()
            .baselineColumns("n_regionkey")
            .baselineValues(0)
            .baselineValues(1)
            .build()
            .run();

        Thread.sleep(1001);
        String uppromote =
            String.format(
                "ALTER TABLE %s.%s CHANGE n_regionkey regionkey bigint", testSchema, tableName);
        test(uppromote);

        testBuilder()
            .sqlQuery(selectFromCreatedTable)
            .unOrdered()
            .baselineColumns("regionkey")
            .baselineValues(0L)
            .baselineValues(1L)
            .build()
            .run();

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void renameFollowedByUppromote() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "changecol6";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as "
                    + "SELECT n_regionkey from cp.\"tpch/nation.parquet\" where n_regionkey < 2 GROUP BY n_regionkey ",
                testSchema, tableName);
        test(createTableQuery);

        final String selectFromCreatedTable =
            String.format("select * from %s.%s", testSchema, tableName);
        testBuilder()
            .sqlQuery(selectFromCreatedTable)
            .unOrdered()
            .baselineColumns("n_regionkey")
            .baselineValues(0)
            .baselineValues(1)
            .build()
            .run();

        Thread.sleep(1001);
        String renameQuery =
            String.format(
                "ALTER TABLE %s.%s CHANGE n_regionkey regionkey int", testSchema, tableName);
        test(renameQuery);

        testBuilder()
            .sqlQuery(selectFromCreatedTable)
            .unOrdered()
            .baselineColumns("regionkey")
            .baselineValues(0)
            .baselineValues(1)
            .build()
            .run();

        Thread.sleep(1001);
        String uppromote =
            String.format(
                "ALTER TABLE %s.%s CHANGE regionkey regionkey bigint", testSchema, tableName);
        test(uppromote);

        testBuilder()
            .sqlQuery(selectFromCreatedTable)
            .unOrdered()
            .baselineColumns("regionkey")
            .baselineValues(0L)
            .baselineValues(1L)
            .build()
            .run();

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void renameAndUppromote() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "changecol7";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as "
                    + "SELECT n_regionkey from cp.\"tpch/nation.parquet\" where n_regionkey < 2 GROUP BY n_regionkey ",
                testSchema, tableName);
        test(createTableQuery);

        final String selectFromCreatedTable =
            String.format("select * from %s.%s", testSchema, tableName);
        testBuilder()
            .sqlQuery(selectFromCreatedTable)
            .unOrdered()
            .baselineColumns("n_regionkey")
            .baselineValues(0)
            .baselineValues(1)
            .build()
            .run();

        Thread.sleep(1001);
        String renameQuery =
            String.format(
                "ALTER TABLE %s.%s CHANGE n_regionkey regionkey bigint", testSchema, tableName);
        test(renameQuery);

        testBuilder()
            .sqlQuery(selectFromCreatedTable)
            .unOrdered()
            .baselineColumns("regionkey")
            .baselineValues(0L)
            .baselineValues(1L)
            .build()
            .run();

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void renamePartitionColumn() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "changecol8";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s PARTITION BY (n_regionkey) as "
                    + "SELECT n_regionkey from cp.\"tpch/nation.parquet\" where n_regionkey < 2 GROUP BY n_regionkey ",
                testSchema, tableName);
        test(createTableQuery);

        final String selectFromCreatedTable =
            String.format("select * from %s.%s", testSchema, tableName);
        testBuilder()
            .sqlQuery(selectFromCreatedTable)
            .unOrdered()
            .baselineColumns("n_regionkey")
            .baselineValues(0)
            .baselineValues(1)
            .build()
            .run();

        Thread.sleep(1001);
        String addColQuery =
            String.format("ALTER TABLE %s.%s ADD COLUMNS(key int)", testSchema, tableName);
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
        String dropColQuery =
            String.format(
                "ALTER TABLE %s.%s CHANGE n_RegiOnkey regionkey int", testSchema, tableName);
        errorMsgTestHelper(
            dropColQuery,
            "[n_regionkey] is a partition column. Partition spec change is not supported.");

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void noContextFail() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "changecol9";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as "
                    + "SELECT n_regionkey from cp.\"tpch/nation.parquet\" where n_regionkey < 2 GROUP BY n_regionkey ",
                testSchema, tableName);
        test(createTableQuery);

        final String selectFromCreatedTable =
            String.format("select * from %s.%s", testSchema, tableName);
        testBuilder()
            .sqlQuery(selectFromCreatedTable)
            .unOrdered()
            .baselineColumns("n_regionkey")
            .baselineValues(0)
            .baselineValues(1)
            .build()
            .run();

        Thread.sleep(1001);
        String changeColQuery =
            String.format("ALTER TABLE %s CHANGE n_RegiOnkey regionkey int", tableName);
        errorMsgTestHelper(changeColQuery, "Table [changecol9] does not exist");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void messageHasPrecAndScale() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "changecol10";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format("CREATE TABLE %s.%s (dec1 DECIMAL(10,2))", testSchema, tableName);
        test(createTableQuery);
        String changeColQuery =
            String.format(
                "ALTER TABLE %s.%s CHANGE dec1 dec1 decimal(11, 3)", testSchema, tableName);
        errorMsgTestHelper(
            changeColQuery,
            "Cannot change data type of column [dec1] from DECIMAL(10, 2) to DECIMAL(11, 3)");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInStruct() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "structcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
                testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query =
            String.format(
                "ALTER TABLE %s.%s CHANGE COLUMN CATALOG_NAME CATALOG_NAME ROW(col1 int,COL1 double)",
                testSchema, tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInListOfStruct() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "listofstructcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
                testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query =
            String.format(
                "ALTER TABLE %s.%s CHANGE COLUMN CATALOG_NAME CATALOG_NAME ARRAY(ROW(col1 int,COL1 double))",
                testSchema, tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInListOfListOfStruct() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "listoflistofstructcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
                testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query =
            String.format(
                "ALTER TABLE %s.%s CHANGE COLUMN CATALOG_NAME CATALOG_NAME ARRAY(ARRAY(ROW(col1 int,COL1 double)))",
                testSchema, tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInStructOfListOfStruct() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "structoflistofstructcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
                testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query =
            String.format(
                "ALTER TABLE %s.%s CHANGE COLUMN CATALOG_NAME CATALOG_NAME ROW(col ARRAY(ROW(col int, col1 int,COL1 double)))",
                testSchema, tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInStructOfStruct() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "structofstructcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery =
            String.format(
                "CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
                testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query =
            String.format(
                "ALTER TABLE %s.%s CHANGE COLUMN CATALOG_NAME CATALOG_NAME ROW(col ROW(col int, col1 int,COL1 double))",
                testSchema, tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }
}

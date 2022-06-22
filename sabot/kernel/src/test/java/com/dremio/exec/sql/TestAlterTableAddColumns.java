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

public class TestAlterTableAddColumns extends BaseTestQuery {

  @Test
  public void badSql() {
    String[] queries = {
        "ALTER TABLE tbl ADD COLUMNS",
        "ALTER TABLE ADD COLUMNS(col1 varchar)",
        "ALTER TABLE tbl ADD COLUMNS()"
    };
    for (String q : queries) {
      errorMsgTestHelper(q, "Failure parsing the query");
    }
  }

  @Test
  public void typeNotSpecified() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "addcol0";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);

        Thread.sleep(1001);
        String[] queries = {
          String.format("ALTER TABLE %s.%s ADD COLUMNS(col1, col2 int)", testSchema, tableName),
          String.format("ALTER TABLE %s.%s ADD COLUMNS(col1)", testSchema, tableName),
          String.format("ALTER TABLE %s.%s ADD COLUMNS(col1 varchar, col2 int, col3)", testSchema, tableName)
        };
        for (String q : queries) {
          errorMsgTestHelper(q, "Column type not specified");
        }
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void invalidType() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "addcol1";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col1 varchar, col2 int, col3 inte)", testSchema,
          tableName);
        errorMsgTestHelper(query, "Invalid column type [`inte`] specified for column [col3].");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void unsupportedType() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "addcol7";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col1 varchar, col2 int, col4 ARRAY(INT), col5 ROW(col6 INT), col3 MAP)", testSchema,
          tableName);
        errorMsgTestHelper(query, "Type conversion error for column col3");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void noContextFail() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "addcol07";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s ADD COLUMNS(col1 varchar, col2 int, col3 MAP)", tableName);
        errorMsgTestHelper(query, "Table [addcol07] not found");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumn() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "addcol2";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(CATALOG_NAME varchar, col2 int, col3 int)", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [CATALOG_NAME] already in the table.");

        query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col1 varchar, col1 int, col3 int)", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInStruct() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "structcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col STRUCT<col1 :int,COL1: double>)", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInListOfStruct() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "listofstructcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col LIST<STRUCT<col1: int,COL1: double>>)", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInListOfListOfStruct() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "listoflistofstructcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col LIST<LIST<STRUCT<col1: int,COL1: double>>>)", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInStructOfListOfStruct() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "structoflistofstructcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col STRUCT<col: LIST<STRUCT<col :int, col1: int,COL1: double>>>)", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInStructOfListOfRow() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "structoflistofrowcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col STRUCT<col: LIST<ROW(col int, col1 int,COL1 double)>>)", testSchema,
          tableName);
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

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col STRUCT<col STRUCT<col: int, col1 :int,COL1 :double>>)", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInRow() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "rowcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col ROW(col1 int,COL1 double))", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInArrayOfRow() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "arrayofrowcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col ARRAY(ROW(col1 int,COL1 double)))", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInArrayOfArrayOfRow() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "arrayofarrayofrowcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col ARRAY(ARRAY(ROW(col1 int,COL1 double))))", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInRowOfArrayOfRow() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "rowoflistofrowcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col ROW(col ARRAY(ROW(col int, col1 int,COL1 double))))", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void addExistingColumnInRowOfRow() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "rowofrowcol";
      try (AutoCloseable c = enableIcebergTables()) {

        final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          testSchema, tableName);
        test(createTableQuery);
        Thread.sleep(1001);

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col ROW(col ROW(col int, col1 int,COL1 double)))", testSchema,
          tableName);
        errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void invalidTable() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "addcol3";
      try (AutoCloseable c = enableIcebergTables()) {

        String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(version varchar, col2 int, col3 int)", testSchema,
          tableName);
        errorMsgTestHelper(query, "Table [" + testSchema + ".addcol3] not found");

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void icebergNotEnabled() throws Exception {
    try (AutoCloseable o = disableIcebergFlag()) {
      for (String testSchema : SCHEMAS_FOR_TEST) {
        String tableName = "addcol4";
        try {
          final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
            testSchema, tableName);
          test(createTableQuery);
          Thread.sleep(1001);

          String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(version varchar, col2 int, col3 int)", testSchema,
            tableName);
          errorMsgTestHelper(query, "contact customer support for steps to enable the iceberg tables");
        } finally {
          FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
        }
      }
    }
  }

  @Test
  public void addColumnAndInsert() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "addcol6";
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
        String alterQuery = String.format("ALTER TABLE %s.%s ADD COLUMNS(newcol1 varchar, newcol2 int, newcol3 int)", testSchema,
          tableName);
        test(alterQuery);

        testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .baselineColumns("n_regionkey", "newcol1", "newcol2", "newcol3")
          .baselineValues(0, null, null, null)
          .baselineValues(1, null, null, null)
          .build()
          .run();

        Thread.sleep(1001);
        String insertQuery = String.format("INSERT INTO %s.%s VALUES(2, 'ab', 1, 2)", testSchema,
          tableName);
        test(insertQuery);

        testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .baselineColumns("n_regionkey", "newcol1", "newcol2", "newcol3")
          .baselineValues(0, null, null, null)
          .baselineValues(1, null, null, null)
          .baselineValues(2, "ab", 1, 2)
          .build()
          .run();

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

}

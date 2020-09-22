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
    String tableName = "addcol0";
    try (AutoCloseable c = enableIcebergTables()) {

      final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from sys.version",
          TEMP_SCHEMA, tableName);
      test(createTableQuery);

      Thread.sleep(1001);
      String[] queries = {
          String.format("ALTER TABLE %s.%s ADD COLUMNS(col1, col2 int)", TEMP_SCHEMA, tableName),
          String.format("ALTER TABLE %s.%s ADD COLUMNS(col1)", TEMP_SCHEMA, tableName),
          String.format("ALTER TABLE %s.%s ADD COLUMNS(col1 varchar, col2 int, col3)", TEMP_SCHEMA, tableName)
      };
      for (String q : queries) {
        errorMsgTestHelper(q, "Column type not specified");
      }
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void invalidType() throws Exception {
    String tableName = "addcol1";
    try (AutoCloseable c = enableIcebergTables()) {

      final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from sys.version",
          TEMP_SCHEMA, tableName);
      test(createTableQuery);
      Thread.sleep(1001);

      String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col1 varchar, col2 int, col3 inte)", TEMP_SCHEMA,
          tableName);
      errorMsgTestHelper(query, "Invalid column type [`inte`] specified for column [col3].");
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void unsupportedType() throws Exception {
    String tableName = "addcol7";
    try (AutoCloseable c = enableIcebergTables()) {

      final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from sys.version",
          TEMP_SCHEMA, tableName);
      test(createTableQuery);
      Thread.sleep(1001);

      String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col1 varchar, col2 int, col3 MAP)", TEMP_SCHEMA,
          tableName);
      errorMsgTestHelper(query, "conversion from arrow type to iceberg type failed for field col3");
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void noContextFail() throws Exception {
    String tableName = "addcol07";
    try (AutoCloseable c = enableIcebergTables()) {

      final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from sys.version",
          TEMP_SCHEMA, tableName);
      test(createTableQuery);
      Thread.sleep(1001);

      String query = String.format("ALTER TABLE %s ADD COLUMNS(col1 varchar, col2 int, col3 MAP)", tableName);
      errorMsgTestHelper(query, "Table [addcol07] not found");
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void addExistingColumn() throws Exception {
    String tableName = "addcol2";
    try (AutoCloseable c = enableIcebergTables()) {

      final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from sys.version",
          TEMP_SCHEMA, tableName);
      test(createTableQuery);
      Thread.sleep(1001);

      String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(version varchar, col2 int, col3 int)", TEMP_SCHEMA,
          tableName);
      errorMsgTestHelper(query, "Column [VERSION] already in the table.");

      query = String.format("ALTER TABLE %s.%s ADD COLUMNS(col1 varchar, col1 int, col3 int)", TEMP_SCHEMA,
          tableName);
      errorMsgTestHelper(query, "Column [COL1] specified multiple times.");
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void invalidTable() throws Exception {
    String tableName = "addcol3";
    try (AutoCloseable c = enableIcebergTables()) {

      String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(version varchar, col2 int, col3 int)", TEMP_SCHEMA,
          tableName);
      errorMsgTestHelper(query, "Table [dfs_test.addcol3] not found");

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void icebergNotEnabled() throws Exception {
    String tableName = "addcol4";

    try {
      final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from sys.version",
          TEMP_SCHEMA, tableName);
      test(createTableQuery);
      Thread.sleep(1001);

      String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(version varchar, col2 int, col3 int)", TEMP_SCHEMA,
          tableName);
      errorMsgTestHelper(query, "contact customer support for steps to enable the iceberg tables");
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void nonIcebergTable() throws Exception {
    String tableName = "addcol5";
    final String createTableQuery = String.format("CREATE TABLE %s.%s as select * from sys.version",
        TEMP_SCHEMA, tableName);
    test(createTableQuery);
    try (AutoCloseable c = enableIcebergTables()) {

      String query = String.format("ALTER TABLE %s.%s ADD COLUMNS(version varchar, col2 int, col3 int)", TEMP_SCHEMA,
          tableName);
      errorMsgTestHelper(query, "Table [dfs_test.addcol5] is not configured to support DML operations");

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void addColumnAndInsert() throws Exception {
    String tableName = "addcol6";
    try (AutoCloseable c = enableIcebergTables()) {

      final String createTableQuery = String.format("CREATE TABLE %s.%s as " +
              "SELECT n_regionkey from cp.\"tpch/nation.parquet\" where n_regionkey < 2 GROUP BY n_regionkey ",
          TEMP_SCHEMA, tableName);
      test(createTableQuery);

      final String selectFromCreatedTable = String.format("select * from %s.%s", TEMP_SCHEMA, tableName);
      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .baselineColumns("n_regionkey")
          .baselineValues(0)
          .baselineValues(1)
          .build()
          .run();


      Thread.sleep(1001);
      String alterQuery = String.format("ALTER TABLE %s.%s ADD COLUMNS(newcol1 varchar, newcol2 int, newcol3 int)", TEMP_SCHEMA,
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
      String insertQuery = String.format("INSERT INTO %s.%s VALUES(2, 'ab', 1, 2)", TEMP_SCHEMA,
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

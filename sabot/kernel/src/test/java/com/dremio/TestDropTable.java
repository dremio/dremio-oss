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
package com.dremio;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.test.TemporarySystemProperties;


public class TestDropTable extends PlanTestBase {
  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  private static final String CREATE_SIMPLE_TABLE = "create table %s as select 1 from cp.\"employee.json\"";
  private static final String CREATE_SIMPLE_VIEW = "create view %s as select 1 from cp.\"employee.json\"";
  private static final String DROP_TABLE = "drop table %s";
  private static final String DROP_TABLE_IF_EXISTS = "drop table if exists %s";
  private static final String DROP_VIEW_IF_EXISTS = "drop view if exists %s";
  private static final String DOUBLE_QUOTE = "\"";
  private static final String REFRESH = "alter table %s refresh metadata";
  private static final String PROMOTE = "alter pds  %s refresh metadata auto promotion";

  @Test
  public void testDropJsonTable() throws Exception {
    test("use dfs_test");
    test("alter session set \"store.format\" = 'json'");

    final String tableName = "simple_json";
    // create a json table
    test(String.format(CREATE_SIMPLE_TABLE, tableName));

    // drop the table
    final String dropSql = String.format(DROP_TABLE, tableName);
    testBuilder()
        .sqlQuery(dropSql)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [dfs_test.%s] dropped", tableName))
        .go();
  }

  @Test
  public void testDropParquetTable() throws Exception {
    test("use dfs_test");
    final String tableName = "simple_parquet";

    // create a parquet table
    test(String.format(CREATE_SIMPLE_TABLE, tableName));

    testNoResult(REFRESH, tableName);

    // drop the table
    final String dropSql = String.format(DROP_TABLE, tableName);
    testBuilder()
        .sqlQuery(dropSql)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [dfs_test.%s] dropped", tableName))
        .go();
  }

  @Test
  public void testDropTextTable() throws Exception {
    test("use dfs_test");

    test("alter session set \"store.format\" = 'csv'");
    final String csvTable = "simple_csv";

    // create a csv table
    test(String.format(CREATE_SIMPLE_TABLE, csvTable));

    testNoResult(REFRESH, csvTable);

    // drop the table
    String dropSql = String.format(DROP_TABLE, csvTable);
    testBuilder()
        .sqlQuery(dropSql)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [dfs_test.%s] dropped", csvTable))
        .go();

    test("alter session set \"store.format\" = 'psv'");
    final String psvTable = "simple_psv";

    // create a psv table
    test(String.format(CREATE_SIMPLE_TABLE, psvTable));

    // drop the table
    dropSql = String.format(DROP_TABLE, psvTable);
    testBuilder()
        .sqlQuery(dropSql)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [dfs_test.%s] dropped", psvTable))
        .go();

    test("alter session set \"store.format\" = 'tsv'");
    final String tsvTable = "simple_tsv";

    // create a tsv table
    test(String.format(CREATE_SIMPLE_TABLE, tsvTable));

    // drop the table
    dropSql = String.format(DROP_TABLE, tsvTable);
    testBuilder()
        .sqlQuery(dropSql)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [dfs_test.%s] dropped", tsvTable))
        .go();
  }

  @Test
  public void testNonHomogenousDrop() throws Exception {
    test("use dfs_test");
    final String tableName = "homogenous_table";

    // create a parquet table
    test("ALTER SESSION SET \"store.format\"='parquet'");
    test(String.format(CREATE_SIMPLE_TABLE, tableName));

    testNoResult(REFRESH, tableName);

    // create a json table within the same directory
    test("alter session set \"store.format\" = 'json'");
    final String nestedJsonTable = tableName + Path.SEPARATOR + "json_table";
    test(String.format(CREATE_SIMPLE_TABLE, DOUBLE_QUOTE + nestedJsonTable + DOUBLE_QUOTE));

    boolean dropFailed = false;
    // this should fail, because the directory contains non-homogenous files
    try {
      test(String.format(DROP_TABLE, tableName));
    } catch (UserException e) {
      Assert.assertTrue(e.getMessage().contains("VALIDATION ERROR"));
      dropFailed = true;
    }

    Assert.assertTrue("Dropping of non-homogeneous table should have failed", dropFailed);

    // drop the individual json table
    testBuilder()
        .sqlQuery(String.format(DROP_TABLE, DOUBLE_QUOTE + nestedJsonTable + DOUBLE_QUOTE))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [dfs_test.\"%s\"] dropped", nestedJsonTable))
        .go();

    // Now drop should succeed
    testBuilder()
        .sqlQuery(String.format(DROP_TABLE, tableName))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [dfs_test.%s] dropped", tableName))
        .go();
  }

  @SuppressWarnings("checkstyle:LocalFinalVariableName")
  @Test
  public void testIsHomogenousShouldNotBeCalledForIcebergTables() throws Exception {
    test("use dfs_test_hadoop");
    final String TABLE_NAME = "test_table";
    final String TABLE_PATH = TEMP_SCHEMA_HADOOP + "." + TABLE_NAME;
    final String ID_COLUMN_NAME = "id";
    final String DATA_COLUMN_NAME = "data";
    final String CREATE_BASIC_ICEBERG_TABLE_SQL =
      String.format("CREATE TABLE %s (%s BIGINT, %s VARCHAR) STORE AS (type => 'iceberg')", TABLE_PATH, ID_COLUMN_NAME, DATA_COLUMN_NAME);
    final String CREATE_BASIC_ICEBERG_TABLE_JSON_SQL =
      String.format("CREATE TABLE %s (%s BIGINT, %s VARCHAR) STORE AS (type => 'iceberg')", DOUBLE_QUOTE + TABLE_PATH + Path.SEPARATOR + "json_table" + DOUBLE_QUOTE,
        ID_COLUMN_NAME, DATA_COLUMN_NAME);

    final String tableName = TABLE_NAME;

    // create an iceberg table
    test(CREATE_BASIC_ICEBERG_TABLE_SQL);

    // create a json table within the same directory
    test("alter session set \"store.format\" = 'json'");
    final String nestedJsonTable = tableName + Path.SEPARATOR + "json_table";
    test(CREATE_BASIC_ICEBERG_TABLE_JSON_SQL);

    // isHomogeneous() should not be called for iceberg tables
    boolean isHomogeneousCalled = false;
    try {
      test(String.format(DROP_TABLE, tableName));
    } catch (UserException e) {
      Assert.assertTrue(e.getMessage().contains("Table contains different file formats"));
      isHomogeneousCalled = true;
    }

    Assert.assertFalse("Dropping of non-homogeneous table should not be called for Iceberg tables", isHomogeneousCalled);
  }

  @Test // DRILL-4673
  public void testDropTableIfExistsWhileTableExists() throws Exception {
    final String existentTableName = "test_table";
    test("use dfs_test");

    // successful dropping of existent table
    test(String.format(CREATE_SIMPLE_TABLE, existentTableName));

    testNoResult(REFRESH, existentTableName);

    testBuilder()
        .sqlQuery(String.format(DROP_TABLE_IF_EXISTS, existentTableName))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [dfs_test.%s] dropped", existentTableName))
        .go();
  }

  @Test // DRILL-4673
  @Ignore("DX-5204")
  public void testDropTableIfExistsWhileTableDoesNotExist() throws Exception {
    final String nonExistentTableName = "test_table";
    test("use dfs_test");

    // dropping of non existent table without error
    testBuilder()
        .sqlQuery(String.format(DROP_TABLE_IF_EXISTS, nonExistentTableName))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [%s] not found", nonExistentTableName))
        .go();
  }

  @Test // DRILL-4673
  public void testDropTableIfExistsWhileItIsAView() throws Exception {
    final String viewName = "test_view";
    try{
      properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
      test("use dfs_test");

      // dropping of non existent table without error if the view with such name is existed
      test(String.format(CREATE_SIMPLE_VIEW, viewName));
      testBuilder()
          .sqlQuery(String.format(DROP_TABLE_IF_EXISTS, viewName))
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Table [dfs_test.%s] not found.", viewName))
          .go();
    } finally {
      test(String.format(DROP_VIEW_IF_EXISTS, viewName));
      properties.clear(DremioConfig.LEGACY_STORE_VIEWS_ENABLED);
    }
  }

  @Test
  public void testDropNonExistentTable() throws Exception {
    test("CREATE TABLE dfs_test.testDropNonExistentTable as SELECT * FROM INFORMATION_SCHEMA.CATALOGS");
    testNoResult(REFRESH, "dfs_test.testDropNonExistentTable");
    test("DROP TABLE dfs_test.testDropNonExistentTable");
    errorMsgTestHelper("DROP TABLE dfs_test.testDropNonExistentTable", "VALIDATION ERROR: Table [dfs_test.testDropNonExistentTable] not found");
    test("DROP TABLE IF EXISTS dfs_test.testDropNonExistentTable");
  }

  @Test
  public void withNestedTablePath() throws Exception {
    test("use dfs_test");
    final String tableName = "\"nested/table/path\"";

    // create a parquet table
    test(String.format(CREATE_SIMPLE_TABLE, tableName));

    testNoResult(REFRESH, tableName);

    // drop the table
    final String dropSql = String.format(DROP_TABLE, tableName);
    testBuilder()
        .sqlQuery(dropSql)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [dfs_test.%s] dropped", tableName))
        .go();
  }

  @Test
  public void dropTableWithChild() throws Exception {

    final String parentTableName = "dfs_test.pt";
    final String childTableName = "dfs_test.pt.ct";
    // create a  table
    test(String.format(CREATE_SIMPLE_TABLE, parentTableName));
    test(String.format(PROMOTE, parentTableName));

    test(String.format(CREATE_SIMPLE_TABLE, childTableName));
    test(String.format(PROMOTE, childTableName));

    // drop the parent table - should fail
    String dropSql = String.format(DROP_TABLE, parentTableName);
    errorMsgTestHelper(String.format(DROP_TABLE, parentTableName), String.format("VALIDATION ERROR: Cannot drop table [%s] since it has child tables",parentTableName));


    // drop the child table - should succeed
    dropSql = String.format(DROP_TABLE, childTableName);
    testBuilder()
      .sqlQuery(dropSql)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, String.format("Table [%s] dropped", childTableName))
      .go();

    // drop the parent table - now it should succeed
    dropSql = String.format(DROP_TABLE, parentTableName);
    testBuilder()
      .sqlQuery(dropSql)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, String.format("Table [%s] dropped", parentTableName))
      .go();
  }
}

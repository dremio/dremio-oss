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

import static com.dremio.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_CONNECT;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_DESCRIPTION;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_NAME;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.common.util.TestTools;
import com.google.common.collect.ImmutableList;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Contains tests for -- InformationSchema -- Queries on InformationSchema such as SHOW TABLES, SHOW
 * SCHEMAS or DESCRIBE table -- USE schema -- SHOW FILES
 */
public class TestInfoSchema extends BaseTestQuery {

  private static final String[] baselineColNames = {
    "COLUMN_NAME",
    "DATA_TYPE",
    "IS_NULLABLE",
    "NUMERIC_PRECISION",
    "NUMERIC_SCALE",
    "EXTENDED_PROPERTIES",
    "MASKING_POLICY",
    "SORT_ORDER_PRIORITY"
  };

  @Test
  public void selectFromAllTables() throws Exception {
    test("select * from INFORMATION_SCHEMA.SCHEMATA");
    test("select * from INFORMATION_SCHEMA.CATALOGS");
    test("select * from INFORMATION_SCHEMA.VIEWS");
    test("select * from INFORMATION_SCHEMA.\"TABLES\"");
    test("select * from INFORMATION_SCHEMA.COLUMNS");
  }

  @Test
  public void catalogs() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM INFORMATION_SCHEMA.CATALOGS")
        .unOrdered()
        .baselineColumns(
            CATS_COL_CATALOG_NAME, CATS_COL_CATALOG_DESCRIPTION, CATS_COL_CATALOG_CONNECT)
        .baselineValues("DREMIO", "The internal metadata used by Dremio", "")
        .go();
  }

  @Test
  public void showTablesFromDb() throws Exception {
    final List<String[]> expected =
        ImmutableList.of(
            new String[] {"INFORMATION_SCHEMA", "VIEWS"},
            new String[] {"INFORMATION_SCHEMA", "COLUMNS"},
            new String[] {"INFORMATION_SCHEMA", "TABLES"},
            new String[] {"INFORMATION_SCHEMA", "CATALOGS"},
            new String[] {"INFORMATION_SCHEMA", "SCHEMATA"});

    final TestBuilder t1 =
        testBuilder()
            .sqlQuery("SHOW TABLES FROM INFORMATION_SCHEMA")
            .unOrdered()
            .baselineColumns("TABLE_SCHEMA", "TABLE_NAME");
    for (String[] expectedRow : expected) {
      t1.baselineValues(expectedRow);
    }
    t1.go();

    final TestBuilder t2 =
        testBuilder()
            .sqlQuery("SHOW TABLES IN INFORMATION_SCHEMA")
            .unOrdered()
            .baselineColumns("TABLE_SCHEMA", "TABLE_NAME");
    for (String[] expectedRow : expected) {
      t2.baselineValues(expectedRow);
    }
    t2.go();
  }

  @Test
  public void showTablesLike() throws Exception {
    testBuilder()
        .sqlQuery("SHOW TABLES LIKE '%CH%'")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE INFORMATION_SCHEMA")
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("INFORMATION_SCHEMA", "SCHEMATA")
        .go();
  }

  @Test
  @Ignore("I think the results might be fine")
  public void showDatabases() throws Exception {
    final List<String[]> expected =
        ImmutableList.of(
            new String[] {"dfs.default"},
            new String[] {"dfs.root"},
            new String[] {"dfs.tmp"},
            new String[] {"cp.default"},
            new String[] {"sys"},
            new String[] {"dfs_test.home"},
            new String[] {"dfs_test.default"},
            new String[] {"dfs_test.tmp"},
            new String[] {"INFORMATION_SCHEMA"});

    test("show databases");
    final TestBuilder t1 =
        testBuilder().sqlQuery("SHOW DATABASES").unOrdered().baselineColumns("SCHEMA_NAME");
    for (String[] expectedRow : expected) {
      t1.baselineValues(expectedRow);
    }
    t1.go();

    final TestBuilder t2 =
        testBuilder().sqlQuery("SHOW SCHEMAS").unOrdered().baselineColumns("SCHEMA_NAME");
    for (String[] expectedRow : expected) {
      t2.baselineValues(expectedRow);
    }
    t2.go();
  }

  @Test
  public void showDatabasesLike() throws Exception {
    testBuilder()
        .sqlQuery("SHOW DATABASES LIKE '%mat%'")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME")
        .baselineValues("INFORMATION_SCHEMA")
        .go();
  }

  @Test
  public void describeTable() throws Exception {
    testBuilder()
        .sqlQuery("DESCRIBE CATALOGS")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE INFORMATION_SCHEMA")
        .baselineColumns(baselineColNames)
        .baselineValues("CATALOG_NAME", "CHARACTER VARYING", "YES", null, null, "[]", null, null)
        .baselineValues(
            "CATALOG_DESCRIPTION", "CHARACTER VARYING", "YES", null, null, "[]", null, null)
        .baselineValues("CATALOG_CONNECT", "CHARACTER VARYING", "YES", null, null, "[]", null, null)
        .go();
  }

  @Test
  public void describeTableWithSchema() throws Exception {
    testBuilder()
        .sqlQuery("DESCRIBE INFORMATION_SCHEMA.\"TABLES\"")
        .unOrdered()
        .baselineColumns(baselineColNames)
        .baselineValues("TABLE_CATALOG", "CHARACTER VARYING", "YES", null, null, "[]", null, null)
        .baselineValues("TABLE_SCHEMA", "CHARACTER VARYING", "YES", null, null, "[]", null, null)
        .baselineValues("TABLE_NAME", "CHARACTER VARYING", "YES", null, null, "[]", null, null)
        .baselineValues("TABLE_TYPE", "CHARACTER VARYING", "YES", null, null, "[]", null, null)
        .go();
  }

  @Ignore("DX-2490")
  @Test
  public void describeWhenSameTableNameExistsInMultipleSchemas() throws Exception {
    try {
      test("USE dfs_test.tmp");
      test("CREATE OR REPLACE VIEW \"TABLES\" AS SELECT full_name FROM cp.\"employee.json\"");

      testBuilder()
          .sqlQuery("DESCRIBE \"TABLES\"")
          .unOrdered()
          .optionSettingQueriesForTestQuery("USE dfs_test.tmp")
          .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
          .baselineValues("full_name", "CHARACTER VARYING", "YES")
          .go();

      testBuilder()
          .sqlQuery("DESCRIBE INFORMATION_SCHEMA.\"TABLES\"")
          .unOrdered()
          .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
          .baselineValues("TABLE_CATALOG", "CHARACTER VARYING", "YES")
          .baselineValues("TABLE_SCHEMA", "CHARACTER VARYING", "YES")
          .baselineValues("TABLE_NAME", "CHARACTER VARYING", "YES")
          .baselineValues("TABLE_TYPE", "CHARACTER VARYING", "YES")
          .go();
    } finally {
      test("DROP VIEW dfs_test.tmp.\"TABLES\"");
    }
  }

  @Test
  public void describeTableWithColumnName() throws Exception {
    testBuilder()
        .sqlQuery("DESCRIBE \"TABLES\" TABLE_CATALOG")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE INFORMATION_SCHEMA")
        .baselineColumns(baselineColNames)
        .baselineValues("TABLE_CATALOG", "CHARACTER VARYING", "YES", null, null, "[]", null, null)
        .go();
  }

  @Test
  public void describeTableWithSchemaAndColumnName() throws Exception {
    testBuilder()
        .sqlQuery("DESCRIBE INFORMATION_SCHEMA.\"TABLES\" TABLE_CATALOG")
        .unOrdered()
        .baselineColumns(baselineColNames)
        .baselineValues("TABLE_CATALOG", "CHARACTER VARYING", "YES", null, null, "[]", null, null)
        .go();
  }

  @Ignore("DX-2290")
  @Test
  public void describeTableWithColQualifier() throws Exception {
    testBuilder()
        .sqlQuery("DESCRIBE COLUMNS 'TABLE%'")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE INFORMATION_SCHEMA")
        .baselineColumns(baselineColNames)
        .baselineValues("TABLE_CATALOG", "CHARACTER VARYING", "YES", null, null, "[]", null)
        .baselineValues("TABLE_SCHEMA", "CHARACTER VARYING", "YES", null, null, "[]", null)
        .baselineValues("TABLE_NAME", "CHARACTER VARYING", "YES", null, null, "[]", null)
        .go();
  }

  @Ignore("DX-2290")
  @Test
  public void describeTableWithSchemaAndColQualifier() throws Exception {
    testBuilder()
        .sqlQuery("DESCRIBE INFORMATION_SCHEMA.SCHEMATA 'SCHEMA%'")
        .unOrdered()
        .baselineColumns(baselineColNames)
        .baselineValues("SCHEMA_NAME", "CHARACTER VARYING", "YES", null, null, "[]", null)
        .baselineValues("SCHEMA_OWNER", "CHARACTER VARYING", "YES", null, null, "[]", null)
        .go();
  }

  @Test
  public void defaultSchemaDfs() throws Exception {
    testBuilder()
        .sqlQuery(
            String.format(
                "SELECT R_REGIONKEY FROM \"%s/sample-data/region.parquet\" LIMIT 1",
                Paths.get(TestTools.getWorkingPath()).getParent().getParent()))
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE dfs")
        .baselineColumns("R_REGIONKEY")
        .baselineValues(0L)
        .go();
  }

  @Test
  public void defaultSchemaClasspath() throws Exception {
    testBuilder()
        .sqlQuery("SELECT full_name FROM \"employee.json\" LIMIT 1")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE cp")
        .baselineColumns("full_name")
        .baselineValues("Sheri Nowmer")
        .go();
  }

  @Test
  public void queryFromNonDefaultSchema() throws Exception {
    testBuilder()
        .sqlQuery("SELECT full_name FROM cp.\"employee.json\" LIMIT 1")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE dfs_test")
        .baselineColumns("full_name")
        .baselineValues("Sheri Nowmer")
        .go();
  }

  @Test
  public void useSchema() throws Exception {
    testBuilder()
        .sqlQuery("USE dfs")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "Default schema changed to [dfs]")
        .go();
  }

  @Test
  public void useSubSchemaWithinSchema() throws Exception {
    testBuilder()
        .sqlQuery("USE dfs_test")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "Default schema changed to [dfs_test]")
        .go();
  }

  @Test
  public void useSchemaNegative() throws Exception {
    errorMsgTestHelper(
        "USE invalid.schema",
        "Schema [invalid.schema] is not valid with respect to either root schema or current default schema.");
  }

  @Ignore("Sensitive to tmp directory changes during execution.")
  @Test
  public void showFilesWithDefaultSchema() throws Exception {
    test("USE dfs_test.\"default\"");
    test("SHOW FILES FROM \"tmp\"");
  }
}

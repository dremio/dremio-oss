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
package com.dremio.exec.catalog.dataplane;

import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createFolderQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableWithColDefsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.folderA;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.folderB;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tableA;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.projectnessie.model.Namespace;

import com.dremio.exec.catalog.VersionContext;
import com.google.common.base.Joiner;

/**
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedInfoSchemaTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */
public class InfoSchemaTestCases {
  private ITDataplanePluginTestSetup base;
  private static final Joiner DOT_JOINER = Joiner.on('.');

  InfoSchemaTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  @Test
  public void selectInformationSchemaTable() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath1));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath1));

    List<String> tableRow = Arrays.asList(
      "DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1)),
      tableName1,
      "TABLE");

    List<String> viewRow = Arrays.asList(
      "DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, viewKey.get(0), viewKey.get(1)),
      viewName,
      "VIEW");

    // INFORMATION_SCHEMA."TABLES" should return correct value.
    // Views should appear
    assertThat(base.runSqlWithResults("select * from INFORMATION_SCHEMA.\"TABLES\""))
      .contains(tableRow)
      .contains(viewRow)
      // Column at index 3 is "TABLE_TYPE"
      .allMatch(row -> (row.get(3).equals("TABLE") || row.get(3).equals("SYSTEM_TABLE") || row.get(3).equals("VIEW")));
  }

  @Test
  public void selectInformationSchemaView() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath));

    List<String> viewRow = Arrays.asList(
      "DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, viewKey.get(0), viewKey.get(1)),
      viewName,
      String.format(
        "SELECT * FROM %s.%s.%s.%s",
        DATAPLANE_PLUGIN_NAME,
        tablePath.get(0),
        tablePath.get(1),
        tableName
      ));

    assertThat(base.runSqlWithResults("select * from INFORMATION_SCHEMA.views"))
      .contains(viewRow)
      // Column at index 2 is "TABLE_NAME"
      .allMatch(row -> !row.get(2).contains(tableName));
  }

  @Test
  public void selectInformationSchemaSchemataForExplicitFolder() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    //Create folders explicitly
    base.getNessieClient().createNamespace()
      .namespace(tablePath.get(0))
      .refName("main")
      .create();
    base.getNessieClient().createNamespace()
      .namespace(Namespace.of(tablePath.subList(0, 2)))
      .refName("main")
      .create();
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> sourceRow = Arrays.asList(
      "DREMIO",
      DATAPLANE_PLUGIN_NAME,
      "<owner>",
      "SIMPLE",
      "NO");

    List<String> row1 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME,
        tablePath.get(0), tablePath.get(1)),
      "<owner>",
      "SIMPLE",
      "NO");
    List<String> row2 = Arrays.asList("DREMIO",
      String.format("%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0)),
      "<owner>",
      "SIMPLE",
      "NO");

    //Result comes out without a where statement.
    assertThat(base.runSqlWithResults("select * from INFORMATION_SCHEMA.SCHEMATA"))
      //Source should Appear
      .contains(sourceRow)
      .contains(row1)
      .contains(row2);
  }

  @Test
  public void selectInformationSchemaColumns() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    List<String> colDef = new ArrayList<>();
    colDef.add("col1 Varchar(255)");
    colDef.add("col2 Varchar(255)");

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, colDef));

    List<List<String>> expected = new ArrayList<>();
    List<String> row1 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
      tableName,
      "col1",
      "1");
    List<String> row2 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
      tableName,
      "col2",
      "2");

    expected.add(row1);
    expected.add(row2);

    // row 1 and row 2 should appear
    assertThat(base.runSqlWithResults("select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS"))
      .containsAll(expected);
    // row 3 should not appear since we didn't create col3.
    // Column at index 3 is "COLUMN_NAME"
    assertThat(base.runSqlWithResults("select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS"))
      // Column at index 2 is "TABLE_NAME"
      .filteredOn(row -> row.get(2).contains(tableName))
      .containsExactlyInAnyOrderElementsOf(expected);

    assertThat(base.runSqlWithResults(String.format("select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS where table_schema = \'%s.%s.%s\'", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))))
      .containsAll(expected);
  }
  @Test public void selectInformationSchemaTableWithSchemaFilter() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath1));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName1);
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath2));

    List<String> tableRow = Arrays.asList(
      "DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1. get(1)),
      tableName1,
      "TABLE");
    //Test INFORMATION_SCHEMA."TABLES" with TABLE SCHEMA
    assertThat(base.runSqlWithResults(String.format("select * from INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA = \'%s.%s.%s\'",DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1))))
      .contains(tableRow)
      .allMatch(row -> !row.get(2).contains(tableName2));
  }

  @Test public void selectInformationSchemaTableWithNameFilter() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath1));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName1);
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath2));

    List<String> tableRow = Arrays.asList(
      "DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1. get(1)),
      tableName1,
      "TABLE");

    // Test INFORMATION_SCHEMA."TABLES" with TABLE NAME
    assertThat(base.runSqlWithResults(String.format("select * from INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_NAME = \'%s\'",tableName1)))
      .contains(tableRow)
      .allMatch(row -> !row.get(2).contains(tableName2));
  }
  @Test public void selectInformationSchemaViewWithSchemaFilter() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath));

    List<String> viewRow = Arrays.asList(
      "DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, viewKey.get(0), viewKey.get(1)),
      viewName ,
      String.format(
        "SELECT * FROM %s.%s.%s.%s",
        DATAPLANE_PLUGIN_NAME,
        tablePath.get(0),
        tablePath.get(1),
        tableName
      ));

    //Test with schema
    assertThat(base.runSqlWithResults(String.format("select * from INFORMATION_SCHEMA.views where table_schema = \'%s.%s.%s\'", DATAPLANE_PLUGIN_NAME, viewKey.get(0), viewKey.get(1))))
      .contains(viewRow)
      .allMatch(row -> !row.get(2).contains(tableName));
  }

  @Test public void selectInformationSchemaViewWithNameFilter() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath));

    List<String> viewRow = Arrays.asList(
      "DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, viewKey.get(0), viewKey.get(1)),
      viewName ,
      String.format(
        "SELECT * FROM %s.%s.%s.%s",
        DATAPLANE_PLUGIN_NAME,
        tablePath.get(0),
        tablePath.get(1),
        tableName
      ));

    //Test with name
    assertThat(base.runSqlWithResults(String.format("select * from INFORMATION_SCHEMA.views where table_name = \'%s\'", viewName)))
      .contains(viewRow)
      .allMatch(row -> !row.get(2).contains(tableName));
  }

  @Test public void selectInformationSchemaSchemataWithFilter() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    //Create folders explicitly
    base.getNessieClient().createNamespace()
      .namespace(tablePath.get(0))
      .refName("main")
      .create();
    base.getNessieClient().createNamespace()
      .namespace(Namespace.of(tablePath.subList(0, 2)))
      .refName("main")
      .create();
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> row1 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME,
        tablePath.get(0), tablePath.get(1)),
      "<owner>",
      "SIMPLE",
      "NO");

    assertThat(base.runSqlWithResults(String.format("select * from INFORMATION_SCHEMA.SCHEMATA where schema_name = \'%s.%s.%s\'",DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))))
      .contains(row1)
      .allMatch(row -> !row.get(1).equals(DATAPLANE_PLUGIN_NAME))
      .allMatch(row -> !row.get(1).equals(String.format("%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0))));
  }

  @Test public void selectInformationSchemaColumnsWithSchemaFilter() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    List<String> colDef = new ArrayList<>();
    colDef.add("col1_table1 Varchar(255)");
    colDef.add("col2_table1 Varchar(255)");

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    List<String> colDef2 = new ArrayList<>();
    colDef2.add("col1_table2 Varchar(255)");
    colDef2.add("col2_table2 Varchar(255)");

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, colDef));
    base.runSQL(createTableWithColDefsQuery(tablePath2, colDef2));

    List<List<String>> expected = new ArrayList<>();
    List<String> row1Table1 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
      tableName,
      "col1_table1",
      "1");
    List<String> row2Table1 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
      tableName,
      "col2_table1",
      "2");

    expected.add(row1Table1);
    expected.add(row2Table1);
    //Test with schema
    assertThat(base.runSqlWithResults(String.format("select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS where table_schema = \'%s.%s.%s\'", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))))
      .containsAll(expected)
      .allMatch(row -> !(row.get(1).contains("col1_table2")))
      .allMatch(row -> !(row.get(1).contains("col2_table2")));
  }

  @Test public void selectInformationSchemaColumnsWithNameFilter() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    List<String> colDef = new ArrayList<>();
    colDef.add("col1 Varchar(255)");
    colDef.add("col2 Varchar(255)");

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, colDef));

    List<List<String>> expected = new ArrayList<>();
    List<String> row1 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
      tableName,
      "col1",
      "1");
    List<String> row2 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
      tableName,
      "col2",
      "2");

    expected.add(row1);
    expected.add(row2);
    //Test with name
    assertThat(base.runSqlWithResults(String.format("select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS where table_name = \'%s\'", tableName)))
      .containsAll(expected);
  }

  @Test public void testLike() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath2));

    List<String> expected = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
      tableName,
      "TABLE");

    String query = String.format("select * from information_schema.\"tables\" where table_name like \'%s\'", tableName);
    assertThat(base.runSqlWithResults(query))
      .contains(expected)
      .allMatch(row -> !(row.get(2).contains(tableName2)))
      .allMatch(row -> !(row.get(1).contains(DOT_JOINER.join(tablePath2))));
  }

  @Test public void testLikeWithNoWildCard() throws Exception {
    final String tableInSourceRoot = generateUniqueTableName();
    List<String> tablePath = new ArrayList<>();
    tablePath.add(tableInSourceRoot);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath1));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath2));

    List<String> expectedRowForTableInSourceRoot = Arrays.asList("DREMIO",
      DATAPLANE_PLUGIN_NAME,
      tableInSourceRoot,
      "TABLE");

    List<String> expectedRowForTable1 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1)),
      tableName1,
      "TABLE");

    String searchTableInSourceRoot = String.format("select * from information_schema.\"tables\" where table_name like \'%s\'", tableInSourceRoot);
    assertThat(base.runSqlWithResults(searchTableInSourceRoot))
      .contains(expectedRowForTableInSourceRoot)
      .allMatch(row -> !(row.get(2).contains(tableName1)) && !(row.get(2).contains(tableName2)))
      .allMatch(row -> !(row.get(1).contains(DOT_JOINER.join(tablePath1))) && !(row.get(1).contains(DOT_JOINER.join(tablePath2))));

    String query = String.format("select * from information_schema.\"tables\" where table_schema like \'%s.%s.%s\'",DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1));
    assertThat(base.runSqlWithResults(query))
      .contains(expectedRowForTable1)
      .allMatch(row -> !(row.get(2).contains(tableInSourceRoot)) && !(row.get(2).contains(tableName2)))
      .allMatch(row -> !(row.get(1).equals(DATAPLANE_PLUGIN_NAME)) && !(row.get(1).contains(DOT_JOINER.join(tablePath2))));
  }

  @Test public void testStartWith() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> expected = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
      tableName,
      "TABLE");

    String query = String.format("select * from information_schema.\"tables\" where table_name like \'%s%%\'", tableName.substring(0, tableName.length() - 2));
    assertThat(base.runSqlWithResults(query))
      .contains(expected);
  }

  @Test public void testContains() throws Exception {
    final String tableInSourceRoot = generateUniqueTableName();
    List<String> tablePath = new ArrayList<>();
    tablePath.add(tableInSourceRoot);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> expected = Arrays.asList("DREMIO",
      DATAPLANE_PLUGIN_NAME,
      tableInSourceRoot,
      "TABLE");

    String query = String.format("select * from information_schema.\"tables\" where table_name like \'%%%s%%\'", tableInSourceRoot.substring(1, tableInSourceRoot.length() - 1));
    assertThat(base.runSqlWithResults(query))
      .contains(expected);
  }

  @Test public void testNoEntriesFound() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
      tableName,
      "TABLE");

    String query = String.format("select * from information_schema.\"tables\" where table_schema like \'%s\' and table_name like \'hello_world\'",DATAPLANE_PLUGIN_NAME);
    assertThat(base.runSqlWithResults(query))
      .doesNotContain(table_row)
      .allMatch(row -> !row.get(2).contains(tableName));
  }

  @Test public void testWithEscape() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
      tableName,
      "TABLE");

    String query = String.format("select * from information_schema.\"tables\" where table_schema like \'%s\' and table_name like 'hello\\\\_world'",DATAPLANE_PLUGIN_NAME);
    assertThat(base.runSqlWithResults(query))
      .doesNotContain(table_row)
      .allMatch(row -> !row.get(2).contains(tableName));
  }

  @Test
  public void testMultipleBackSlash() throws Exception {
    //we need to add quotation at the front since it contains special character
    final String escapedTableName = "\"\\\\\\table\"";
    final String resultEscapedTableName = "\\\\\\table";
    final List<String> escapedTablePath = tablePathWithFolders(escapedTableName);
    createFolders(escapedTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(escapedTablePath));

    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, escapedTablePath.get(0), escapedTablePath.get(1)),
      resultEscapedTableName,
      "TABLE");

    String query = String.format("select * from information_schema.\"tables\" where table_name = \'%s\'", resultEscapedTableName);
    assertThat(base.runSqlWithResults(query))
      .contains(table_row)
      .allMatch(row -> !row.get(2).equals(tableName));

  }

  @Test
  public void testStarTable() throws Exception {
    //we need to add quotation at the front since it contains special character
    final String starTableName = "\"*table\"";
    final String inputStarTableName = "*table";
    final List<String> starTablePath = tablePathWithFolders(starTableName);
    createFolders(starTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(starTablePath));

    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, starTablePath.get(0), starTablePath.get(1)),
      inputStarTableName,
      "TABLE");

    String query = String.format("select * from information_schema.\"tables\" where table_name like \'%s\'", inputStarTableName);
    assertThat(base.runSqlWithResults(query))
      .contains(table_row)
      .allMatch(row -> !row.get(2).equals(tableName));
  }

  @Test public void testQuestionMarkTable() throws Exception {
    //we need to add quotation at the front since it contains special character
    final String questionTableName = "\"?table\"";
    final String inputQuestionTableName = "?table";
    final List<String> questionTablePath = tablePathWithFolders(questionTableName);
    createFolders(questionTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(questionTablePath));

    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, questionTablePath.get(0), questionTablePath.get(1)),
      inputQuestionTableName,
      "TABLE");

    String query = String.format("select * from information_schema.\"tables\" where table_name = \'%s\'", inputQuestionTableName);
    assertThat(base.runSqlWithResults(query))
      .contains(table_row)
      .allMatch(row -> !row.get(2).equals(tableName));
  }

  @Test public void testMultipleStatements() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath1));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath2));

    final String tableName3 = generateUniqueTableName();
    final List<String> tablePath3 = tablePathWithFolders(tableName3);
    createFolders(tablePath3, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath3));

    final String tableName4 = generateUniqueTableName();
    final List<String> tablePath4 = tablePathWithFolders(tableName4);
    createFolders(tablePath4, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath4));

    final String tableName5 = generateUniqueTableName();
    final List<String> tablePath5 = tablePathWithFolders(tableName5);
    createFolders(tablePath5, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath5));

    List<String> table_row1 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1)),
      tableName1,
      "TABLE");

    List<String> table_row2 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath2.get(0), tablePath2.get(1)),
      tableName2,
      "TABLE");

    List<String> table_row3 = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath3.get(0), tablePath3.get(1)),
      tableName3,
      "TABLE");

    String query = String.format(
      "select * from information_schema.\"tables\" where table_schema like \'%s.%s.%s\' or (table_name = \'%s\' or table_name like \'%s\' or table_schema like \'%%%s%%\')",
      DATAPLANE_PLUGIN_NAME,
      tablePath1.get(0),
      tablePath1.get(1),
      tableName2,
      tableName3,
      tablePath3.get(1));
    assertThat(base.runSqlWithResults(query))
      .contains(table_row1)
      .contains(table_row2)
      .contains(table_row3)
      .allMatch(row -> !(row.get(2).contains(tableName4)) && !(row.get(2).contains(tableName5)))
      .allMatch(row -> !(row.get(1).contains(DOT_JOINER.join(tablePath4))) && !(row.get(1).contains(DOT_JOINER.join(tablePath5))));
  }

  @Test public void testNestedFolders() throws Exception {
    /*
     * select * from information_schema."SCHEMATA" where schema_name = 'nessie_t.folder1'
     * select * from information_schema."SCHEMATA" where schema_name like 'nessie_t.folder1%'
     */
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    //Create folders explicitly
    base.getNessieClient().createNamespace()
      .namespace(tablePath.get(0))
      .refName("main")
      .create();
    base.getNessieClient().createNamespace()
      .namespace(Namespace.of(tablePath.subList(0, 2)))
      .refName("main")
      .create();
    base.runSQL(createEmptyTableQuery(tablePath));
    String nestedFolderEqual = String.format("select * from information_schema.SCHEMATA where schema_name = '%s.%s'", DATAPLANE_PLUGIN_NAME, tablePath.get(0));
    String nestedFolderLikeStartWith = String.format("select * from information_schema.SCHEMATA where schema_name like '%s.%s%%'", DATAPLANE_PLUGIN_NAME, tablePath.get(0));

    List<String> folder1Row = Arrays.asList(
      "DREMIO",
      String.format("%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0)),
      "<owner>",
      "SIMPLE",
      "NO");

    List<String> folder2Row = Arrays.asList(
      "DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
      "<owner>",
      "SIMPLE",
      "NO");

    assertThat(base.runSqlWithResults(nestedFolderEqual))
      .contains(folder1Row)
      .allMatch(row -> !row.get(1).equals(String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))));
    assertThat(base.runSqlWithResults(nestedFolderLikeStartWith))
      .contains(folder1Row)
      .contains(folder2Row);
  }

  @Test public void testSelectFromNonArcticSources() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String selectFromNonArcticSource = "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA = 'cp'";
    String tablePathIncludeSource = String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1));

    assertThat(base.runSqlWithResults(selectFromNonArcticSource))
      .allMatch(row -> !row.get(1).equals(tablePathIncludeSource));
  }

  @Test public void testSelectEscapeQuoteWithDots() throws Exception {
    List<String> schemaPath = new ArrayList<>();
    schemaPath.add("folder1");
    schemaPath.add("folder2");
    schemaPath.add("\"dot.dot.dot.dot\"");
    //Create folders explicitly
    base.getNessieClient().createNamespace()
      .namespace(schemaPath.get(0))
      .refName("main")
      .create();
    base.getNessieClient().createNamespace()
      .namespace(Namespace.of(schemaPath.subList(0, 2)))
      .refName("main")
      .create();
    base.getNessieClient().createNamespace()
      .namespace(Namespace.of(schemaPath.subList(0, 3)))
      .refName("main")
      .create();
    String selectTableWithDots = String.format("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = \'%s.%s.%s.%s\'",DATAPLANE_PLUGIN_NAME, schemaPath.get(0), schemaPath.get(1),schemaPath.get(2));
    String tablePathIncludeSource = String.format("%s.%s.%s.%s", DATAPLANE_PLUGIN_NAME, schemaPath.get(0), schemaPath.get(1), schemaPath.get(2));
    assertThat(base.runSqlWithResults(selectTableWithDots))
      .allMatch(row -> row.get(1).equals(tablePathIncludeSource));
  }

  @Test public void testTableUnderOneFolder() throws Exception {
    final String tableUnderOneFolder = generateUniqueTableName();
    List<String> pathUnderOneFolder = new ArrayList<>();
    pathUnderOneFolder.add("folder1");
    pathUnderOneFolder.add(tableUnderOneFolder);
    createFolders(pathUnderOneFolder, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(pathUnderOneFolder));

    final String tableName = generateUniqueTableName();
    List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row = Arrays.asList("DREMIO",
      String.format("%s.%s", DATAPLANE_PLUGIN_NAME, pathUnderOneFolder.get(0)),
      tableUnderOneFolder,
      "TABLE");

    String selectTableUnderOneFolder = String.format("SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA like \'%s.%s\'", DATAPLANE_PLUGIN_NAME, pathUnderOneFolder.get(0));
    assertThat(base.runSqlWithResults(selectTableUnderOneFolder))
      .contains(table_row)
      .allMatch(row -> !row.get(1).equals(String.format("%s.%s.%s",DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))));
  }

  @Test public void testTableUnderSource() throws Exception {
    final String tableUnderSource = generateUniqueTableName();
    List<String> pathUnderSource = new ArrayList<>();
    pathUnderSource.add(tableUnderSource);
    createFolders(pathUnderSource, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(pathUnderSource));

    final String tableName = generateUniqueTableName();
    List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row = Arrays.asList("DREMIO",
      String.format("%s", DATAPLANE_PLUGIN_NAME),
      tableUnderSource,
      "TABLE");

    String selectTableUnderSource = String.format("SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA like \'%s\'", DATAPLANE_PLUGIN_NAME);
    assertThat(base.runSqlWithResults(selectTableUnderSource))
      .contains(table_row)
      .allMatch(row -> !row.get(1).equals(String.format("%s.%s.%s",DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))));
  }

  @Test
  public void testNamespaceLinksToCorrectNamespace() throws Exception {
    //create folder A
    List<String> folderPath1 = Arrays.asList(DATAPLANE_PLUGIN_NAME, folderA);
    base.runSqlWithResults(createFolderQuery(folderPath1));

    //create folder B
    List<String> folderPath2 = Arrays.asList(DATAPLANE_PLUGIN_NAME, folderA, folderB);
    base.runSqlWithResults(createFolderQuery(folderPath2));

    //create table A
    List<String> tablePath = Arrays.asList(folderA, folderB, tableA);
    base.runSqlWithResults(createTableAsQuery(tablePath, 10));

    //create folder B directly under folder A
    List<String> folderPath3 = Arrays.asList(DATAPLANE_PLUGIN_NAME, folderB);
    base.runSqlWithResults(createFolderQuery(folderPath3));

    List<String> expectedTableRow = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, folderA, folderB),
      tableA,
      "TABLE");

    List<String> expectedSchemataRow = Arrays.asList("DREMIO",
      String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, folderA, folderB),
      "<owner>",
      "SIMPLE",
      "NO");

    String selectInfoSchemaTables = String.format("SELECT * FROM INFORMATION_SCHEMA.\"TABLES\"");
    assertThat(base.runSqlWithResults(selectInfoSchemaTables))
      .contains(expectedTableRow)
      .allMatch(row -> !row.get(1).equals(String.format("%s.%s.%s",DATAPLANE_PLUGIN_NAME, folderB, folderB)))
      .allMatch(row -> !row.get(1).equals(String.format("%s.%s.%s",DATAPLANE_PLUGIN_NAME, folderA, folderA)))
      .allMatch(row -> !row.get(1).equals(String.format("%s.%s.%s",DATAPLANE_PLUGIN_NAME, folderB, folderA)))
      .allMatch(row -> !row.get(1).equals(String.format("%s.%s",DATAPLANE_PLUGIN_NAME, folderA)))
      .allMatch(row -> !row.get(1).equals(String.format("%s.%s",DATAPLANE_PLUGIN_NAME, folderB)))
      .allMatch(row -> !row.get(1).equals(DATAPLANE_PLUGIN_NAME));

    String selectInfoSchemaSchemata = String.format("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA");
    assertThat(base.runSqlWithResults(selectInfoSchemaSchemata))
      .contains(expectedSchemataRow)
      .allMatch(row -> !row.get(1).equals(String.format("%s.%s.%s",DATAPLANE_PLUGIN_NAME, folderB, folderB)))
      .allMatch(row -> !row.get(1).equals(String.format("%s.%s.%s",DATAPLANE_PLUGIN_NAME, folderA, folderA)));
  }
}

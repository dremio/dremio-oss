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

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createFolderQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableWithColDefsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.folderA;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.folderB;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tableA;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Namespace;

public class ITDataplanePluginInfoSchema extends ITDataplanePluginTestSetup {

  private static final Joiner DOT_JOINER = Joiner.on('.');

  @Test
  public void selectInformationSchemaTable() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createEmptyTableQuery(tablePath1));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    runSQL(createViewQuery(viewKey, tablePath1));

    List<String> tableRow =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1)),
            tableName1,
            "TABLE");

    List<String> viewRow =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, viewKey.get(0), viewKey.get(1)),
            viewName,
            "VIEW");

    // INFORMATION_SCHEMA."TABLES" should return correct value.
    // Views should appear
    assertThat(runSqlWithResults("select * from INFORMATION_SCHEMA.\"TABLES\""))
        .contains(tableRow)
        .contains(viewRow)
        // Column at index 3 is "TABLE_TYPE"
        .allMatch(
            row ->
                (row.get(3).equals("TABLE")
                    || row.get(3).equals("SYSTEM_TABLE")
                    || row.get(3).equals("VIEW")));
  }

  @Test
  public void selectInformationSchemaView() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    runSQL(createViewQuery(viewKey, tablePath));

    List<String> viewRow =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, viewKey.get(0), viewKey.get(1)),
            viewName,
            String.format(
                "SELECT * FROM %s.%s.%s.%s",
                DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1), tableName));

    assertThat(runSqlWithResults("select * from INFORMATION_SCHEMA.views"))
        .contains(viewRow)
        // Column at index 2 is "TABLE_NAME"
        .allMatch(row -> !row.get(2).contains(tableName));
  }

  @Test
  public void selectInformationSchemaSchemataForExplicitFolder() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Create folders explicitly
    getNessieApi().createNamespace().namespace(tablePath.get(0)).refName("main").create();
    getNessieApi()
        .createNamespace()
        .namespace(Namespace.of(tablePath.subList(0, 2)))
        .refName("main")
        .create();
    runSQL(createEmptyTableQuery(tablePath));

    List<String> sourceRow =
        Arrays.asList("DREMIO", DATAPLANE_PLUGIN_NAME, "<owner>", "SIMPLE", "NO");

    List<String> row1 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            "<owner>",
            "SIMPLE",
            "NO");
    List<String> row2 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0)),
            "<owner>",
            "SIMPLE",
            "NO");

    // Result comes out without a where statement.
    assertThat(runSqlWithResults("select * from INFORMATION_SCHEMA.SCHEMATA"))
        // Source should Appear
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

    runSQL(createTableWithColDefsQuery(tablePath, colDef));

    List<List<String>> expected = new ArrayList<>();
    List<String> row1 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            tableName,
            "col1",
            "1");
    List<String> row2 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            tableName,
            "col2",
            "2");

    expected.add(row1);
    expected.add(row2);

    // row 1 and row 2 should appear
    assertThat(
            runSqlWithResults(
                "select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS"))
        .containsAll(expected);
    // row 3 should not appear since we didn't create col3.
    // Column at index 3 is "COLUMN_NAME"
    assertThat(
            runSqlWithResults(
                "select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS"))
        // Column at index 2 is "TABLE_NAME"
        .filteredOn(row -> row.get(2).contains(tableName))
        .containsExactlyInAnyOrderElementsOf(expected);

    assertThat(
            runSqlWithResults(
                String.format(
                    "select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS where table_schema = '%s.%s.%s'",
                    DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))))
        .containsAll(expected);
  }

  @Test
  public void selectInformationSchemaTableWithSchemaFilter() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createEmptyTableQuery(tablePath1));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName1);
    runSQL(createEmptyTableQuery(tablePath2));

    List<String> tableRow =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1)),
            tableName1,
            "TABLE");
    // Test INFORMATION_SCHEMA."TABLES" with TABLE SCHEMA
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA = '%s.%s.%s'",
                    DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1))))
        .contains(tableRow)
        .allMatch(row -> !row.get(2).contains(tableName2));
  }

  @Test
  public void selectInformationSchemaTableWithNameFilter() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createEmptyTableQuery(tablePath1));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName1);
    runSQL(createEmptyTableQuery(tablePath2));

    List<String> tableRow =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1)),
            tableName1,
            "TABLE");

    // Test INFORMATION_SCHEMA."TABLES" with TABLE NAME
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_NAME = '%s'",
                    tableName1)))
        .contains(tableRow)
        .allMatch(row -> !row.get(2).contains(tableName2));
  }

  @Test
  public void selectInformationSchemaViewWithSchemaFilter() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(createViewQuery(viewKey, tablePath));

    List<String> viewRow =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, viewKey.get(0), viewKey.get(1)),
            viewName,
            String.format(
                "SELECT * FROM %s.%s.%s.%s",
                DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1), tableName));

    // Test with schema
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.views where table_schema = '%s.%s.%s'",
                    DATAPLANE_PLUGIN_NAME, viewKey.get(0), viewKey.get(1))))
        .contains(viewRow)
        .allMatch(row -> !row.get(2).contains(tableName));
  }

  @Test
  public void selectInformationSchemaViewWithNameFilter() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(createViewQuery(viewKey, tablePath));

    List<String> viewRow =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, viewKey.get(0), viewKey.get(1)),
            viewName,
            String.format(
                "SELECT * FROM %s.%s.%s.%s",
                DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1), tableName));

    // Test with name
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.views where table_name = '%s'", viewName)))
        .contains(viewRow)
        .allMatch(row -> !row.get(2).contains(tableName));
  }

  @Test
  public void selectInformationSchemaSchemataWithFilter() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Create folders explicitly
    getNessieApi().createNamespace().namespace(tablePath.get(0)).refName("main").create();
    getNessieApi()
        .createNamespace()
        .namespace(Namespace.of(tablePath.subList(0, 2)))
        .refName("main")
        .create();
    runSQL(createEmptyTableQuery(tablePath));

    List<String> row1 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            "<owner>",
            "SIMPLE",
            "NO");

    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.SCHEMATA where schema_name = '%s.%s.%s'",
                    DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))))
        .contains(row1)
        .allMatch(row -> !row.get(1).equals(DATAPLANE_PLUGIN_NAME))
        .allMatch(
            row ->
                !row.get(1)
                    .equals(String.format("%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0))));
  }

  @Test
  public void selectInformationSchemaColumnsWithSchemaFilter() throws Exception {
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

    runSQL(createTableWithColDefsQuery(tablePath, colDef));
    runSQL(createTableWithColDefsQuery(tablePath2, colDef2));

    List<List<String>> expected = new ArrayList<>();
    List<String> row1Table1 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            tableName,
            "col1_table1",
            "1");
    List<String> row2Table1 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            tableName,
            "col2_table1",
            "2");

    expected.add(row1Table1);
    expected.add(row2Table1);
    // Test with schema
    assertThat(
            runSqlWithResults(
                String.format(
                    "select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS where table_schema = '%s.%s.%s'",
                    DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))))
        .containsAll(expected)
        .allMatch(row -> !(row.get(1).contains("col1_table2")))
        .allMatch(row -> !(row.get(1).contains("col2_table2")));
  }

  @Test
  public void selectInformationSchemaColumnsWithNameFilter() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    List<String> colDef = new ArrayList<>();
    colDef.add("col1 Varchar(255)");
    colDef.add("col2 Varchar(255)");

    runSQL(createTableWithColDefsQuery(tablePath, colDef));

    List<List<String>> expected = new ArrayList<>();
    List<String> row1 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            tableName,
            "col1",
            "1");
    List<String> row2 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            tableName,
            "col2",
            "2");

    expected.add(row1);
    expected.add(row2);
    // Test with name
    assertThat(
            runSqlWithResults(
                String.format(
                    "select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS where table_name = '%s'",
                    tableName)))
        .containsAll(expected);
  }

  @Test
  public void testLike() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    runSQL(createEmptyTableQuery(tablePath2));

    List<String> expected =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            tableName,
            "TABLE");

    String query =
        String.format(
            "select * from information_schema.\"tables\" where table_name like '%s'", tableName);
    assertThat(runSqlWithResults(query))
        .contains(expected)
        .allMatch(row -> !(row.get(2).contains(tableName2)))
        .allMatch(row -> !(row.get(1).contains(DOT_JOINER.join(tablePath2))));
  }

  @Test
  public void testLikeWithNoWildCard() throws Exception {
    final String tableInSourceRoot = generateUniqueTableName();
    List<String> tablePath = new ArrayList<>();
    tablePath.add(tableInSourceRoot);
    runSQL(createEmptyTableQuery(tablePath));

    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createEmptyTableQuery(tablePath1));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    runSQL(createEmptyTableQuery(tablePath2));

    List<String> expectedRowForTableInSourceRoot =
        Arrays.asList("DREMIO", DATAPLANE_PLUGIN_NAME, tableInSourceRoot, "TABLE");

    List<String> expectedRowForTable1 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1)),
            tableName1,
            "TABLE");

    String searchTableInSourceRoot =
        String.format(
            "select * from information_schema.\"tables\" where table_name like '%s'",
            tableInSourceRoot);
    assertThat(runSqlWithResults(searchTableInSourceRoot))
        .contains(expectedRowForTableInSourceRoot)
        .allMatch(row -> !(row.get(2).contains(tableName1)) && !(row.get(2).contains(tableName2)))
        .allMatch(
            row ->
                !(row.get(1).contains(DOT_JOINER.join(tablePath1)))
                    && !(row.get(1).contains(DOT_JOINER.join(tablePath2))));

    String query =
        String.format(
            "select * from information_schema.\"tables\" where table_schema like '%s.%s.%s'",
            DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1));
    assertThat(runSqlWithResults(query))
        .contains(expectedRowForTable1)
        .allMatch(
            row -> !(row.get(2).contains(tableInSourceRoot)) && !(row.get(2).contains(tableName2)))
        .allMatch(
            row ->
                !(row.get(1).equals(DATAPLANE_PLUGIN_NAME))
                    && !(row.get(1).contains(DOT_JOINER.join(tablePath2))));
  }

  @Test
  public void testStartWith() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    List<String> expected =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            tableName,
            "TABLE");

    String query =
        String.format(
            "select * from information_schema.\"tables\" where table_name like '%s%%'",
            tableName.substring(0, tableName.length() - 2));
    assertThat(runSqlWithResults(query)).contains(expected);
  }

  @Test
  public void testContains() throws Exception {
    final String tableInSourceRoot = generateUniqueTableName();
    List<String> tablePath = new ArrayList<>();
    tablePath.add(tableInSourceRoot);
    runSQL(createEmptyTableQuery(tablePath));

    List<String> expected =
        Arrays.asList("DREMIO", DATAPLANE_PLUGIN_NAME, tableInSourceRoot, "TABLE");

    String query =
        String.format(
            "select * from information_schema.\"tables\" where table_name like '%%%s%%'",
            tableInSourceRoot.substring(1, tableInSourceRoot.length() - 1));
    assertThat(runSqlWithResults(query)).contains(expected);
  }

  @Test
  public void testNoEntriesFound() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            tableName,
            "TABLE");

    String query =
        String.format(
            "select * from information_schema.\"tables\" where table_schema like '%s' and table_name like 'hello_world'",
            DATAPLANE_PLUGIN_NAME);
    assertThat(runSqlWithResults(query))
        .doesNotContain(table_row)
        .allMatch(row -> !row.get(2).contains(tableName));
  }

  @Test
  public void testWithEscape() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            tableName,
            "TABLE");

    String query =
        String.format(
            "select * from information_schema.\"tables\" where table_schema like '%s' and table_name like 'hello\\\\_world'",
            DATAPLANE_PLUGIN_NAME);
    assertThat(runSqlWithResults(query))
        .doesNotContain(table_row)
        .allMatch(row -> !row.get(2).contains(tableName));
  }

  @Test
  @Disabled("Re-enable after DX-83690")
  public void testMultipleBackSlash() throws Exception {
    // we need to add quotation at the front since it contains special character
    final String escapedTableName = "\"\\\\table\"";
    final String resultEscapedTableName = "\\\\table";
    final String resultEscapedTableNamePattern = "\\\\\\\\table"; // (equivalent to r'(\\\\table)

    final List<String> escapedTablePath = tablePathWithFolders(escapedTableName);
    runSQL(createEmptyTableQuery(escapedTablePath));

    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row =
        Arrays.asList(
            "DREMIO",
            String.format(
                "%s.%s.%s",
                DATAPLANE_PLUGIN_NAME, escapedTablePath.get(0), escapedTablePath.get(1)),
            resultEscapedTableName,
            "TABLE");

    String query1 =
        String.format(
            "select * from information_schema.\"tables\" where table_name = '%s'",
            resultEscapedTableName);
    assertThat(runSqlWithResults(query1))
        .contains(table_row)
        .allMatch(row -> !row.get(2).equals(tableName));

    String query2 =
        String.format(
            "select * from information_schema.\"tables\" where table_name like '%s'",
            resultEscapedTableNamePattern);
    assertThat(runSqlWithResults(query2))
        .contains(table_row)
        .allMatch(row -> !row.get(2).equals(tableName));
  }

  @Test
  public void testStarTable() throws Exception {
    // we need to add quotation at the front since it contains special character
    final String starTableName = "*table";
    final List<String> starTablePath = tablePathWithFolders(starTableName);
    runSQL(createEmptyTableQuery(starTablePath));

    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row =
        Arrays.asList(
            "DREMIO",
            String.format(
                "%s.%s.%s", DATAPLANE_PLUGIN_NAME, starTablePath.get(0), starTablePath.get(1)),
            starTableName,
            "TABLE");

    String query =
        String.format(
            "select * from information_schema.\"tables\" where table_name like '%s'",
            starTableName);
    assertThat(runSqlWithResults(query))
        .contains(table_row)
        .allMatch(row -> !row.get(2).equals(tableName));
  }

  @Test
  public void testQuestionMarkTable() throws Exception {
    // we need to add quotation at the front since it contains special character
    final String questionTableName = "?table";
    final List<String> questionTablePath = tablePathWithFolders(questionTableName);
    runSQL(createEmptyTableQuery(questionTablePath));

    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row =
        Arrays.asList(
            "DREMIO",
            String.format(
                "%s.%s.%s",
                DATAPLANE_PLUGIN_NAME, questionTablePath.get(0), questionTablePath.get(1)),
            questionTableName,
            "TABLE");

    String query =
        String.format(
            "select * from information_schema.\"tables\" where table_name = '%s'",
            questionTableName);
    assertThat(runSqlWithResults(query))
        .contains(table_row)
        .allMatch(row -> !row.get(2).equals(tableName));
  }

  @Test
  public void testMultipleStatements() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createEmptyTableQuery(tablePath1));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    runSQL(createEmptyTableQuery(tablePath2));

    final String tableName3 = generateUniqueTableName();
    final List<String> tablePath3 = tablePathWithFolders(tableName3);
    runSQL(createEmptyTableQuery(tablePath3));

    final String tableName4 = generateUniqueTableName();
    final List<String> tablePath4 = tablePathWithFolders(tableName4);
    runSQL(createEmptyTableQuery(tablePath4));

    final String tableName5 = generateUniqueTableName();
    final List<String> tablePath5 = tablePathWithFolders(tableName5);
    runSQL(createEmptyTableQuery(tablePath5));

    List<String> table_row1 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1)),
            tableName1,
            "TABLE");

    List<String> table_row2 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath2.get(0), tablePath2.get(1)),
            tableName2,
            "TABLE");

    List<String> table_row3 =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath3.get(0), tablePath3.get(1)),
            tableName3,
            "TABLE");

    String query =
        String.format(
            "select * from information_schema.\"tables\" where table_schema like '%s.%s.%s' or (table_name = '%s' or table_name like '%s' or table_schema like '%%%s%%')",
            DATAPLANE_PLUGIN_NAME,
            tablePath1.get(0),
            tablePath1.get(1),
            tableName2,
            tableName3,
            tablePath3.get(1));
    assertThat(runSqlWithResults(query))
        .contains(table_row1)
        .contains(table_row2)
        .contains(table_row3)
        .allMatch(row -> !(row.get(2).contains(tableName4)) && !(row.get(2).contains(tableName5)))
        .allMatch(
            row ->
                !(row.get(1).contains(DOT_JOINER.join(tablePath4)))
                    && !(row.get(1).contains(DOT_JOINER.join(tablePath5))));
  }

  @Test
  public void testNestedFolders() throws Exception {
    /*
     * select * from information_schema."SCHEMATA" where schema_name = 'nessie_t.folder1'
     * select * from information_schema."SCHEMATA" where schema_name like 'nessie_t.folder1%'
     */
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Create folders explicitly
    getNessieApi().createNamespace().namespace(tablePath.get(0)).refName("main").create();

    // Value of tablePath(1) which is a folder name will be get created under the root folder (Value
    // of tablePath(0)) in the source
    getNessieApi()
        .createNamespace()
        .namespace(Namespace.of(tablePath.subList(0, 2)))
        .refName("main")
        .create();

    // Value of tablePath(1) which is a folder name will be get created under the source
    // this folder name is created in two places - one within the subfolder and one under the root
    getNessieApi().createNamespace().namespace(tablePath.get(1)).refName("main").create();

    runSQL(createEmptyTableQuery(tablePath));
    String nestedFolderEqual =
        String.format(
            "select * from information_schema.SCHEMATA where schema_name = '%s.%s'",
            DATAPLANE_PLUGIN_NAME, tablePath.get(0));
    String nestedFolderLikeStartWith =
        String.format(
            "select * from information_schema.SCHEMATA where schema_name like '%s.%s%%'",
            DATAPLANE_PLUGIN_NAME, tablePath.get(0));

    List<String> folder1Row =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0)),
            "<owner>",
            "SIMPLE",
            "NO");

    List<String> folder2Row =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1)),
            "<owner>",
            "SIMPLE",
            "NO");

    assertThat(runSqlWithResults(nestedFolderEqual))
        .contains(folder1Row)
        .allMatch(
            row ->
                !row.get(1)
                    .equals(
                        String.format(
                            "%s.%s.%s",
                            DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))));
    assertThat(runSqlWithResults(nestedFolderLikeStartWith))
        .contains(folder1Row)
        .contains(folder2Row);

    // Test the nested folder case where same folder name is created under two different places
    /*
     * For e.g: refer to below hierarchy:
     * <p>
     * - folder
     *    - folder2
     * - folder2
     */
    String sameFolderNameLikeQuery =
        String.format(
            "select * from information_schema.SCHEMATA where schema_name like '%s.%s'",
            DATAPLANE_PLUGIN_NAME, tablePath.get(1));
    List<List<String>> rows = runSqlWithResults(sameFolderNameLikeQuery);
    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows)
        .allMatch(
            row ->
                row.get(1).equals(String.format("%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(1))));

    sameFolderNameLikeQuery =
        String.format(
            "select * from information_schema.SCHEMATA where schema_name like '%s.%s.%s'",
            DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1));
    rows = runSqlWithResults(sameFolderNameLikeQuery);
    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows)
        .allMatch(
            row ->
                row.get(1)
                    .equals(
                        String.format(
                            "%s.%s.%s",
                            DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))));
  }

  @Test
  public void testSelectFromNonVersionedSources() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));
    String selectFromNonVersionedSource =
        "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA = 'cp'";
    String tablePathIncludeSource =
        String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1));

    assertThat(runSqlWithResults(selectFromNonVersionedSource))
        .allMatch(row -> !row.get(1).equals(tablePathIncludeSource));
  }

  @Test
  public void testSelectEscapeQuoteWithDots() throws Exception {
    List<String> schemaPath = new ArrayList<>();
    schemaPath.add(generateUniqueFolderName());
    schemaPath.add(generateUniqueFolderName());
    schemaPath.add("\"dot.dot.dot.dot\"");
    // Create folders explicitly
    getNessieApi().createNamespace().namespace(schemaPath.get(0)).refName("main").create();
    getNessieApi()
        .createNamespace()
        .namespace(Namespace.of(schemaPath.subList(0, 2)))
        .refName("main")
        .create();
    getNessieApi()
        .createNamespace()
        .namespace(Namespace.of(schemaPath.subList(0, 3)))
        .refName("main")
        .create();
    String selectTableWithDots =
        String.format(
            "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s.%s.%s.%s'",
            DATAPLANE_PLUGIN_NAME, schemaPath.get(0), schemaPath.get(1), schemaPath.get(2));
    String tablePathIncludeSource =
        String.format(
            "%s.%s.%s.%s",
            DATAPLANE_PLUGIN_NAME, schemaPath.get(0), schemaPath.get(1), schemaPath.get(2));
    assertThat(runSqlWithResults(selectTableWithDots))
        .allMatch(row -> row.get(1).equals(tablePathIncludeSource));
  }

  @Test
  public void testTableUnderOneFolder() throws Exception {
    final String tableUnderOneFolder = generateUniqueTableName();
    List<String> pathUnderOneFolder = new ArrayList<>();
    pathUnderOneFolder.add(generateUniqueFolderName());
    pathUnderOneFolder.add(tableUnderOneFolder);
    runSQL(createEmptyTableQuery(pathUnderOneFolder));

    final String tableName = generateUniqueTableName();
    List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s", DATAPLANE_PLUGIN_NAME, pathUnderOneFolder.get(0)),
            tableUnderOneFolder,
            "TABLE");

    String selectTableUnderOneFolder =
        String.format(
            "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA like '%s.%s'",
            DATAPLANE_PLUGIN_NAME, pathUnderOneFolder.get(0));
    assertThat(runSqlWithResults(selectTableUnderOneFolder))
        .contains(table_row)
        .allMatch(
            row ->
                !row.get(1)
                    .equals(
                        String.format(
                            "%s.%s.%s",
                            DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))));
  }

  @Test
  public void testTableUnderSource() throws Exception {
    final String tableUnderSource = generateUniqueTableName();
    List<String> pathUnderSource = new ArrayList<>();
    pathUnderSource.add(tableUnderSource);
    runSQL(createEmptyTableQuery(pathUnderSource));

    final String tableName = generateUniqueTableName();
    List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    List<String> table_row =
        Arrays.asList(
            "DREMIO", String.format("%s", DATAPLANE_PLUGIN_NAME), tableUnderSource, "TABLE");

    String selectTableUnderSource =
        String.format(
            "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA like '%s'",
            DATAPLANE_PLUGIN_NAME);
    assertThat(runSqlWithResults(selectTableUnderSource))
        .contains(table_row)
        .allMatch(
            row ->
                !row.get(1)
                    .equals(
                        String.format(
                            "%s.%s.%s",
                            DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1))));
  }

  @Test
  public void testNamespaceLinksToCorrectNamespace() throws Exception {
    // create folder A
    List<String> folderPath1 = Arrays.asList(DATAPLANE_PLUGIN_NAME, folderA);
    runSqlWithResults(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath1));

    // create folder B
    List<String> folderPath2 = Arrays.asList(DATAPLANE_PLUGIN_NAME, folderA, folderB);
    runSqlWithResults(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath2));

    // create table A
    List<String> tablePath = Arrays.asList(folderA, folderB, tableA);
    runSqlWithResults(createTableAsQuery(tablePath, 10));

    // create folder B directly under folder A
    List<String> folderPath3 = Arrays.asList(DATAPLANE_PLUGIN_NAME, folderB);
    runSqlWithResults(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath3));

    List<String> expectedTableRow =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, folderA, folderB),
            tableA,
            "TABLE");

    List<String> expectedSchemataRow =
        Arrays.asList(
            "DREMIO",
            String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, folderA, folderB),
            "<owner>",
            "SIMPLE",
            "NO");

    String selectInfoSchemaTables = "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\"";
    assertThat(runSqlWithResults(selectInfoSchemaTables))
        .contains(expectedTableRow)
        .allMatch(
            row ->
                !row.get(1)
                    .equals(String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, folderB, folderB)))
        .allMatch(
            row ->
                !row.get(1)
                    .equals(String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, folderA, folderA)))
        .allMatch(
            row ->
                !row.get(1)
                    .equals(String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, folderB, folderA)))
        .allMatch(row -> !row.get(1).equals(String.format("%s.%s", DATAPLANE_PLUGIN_NAME, folderA)))
        .allMatch(row -> !row.get(1).equals(String.format("%s.%s", DATAPLANE_PLUGIN_NAME, folderB)))
        .allMatch(row -> !row.get(1).equals(DATAPLANE_PLUGIN_NAME));

    String selectInfoSchemaSchemata = "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA";
    assertThat(runSqlWithResults(selectInfoSchemaSchemata))
        .contains(expectedSchemataRow)
        .allMatch(
            row ->
                !row.get(1)
                    .equals(String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, folderB, folderB)))
        .allMatch(
            row ->
                !row.get(1)
                    .equals(String.format("%s.%s.%s", DATAPLANE_PLUGIN_NAME, folderA, folderA)));
  }
}

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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_COUNT_COLUMN;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTableAddColumnsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTableDropColumnQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterViewPropertyQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createReplaceViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableWithColDefsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewQueryWithEmptySql;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewQueryWithIncompleteSql;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewSelectQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateSchemaPath;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertSelectQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinTablesQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinTpcdsTablesQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.quoted;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectCountQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.updateViewSelectQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useContextQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieDoesNotHaveView;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasTable;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasView;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.util.TimestampString;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Test;

import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.TableVersionType;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.UserExceptionAssert;

/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedViewTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */

public class ViewTestCases {
  private ITDataplanePluginTestSetup base;

  ViewTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  @Test
  public void createView() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath));

    // Assert
    assertNessieHasView(viewKey, DEFAULT_BRANCH_NAME, base);
  }

  @Test
  public void createViewTwice() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath));

    // Assert
    assertThatThrownBy(() -> base.runSQL(createViewQuery(viewKey, tablePath)))
      .hasMessageContaining("already exists");
  }

  @Test
  public void createViewOnNonExistentTable() {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    assertThatThrownBy(() -> base.runSQL(createViewQuery(viewKey, tablePath)))
      .hasMessageContaining("Object '" + tablePath.get(0) + "' not found within");
  }

  @Test
  public void createViewWithIncompleteSql() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> viewKey = tablePathWithFolders(tableName);
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    assertThatThrownBy(() -> base.runSQL(createViewQueryWithIncompleteSql(viewKey, tablePath)))
      .hasMessageContaining("PARSE ERROR:");
  }

  @Test
  public void dropView() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath));
    assertNessieHasView(viewKey, DEFAULT_BRANCH_NAME, base);
    base.runSQL(dropViewQuery(viewKey));

    // Assert
    assertNessieDoesNotHaveView(viewKey, DEFAULT_BRANCH_NAME, base);
  }

  @Test
  public void dropViewTwice() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath));
    assertNessieHasView(viewKey, DEFAULT_BRANCH_NAME, base);
    base.runSQL(dropViewQuery(viewKey));
    assertNessieDoesNotHaveView(viewKey, DEFAULT_BRANCH_NAME, base);

    // Assert
    assertThatThrownBy(() -> base.runSQL(dropViewQuery(viewKey)))
      .hasMessageContaining("Unknown view");
  }

  @Test
  public void dropViewNonExist() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    // Assert
    assertThatThrownBy(() -> base.runSQL(dropViewQuery(viewKey)))
      .hasMessageContaining("Unknown view");
  }

  @Test
  public void dropViewAsTable() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath));
    assertNessieHasView(viewKey, DEFAULT_BRANCH_NAME, base);

    // Assert
    assertThatThrownBy(() -> base.runSQL(dropTableQuery(viewKey)))
      .hasMessageContaining("is not a TABLE");
  }

  @Test
  public void createViewWithTagSet() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final String tableName = generateUniqueTableName();
    String firstTag = generateUniqueTagName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    base.runSQL(insertSelectQuery(tablePath, 5));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 10);
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "BRANCH " + DEFAULT_BRANCH_NAME), DEFAULT_COUNT_COLUMN, 10);
    // Create tag
    base.runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    //Insert 5 more rows into the table
    base.runSQL(insertSelectQuery(tablePath, 5));
    //AT query
    String selectATQuery = String.format("select * from %s.%s AT TAG %s ",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      firstTag);

    //Act
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, selectATQuery));

    //Assert
    base.assertViewHasExpectedNumRows(viewPath, 10);
    base.assertTableHasExpectedNumRows(tablePath, 15);


  }

  @Test
  public void createViewClashWithTable() throws Exception {
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Create table with 10 rows
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 10));

    //Act and Assert
    assertThatThrownBy(() -> base.runSQL(createViewQuery(tablePath, tablePath)))
      .hasMessageContaining("A non-view table with given name ")
      .hasMessageContaining("already exists in schema");
  }

  //View will be created with fully qualified name represented by viewCreationPath
  //Table tableName1 will be resolved to the current schema path (workspaceSchemaPath) set in the context
  //viewCreationPath != workspaceSchemaPath
  @Test
  public void createViewWithTableOnDifferentPathContext() throws Exception {
    List<String> workspaceSchemaPath = generateSchemaPath();
    //current schema context
    useContextQuery(workspaceSchemaPath);
    // Create table1 with 10 rows
    String tableName1 = generateUniqueTableName();
    base.runSQL(createTableAsQuery(Collections.singletonList(tableName1), 10));
    final String viewName = generateUniqueViewName();
    List<String> viewCreationPath = tablePathWithFolders(viewName);
    createFolders(viewCreationPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewCreationPath, Collections.singletonList(tableName1)));
    assertThat(viewCreationPath != workspaceSchemaPath).isTrue();
    base.assertViewHasExpectedNumRows(viewCreationPath, 10);
  }

  @Test
  public void updateView() throws Exception {
    //Arrange
    String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    // Create table1 with 10 rows
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath1, 10));
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath1));
    base.assertViewHasExpectedNumRows(viewKey, 10);
    long mtime1 = base.getMtimeForTable(viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    //Create table2 with 20 rows.
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath2, 20));

    //Act
    base.runSQL(createReplaceViewQuery(viewKey, tablePath2));
    long mtime2 = base.getMtimeForTable(viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    //Assert
    base.assertViewHasExpectedNumRows(viewKey, 20);
    AssertionsForClassTypes.assertThat(mtime2 > mtime1).isTrue();
  }

  @Test
  public void alterViewProperty() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);

    // Create a table with 10 rows.
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath1, 10));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view.
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath1));

    final String attribute = "enable_default_reflection";
    final String value = "true";
    final List<String> expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options updated", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    assertThat(base.runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);
  }

  @Test
  public void alterViewPropertyTwice() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);

    // Create a table with 10 rows.
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath1, 10));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view.
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath1));

    final String attribute = "enable_default_reflection";
    final String value = "true";
    List<String> expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options updated", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    assertThat(base.runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);

    expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options did not change",
                DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));
    assertThat(base.runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);
  }

  @Test
  public void alterViewPropertyWithDifferentValue() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);

    // Create a table with 10 rows.
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath1, 10));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view.
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath1));

    final String attribute = "enable_default_reflection";
    String value = "true";
    List<String> expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options updated", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    assertThat(base.runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);

    value = "false";
    assertThat(base.runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);
  }

  @Test
  public void updateViewKeepProperties() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    // Create a table with 10 rows.
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath1, 10));

    // Create a table with 20 rows.
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath2, 20));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view.
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath1));

    final String attribute = "enable_default_reflection";
    final String value = "true";

    List<String> expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options updated", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    assertThat(base.runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);

    base.runSQL(createReplaceViewQuery(viewKey, tablePath2));

    expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options did not change",
                DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    assertThat(base.runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);
  }

  @Test
  public void createViewWithNoSql() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    List<String> viewKey = tablePathWithFolders(tableName);

    UserExceptionAssert
      .assertThatThrownBy(() -> base.runSQL(createViewQueryWithEmptySql(viewKey, tablePath)))
      .hasErrorType(UserBitShared.DremioPBError.ErrorType.PARSE);
  }

  @Test
  public void selectFromView() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    base.runSQL(insertTableQuery(tablePath));
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewPath, tablePath));


    //Act and Assert
    base.assertTableHasExpectedNumRows(tablePath, 3);
    base.assertViewHasExpectedNumRows(viewPath, 3);
    //cleanup

    base.runSQL(dropViewQuery(viewPath));
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void selectFromViewWithJoin() throws Exception {
    // Arrange
    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, joinTpcdsTablesQuery()));

    //Act and Assert
    base.assertViewHasExpectedNumRows(viewPath, 22500000);
    //cleanup
    base.runSQL(dropViewQuery(viewPath));

  }

  @Test
  public void selectFromViewDifferentTags() throws Exception {
    // Arrange
    String firstTag = generateUniqueTagName();
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Create table1 on default branch with 10 rows
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 10));
    base.assertTableHasExpectedNumRows(tablePath, 10);

    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewPath, tablePath));
    // Create a tag to mark it
    base.runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));
    // Insert 10 more rows
    base.runSQL(insertSelectQuery(tablePath, 10));
    base.assertViewHasExpectedNumRows(viewPath, 20);

    // Act
    // Go back to tag1
    base.runSQL(useTagQuery(firstTag));

    // Assert
    // Select from view should return 10
    base.assertViewHasExpectedNumRows(tablePath, 10);

    //cleanup
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropViewQuery(viewPath));
    base.runSQL(dropTableQuery(tablePath));
  }

  // This tests a view with column specified in select list
  // This will not pick up underlying table schema changes for columns that are not in its select list
  @Test
  public void selectFromViewOnDiffBranchesWithAddColumn() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Arrays.asList("col2 int", "col3 int", "col4 varchar");
    final List<String> columnValuesBeforeAdd = Arrays.asList("(1)", "(2)");
    final List<String> columnValuesAfterAdd = Arrays.asList("(3,3,3,'three')", "(4,4,4,'four')");

    //Setup
    // Set context to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    //Insert  2 rows into table
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    String sqlQuery = String.format("select col1 from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, sqlQuery));
    final String devBranchName = generateUniqueBranchName();
    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));


    // Act
    base.runSQL(useBranchQuery(devBranchName));
    //Alter underlying table
    base.runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));

    //Assert
    //Execute view in context of main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    //Select the  rows from view


    base.testBuilder()
      .sqlQuery(selectStarQuery(viewPath))
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(1)
      .baselineValues(2)
      .go();

    //Execute view in context of dev
    base.runSQL(useBranchQuery(devBranchName));
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col2", "col3", "col4")
      .baselineValues(1, null, null, null)
      .baselineValues(2, null, null, null)
      .baselineValues(3, 3, 3, "three")
      .baselineValues(4, 4, 4, "four")
      .go();
    //Select the new rows from new columns
    base.testBuilder()
      .sqlQuery(selectStarQuery(viewPath))
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(1)
      .baselineValues(2)
      .baselineValues(3)
      .baselineValues(4)
      .go();

    //cleanup
    base.runSQL(dropViewQuery(viewPath));
    base.runSQL(dropTableQuery(tablePath));
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(tablePath));


  }

  // This tests a view with a '*' specification -  no columns specified in select list
  // This should pick up the underlying table schema changes
  @Test
  public void selectFromStarViewOnDiffBranchesWithAddColumn() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Arrays.asList("col2 int", "col3 int", "col4 varchar");
    final List<String> columnValuesBeforeAdd = Arrays.asList("(1)", "(2)");
    final List<String> columnValuesAfterAdd = Arrays.asList("(3,3,3,'three')", "(4,4,4,'four')");

    // Set context to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    //Insert  2 rows into table
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    String sqlQuery = String.format("select * from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, sqlQuery));
    final String devBranchName = generateUniqueBranchName();
    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));


    // Act
    base.runSQL(useBranchQuery(devBranchName));
    //Alter underlying table
    base.runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));

    //Assert

    //Execute view in context of main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    //Select the  rows from view

    base.testBuilder()
      .sqlQuery(selectStarQuery(viewPath))
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(1)
      .baselineValues(2)
      .go();

    //Execute select from table in context of dev to reflect the added  columns
    base.runSQL(useBranchQuery(devBranchName));
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col2", "col3", "col4")
      .baselineValues(1, null, null, null)
      .baselineValues(2, null, null, null)
      .baselineValues(3, 3, 3, "three")
      .baselineValues(4, 4, 4, "four")
      .go();
    // Execute view in context of dev
    // First attempt  should show the exception raised (retryable)
    base.assertQueryThrowsExpectedError(
      selectStarQuery(viewPath),
      "SCHEMA_CHANGE ERROR: Some virtual datasets were out of date and have been corrected");

    //Second attempt shoild show updates to get all the undelrying table changes
    base.testBuilder()
      .sqlQuery(selectStarQuery(viewPath))
      .unOrdered()
      .baselineColumns("col1", "col2", "col3", "col4")
      .baselineValues(1, null, null, null)
      .baselineValues(2, null, null, null)
      .baselineValues(3, 3, 3, "three")
      .baselineValues(4, 4, 4, "four")
      .go();

    //cleanup TODO : Cleanup view after dropView support
    base.runSQL(dropTableQuery(tablePath));
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(tablePath));


  }

  // Negative tests a view with a '*' specification . Select with a tag will fail auto fixup
  @Test
  public void selectFromStarViewWithTagWithAddColumn() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    String tagName = generateUniqueTagName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Arrays.asList("col2 int", "col3 int", "col4 varchar");
    final List<String> columnValuesBeforeAdd = Arrays.asList("(1)", "(2)");
    final List<String> columnValuesAfterAdd = Arrays.asList("(3,3,3,'three')", "(4,4,4,'four')");

    // Set context to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    //Insert  2 rows into table
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    String sqlQuery = String.format("select * from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, sqlQuery));
    final String devBranchName = generateUniqueBranchName();
    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));


    // Act
    base.runSQL(useBranchQuery(devBranchName));
    //Alter underlying table
    base.runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));

    //Create a tag
    base.runSQL(createTagQuery(tagName, devBranchName));

    //Assert
    //Execute view in context of tag
    base.runSQL(useTagQuery(tagName));
    //Execute select from table in context of dev to reflect the added  columns
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col2", "col3", "col4")
      .baselineValues(1, null, null, null)
      .baselineValues(2, null, null, null)
      .baselineValues(3, 3, 3, "three")
      .baselineValues(4, 4, 4, "four")
      .go();

    //Execute view in context of dev - should fail - cannot fixup

    base.assertQueryThrowsExpectedError(
      selectStarQuery(viewPath),
      "VALIDATION ERROR: Some virtual datasets are out of date and need to be manually updated.");

    //cleanup
    base.runSQL(useBranchQuery(devBranchName));
    base.runSQL(dropViewQuery(viewPath));
    base.runSQL(dropTableQuery(tablePath));
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(tablePath));


  }

  @Test
  public void selectFromViewOnDiffBranchesWithDropColumn() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Arrays.asList("col1 int", "col2  varchar");
    final List<String> dropCols = Arrays.asList("col2");
    final List<String> columnValuesBeforeAdd = Arrays.asList("(1,'one')", "(2,'two')");
    final List<String> columnValuesAfterDrop = Arrays.asList("(3)", "(4)");

    //Setup
    // Set context to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    //Insert  2 rows into table
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    String sqlQuery = String.format("select col2 from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, sqlQuery));
    final String devBranchName = generateUniqueBranchName();
    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));


    // Act
    base.runSQL(useBranchQuery(devBranchName));
    //Alter underlying table
    base.runSQL(alterTableDropColumnQuery(tablePath, dropCols));
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterDrop));

    //Assert
    //Execute view in context of main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    //Select the  rows from view


    base.testBuilder()
      .sqlQuery(selectStarQuery(viewPath))
      .unOrdered()
      .baselineColumns("col2")
      .baselineValues("one")
      .baselineValues("two")
      .go();

    //Select  in context of dev to show one column
    base.runSQL(useBranchQuery(devBranchName));
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(1)
      .baselineValues(2)
      .baselineValues(3)
      .baselineValues(4)
      .go();
    //Execute view in context of dev - should error out
    base.assertQueryThrowsExpectedError(
      selectStarQuery(viewPath),
      "Error while expanding view "
        + String.format("%s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewPath))
        + ". Column 'col2' not found in any table. Verify the view’s SQL definition.");

    //cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void selectFromViewWithStarQueryAndDropUnderlyingColumn() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Arrays.asList("col1 int", "col2  varchar");
    final List<String> dropCols = Arrays.asList("col2");
    final List<String> columnValuesBeforeAdd = Arrays.asList("(1,'one')", "(2,'two')");
    final List<String> columnValuesAfterDrop = Arrays.asList("(3)", "(4)");

    //Setup
    // Set context to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    //Insert  2 rows into table
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    String sqlQuery = String.format("select * from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, sqlQuery));
    final String devBranchName = generateUniqueBranchName();
    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));


    // Act
    base.runSQL(useBranchQuery(devBranchName));
    //Alter underlying table
    base.runSQL(alterTableDropColumnQuery(tablePath, dropCols));
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterDrop));

    //Assert
    //Execute view in context of main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    //Select the  rows from view


    base.testBuilder()
      .sqlQuery(selectStarQuery(viewPath))
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(1, "one")
      .baselineValues(2, "two")
      .go();

    //Select  in context of dev to show one column
    base.runSQL(useBranchQuery(devBranchName));
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(1)
      .baselineValues(2)
      .baselineValues(3)
      .baselineValues(4)
      .go();
    //Execute view in context of dev - should work since it's a star query

    //First attempt should show the exception raised (retryable)
    base.assertQueryThrowsExpectedError(
      selectStarQuery(viewPath),
      "SCHEMA_CHANGE ERROR: Some virtual datasets were out of date and have been corrected");
    //Second attempt should show results
    base.testBuilder()
      .sqlQuery(selectStarQuery(viewPath))
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(1)
      .baselineValues(2)
      .baselineValues(3)
      .baselineValues(4)
      .go();

    //cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void selectFromViewVirtualInt() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    //Act
    String viewSQL = String.format("select 1, 2, 3 ");
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, viewSQL));

    //Assert
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      1);
  }

  @Test
  public void selectFromViewVirtualVarcharWithCast() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    String viewSQL = String.format("select CAST('abc' AS VARCHAR(65536)) as varcharcol");
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, viewSQL));

    // Assert
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      1);
  }

  @Test
  public void selectFromViewVirtualVarchar() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    String viewSQL = String.format("select 0 , 1  , 2 , 'abc' ");
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, viewSQL));

    // Assert
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      1);
  }

  @Test
  public void selectFromViewConcat() throws Exception {
    // Arrange
    final String viewName1 = generateUniqueTableName();
    final String tableName = generateUniqueTableName();
    final List<String> viewPath1 = tablePathWithFolders(viewName1);
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));

    String viewSQL1 = String.format("select CONCAT(name, ' of view ') from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    createFolders(viewPath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath1, viewSQL1));

    // Select
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(viewPath1, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      3);
  }

  @Test
  public void selectFromViewCaseInt() throws Exception {
    // Arrange
    final String viewName1 = generateUniqueTableName();
    final String tableName = generateUniqueTableName();
    final List<String> viewPath1 = tablePathWithFolders(viewName1);
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));
    base.runSQL(String.format("insert into %s.%s values (-1, 'invalid id', 10.0)",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)));

    String viewSQL1 = String.format("select case when id > 0 THEN 1 ELSE 0 END AS C5  from %s.%s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath));
    // Act
    createFolders(viewPath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath1, viewSQL1));

    // Assert
    base.assertViewHasExpectedNumRows(viewPath1, 4);
    String viewWhereQuery = String.format("select count(*) as c1 from %s.%s where C5 = 1", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewPath1));
    base.assertSQLReturnsExpectedNumRows(viewWhereQuery, "c1", 3);

  }

  @Test
  public void selectFromViewCaseVarchar() throws Exception {
    // Arrange
    final String viewName1 = generateUniqueTableName();
    final String tableName = generateUniqueTableName();
    final List<String> viewPath1 = tablePathWithFolders(viewName1);
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));
    base.runSQL(String.format("insert into %s.%s values (-1, 'invalid id', 10.0)",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)));

    String viewSQL1 = String.format("select case when id > 0 THEN 'positive' ELSE 'invalid' END AS C5  from %s.%s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath));

    createFolders(viewPath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath1, viewSQL1));

    // Select
    base.assertViewHasExpectedNumRows(viewPath1, 4);
    String viewWhereQuery = String.format("select count(*) as c1 from %s.%s where C5 = 'invalid'", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewPath1));
    base.assertSQLReturnsExpectedNumRows(viewWhereQuery, "c1", 1);

  }

  @Test
  public void selectViewWithSpecifierTag() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final String tableName = generateUniqueTableName();
    String firstTag = generateUniqueTagName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    base.runSQL(insertSelectQuery(tablePath, 5));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 10);
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "BRANCH " + DEFAULT_BRANCH_NAME), DEFAULT_COUNT_COLUMN, 10);
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewPath, tablePath));
    // Create tag
    base.runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    //Insert 10 more rows into table
    base.runSQL(insertSelectQuery(tablePath, 10));
    //Verify view can see the new rows.
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      20);

    //Act and Assert
    //Now run the query with AT syntax on tag to verify only 5 rows are returned.
    base.assertSQLReturnsExpectedNumRows(
      selectCountQueryWithSpecifier(viewPath,
        DEFAULT_COUNT_COLUMN,
        "TAG " + firstTag),
      DEFAULT_COUNT_COLUMN,
      10);
  }

  @Test
  public void selectViewWithSpecifierCommitAndBranch() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();

    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    base.runSQL(insertSelectQuery(tablePath, 5));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 10);
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "BRANCH " + DEFAULT_BRANCH_NAME), DEFAULT_COUNT_COLUMN, 10);
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewPath, tablePath));

    // Create dev branch
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(devBranch));
    String commitHashBranchBeforeInsert = base.getCommitHashForBranch(devBranch);
    //Insert 10 more rows into table
    base.runSQL(insertSelectQuery(tablePath, 10));
    String commitHashBranchAfterInsert = base.getCommitHashForBranch(devBranch);

    //Act and Assert

    //Verify view can see the new rows at each commit
    base.assertSQLReturnsExpectedNumRows(
      selectCountQueryWithSpecifier(viewPath, DEFAULT_COUNT_COLUMN,
        "COMMIT " + quoted(commitHashBranchBeforeInsert)),
      DEFAULT_COUNT_COLUMN,
      10);

    base.assertSQLReturnsExpectedNumRows(
      selectCountQueryWithSpecifier(viewPath, DEFAULT_COUNT_COLUMN,
        "COMMIT " + quoted(commitHashBranchAfterInsert)),
      DEFAULT_COUNT_COLUMN,
      20);

  }

  @Test
  public void selectViewWithSpecifierAndJoin() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final String tableName = generateUniqueTableName();
    final String tableName2 = generateUniqueTableName();

    String firstTag = generateUniqueTagName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath2, 10));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 5);
    base.assertTableHasExpectedNumRows(tablePath2, 10);


    String table1 = joinedTableKey(tablePath);
    String table2 = joinedTableKey(tablePath2);
    String condition = " TRUE ";
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, joinTablesQuery(table1, table2, condition)));

    // Create tag
    base.runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    //Verify view can see the new rows.
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      50);

    base.runSQL(insertSelectQuery(tablePath, 10));

    //Verify view can see the new rows.
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      150);

    //Act and Assert
    //Now run the query with AT syntax on tag to verify only 50 rows are returned.
    base.assertSQLReturnsExpectedNumRows(
      selectCountQueryWithSpecifier(viewPath,
        DEFAULT_COUNT_COLUMN,
        "TAG " + firstTag),
      DEFAULT_COUNT_COLUMN,
      50);

  }

  @Test
  public void selectFromViewOnViewWithAt() throws Exception {
    // Arrange
    /*
      view2
           ---> view1
                    --->table1
      -Test query with view2 AT TAG < tag>
      -Both view1 and table 1 should resolve with version <tag>

     */
    final String viewName1 = generateUniqueViewName();
    final String viewName2 = generateUniqueViewName();
    final String tableName = generateUniqueTableName();
    String firstTag = generateUniqueTagName();
    final List<String> viewPath1 = tablePathWithFolders(viewName1);
    final List<String> viewPath2 = tablePathWithFolders(viewName2);
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));

    //Act
    String viewSQL1 = String.format("select id+10 AS idv1, id+20 AS idv2 from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    createFolders(viewPath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath1, viewSQL1));

    String viewSQL2 = String.format("select idv1/10 AS idv10, idv2/10 AS idv20 from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewPath1));
    createFolders(viewPath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath2, viewSQL2));
    base.runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));
    // Insert an extra row in table
    base.runSQL(String.format("insert into %s.%s values (4, 'fourth row', 40.0)",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)));

    // Modify the first view (after tag) - view1 contains 1 column.
    String viewSQL11 = String.format("select id+10 AS idv1 from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    base.runSQL(updateViewSelectQuery(viewPath1, viewSQL11));

    // Assert
    //Ensure that the select from view2 are the previous tag is able to expand and see 2 columns and 3 rows
    base.testBuilder()
      .sqlQuery(selectStarQueryWithSpecifier(viewPath2, "TAG " + firstTag))
      .unOrdered()
      .baselineColumns("idv10", "idv20")
      .baselineValues(1, 2)
      .baselineValues(1, 2)
      .baselineValues(1, 2)
      .go();

    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(viewPath2, "c1", "TAG " + firstTag), "c1", 3);

  }

  @Test
  public void selectFromViewFromSessionVersionContext() throws Exception {
    // Arrange
     /*
      view - only exists in branch2
           ---> table with AT < branch 1> (table only exists in branch 1)

      - Lookup of view2 should succeed only in context of branch2
      - expansion of view should pick up table1 only at branch1

     */
    // Arrange
    final String viewName = generateUniqueTableName();
    final String tableName = generateUniqueTableName();

    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();
    // Create dev branch
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    //create table in dev
    base.runSQL(useBranchQuery(devBranch));
    createFolders(tablePath, VersionContext.ofBranch(devBranch));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));

    //create view in main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    String viewSQL = String.format("select *  from %s.%s  AT BRANCH %s" ,
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      devBranch);
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, viewSQL));

    //Act and Assert

    //In context of main, select from query. Underlying table should be resolved to dev.(does not exist in main)
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    //Verify view can see the new rows.
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      3);

  }


  @Test
  public void selectFromNestedViewsWithAT() throws Exception {
    // Arrange
     /*
      view3 - only exists in branch main
           ---> view2 with AT < branch dev> (view2 only exists in dev2)
                    ---> view1 (only exists in dev1)
                            ---> table1 (only exists in dev0)
      - Test query with view3 with branch set to main
      - Lookup of view2 should resolve to dev
      - Lookup of view1 and table1 should resolve with dev.
     */
    // Arrange
    final String view1 = generateUniqueTableName();
    final String view2 = generateUniqueTableName();
    final String view3 = generateUniqueTableName();
    final String tableName = generateUniqueTableName();

    final List<String> view1Path = tablePathWithFolders(view1);
    final List<String> view2Path = tablePathWithFolders(view2);
    final List<String> view3Path = tablePathWithFolders(view3);
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String dev0 = generateUniqueBranchName();
    final String dev1 = generateUniqueBranchName();
    final String dev2 = generateUniqueBranchName();

    // Create dev
    base.runSQL(createBranchAtBranchQuery(dev0, DEFAULT_BRANCH_NAME));
    base.runSQL(createBranchAtBranchQuery(dev1, DEFAULT_BRANCH_NAME));
    base.runSQL(createBranchAtBranchQuery(dev2, DEFAULT_BRANCH_NAME));

    //create table in dev0
    base.runSQL(useBranchQuery(dev0));
    createFolders(tablePath, VersionContext.ofBranch(dev0));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));

    //create view1 in dev1
    base.runSQL(useBranchQuery(dev1));
    String view1SQL = String.format("select *  from %s.%s  AT BRANCH %s" ,
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      dev0
     );
    createFolders(view1Path, VersionContext.ofBranch(dev1));
    base.runSQL(createViewSelectQuery(view1Path, view1SQL));

    //create view2 in dev2
    base.runSQL(useBranchQuery(dev2));
    String view2SQL = String.format("select *  from %s.%s  AT BRANCH %s" ,
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(view1Path),
      dev1
    );
    createFolders(view2Path, VersionContext.ofBranch(dev2));
    base.runSQL(createViewSelectQuery(view2Path, view2SQL));

    //create view3 in main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    String view3SQL = String.format("select *  from %s.%s  AT BRANCH %s" ,
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(view2Path),
      dev2
    );
    createFolders(view3Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(view3Path, view3SQL));
    //Act and Assert

    //In context of main, select from query. Underlying table should be resolved to dev.(does not exist in main)
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    //Verify view can see the new rows.
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(view3Path, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      3);

  }


  @Test
  void selectFromNestedViewInnerTaggedVersion() throws Exception{
     /*
      view2 - Defined with AT TAG  atag on V1
           ---> view1 - Defined on table1

      - Test query with view2 accessed with AT BRANCH main.
      - Lookup of view1 should  still resolve to TAG atag
     */
    // Arrange
    final String view1 = generateUniqueTableName();
    final String view2 = generateUniqueTableName();
    final String tableName = generateUniqueTableName();
    final String atag = generateUniqueTagName();

    final List<String> view1Path = tablePathWithFolders(view1);
    final List<String> view2Path = tablePathWithFolders(view2);
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    base.runSQL(insertSelectQuery(tablePath, 5));
    String view1SQL = String.format("select *  from %s.%s" ,
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)
    );
    createFolders(view1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(view1Path, view1SQL));
    //Verify view can see the new rows.
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(view1Path, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      10);
    base.runSQL(createTagQuery(atag, DEFAULT_BRANCH_NAME));
    String view2SQL = String.format("select *  from %s.%s  AT TAG %s" ,
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(view1Path),
      atag
    );
    createFolders(view2Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(view2Path, view2SQL));
    base.runSQL(insertSelectQuery(tablePath, 5));

    //Verify view can see the new rows.
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(view2Path, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      10);
  }

  //Negative test
  @Test
  public void createViewWithTimeTravelQuery() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 5);
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "BRANCH " + DEFAULT_BRANCH_NAME), DEFAULT_COUNT_COLUMN, 5);

    final TimestampString ts1 = TimestampString.fromMillisSinceEpoch(System.currentTimeMillis());

    // Insert rows
    base.runSQL(insertSelectQuery(tablePath, 2));
    // Verify number of rows.
    // on main branch, at this timestamp
    base.assertTableHasExpectedNumRows(tablePath, 7 );
    // on main branch, at this timestamp
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "TIMESTAMP '" + ts1 + "'"), DEFAULT_COUNT_COLUMN, 5);

    //AT query
    String selectATQuery = String.format("select * from %s.%s AT TIMESTAMP '%s' ",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      ts1);

    //Act and Assert
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    assertThatThrownBy(() -> base.runSQL(createViewSelectQuery(viewPath, selectATQuery)))
      .hasMessageContaining("Versioned views not supported for time travel queries");

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  //Negative test
  @Test
  public void selectFromArcticViewWithTimeTravel() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    String countCol = "countCol";

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 5);
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "BRANCH " + DEFAULT_BRANCH_NAME), DEFAULT_COUNT_COLUMN, 5);
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewPath, tablePath));

    final TimestampString ts1 = TimestampString.fromMillisSinceEpoch(System.currentTimeMillis());

    // Insert rows
    base.runSQL(insertSelectQuery(tablePath, 2));
    // Verify number of rows.
    // on main branch, at this timestamp
    base.assertTableHasExpectedNumRows(tablePath, 7 );
    // on main branch, at this timestamp
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "TIMESTAMP '" + ts1 + "'"), DEFAULT_COUNT_COLUMN, 5);

    //Act and Assert
    assertThatThrownBy(() -> base.runSQL(selectCountQueryWithSpecifier(viewPath, countCol, "TIMESTAMP '" + ts1 + "'")))
      .hasMessageContaining("Time travel is not supported on views");

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  //Negative test
  @Test
  public void createViewWithSnapshotQuery() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 5);
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "BRANCH " + DEFAULT_BRANCH_NAME), DEFAULT_COUNT_COLUMN, 5);

    final long snapshotId = 1000;

    // Insert rows
    base.runSQL(insertSelectQuery(tablePath, 2));
    // Verify number of rows.
    // on main branch, at this timestamp
    base.assertTableHasExpectedNumRows(tablePath, 7 );

    //AT query
    String selectATQuery = String.format("select * from %s.%s AT SNAPSHOT '%d' ",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      snapshotId);

    //Act and Assert
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    assertThatThrownBy(() -> base.runSQL(createViewSelectQuery(viewPath, selectATQuery)))
      .hasMessageContaining("Versioned views not supported for time travel queries");

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  /**
   * Verify CAST on Calcite schema with INTEGER NOT NULL to Iceberg View schema with nullable INTEGER
   * @throws Exception
   */
  @Test
  public void selectStarFromViewVirtualInt() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    //Act
    String viewSQL = String.format("select 1 as col1");
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, viewSQL));

    //Assert
    base.testBuilder()
      .sqlQuery(selectStarQuery(viewPath))
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(1)
      .go();
  }

  /**
   * Verify CAST on Calcite schema with VARCHAR(3) NOT NULL to Iceberg View schema with nullable VARCHAR(65536)
   * @throws Exception
   */
  @Test
  public void selectStarFromViewVirtualVarchar() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    //Act
    String viewSQL = String.format("select 'xyz' as col1");
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewSelectQuery(viewPath, viewSQL));

    //Assert
    base.testBuilder()
      .sqlQuery(selectStarQuery(viewPath))
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues("xyz")
      .go();
  }

  /**
   * Verify view property can be set and retrieved correctly.
   * @throws Exception
   */
  @Test
  public void setViewProperty() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath));

    // Assert
    assertNessieHasView(viewKey, DEFAULT_BRANCH_NAME, base);

    // Disable the default reflection
    base.runSQL(alterViewPropertyQuery(viewKey, "enable_default_reflection", "False"));

    final String versionedDatasetId =
        base.getVersionedDatatsetId(
            viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    final DremioTable dremioTable = base.getTableFromId(versionedDatasetId, base);

    // Assert
    assertThat(dremioTable.getDatasetConfig().getVirtualDataset().getDefaultReflectionEnabled())
        .isFalse();

    // Enable the default reflection
    base.runSQL(alterViewPropertyQuery(viewKey, "enable_default_reflection", "True"));

    final String newVersionedDatasetId =
        base.getVersionedDatatsetId(
            viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    final DremioTable newDremioTable = base.getTableFromId(newVersionedDatasetId, base);

    // Assert
    assertThat(newDremioTable.getDatasetConfig().getVirtualDataset().getDefaultReflectionEnabled())
        .isTrue();
  }

  @Test
  public void createViewWithImplicitFolders() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    // Act
    base.runSQL(createEmptyTableQuery(tablePath));

    // Assert
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);

    // Arrange
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act + Assert
    base.assertQueryThrowsExpectedError(createViewQuery(viewKey, tablePath),
      String.format("VALIDATION ERROR: Namespace '%s' must exist.",
        String.join(".", viewKey.subList(0, viewKey.size()-1))));
  }

  @Test
  public void createViewInNonBranchVersionContext() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final String tag = generateUniqueTagName();
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    base.runSQL(useTagQuery(tag));

    // Act and Assert
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.assertQueryThrowsExpectedError(createViewQuery( viewPath, tablePath),
            String.format("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
                    tag));
  }

  @Test
  public void updateViewInNonBranchVersionContext() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final String tag = generateUniqueTagName();
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewPath, tablePath));
    base.runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    base.runSQL(useTagQuery(tag));

    // Act and Assert
    String viewSQLupdate = String.format("select id+10 AS idv1 from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    base.assertQueryThrowsExpectedError(updateViewSelectQuery( viewPath, viewSQLupdate),
            String.format("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
                    tag));
  }

}

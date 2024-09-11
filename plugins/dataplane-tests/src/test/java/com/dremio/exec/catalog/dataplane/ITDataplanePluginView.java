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

import static com.dremio.exec.catalog.CatalogOptions.SUPPORT_V1_ICEBERG_VIEWS;
import static com.dremio.exec.catalog.CatalogOptions.V0_ICEBERG_VIEW_WRITES;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_COUNT_COLUMN;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableAddColumnsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableDropColumnQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterViewPropertyQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterViewPropertyQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createFolderQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createReplaceViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableWithColDefsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQueryWithEmptySql;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQueryWithIncompleteSql;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewSelectQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropViewQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateSchemaPath;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFunctionName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertSelectQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinTablesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinTpcdsTablesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.quoted;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountDataFilesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.showObjectWithSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.updateViewSelectQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useContextQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveEntity;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasTable;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasView;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.IcebergViewMetadata;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadata;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadataParser;
import com.dremio.test.UserExceptionAssert;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.util.TimestampString;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginView extends ITDataplanePluginTestSetup {

  @AfterEach
  void afterEach() {
    // Reset support keys
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "false");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "true");
  }

  @Test
  public void createView() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    runSQL(createViewQuery(viewKey, tablePath));

    // Assert
    assertNessieHasView(viewKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createViewTwice() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    runSQL(createViewQuery(viewKey, tablePath));

    // Assert
    assertThatThrownBy(() -> runSQL(createViewQuery(viewKey, tablePath)))
        .hasMessageContaining("already exists");
  }

  @Test
  public void createViewWithAlias() throws Exception {
    // Arrange
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "CREATE VIEW %s.%s (aliasField) AS SELECT 'c1' AS realField",
                        DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey))))
        .hasMessageContaining("Versioned views don't support field aliases");

    // Should throw an error

  }

  @Test
  public void createViewOnNonExistentTable() {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    assertThatThrownBy(() -> runSQL(createViewQuery(viewKey, tablePath)))
        .hasMessageContaining("VALIDATION ERROR: Validation of view sql failed")
        .hasMessageContaining("not found within '" + DATAPLANE_PLUGIN_NAME + "'");
  }

  @Test
  public void createViewWithIncompleteSql() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    List<String> viewKey = tablePathWithFolders(tableName);

    assertThatThrownBy(() -> runSQL(createViewQueryWithIncompleteSql(viewKey, tablePath)))
        .hasMessageContaining("PARSE ERROR:");
  }

  @Test
  public void dropView() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    runSQL(createViewQuery(viewKey, tablePath));
    assertNessieHasView(viewKey, DEFAULT_BRANCH_NAME, this);
    runSQL(dropViewQuery(viewKey));

    // Assert
    assertNessieDoesNotHaveEntity(viewKey, DEFAULT_BRANCH_NAME, this);

    assertCommitLogTail(
        String.format("CREATE VIEW %s", joinedTableKey(viewKey)),
        String.format("DROP VIEW %s", joinedTableKey(viewKey)));
  }

  @Test
  public void dropViewTwice() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    runSQL(createViewQuery(viewKey, tablePath));
    assertNessieHasView(viewKey, DEFAULT_BRANCH_NAME, this);
    runSQL(dropViewQuery(viewKey));
    assertNessieDoesNotHaveEntity(viewKey, DEFAULT_BRANCH_NAME, this);

    // Assert
    assertThatThrownBy(() -> runSQL(dropViewQuery(viewKey))).hasMessageContaining("Unknown view");
  }

  @Test
  public void dropViewNonExist() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Assert
    assertThatThrownBy(() -> runSQL(dropViewQuery(viewKey))).hasMessageContaining("Unknown view");
  }

  @Test
  public void dropViewAsTable() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    runSQL(createViewQuery(viewKey, tablePath));
    assertNessieHasView(viewKey, DEFAULT_BRANCH_NAME, this);

    // Assert
    assertThatThrownBy(() -> runSQL(dropTableQuery(viewKey)))
        .hasMessageContaining("is not a TABLE");
  }

  @Test
  public void createViewAndDropViewWithATSyntaxInDevBranch() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();

    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQueryWithAt(tablePath, devBranch));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = Collections.singletonList(viewName);

    // Act + Assert
    runSQL(createViewQueryWithAt(viewKey, tablePath, devBranch));
    assertNessieHasView(viewKey, devBranch, this);

    runSQL(dropViewQueryWithAt(viewKey, devBranch));
    assertNessieDoesNotHaveEntity(viewKey, devBranch, this);
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
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath, 5));
    runSQL(insertSelectQuery(tablePath, 5));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 10);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
        DEFAULT_COUNT_COLUMN,
        10);
    // Create tag
    runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    // Insert 5 more rows into the table
    runSQL(insertSelectQuery(tablePath, 5));
    // AT query
    String selectATQuery =
        String.format(
            "select * from %s.%s AT TAG %s ",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath), firstTag);

    // Act
    runSQL(createViewSelectQuery(viewPath, selectATQuery));

    // Assert
    assertViewHasExpectedNumRows(viewPath, 10);
    assertTableHasExpectedNumRows(tablePath, 15);
  }

  @Test
  public void createViewClashWithTable() throws Exception {
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Create table with 10 rows
    runSQL(createTableAsQuery(tablePath, 10));

    // Act and Assert
    assertThatThrownBy(() -> runSQL(createViewQuery(tablePath, tablePath)))
        .hasMessageContaining("A non-view table with given name ")
        .hasMessageContaining("already exists in schema");
  }

  @Test
  public void createViewClashWithUdf() throws Exception {
    enableVersionedSourceUdf();
    String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    runSQL(createUdfQuery(functionKey));

    String tableName = generateUniqueTableName();
    List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act and Assert
    assertThatThrownBy(() -> runSQL(createViewQuery(functionKey, tablePath)))
        .hasMessageContaining("An Entity of type UDF with given name ")
        .hasMessageContaining("already exists");
  }

  // View will be created with fully qualified name represented by viewCreationPath
  // Table tableName1 will be resolved to the current schema path (workspaceSchemaPath) set in the
  // context
  // viewCreationPath != workspaceSchemaPath
  @Test
  public void createViewWithTableOnDifferentPathContext() throws Exception {
    List<String> workspaceSchemaPath = generateSchemaPath();
    runSQL(
        createFolderQuery(
            workspaceSchemaPath.get(0),
            workspaceSchemaPath.subList(1, workspaceSchemaPath.size())));

    // current schema context
    runSQL(useContextQuery(workspaceSchemaPath));
    // Create table1 with 10 rows
    String tableName1 = generateUniqueTableName();
    runSQL(createTableAsQuery(Collections.singletonList(tableName1), 10));
    final String viewName = generateUniqueViewName();
    List<String> viewCreationPath = tablePathWithFolders(viewName);
    runSQL(createViewQuery(viewCreationPath, Collections.singletonList(tableName1)));
    assertThat(viewCreationPath != workspaceSchemaPath).isTrue();
    assertViewHasExpectedNumRows(viewCreationPath, 10);
  }

  @Test
  public void updateView() throws Exception {
    // Arrange
    // Enable V0 + disable V1 support key for creating views
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "true");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "false");
    FileIO fileIO = getFileIO(getDataplanePlugin());
    String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    // Create table1 with 10 rows
    runSQL(createTableAsQuery(tablePath1, 10));
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    runSQL(createViewQuery(viewKey, tablePath1));
    String viewMetadataLocation = getMetadataLocationForViewKey(viewKey);
    ViewVersionMetadata origViewMetadata =
        ViewVersionMetadataParser.read(fileIO.newInputFile(viewMetadataLocation));

    assertViewHasExpectedNumRows(viewKey, 10);
    long mtime1 =
        getMtimeForTable(
            viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Create table2 with 20 rows.
    runSQL(createTableAsQuery(tablePath2, 20));

    // Act
    runSQL(createReplaceViewQuery(viewKey, tablePath2));
    viewMetadataLocation = getMetadataLocationForViewKey(viewKey);
    ViewVersionMetadata newViewMetadata =
        ViewVersionMetadataParser.read(fileIO.newInputFile(viewMetadataLocation));
    long mtime2 =
        getMtimeForTable(
            viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    // Assert
    assertViewHasExpectedNumRows(viewKey, 20);
    assertThat(mtime2).isGreaterThan(mtime1);

    assertCommitLogTail(
        String.format("CREATE VIEW %s", joinedTableKey(viewKey)),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath2)),
        String.format("ALTER on VIEW %s", joinedTableKey(viewKey)));
  }

  @Test
  public void alterViewProperty() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);

    // Create a table with 10 rows.
    runSQL(createTableAsQuery(tablePath1, 10));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view.
    runSQL(createViewQuery(viewKey, tablePath1));

    final String attribute = "enable_default_reflection";
    final String value = "true";
    final List<String> expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options updated", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    assertThat(runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);

    assertCommitLogTail(
        String.format("CREATE VIEW %s", joinedTableKey(viewKey)),
        String.format("ALTER on VIEW %s", joinedTableKey(viewKey)));
  }

  @Test
  public void alterViewPropertyWithAtInDevBranch() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = Collections.singletonList(tableName1);
    final String devBranch = generateUniqueBranchName();

    // Create a table with 10 rows.
    runSQL(createTableAsQuery(tablePath1, 10));

    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = Collections.singletonList(viewName);

    // Create a view.
    runSQL(createViewQueryWithAt(viewKey, tablePath1, devBranch));

    final String attribute = "enable_default_reflection";
    final String value = "true";
    final List<String> expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options updated", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    assertThat(
            runSqlWithResults(alterViewPropertyQueryWithAt(viewKey, attribute, value, devBranch)))
        .contains(expectResult);

    assertCommitLogTail(
        VersionContext.ofBranch(devBranch),
        String.format("CREATE VIEW %s", joinedTableKey(viewKey)),
        String.format("ALTER on VIEW %s", joinedTableKey(viewKey)));
  }

  @Test
  public void alterViewPropertyTwice() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);

    // Create a table with 10 rows.
    runSQL(createTableAsQuery(tablePath1, 10));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view.
    runSQL(createViewQuery(viewKey, tablePath1));

    final String attribute = "enable_default_reflection";
    final String value = "true";
    List<String> expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options updated", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    assertThat(runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);

    expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options did not change",
                DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));
    assertThat(runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);
  }

  @Test
  public void alterViewPropertyWithDifferentValue() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);

    // Create a table with 10 rows.
    runSQL(createTableAsQuery(tablePath1, 10));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view.
    runSQL(createViewQuery(viewKey, tablePath1));

    final String attribute = "enable_default_reflection";
    String value = "true";
    List<String> expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options updated", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    assertThat(runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);

    value = "false";
    assertThat(runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);
  }

  @Test
  public void updateViewKeepProperties() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    // Create a table with 10 rows.
    runSQL(createTableAsQuery(tablePath1, 10));

    // Create a table with 20 rows.
    runSQL(createTableAsQuery(tablePath2, 20));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view.
    runSQL(createViewQuery(viewKey, tablePath1));

    final String attribute = "enable_default_reflection";
    final String value = "true";

    List<String> expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options updated", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    assertThat(runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);

    runSQL(createReplaceViewQuery(viewKey, tablePath2));

    expectResult =
        Arrays.asList(
            "true",
            String.format(
                "Table [%s.%s] options did not change",
                DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    assertThat(runSqlWithResults(alterViewPropertyQuery(viewKey, attribute, value)))
        .contains(expectResult);
  }

  @Test
  public void createViewWithNoSql() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    List<String> viewKey = tablePathWithFolders(tableName);

    UserExceptionAssert.assertThatThrownBy(
            () -> runSQL(createViewQueryWithEmptySql(viewKey, tablePath)))
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.PARSE);
  }

  @Test
  public void selectFromView() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    runSQL(insertTableQuery(tablePath));
    runSQL(createViewQuery(viewPath, tablePath));

    // Act and Assert
    assertTableHasExpectedNumRows(tablePath, 3);
    assertViewHasExpectedNumRows(viewPath, 3);
    // cleanup

    runSQL(dropViewQuery(viewPath));
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void selectFromViewWithJoin() throws Exception {
    // Arrange
    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    runSQL(createViewSelectQuery(viewPath, joinTpcdsTablesQuery()));

    // Act and Assert
    assertViewHasExpectedNumRows(viewPath, 22500000);
    // cleanup
    runSQL(dropViewQuery(viewPath));
  }

  @Test
  public void selectFromViewDifferentTags() throws Exception {
    // Arrange
    String firstTag = generateUniqueTagName();
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Create table1 on default branch with 10 rows
    runSQL(createTableAsQuery(tablePath, 10));
    assertTableHasExpectedNumRows(tablePath, 10);

    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    runSQL(createViewQuery(viewPath, tablePath));
    // Create a tag to mark it
    runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));
    // Insert 10 more rows
    runSQL(insertSelectQuery(tablePath, 10));
    assertViewHasExpectedNumRows(viewPath, 20);

    // Act
    // Go back to tag1
    runSQL(useTagQuery(firstTag));

    // Assert
    // Select from view should return 10
    assertViewHasExpectedNumRows(tablePath, 10);

    // cleanup
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropViewQuery(viewPath));
    runSQL(dropTableQuery(tablePath));
  }

  // This tests a view with column specified in select list
  // This will not pick up underlying table schema changes for columns that are not in its select
  // list
  @Test
  public void selectFromViewOnDiffBranchesWithAddColumn() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Arrays.asList("col2 int", "col3 int", "col4 varchar");
    final List<String> columnValuesBeforeAdd = Arrays.asList("(1)", "(2)");
    final List<String> columnValuesAfterAdd = Arrays.asList("(3,3,3,'three')", "(4,4,4,'four')");

    // Setup
    // Set context to main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    // Insert  2 rows into table
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    String sqlQuery =
        String.format("select col1 from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    runSQL(createViewSelectQuery(viewPath, sqlQuery));
    final String devBranchName = generateUniqueBranchName();
    // Create a dev branch from main
    runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Act
    runSQL(useBranchQuery(devBranchName));
    // Alter underlying table
    runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));

    // Assert
    // Execute view in context of main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Select the  rows from view

    testBuilder()
        .sqlQuery(selectStarQuery(viewPath))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .baselineValues(2)
        .go();

    // Execute view in context of dev
    runSQL(useBranchQuery(devBranchName));
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4")
        .baselineValues(1, null, null, null)
        .baselineValues(2, null, null, null)
        .baselineValues(3, 3, 3, "three")
        .baselineValues(4, 4, 4, "four")
        .go();
    // Select the new rows from new columns
    testBuilder()
        .sqlQuery(selectStarQuery(viewPath))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .go();

    // cleanup
    runSQL(dropViewQuery(viewPath));
    runSQL(dropTableQuery(tablePath));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(tablePath));
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
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    // Insert  2 rows into table
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    String sqlQuery =
        String.format("select * from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    runSQL(createViewSelectQuery(viewPath, sqlQuery));
    final String devBranchName = generateUniqueBranchName();
    // Create a dev branch from main
    runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Act
    runSQL(useBranchQuery(devBranchName));
    // Alter underlying table
    runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));

    // Assert

    // Execute view in context of main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Select the  rows from view

    testBuilder()
        .sqlQuery(selectStarQuery(viewPath))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .baselineValues(2)
        .go();

    // Execute select from table in context of dev to reflect the added  columns
    runSQL(useBranchQuery(devBranchName));
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4")
        .baselineValues(1, null, null, null)
        .baselineValues(2, null, null, null)
        .baselineValues(3, 3, 3, "three")
        .baselineValues(4, 4, 4, "four")
        .go();

    // First attempt should show updates to get all the underlying table changes
    testBuilder()
        .sqlQuery(selectStarQuery(viewPath))
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4")
        .baselineValues(1, null, null, null)
        .baselineValues(2, null, null, null)
        .baselineValues(3, 3, 3, "three")
        .baselineValues(4, 4, 4, "four")
        .go();

    // cleanup TODO : Cleanup view after dropView support
    runSQL(dropTableQuery(tablePath));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(tablePath));
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
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    // Insert  2 rows into table
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    String sqlQuery =
        String.format("select * from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    runSQL(createViewSelectQuery(viewPath, sqlQuery));
    final String devBranchName = generateUniqueBranchName();
    // Create a dev branch from main
    runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Act
    runSQL(useBranchQuery(devBranchName));
    // Alter underlying table
    runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));

    // Create a tag
    runSQL(createTagQuery(tagName, devBranchName));

    // Assert
    // Execute view in context of tag
    runSQL(useTagQuery(tagName));
    // Execute select from table in context of dev to reflect the added  columns
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4")
        .baselineValues(1, null, null, null)
        .baselineValues(2, null, null, null)
        .baselineValues(3, 3, 3, "three")
        .baselineValues(4, 4, 4, "four")
        .go();

    long mtimeBefore =
        getMtimeForTable(
            viewPath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    // Execute view in context of tag.  While view schema will be out of sync with the view's SQL,
    // planner will
    // still plan using the SQL.
    testBuilder()
        .sqlQuery(selectStarQuery(viewPath))
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4")
        .baselineValues(1, null, null, null)
        .baselineValues(2, null, null, null)
        .baselineValues(3, 3, 3, "three")
        .baselineValues(4, 4, 4, "four")
        .go();

    // However, ensure we didn't update the view schema.
    long mtimeAfter =
        getMtimeForTable(
            viewPath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    assertThat(mtimeAfter).isEqualTo(mtimeBefore);

    // cleanup
    runSQL(useBranchQuery(devBranchName));
    runSQL(dropViewQuery(viewPath));
    runSQL(dropTableQuery(tablePath));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void selectFromViewOnDiffBranchesWithDropColumn() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Arrays.asList("col1 int", "col2  varchar");
    final List<String> dropCols = Collections.singletonList("col2");
    final List<String> columnValuesBeforeAdd = Arrays.asList("(1,'one')", "(2,'two')");
    final List<String> columnValuesAfterDrop = Arrays.asList("(3)", "(4)");

    // Setup
    // Set context to main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    // Insert  2 rows into table
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    String sqlQuery =
        String.format("select col2 from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    runSQL(createViewSelectQuery(viewPath, sqlQuery));
    final String devBranchName = generateUniqueBranchName();
    // Create a dev branch from main
    runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Act
    runSQL(useBranchQuery(devBranchName));
    // Alter underlying table
    runSQL(alterTableDropColumnQuery(tablePath, dropCols));
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterDrop));

    // Assert
    // Execute view in context of main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Select the  rows from view

    testBuilder()
        .sqlQuery(selectStarQuery(viewPath))
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues("one")
        .baselineValues("two")
        .go();

    // Select  in context of dev to show one column
    runSQL(useBranchQuery(devBranchName));
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .go();
    // Execute view in context of dev - should error out
    assertQueryThrowsExpectedError(
        selectStarQuery(viewPath),
        "Error while expanding view "
            + String.format("%s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewPath))
            + ". Column 'col2' not found in any table. Verify the view’s SQL definition.");

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void selectFromViewWithStarQueryAndDropUnderlyingColumn() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Arrays.asList("col1 int", "col2  varchar");
    final List<String> dropCols = Collections.singletonList("col2");
    final List<String> columnValuesBeforeAdd = Arrays.asList("(1,'one')", "(2,'two')");
    final List<String> columnValuesAfterDrop = Arrays.asList("(3)", "(4)");

    // Setup
    // Set context to main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    // Insert  2 rows into table
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    String sqlQuery =
        String.format("select * from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    runSQL(createViewSelectQuery(viewPath, sqlQuery));
    final String devBranchName = generateUniqueBranchName();
    // Create a dev branch from main
    runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Act
    runSQL(useBranchQuery(devBranchName));
    // Alter underlying table
    runSQL(alterTableDropColumnQuery(tablePath, dropCols));
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterDrop));

    // Assert
    // Execute view in context of main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Select the  rows from view

    testBuilder()
        .sqlQuery(selectStarQuery(viewPath))
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, "one")
        .baselineValues(2, "two")
        .go();

    // Select  in context of dev to show one column
    runSQL(useBranchQuery(devBranchName));
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .go();
    // Execute view in context of dev - should work since it's a star query

    // First attempt should show results
    testBuilder()
        .sqlQuery(selectStarQuery(viewPath))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .go();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void selectFromViewVirtualInt() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Act
    String viewSQL = "select 1, 2, 3 ";
    runSQL(createViewSelectQuery(viewPath, viewSQL));

    // Assert
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 1);
  }

  @Test
  public void selectFromViewVirtualVarcharWithCast() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    String viewSQL = "select CAST('abc' AS VARCHAR(65536)) as varcharcol";
    runSQL(createViewSelectQuery(viewPath, viewSQL));

    // Assert
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 1);
  }

  @Test
  public void selectFromViewVirtualVarchar() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    String viewSQL = "select 0 , 1  , 2 , 'abc' ";
    runSQL(createViewSelectQuery(viewPath, viewSQL));

    // Assert
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 1);
  }

  @Test
  public void selectFromViewConcat() throws Exception {
    // Arrange
    final String viewName1 = generateUniqueTableName();
    final String tableName = generateUniqueTableName();
    final List<String> viewPath1 = tablePathWithFolders(viewName1);
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    String viewSQL1 =
        String.format(
            "select CONCAT(name, ' of view ') from %s.%s",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    runSQL(createViewSelectQuery(viewPath1, viewSQL1));

    // Select
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(viewPath1, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 3);
  }

  @Test
  public void selectFromViewCaseInt() throws Exception {
    // Arrange
    final String viewName1 = generateUniqueTableName();
    final String tableName = generateUniqueTableName();
    final List<String> viewPath1 = tablePathWithFolders(viewName1);
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    runSQL(
        String.format(
            "insert into %s.%s values (-1, 'invalid id', 10.0)",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)));

    String viewSQL1 =
        String.format(
            "select case when id > 0 THEN 1 ELSE 0 END AS C5  from %s.%s",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    // Act
    runSQL(createViewSelectQuery(viewPath1, viewSQL1));

    // Assert
    assertViewHasExpectedNumRows(viewPath1, 4);
    String viewWhereQuery =
        String.format(
            "select count(*) as c1 from %s.%s where C5 = 1",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(viewPath1));
    assertSQLReturnsExpectedNumRows(viewWhereQuery, "c1", 3);
  }

  @Test
  public void selectFromViewCaseVarchar() throws Exception {
    // Arrange
    final String viewName1 = generateUniqueTableName();
    final String tableName = generateUniqueTableName();
    final List<String> viewPath1 = tablePathWithFolders(viewName1);
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    runSQL(
        String.format(
            "insert into %s.%s values (-1, 'invalid id', 10.0)",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)));

    String viewSQL1 =
        String.format(
            "select case when id > 0 THEN 'positive' ELSE 'invalid' END AS C5  from %s.%s",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));

    runSQL(createViewSelectQuery(viewPath1, viewSQL1));

    // Select
    assertViewHasExpectedNumRows(viewPath1, 4);
    String viewWhereQuery =
        String.format(
            "select count(*) as c1 from %s.%s where C5 = 'invalid'",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(viewPath1));
    assertSQLReturnsExpectedNumRows(viewWhereQuery, "c1", 1);
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
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath, 5));
    runSQL(insertSelectQuery(tablePath, 5));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 10);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
        DEFAULT_COUNT_COLUMN,
        10);
    runSQL(createViewQuery(viewPath, tablePath));
    // Create tag
    runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    // Insert 10 more rows into table
    runSQL(insertSelectQuery(tablePath, 10));
    // Verify view can see the new rows.
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 20);

    // Act and Assert
    // Now run the query with AT syntax on tag to verify only 5 rows are returned.
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(viewPath, DEFAULT_COUNT_COLUMN, "TAG " + firstTag),
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
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath, 5));
    runSQL(insertSelectQuery(tablePath, 5));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 10);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
        DEFAULT_COUNT_COLUMN,
        10);
    runSQL(createViewQuery(viewPath, tablePath));

    // Create dev branch
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    String commitHashBranchBeforeInsert = getCommitHashForBranch(devBranch);
    // Insert 10 more rows into table
    runSQL(insertSelectQuery(tablePath, 10));
    String commitHashBranchAfterInsert = getCommitHashForBranch(devBranch);

    // Act and Assert

    // Verify view can see the new rows at each commit
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            viewPath, DEFAULT_COUNT_COLUMN, "COMMIT " + quoted(commitHashBranchBeforeInsert)),
        DEFAULT_COUNT_COLUMN,
        10);

    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            viewPath, DEFAULT_COUNT_COLUMN, "COMMIT " + quoted(commitHashBranchAfterInsert)),
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
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath, 5));
    runSQL(createTableAsQuery(tablePath2, 10));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 5);
    assertTableHasExpectedNumRows(tablePath2, 10);

    String table1 = joinedTableKey(tablePath);
    String table2 = joinedTableKey(tablePath2);
    String condition = " TRUE ";
    runSQL(createViewSelectQuery(viewPath, joinTablesQuery(table1, table2, condition)));

    // Create tag
    runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    // Verify view can see the new rows.
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 50);

    runSQL(insertSelectQuery(tablePath, 10));

    // Verify view can see the new rows.
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 150);

    // Act and Assert
    // Now run the query with AT syntax on tag to verify only 50 rows are returned.
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(viewPath, DEFAULT_COUNT_COLUMN, "TAG " + firstTag),
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
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    // Act
    String viewSQL1 =
        String.format(
            "select id+10 AS idv1, id+20 AS idv2 from %s.%s",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    runSQL(createViewSelectQuery(viewPath1, viewSQL1));

    String viewSQL2 =
        String.format(
            "select idv1/10 AS idv10, idv2/10 AS idv20 from %s.%s",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(viewPath1));
    runSQL(createViewSelectQuery(viewPath2, viewSQL2));
    runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));
    // Insert an extra row in table
    runSQL(
        String.format(
            "insert into %s.%s values (4, 'fourth row', 40.0)",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)));

    // Modify the first view (after tag) - view1 contains 1 column.
    String viewSQL11 =
        String.format(
            "select id+10 AS idv1 from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    runSQL(updateViewSelectQuery(viewPath1, viewSQL11));

    // Assert
    // Ensure that the select from view2 are the previous tag is able to expand and see 2 columns
    // and 3 rows
    testBuilder()
        .sqlQuery(selectStarQueryWithSpecifier(viewPath2, "TAG " + firstTag))
        .unOrdered()
        .baselineColumns("idv10", "idv20")
        .baselineValues(1, 2)
        .baselineValues(1, 2)
        .baselineValues(1, 2)
        .go();

    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(viewPath2, "c1", "TAG " + firstTag), "c1", 3);
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
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    // create table in dev
    runSQL(useBranchQuery(devBranch));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    // create view in main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    String viewSQL =
        String.format(
            "select *  from %s.%s  AT BRANCH %s",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath), devBranch);
    runSQL(createViewSelectQuery(viewPath, viewSQL));

    // Act and Assert

    // In context of main, select from query. Underlying table should be resolved to dev.(does not
    // exist in main)
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Verify view can see the new rows.
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(viewPath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 3);
  }

  @Test
  public void selectFromNestedViewsWithAT() throws Exception {
    // Arrange
    /*
     view3 - only exists in branch main
          ---> view2 with AT < branch dev2> (view2 only exists in dev2)
                   ---> view1 (only exists in dev1)
                           ---> table1 (only exists in dev0)
     - Test query with view3 with branch set to main
     - Lookup of view2 should resolve to dev2
     - Lookup of view1 and table1 should resolve with dev1.
    */
    // Arrange
    final String view1 = generateUniqueViewName();
    final String view2 = generateUniqueViewName();
    final String view3 = generateUniqueViewName();
    final String tableName = generateUniqueTableName();

    final List<String> view1Path = tablePathWithFolders(view1);
    final List<String> view2Path = tablePathWithFolders(view2);
    final List<String> view3Path = tablePathWithFolders(view3);
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String dev0 = generateUniqueBranchName();
    final String dev1 = generateUniqueBranchName();
    final String dev2 = generateUniqueBranchName();

    // Create dev
    runSQL(createBranchAtBranchQuery(dev0, DEFAULT_BRANCH_NAME));
    runSQL(createBranchAtBranchQuery(dev1, DEFAULT_BRANCH_NAME));
    runSQL(createBranchAtBranchQuery(dev2, DEFAULT_BRANCH_NAME));

    // create table in dev0
    runSQL(useBranchQuery(dev0));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    // create view1 in dev1
    runSQL(useBranchQuery(dev1));
    String view1SQL =
        String.format(
            "select *  from %s.%s  AT BRANCH %s",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath), dev0);
    runSQL(createViewSelectQuery(view1Path, view1SQL));

    // create view2 in dev2
    runSQL(useBranchQuery(dev2));
    String view2SQL =
        String.format(
            "select *  from %s.%s  AT BRANCH %s",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(view1Path), dev1);
    runSQL(createViewSelectQuery(view2Path, view2SQL));

    // create view3 in main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    String view3SQL =
        String.format(
            "select *  from %s.%s  AT BRANCH %s",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(view2Path), dev2);
    runSQL(createViewSelectQuery(view3Path, view3SQL));
    // Act and Assert

    // In context of main, select from query. Underlying table should be resolved to dev.(does not
    // exist in main)
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Verify view can see the new rows.
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(view3Path, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 3);
  }

  @Test
  void selectFromNestedViewInnerTaggedVersion() throws Exception {
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

    runSQL(createTableAsQuery(tablePath, 5));
    runSQL(insertSelectQuery(tablePath, 5));
    String view1SQL =
        String.format("select *  from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    runSQL(createViewSelectQuery(view1Path, view1SQL));
    // Verify view can see the new rows.
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(view1Path, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 10);
    runSQL(createTagQuery(atag, DEFAULT_BRANCH_NAME));
    String view2SQL =
        String.format(
            "select *  from %s.%s  AT TAG %s",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(view1Path), atag);
    runSQL(createViewSelectQuery(view2Path, view2SQL));
    runSQL(insertSelectQuery(tablePath, 5));

    // Verify view can see the new rows.
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(view2Path, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 10);
  }

  // Negative test
  @Test
  public void createViewWithTimeTravelQuery() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath, 5));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 5);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
        DEFAULT_COUNT_COLUMN,
        5);

    final TimestampString ts1 = TimestampString.fromMillisSinceEpoch(System.currentTimeMillis());

    // Insert rows
    runSQL(insertSelectQuery(tablePath, 2));
    // Verify number of rows.
    // on main branch, at this timestamp
    assertTableHasExpectedNumRows(tablePath, 7);
    // on main branch, at this timestamp
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN, "TIMESTAMP '" + ts1 + "'"),
        DEFAULT_COUNT_COLUMN,
        5);

    // AT query
    String selectATQuery =
        String.format(
            "select * from %s.%s AT TIMESTAMP '%s' ",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath), ts1);

    // Act and Assert
    assertThatThrownBy(() -> runSQL(createViewSelectQuery(viewPath, selectATQuery)))
        .hasMessageContaining("Versioned views do not support AT SNAPSHOT or AT TIMESTAMP");

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  // Negative test
  @Test
  public void selectFromVersionedViewWithTimeTravel() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> tablePath = tablePathWithFolders(tableName);
    String countCol = "countCol";

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath, 5));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 5);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
        DEFAULT_COUNT_COLUMN,
        5);
    runSQL(createViewQuery(viewPath, tablePath));

    final TimestampString ts1 = TimestampString.fromMillisSinceEpoch(System.currentTimeMillis());

    // Insert rows
    runSQL(insertSelectQuery(tablePath, 2));
    // Verify number of rows.
    // on main branch, at this timestamp
    assertTableHasExpectedNumRows(tablePath, 7);
    // on main branch, at this timestamp
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN, "TIMESTAMP '" + ts1 + "'"),
        DEFAULT_COUNT_COLUMN,
        5);

    // Act and Assert
    assertThatThrownBy(
            () ->
                runSQL(
                    selectCountQueryWithSpecifier(viewPath, countCol, "TIMESTAMP '" + ts1 + "'")))
        .hasMessageContaining("Versioned views do not support AT SNAPSHOT or AT TIMESTAMP");

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  // Negative test
  @Test
  public void createViewWithSnapshotQuery() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath, 5));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 5);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
        DEFAULT_COUNT_COLUMN,
        5);

    final long snapshotId = 1000;

    // Insert rows
    runSQL(insertSelectQuery(tablePath, 2));
    // Verify number of rows.
    // on main branch, at this timestamp
    assertTableHasExpectedNumRows(tablePath, 7);

    // AT query
    String selectATQuery =
        String.format(
            "select * from %s.%s AT SNAPSHOT '%d' ",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath), snapshotId);

    // Act and Assert
    assertThatThrownBy(() -> runSQL(createViewSelectQuery(viewPath, selectATQuery)))
        .hasMessageContaining("Versioned views do not support AT SNAPSHOT or AT TIMESTAMP");

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  /**
   * Verify CAST on Calcite schema with INTEGER NOT NULL to Iceberg View schema with nullable
   * INTEGER
   */
  @Test
  public void selectStarFromViewVirtualInt() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Act
    String viewSQL = "select 1 as col1";
    runSQL(createViewSelectQuery(viewPath, viewSQL));

    // Assert
    testBuilder()
        .sqlQuery(selectStarQuery(viewPath))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .go();
  }

  /**
   * Verify CAST on Calcite schema with VARCHAR(3) NOT NULL to Iceberg View schema with nullable
   * VARCHAR(65536)
   */
  @Test
  public void selectStarFromViewVirtualVarchar() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Act
    String viewSQL = "select 'xyz' as col1";
    runSQL(createViewSelectQuery(viewPath, viewSQL));

    // Assert
    testBuilder()
        .sqlQuery(selectStarQuery(viewPath))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues("xyz")
        .go();
  }

  /**
   * We don't support metadata functions (ie. table_snapshot or table_files) on views. Expect
   * validation error to be thrown.
   */
  @Test
  public void metadataFunctionNotSupportedOnView() throws Exception {
    // Arrange
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Act
    String viewSQL = "select 1 as col1";
    runSQL(createViewSelectQuery(viewPath, viewSQL));

    // Assert
    assertQueryThrowsExpectedError(
        selectCountDataFilesQuery(viewPath, DEFAULT_COUNT_COLUMN),
        String.format(
            "VALIDATION ERROR: Metadata function ('table_files') is not supported on versioned table '%s.%s'",
            DATAPLANE_PLUGIN_NAME, String.join(".", viewPath)));

    // Cleanup
    runSQL(dropViewQuery(viewPath));
  }

  /** Verify view property can be set and retrieved correctly. */
  @Test
  public void setViewProperty() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act
    runSQL(createViewQuery(viewKey, tablePath));

    // Assert
    assertNessieHasView(viewKey, DEFAULT_BRANCH_NAME, this);

    // Disable the default reflection
    runSQL(alterViewPropertyQuery(viewKey, "enable_default_reflection", "False"));

    final String versionedDatasetId =
        getVersionedDatatsetId(
            viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    final DremioTable dremioTable = getTableFromId(versionedDatasetId, this);

    // Assert
    assertThat(dremioTable.getDatasetConfig().getVirtualDataset().getDefaultReflectionEnabled())
        .isFalse();

    // Enable the default reflection
    runSQL(alterViewPropertyQuery(viewKey, "enable_default_reflection", "True"));

    final String newVersionedDatasetId =
        getVersionedDatatsetId(
            viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    final DremioTable newDremioTable = getTableFromId(newVersionedDatasetId, this);

    // Assert
    assertThat(newDremioTable.getDatasetConfig().getVirtualDataset().getDefaultReflectionEnabled())
        .isTrue();
  }

  @Test
  public void createViewWithImplicitFolders() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    runSQL(createEmptyTableQuery(tablePath));

    // Assert
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);

    // Arrange
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);

    // Act + Assert
    runSQL(createViewQuery(viewKey, tablePath));
    assertNessieHasView(viewKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createViewInNonBranchVersionContext() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final String tag = generateUniqueTagName();
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    runSQL(useTagQuery(tag));

    // Act and Assert
    assertQueryThrowsExpectedError(
        createViewQuery(viewPath, tablePath),
        String.format(
            "DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
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
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(createViewQuery(viewPath, tablePath));
    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    runSQL(useTagQuery(tag));

    // Act and Assert
    String viewSQLupdate =
        String.format(
            "select id+10 AS idv1 from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
    assertQueryThrowsExpectedError(
        updateViewSelectQuery(viewPath, viewSQLupdate),
        String.format(
            "DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
            tag));
  }

  @Test
  public void selectNestedViews() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(
        String.format(
            "CREATE VIEW %s.%s AS SELECT COUNT(*) FROM %s.%s",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(viewKey),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(tablePath)));

    final String viewName2 = generateUniqueViewName();
    List<String> viewKey2 = tablePathWithFolders(viewName2);
    runSQL(createViewQuery(viewKey2, viewKey));

    // Act and Assert
    assertViewHasExpectedNumRows(viewKey2, 1);
  }

  @Test
  public void selectNestedViewsAtBranch() throws Exception {
    // Arrange
    // Create a dev branch from main
    final String devBranchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranchName));

    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(
        String.format(
            "CREATE VIEW %s.%s AS SELECT COUNT(*) FROM %s.%s AT BRANCH %s",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(viewKey),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(tablePath),
            devBranchName));

    final String viewName2 = generateUniqueViewName();
    List<String> viewKey2 = tablePathWithFolders(viewName2);
    runSQL(
        String.format(
            "CREATE VIEW %s.%s AS SELECT * FROM %s.%s AT BRANCH %s ",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(viewKey2),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(viewKey),
            devBranchName));

    // Act and Assert
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(viewKey2, DEFAULT_COUNT_COLUMN, "BRANCH " + devBranchName),
        DEFAULT_COUNT_COLUMN,
        1);
  }

  @Test
  public void selectSameView() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(
        String.format(
            "CREATE VIEW %s.%s AS SELECT COUNT(*) AS %s FROM %s.%s",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(viewKey),
            DEFAULT_COUNT_COLUMN,
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(tablePath)));

    // Act and Assert
    assertSQLReturnsExpectedNumRows(
        String.format(
            "SELECT COUNT(*) AS %s FROM (SELECT %s FROM %s.%s UNION ALL SELECT %s FROM %s.%s) X",
            DEFAULT_COUNT_COLUMN,
            DEFAULT_COUNT_COLUMN,
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(viewKey),
            DEFAULT_COUNT_COLUMN,
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(viewKey)),
        DEFAULT_COUNT_COLUMN,
        2);
  }

  @Test
  public void selectNoSchemaLearn() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    final String parentViewName = generateUniqueViewName();
    List<String> parentViewPath = tablePathWithFolders(parentViewName);
    runSQL(
        String.format(
            "CREATE VIEW %s.%s AS SELECT 'XYZ', COUNT(*) FROM %s.%s",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(parentViewPath),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(tablePath)));

    final String childViewName = generateUniqueViewName();
    List<String> childViewPath = tablePathWithFolders(childViewName);
    runSQL(createViewQuery(childViewPath, parentViewPath));
    testBuilder()
        .sqlQuery(selectStarQuery(childViewPath))
        .unOrdered()
        .baselineColumns("EXPR$0", "EXPR$1")
        .baselineValues("XYZ", 3L)
        .go();

    assertViewHasExpectedNumRows(childViewPath, 1);

    // Change parent view so that child view's schema is now out of date
    runSQL(
        String.format(
            "CREATE OR REPLACE VIEW %s.%s AS SELECT COUNT(*) FROM %s.%s",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(parentViewPath),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(tablePath)));

    // Assert
    testBuilder()
        .sqlQuery(selectStarQuery(childViewPath))
        .unOrdered()
        .baselineColumns("EXPR$0") // Only a single columns
        .baselineValues(3L)
        .go();

    assertViewHasExpectedNumRows(childViewPath, 1);

    assertThat(
            runSqlWithResults(
                    String.format(
                        "describe %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(childViewPath)))
                .size())
        .isEqualTo(
            2); // View schema still has two columns indicating the view was not updated because
    // query type is JDBC
  }

  /**
   * Validates that view schema learning does not happen with complex types and that we can
   * successfully plan and execute a query on such view.
   */
  @Test
  public void selectOnComplexType() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath)); // by default will have 3 columns

    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(
        String.format(
            "CREATE VIEW %s.%s AS SELECT REGEXP_SPLIT('REGULAR AIR', 'R', 'LAST', -1) AS R_LESS_SHIPMENT_TYPE",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    // Act and Assert
    assertSQLReturnsExpectedNumRows(
        String.format(
            "select count(*) %s from %s.%s where R_LESS_SHIPMENT_TYPE[0] = 'REGULAR AI'",
            DEFAULT_COUNT_COLUMN, DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)),
        DEFAULT_COUNT_COLUMN,
        1);
  }

  @Test
  @Disabled("Until DX-65900 is fixed")
  public void createViewWithFieldNames() throws Exception {
    // Arrange
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(
        String.format(
            "CREATE VIEW %s.%s (alias, alias2, alias3) AS SELECT 'c1' AS duplicate, 'c2' AS duplicate, COUNT(*) AS cnt;",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)));

    // Verify schema learning does not occur and view's schema has the aliased fields
    assertSQLReturnsExpectedNumRows(
        String.format("SELECT * from %s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(viewKey)),
        "alias3",
        1);
  }

  @Test
  public void showViewsWithTimestamp() throws Exception {
    // Arrange
    final Duration delayBetweenCommits = Duration.ofMillis(5000);
    final List<String> tablePath = Arrays.asList(generateUniqueTableName());
    final List<String> viewPath = Arrays.asList(generateUniqueViewName());
    final List<String> secondViewPath = Arrays.asList(generateUniqueViewName());

    // create table
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(createViewQueryWithAt(viewPath, tablePath, DEFAULT_BRANCH_NAME));

    Thread.sleep(delayBetweenCommits.toMillis());
    final Instant timeInBetweenCommits = Instant.now();

    Thread.sleep(delayBetweenCommits.toMillis());
    runSQL(createViewQueryWithAt(secondViewPath, tablePath, DEFAULT_BRANCH_NAME));

    Thread.sleep(delayBetweenCommits.toMillis());
    final Instant timeAfterCommits = Instant.now();

    // ACT + ASSERT
    String showLogsWithTimestampBeforeCommit =
        String.format(
            "AT BRANCH %s AS OF '%s'", DEFAULT_BRANCH_NAME, Timestamp.from(timeInBetweenCommits));
    String showLogsWithTimestampAfterCommit =
        String.format(
            "AT BRANCH %s AS OF '%s'", DEFAULT_BRANCH_NAME, Timestamp.from(timeAfterCommits));
    List<List<String>> viewBefore =
        runSqlWithResults(showObjectWithSpecifierQuery("VIEWS", showLogsWithTimestampBeforeCommit));
    List<List<String>> viewsAfter =
        runSqlWithResults(showObjectWithSpecifierQuery("VIEWS", showLogsWithTimestampAfterCommit));
    assertThat(viewBefore).hasSize(1);
    assertThat(viewsAfter).hasSize(2);

    // Drop table + views
    runSQL(dropTableQuery(tablePath));
    runSQL(dropViewQuery(viewPath));
    runSQL(dropViewQuery(secondViewPath));
  }

  @Test
  public void testViewVersionUtilV0ViewForCreate() throws Exception {
    // Arrange
    // Enable V0 + disable V1 support key for creating views
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "true");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "false");
    final String viewName = generateUniqueViewName();
    final String tableName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> tablePath = tablePathWithFolders(tableName);

    runSQL(createTableAsQuery(tablePath, 5));
    runSQL(insertSelectQuery(tablePath, 5));
    runSQL(createViewQuery(viewPath, tablePath));
    String viewMetadataLocation = getMetadataLocationForViewKey(viewPath);
    FileIO fileIO = getFileIO(getDataplanePlugin());
    // Act
    IcebergViewMetadata.SupportedIcebergViewSpecVersion viewSpecVersion =
        IcebergUtils.findIcebergViewVersion(viewMetadataLocation, fileIO);
    // Assert
    assertThat(viewSpecVersion).isEqualTo(IcebergViewMetadata.SupportedIcebergViewSpecVersion.V0);
  }

  @Test
  public void testViewVersionUtilV0ViewForReplaceOrUpdate() throws Exception {
    // Arrange
    // Enable V0 + disable V1 support key for creating views
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "true");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "false");
    FileIO fileIO = getFileIO(getDataplanePlugin());
    String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    // Create table1 with 10 rows
    runSQL(createTableAsQuery(tablePath1, 10));
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(createViewQuery(viewKey, tablePath1));

    String viewMetadataLocation = getMetadataLocationForViewKey(viewKey);

    assertViewHasExpectedNumRows(viewKey, 10);
    ViewVersionMetadata origViewMetadata =
        ViewVersionMetadataParser.read(fileIO.newInputFile(viewMetadataLocation));
    assertThat(origViewMetadata.currentVersion().versionId()).isEqualTo(1);

    // Create table2 with 20 rows.
    runSQL(createTableAsQuery(tablePath2, 20));

    // Act
    runSQL(createReplaceViewQuery(viewKey, tablePath2));

    // Assert
    assertViewHasExpectedNumRows(viewKey, 20);
    viewMetadataLocation = getMetadataLocationForViewKey(viewKey);
    ViewVersionMetadata newViewMetadata =
        ViewVersionMetadataParser.read(fileIO.newInputFile(viewMetadataLocation));
    assertThat(newViewMetadata.currentVersion().versionId()).isEqualTo(2);
  }

  @Test
  public void testViewVersionUtilV1ViewForCreate() throws Exception {
    // Arrange
    final String viewName = generateUniqueViewName();
    final String tableName = generateUniqueTableName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> tablePath = tablePathWithFolders(tableName);

    runSQL(createTableAsQuery(tablePath, 5));
    runSQL(insertSelectQuery(tablePath, 5));
    runSQL(createViewQuery(viewPath, tablePath));
    String viewMetadataLocation = getMetadataLocationForViewKey(viewPath);
    FileIO fileIO = getFileIO(getDataplanePlugin());
    // Act
    InputFile inputFile = fileIO.newInputFile(viewMetadataLocation);

    ViewMetadata viewMetadata = ViewMetadataParser.read(inputFile);

    IcebergViewMetadata.SupportedIcebergViewSpecVersion viewSpecVersion =
        IcebergUtils.findIcebergViewVersion(viewMetadataLocation, fileIO);

    // Assert
    assertThat(viewMetadata.uuid()).isNotNull();
    assertThat(viewSpecVersion).isEqualTo(IcebergViewMetadata.SupportedIcebergViewSpecVersion.V1);
  }

  @Test
  public void testViewVersionUtilV1ViewForReplaceOrUpdate() throws Exception {
    // Arrange
    FileIO fileIO = getFileIO(getDataplanePlugin());
    String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    // Create table1 with 10 rows
    runSQL(createTableAsQuery(tablePath1, 10));
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(createViewQuery(viewKey, tablePath1));

    String viewMetadataLocation = getMetadataLocationForViewKey(viewKey);

    assertViewHasExpectedNumRows(viewKey, 10);
    ViewMetadata origViewMetadata =
        ViewMetadataParser.read(fileIO.newInputFile(viewMetadataLocation));
    assertThat(origViewMetadata.uuid()).isNotNull();

    // Create table2 with 20 rows.
    runSQL(createTableAsQuery(tablePath2, 20));

    // Act
    runSQL(createReplaceViewQuery(viewKey, tablePath2));

    // Assert
    assertViewHasExpectedNumRows(viewKey, 20);
    viewMetadataLocation = getMetadataLocationForViewKey(viewKey);
    ViewMetadata newViewMetadata =
        ViewMetadataParser.read(fileIO.newInputFile(viewMetadataLocation));
    assertThat(newViewMetadata.uuid()).isNotNull();
  }

  @Test
  public void testViewVersionUtilV1ViewForAlter() throws Exception {
    FileIO fileIO = getFileIO(getDataplanePlugin());
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);

    // Create a table with 10 rows.
    runSQL(createTableAsQuery(tablePath1, 10));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view.
    runSQL(createViewQuery(viewKey, tablePath1));
    String viewMetadataLocationBeforeAlter = getMetadataLocationForViewKey(viewKey);
    ViewMetadata origViewMetadata =
        ViewMetadataParser.read(fileIO.newInputFile(viewMetadataLocationBeforeAlter));
    assertThat(origViewMetadata.properties()).isNullOrEmpty();

    final String attribute = "enable_default_reflection";
    final String value = "true";

    runSQL(alterViewPropertyQuery(viewKey, attribute, value));
    String viewMetadataLocationAfterAlter = getMetadataLocationForViewKey(viewKey);
    ViewMetadata newViewMetadata =
        ViewMetadataParser.read(fileIO.newInputFile(viewMetadataLocationAfterAlter));
    assertThat(newViewMetadata.properties()).isNotNull();
  }

  @Test
  public void testTranslateV0ToV1AndUpdateOrReplaceV1View() throws Exception {
    // Arrange
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "false");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "false");
    FileIO fileIO = getFileIO(getDataplanePlugin());
    String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    // Create table1 with 10 rows
    runSQL(createTableAsQuery(tablePath1, 10));
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(createViewQuery(viewKey, tablePath1));

    String viewMetadataLocation = getMetadataLocationForViewKey(viewKey);

    assertViewHasExpectedNumRows(viewKey, 10);
    ViewVersionMetadata origViewMetadata =
        ViewVersionMetadataParser.read(fileIO.newInputFile(viewMetadataLocation));
    assertThat(origViewMetadata).isNotNull();

    // Create table2 with 20 rows.
    runSQL(createTableAsQuery(tablePath2, 20));

    // Switch over to v1 format when updating
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "false");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "true");

    // Act
    runSQL(createReplaceViewQuery(viewKey, tablePath2));

    // Assert
    assertViewHasExpectedNumRows(viewKey, 20);
    viewMetadataLocation = getMetadataLocationForViewKey(viewKey);
    ViewMetadata newViewMetadata =
        ViewMetadataParser.read(fileIO.newInputFile(viewMetadataLocation));
    assertThat(newViewMetadata.uuid()).isNotNull();
  }

  @Test
  public void testTranslateV1ToV0AndUpdateOrReplaceV0View() throws Exception {
    // Arrange
    FileIO fileIO = getFileIO(getDataplanePlugin());
    String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    // Create table1 with 10 rows
    runSQL(createTableAsQuery(tablePath1, 10));
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(createViewQuery(viewKey, tablePath1));

    String viewMetadataLocation = getMetadataLocationForViewKey(viewKey);

    assertViewHasExpectedNumRows(viewKey, 10);
    ViewMetadata origViewMetadata =
        ViewMetadataParser.read(fileIO.newInputFile(viewMetadataLocation));
    assertThat(origViewMetadata.uuid()).isNotNull();

    // Create table2 with 20 rows.
    runSQL(createTableAsQuery(tablePath2, 20));

    // Switch over to v0 format when updating
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "true");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "false");

    // Act
    runSQL(createReplaceViewQuery(viewKey, tablePath2));

    // Assert
    assertViewHasExpectedNumRows(viewKey, 20);
    viewMetadataLocation = getMetadataLocationForViewKey(viewKey);
    ViewVersionMetadata newViewMetadata =
        ViewVersionMetadataParser.read(fileIO.newInputFile(viewMetadataLocation));
    assertThat(newViewMetadata).isNotNull();
  }

  @Test
  public void testTranslateV0ToV1AndAlterV1View() throws Exception {
    // Enable V0 + disable V1 support key for creating views
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "true");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "false");
    FileIO fileIO = getFileIO(getDataplanePlugin());
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);

    // Create a table with 10 rows.
    runSQL(createTableAsQuery(tablePath1, 10));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view.
    runSQL(createViewQuery(viewKey, tablePath1));
    String viewMetadataLocationBeforeAlter = getMetadataLocationForViewKey(viewKey);
    ViewVersionMetadata origViewMetadata =
        ViewVersionMetadataParser.read(fileIO.newInputFile(viewMetadataLocationBeforeAlter));
    assertThat(origViewMetadata.properties()).isNullOrEmpty();

    // Switch over to v1 format when updating
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "false");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "true");

    final String attribute = "enable_default_reflection";
    final String value = "true";
    runSQL(alterViewPropertyQuery(viewKey, attribute, value));
    String viewMetadataLocationAfterAlter = getMetadataLocationForViewKey(viewKey);
    ViewMetadata newViewMetadata =
        ViewMetadataParser.read(fileIO.newInputFile(viewMetadataLocationAfterAlter));
    assertThat(newViewMetadata.properties()).isNotNull();
    assertThat(
            runSqlWithResults(
                selectStarQueryWithSpecifier(viewKey, "BRANCH " + DEFAULT_BRANCH_NAME)))
        .hasSize(10);
  }

  @Test
  public void testTranslateV1ToV0AndAlterV0View() throws Exception {
    FileIO fileIO = getFileIO(getDataplanePlugin());
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);

    // Create a table with 10 rows.
    runSQL(createTableAsQuery(tablePath1, 10));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view.
    runSQL(createViewQuery(viewKey, tablePath1));
    String viewMetadataLocationBeforeAlter = getMetadataLocationForViewKey(viewKey);
    ViewMetadata origViewMetadata =
        ViewMetadataParser.read(fileIO.newInputFile(viewMetadataLocationBeforeAlter));
    assertThat(origViewMetadata.properties()).isNullOrEmpty();

    // Switch over to v0 format when updating previous view
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "true");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "false");

    final String attribute = "enable_default_reflection";
    final String value = "true";
    runSQL(alterViewPropertyQuery(viewKey, attribute, value));
    String viewMetadataLocationAfterAlter = getMetadataLocationForViewKey(viewKey);
    ViewVersionMetadata newViewMetadata =
        ViewVersionMetadataParser.read(fileIO.newInputFile(viewMetadataLocationAfterAlter));
    assertThat(newViewMetadata.properties()).isNotNull();
  }

  @Test
  public void testCreateAndSelectV0ViewAndThenSelectWithV1SupportKeyOn() throws Exception {
    // Switch on v1 support key
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "true");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "false");
    FileIO fileIO = getFileIO(getDataplanePlugin());
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);

    // Create a table with 10 rows.
    runSQL(createTableAsQuery(tablePath1, 10));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view and select from it with V0 support key enabled
    runSQL(createViewQuery(viewKey, tablePath1));
    String viewMetadataLocationAfterAlter = getMetadataLocationForViewKey(viewKey);
    ViewVersionMetadata origViewMetadata =
        ViewVersionMetadataParser.read(fileIO.newInputFile(viewMetadataLocationAfterAlter));
    assertThat(origViewMetadata.properties()).isNullOrEmpty();
    // Perform select so view cache gets populated
    assertThat(
            runSqlWithResults(
                selectStarQueryWithSpecifier(viewKey, "BRANCH " + DEFAULT_BRANCH_NAME)))
        .hasSize(10);

    // Switch on v1 support key
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "false");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "true");
    // Select from view key with v1 support key enabled
    assertThat(
            runSqlWithResults(
                selectStarQueryWithSpecifier(viewKey, "BRANCH " + DEFAULT_BRANCH_NAME)))
        .hasSize(10);
  }

  @Test
  public void testCreateV0ViewAndThenSelectWithV1SupportKeyOn() throws Exception {
    // Switch on v0 support key
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "true");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "false");
    FileIO fileIO = getFileIO(getDataplanePlugin());
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);

    // Create a table with 10 rows.
    runSQL(createTableAsQuery(tablePath1, 10));

    final String viewName = generateUniqueViewName();
    final List<String> viewKey = tablePathWithFolders(viewName);

    // Create a view and select from it with V0 support key enabled
    runSQL(createViewQuery(viewKey, tablePath1));
    String viewMetadataLocationAfterAlter = getMetadataLocationForViewKey(viewKey);
    ViewVersionMetadata origViewMetadata =
        ViewVersionMetadataParser.read(fileIO.newInputFile(viewMetadataLocationAfterAlter));
    assertThat(origViewMetadata.properties()).isNullOrEmpty();

    // Switch on v1 support key
    setSystemOption(V0_ICEBERG_VIEW_WRITES, "false");
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, "true");
    // Select from view key with v1 support key enabled
    assertThat(
            runSqlWithResults(
                selectStarQueryWithSpecifier(viewKey, "BRANCH " + DEFAULT_BRANCH_NAME)))
        .hasSize(10);
  }

  @Test
  public void testCreateViewErrorExistingFolder() throws Exception {
    // Arrange
    final String name = generateUniqueFolderName();
    final List<String> path = Collections.singletonList(name);
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, path));

    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act and Assert
    assertQueryThrowsExpectedError(
        createViewQuery(path, tablePath),
        String.format(
            "An Entity of type FOLDER with given name [%s] already exists", joinedTableKey(path)));
  }

  private static AutoCloseable enableVersionedSourceUdf() {
    setSystemOption(CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getOptionName(), "true");
    return () ->
        setSystemOption(
            CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getOptionName(),
            CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getDefault().getBoolVal().toString());
  }
}

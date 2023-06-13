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

import static com.dremio.BaseTestQuery.test;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_COUNT_COLUMN;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertSelectQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.mergeBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.quoted;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectCountQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useCommitQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;

import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.api.Test;

import com.dremio.exec.catalog.CatalogEntityKey;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.TableVersionType;
import com.dremio.exec.catalog.VersionContext;

/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedSelectTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */

public class SelectTestCases {
  private ITDataplanePluginTestSetup base;

  SelectTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  @Test
  public void selectFromEmptyTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act and Assert
    base.assertTableHasExpectedNumRows(tablePath, 0);
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  void selectAfterDropTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(dropTableQuery(tablePath));

    // Act and Assert
    // Expect validation error. validateAndConvert calls VersionedDatasetAdapter#build . That returns null if unable to
    // get an IcebergDatasetHandle (VersionedDatasetAdapter#tryGetHandleToIcebergFormatPlugin).
    // The top level resolution then returns this error.
    base.assertQueryThrowsExpectedError(selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
      String.format("Object '%s' not found within '%s'",
        tablePath.get(0), DATAPLANE_PLUGIN_NAME));
  }
  @Test
  public void selectTableInNonExistentBranch() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    final String invalidBranch = "xyz";

    // Act and Assert
    base.assertQueryThrowsExpectedError(selectStarQueryWithSpecifier(tablePath, "BRANCH "+invalidBranch),
      String.format("Branch %s is not found",
        invalidBranch));
  }

  @Test
  public void selectWithSpecifiers() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();
    String firstTag = generateUniqueTagName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    long mtime1 = base.getMtimeForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 5);
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "BRANCH " + DEFAULT_BRANCH_NAME), DEFAULT_COUNT_COLUMN, 5);
    // Create tag
    base.runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    // Create dev branch
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    final TimestampString ts1 = TimestampString.fromMillisSinceEpoch(System.currentTimeMillis());

    // Switch to dev
    test("USE dfs_test");
    base.runSQL(useBranchQuery(devBranch));
    // Insert rows
    base.runSQL(insertSelectQuery(tablePath, 2));
    // Verify number of rows.
    long mtime2 = base.getMtimeForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, devBranch), base);
    final TimestampString ts2 = TimestampString.fromMillisSinceEpoch(System.currentTimeMillis());

    base.assertTableHasExpectedNumRows(tablePath, 7);
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "BRANCH " + DEFAULT_BRANCH_NAME), DEFAULT_COUNT_COLUMN, 5);
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "BRANCH " + devBranch), DEFAULT_COUNT_COLUMN, 7);
    assertThat(mtime2 > mtime1).isTrue();

    // on devBranch branch, at this timestamp
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "TIMESTAMP '" + ts1 + "'"), DEFAULT_COUNT_COLUMN, 5);
    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "TIMESTAMP '" + ts2 + "'"), DEFAULT_COUNT_COLUMN, 7);

    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "CURRENT_TIMESTAMP()" ), DEFAULT_COUNT_COLUMN, 7);

    // Switch back to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Verify number of rows
    base.assertTableHasExpectedNumRows(tablePath, 5);

    // Act
    // Merge dev to main
    base.runSQL(mergeBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Assert
    base.assertTableHasExpectedNumRows(tablePath, 7);
    long mtime3 = base.getMtimeForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    assertThat(mtime3 > mtime1).isTrue();
    assertThat(mtime3 == mtime2).isTrue();

    base.assertSQLReturnsExpectedNumRows(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN,
      "TAG " + firstTag), DEFAULT_COUNT_COLUMN, 5);

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
    CatalogEntityKey ckey = CatalogEntityKey.newBuilder()
      .keyComponents(tablePath)
      .tableVersionContext(new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME))
      .build();
    DremioTable droppedTable = CatalogUtil.getTable(ckey, base.getCatalog());

    base.assertQueryThrowsExpectedError(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
      String.format("Table '%s.%s' not found",
        DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)));

    assertThat(droppedTable).isNull();

  }

  @Test
  void selectAfterDropWithOlderTag() throws Exception {
    // Arrange
    String firstTag = generateUniqueTagName();
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Create table1 on default branch
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.assertTableHasExpectedNumRows(tablePath, 0);

    // Create a tag to mark it
    base.runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    // Drop table table1 on default branch
    base.runSQL(dropTableQuery(tablePath));

    // Ensure it cannot be selected from the tip of the branch
    base.assertQueryThrowsExpectedError(selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
      String.format("Object '%s' not found within '%s'", tablePath.get(0), DATAPLANE_PLUGIN_NAME));

    // Act
    // Go back to tag1
    base.runSQL(useTagQuery(firstTag));

    // Assert
    // Try to select from table1 - should succeed
    base.assertTableHasExpectedNumRows(tablePath, 0);

    // Go back to branch reference
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    base.assertQueryThrowsExpectedError(selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
      String.format("Table '%s.%s' not found",
        DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)));
  }

  @Test
  void selectUseCommit() throws Exception {
    // Arrange
    final String tableOnMainAndBranchName = generateUniqueTableName();
    final List<String> tableOnMainAndBranchPath = tablePathWithFolders(tableOnMainAndBranchName);
    final int tableOnMainAndBranchNumRows = 10;
    final String tableOnBranchOnlyName = generateUniqueTableName();
    final List<String> tableOnBranchOnlyPath = tablePathWithFolders(tableOnBranchOnlyName);
    final int tableOnBranchOnlyNumRows = 15;
    final String branchName = generateUniqueBranchName();

    // "tableOnMainAndBranch" in both main and new branch
    // "tableOnBranchOnly" only in new branch
    String commitHashMainAtBeginning = base.getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    createFolders(tableOnMainAndBranchPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tableOnMainAndBranchPath, tableOnMainAndBranchNumRows));
    String commitHashMainAfterTable = base.getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(branchName));
    createFolders(tableOnBranchOnlyPath, VersionContext.ofBranch(branchName));
    base.runSQL(createTableAsQuery(tableOnBranchOnlyPath, tableOnBranchOnlyNumRows));
    String commitHashBranchAfterTable = base.getCommitHashForBranch(branchName);

    // Act + Assert
    base.runSQL(useCommitQuery(commitHashBranchAfterTable));
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(tableOnBranchOnlyPath, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      tableOnBranchOnlyNumRows);
    base.runSQL(useCommitQuery(commitHashMainAfterTable));
    base.assertSQLReturnsExpectedNumRows(
      selectCountQuery(tableOnMainAndBranchPath, DEFAULT_COUNT_COLUMN),
      DEFAULT_COUNT_COLUMN,
      tableOnMainAndBranchNumRows);
    base.runSQL(useCommitQuery(commitHashMainAtBeginning));
    base.assertQueryThrowsExpectedError(
      selectCountQuery(
        tableOnMainAndBranchPath,
        DEFAULT_COUNT_COLUMN),
      "not found");
  }

  @Test
  void selectCommitAt() throws Exception {
    // Arrange
    final String tableOnMainAndBranchName = generateUniqueTableName();
    final List<String> tableOnMainAndBranchPath = tablePathWithFolders(tableOnMainAndBranchName);
    final int tableOnMainAndBranchNumRows = 10;
    final String tableOnBranchOnlyName = generateUniqueTableName();
    final List<String> tableOnBranchOnlyPath = tablePathWithFolders(tableOnBranchOnlyName);
    final int tableOnBranchOnlyNumRows = 15;
    final String branchName = generateUniqueBranchName();

    // "tableOnMainAndBranch" in both main and new branch
    // "tableOnBranchOnly" only in new branch
    String commitHashMainAtBeginning = base.getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    createFolders(tableOnMainAndBranchPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tableOnMainAndBranchPath, tableOnMainAndBranchNumRows));
    String commitHashMainAfterTable = base.getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(branchName));
    createFolders(tableOnBranchOnlyPath, VersionContext.ofBranch(branchName));
    base.runSQL(createTableAsQuery(tableOnBranchOnlyPath, tableOnBranchOnlyNumRows));
    String commitHashBranchAfterTable = base.getCommitHashForBranch(branchName);

    // Act + Assert
    base.assertSQLReturnsExpectedNumRows(
      selectCountQueryWithSpecifier(
        tableOnBranchOnlyPath,
        DEFAULT_COUNT_COLUMN,
        "COMMIT " + quoted(commitHashBranchAfterTable)),
      DEFAULT_COUNT_COLUMN,
      tableOnBranchOnlyNumRows);
    base.assertSQLReturnsExpectedNumRows(
      selectCountQueryWithSpecifier(
        tableOnMainAndBranchPath,
        DEFAULT_COUNT_COLUMN,
        "COMMIT " + quoted(commitHashMainAfterTable)),
      DEFAULT_COUNT_COLUMN,
      tableOnMainAndBranchNumRows);
    base.assertQueryThrowsExpectedError(
      selectCountQueryWithSpecifier(
        tableOnMainAndBranchPath,
        DEFAULT_COUNT_COLUMN,
        "COMMIT " + quoted(commitHashMainAtBeginning)),
      "not found");
  }

  /**
   * @throws Exception test case for select * from table(table_snapshot('icebergTable'))
   */
  @Test
  public void icebergSnapshotMFunctionSQL() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    base.assertTableHasExpectedNumOfSnapshots(tablePath, 1);

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }
  /**
   * @throws Exception test case for select * from table(table_files('icebergTable'))
   */
  @Test
  public void icebergTableFilesMFunctionSQL() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    base.assertTableHasExpectedNumOfDataFiles(tablePath, 1);

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void selectStarVersionedTableWithQuotedPath() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath1));
    base.runSqlWithResults(String.format("select * from \"%s.%s.%s\".\"%s\"", DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1), tablePath1.get(2)));
  }

}

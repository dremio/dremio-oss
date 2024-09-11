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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_COUNT_COLUMN;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.FIRST_DEFAULT_VALUE_CLAUSE;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.SECOND_DEFAULT_VALUE_CLAUSE;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createPartitionTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertSelectQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.mergeBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.pathWithoutTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.quoted;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountTablePartitionQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithSource;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useCommitQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useContextQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useTagQuery;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.dremio.service.namespace.NamespaceKey;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginSelect extends ITDataplanePluginTestSetup {

  @Test
  public void selectFromEmptyTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    runSQL(createEmptyTableQuery(tablePath));

    // Act and Assert
    assertTableHasExpectedNumRows(tablePath, 0);
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void selectFromEmptyTableWithAt() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    runSQL(createEmptyTableQueryWithAt(tablePath, devBranch));

    // Act and Assert
    assertTableAtBranchHasExpectedNumRows(tablePath, devBranch, 0);
    runSQL(dropTableQueryWithAt(tablePath, devBranch));
  }

  @Test
  void selectAfterDropTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    runSQL(createEmptyTableQuery(tablePath));
    runSQL(dropTableQuery(tablePath));

    // Act and Assert
    // Expect validation error. validateAndConvert calls VersionedDatasetAdapter#build . That
    // returns null if unable to
    // get an IcebergDatasetHandle (VersionedDatasetAdapter#tryGetHandleToIcebergFormatPlugin).
    // The top level resolution then returns this error.
    assertQueryThrowsExpectedError(
        selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
        String.format(
            "Object '%s' not found within '%s.%s'",
            tablePath.get(tablePath.size() - 1),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(pathWithoutTableName(tablePath))));
  }

  @Test
  public void selectTableInNonExistentBranch() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableAsQuery(tablePath, 5));
    final String invalidBranch = "xyz";

    // Act and Assert
    assertQueryThrowsExpectedError(
        selectStarQueryWithSpecifier(tablePath, "BRANCH " + invalidBranch),
        String.format(
            "VALIDATION ERROR: Table '%s' not found",
            joinedTableKey(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath))));
  }

  @Test
  public void selectWithSpecifiers() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();
    String firstTag = generateUniqueTagName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath, 5));
    long mtime1 =
        getMtimeForTable(
            tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 5);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
        DEFAULT_COUNT_COLUMN,
        5);
    // Create tag
    runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    // Create dev branch
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    final TimestampString ts1 = TimestampString.fromMillisSinceEpoch(System.currentTimeMillis());

    // Switch to dev
    test("USE dfs_test");
    runSQL(useBranchQuery(devBranch));
    // Insert rows
    runSQL(insertSelectQuery(tablePath, 2));
    // Verify number of rows.
    long mtime2 =
        getMtimeForTable(
            tablePath, new TableVersionContext(TableVersionType.BRANCH, devBranch), this);
    final TimestampString ts2 = TimestampString.fromMillisSinceEpoch(System.currentTimeMillis());

    assertTableHasExpectedNumRows(tablePath, 7);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
        DEFAULT_COUNT_COLUMN,
        5);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + devBranch),
        DEFAULT_COUNT_COLUMN,
        7);
    assertThat(mtime2 > mtime1).isTrue();

    // on devBranch branch, at this timestamp
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN, "TIMESTAMP '" + ts1 + "'"),
        DEFAULT_COUNT_COLUMN,
        5);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN, "TIMESTAMP '" + ts2 + "'"),
        DEFAULT_COUNT_COLUMN,
        7);

    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN, "CURRENT_TIMESTAMP()"),
        DEFAULT_COUNT_COLUMN,
        7);

    // Switch back to main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Verify number of rows
    assertTableHasExpectedNumRows(tablePath, 5);

    // Act
    // Merge dev to main
    runSQL(mergeBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Assert
    assertTableHasExpectedNumRows(tablePath, 7);
    long mtime3 =
        getMtimeForTable(
            tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    assertThat(mtime3 > mtime1).isTrue();
    assertThat(mtime3 == mtime2).isTrue();

    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(tablePath, DEFAULT_COUNT_COLUMN, "TAG " + firstTag),
        DEFAULT_COUNT_COLUMN,
        5);

    // Cleanup
    runSQL(dropTableQuery(tablePath));
    CatalogEntityKey ckey =
        CatalogEntityKey.newBuilder()
            .keyComponents(tablePath)
            .tableVersionContext(
                new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME))
            .build();
    DremioTable droppedTable = getCatalog().getTable(ckey);

    assertQueryThrowsExpectedError(
        selectCountQueryWithSpecifier(
            tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
        String.format("Table '%s.%s' not found", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)));

    assertThat(droppedTable).isNull();
  }

  @Test
  void selectAfterDropWithOlderTag() throws Exception {
    // Arrange
    String firstTag = generateUniqueTagName();
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Create table1 on default branch
    runSQL(createEmptyTableQuery(tablePath));
    assertTableHasExpectedNumRows(tablePath, 0);

    // Create a tag to mark it
    runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    // Drop table table1 on default branch
    runSQL(dropTableQuery(tablePath));

    // Ensure it cannot be selected from the tip of the branch
    assertQueryThrowsExpectedError(
        selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
        String.format(
            "Object '%s' not found within '%s.%s'",
            tablePath.get(tablePath.size() - 1),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(pathWithoutTableName(tablePath))));

    // Act
    // Go back to tag1
    runSQL(useTagQuery(firstTag));

    // Assert
    // Try to select from table1 - should succeed
    assertTableHasExpectedNumRows(tablePath, 0);

    // Go back to branch reference
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    assertQueryThrowsExpectedError(
        selectCountQueryWithSpecifier(
            tablePath, DEFAULT_COUNT_COLUMN, "BRANCH " + DEFAULT_BRANCH_NAME),
        String.format("Table '%s.%s' not found", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)));
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
    String commitHashMainAtBeginning = getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    runSQL(createTableAsQuery(tableOnMainAndBranchPath, tableOnMainAndBranchNumRows));
    String commitHashMainAfterTable = getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));
    runSQL(createTableAsQuery(tableOnBranchOnlyPath, tableOnBranchOnlyNumRows));
    String commitHashBranchAfterTable = getCommitHashForBranch(branchName);

    // Act + Assert
    runSQL(useCommitQuery(commitHashBranchAfterTable));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(tableOnBranchOnlyPath, DEFAULT_COUNT_COLUMN),
        DEFAULT_COUNT_COLUMN,
        tableOnBranchOnlyNumRows);
    runSQL(useCommitQuery(commitHashMainAfterTable));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(tableOnMainAndBranchPath, DEFAULT_COUNT_COLUMN),
        DEFAULT_COUNT_COLUMN,
        tableOnMainAndBranchNumRows);
    runSQL(useCommitQuery(commitHashMainAtBeginning));
    assertQueryThrowsExpectedError(
        selectCountQuery(tableOnMainAndBranchPath, DEFAULT_COUNT_COLUMN), "not found");
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
    String commitHashMainAtBeginning = getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    runSQL(createTableAsQuery(tableOnMainAndBranchPath, tableOnMainAndBranchNumRows));
    String commitHashMainAfterTable = getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));
    runSQL(createTableAsQuery(tableOnBranchOnlyPath, tableOnBranchOnlyNumRows));
    String commitHashBranchAfterTable = getCommitHashForBranch(branchName);

    // Act + Assert
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            tableOnBranchOnlyPath,
            DEFAULT_COUNT_COLUMN,
            "COMMIT " + quoted(commitHashBranchAfterTable)),
        DEFAULT_COUNT_COLUMN,
        tableOnBranchOnlyNumRows);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(
            tableOnMainAndBranchPath,
            DEFAULT_COUNT_COLUMN,
            "COMMIT " + quoted(commitHashMainAfterTable)),
        DEFAULT_COUNT_COLUMN,
        tableOnMainAndBranchNumRows);
    assertQueryThrowsExpectedError(
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
    runSQL(createTableAsQuery(tablePath, 5));
    assertTableHasExpectedNumOfSnapshots(tablePath, 1);

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void icebergSnapshotMFunctionSQLWithContextSetToVersioned() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    runSQL(String.format("use %s", DATAPLANE_PLUGIN_NAME));
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    runSQL(createTableAsQuery(tablePath, 5));
    assertTableHasExpectedNumOfSnapshots(tablePath, 1);

    // Cleanup
    runSQL(dropTableQuery(tablePath));
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
    runSQL(createTableAsQuery(tablePath, 5));
    assertTableHasExpectedNumOfDataFiles(tablePath, 1);

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void icebergPartitionTablesMFunctionSQL() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    runSQL(createPartitionTableQuery(tablePath));
    assertTableHasExpectedNumOfTablePartitionFiles(tablePath, 0);

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void icebergPartitionTablesMFunctionSQLThrowsException() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    List<String> namespaceKeyPath = new ArrayList<>(tablePath);
    namespaceKeyPath.add(0, DATAPLANE_PLUGIN_NAME);
    final NamespaceKey namespaceKey = new NamespaceKey(namespaceKeyPath);

    // Act
    runSQL(createEmptyTableQuery(tablePath));
    // Assert
    assertQueryThrowsExpectedError(
        selectCountTablePartitionQuery(tablePath, DEFAULT_COUNT_COLUMN),
        String.format("Table %s is not partitioned.", namespaceKey.getSchemaPath()));

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void icebergPartitionTablesMFunctionSQLInDevBranchThrowsException() throws Exception {
    // Arrange
    final String devBranch = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    List<String> namespaceKeyPath = new ArrayList<>(tablePath);
    namespaceKeyPath.add(0, DATAPLANE_PLUGIN_NAME);
    final NamespaceKey namespaceKey = new NamespaceKey(namespaceKeyPath);
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Act + Assert
    runSQL(useBranchQuery(devBranch));
    runSQL(createEmptyTableQuery(tablePath));
    assertQueryThrowsExpectedError(
        selectCountTablePartitionQuery(tablePath, DEFAULT_COUNT_COLUMN),
        String.format("Table %s is not partitioned.", namespaceKey.getSchemaPath()));

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void selectStarVersionedTableWithQuotedPath() throws Exception {
    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createEmptyTableQuery(tablePath1));
    runSqlWithResults(
        String.format(
            "select * from \"%s.%s.%s\".\"%s\"",
            DATAPLANE_PLUGIN_NAME, tablePath1.get(0), tablePath1.get(1), tablePath1.get(2)));
  }

  @Test
  public void selectFromBranchAsOfTimestamp() throws Exception {
    // Arrange
    final String devBranch = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();

    // Avoid main branch to reduce chance of collisions with other tests
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));

    // Make commits by adding tables (any kind of commit would work)
    runSQL(createEmptyTableQuery(Collections.singletonList(tableName)));

    // Set up a commit where we know the time before and after
    runSQL(
        insertTableWithValuesQuery(
            Collections.singletonList(tableName),
            Collections.singletonList(FIRST_DEFAULT_VALUE_CLAUSE)));
    final Instant timeInBetweenCommits = Instant.now();
    runSQL(
        insertTableWithValuesQuery(
            Collections.singletonList(tableName),
            Collections.singletonList(SECOND_DEFAULT_VALUE_CLAUSE)));

    // Act
    assertThat(
            runSqlWithResults(
                selectStarQueryWithSpecifier(
                    Collections.singletonList(tableName),
                    "BRANCH "
                        + devBranch
                        + " AS OF '"
                        + Timestamp.from(timeInBetweenCommits)
                        + "'")))
        .hasSize(1);
  }

  @Test
  public void selectFromTagAsOfTimestamp() throws Exception {
    // Arrange
    final String devBranch = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final String tagName = generateUniqueTagName();

    // Avoid main branch to reduce chance of collisions with other tests
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));

    // Make commits by adding tables (any kind of commit would work)
    runSQL(createEmptyTableQuery(Collections.singletonList(tableName)));

    // Set up a commit where we know the time before and after
    runSQL(
        insertTableWithValuesQuery(
            Collections.singletonList(tableName),
            Collections.singletonList(FIRST_DEFAULT_VALUE_CLAUSE)));
    runSQL(createTagQuery(tagName, devBranch));
    final Instant timeInBetweenCommits = Instant.now();
    runSQL(
        insertTableWithValuesQuery(
            Collections.singletonList(tableName),
            Collections.singletonList(SECOND_DEFAULT_VALUE_CLAUSE)));

    // Act
    assertThat(
            runSqlWithResults(
                selectStarQueryWithSpecifier(
                    Collections.singletonList(tableName),
                    "TAG " + tagName + " AS OF '" + Timestamp.from(timeInBetweenCommits) + "'")))
        .hasSize(1);
  }

  @Test
  public void selectFromRefAsOfTimestampBeforeAnyCommits() throws Exception {
    // Arrange
    final String devBranch = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();

    // Avoid main branch to reduce chance of collisions with other tests
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));

    // Make commits by adding tables (any kind of commit would work)
    final Instant timeBeforeCommits = Instant.now();
    runSQL(createEmptyTableQuery(Collections.singletonList(tableName)));

    // Set up a commit where we know the time before and after
    runSQL(
        insertTableWithValuesQuery(
            Collections.singletonList(tableName),
            Collections.singletonList(FIRST_DEFAULT_VALUE_CLAUSE)));
    runSQL(
        insertTableWithValuesQuery(
            Collections.singletonList(tableName),
            Collections.singletonList(SECOND_DEFAULT_VALUE_CLAUSE)));

    // Act
    assertQueryThrowsExpectedError(
        selectStarQueryWithSpecifier(
            Collections.singletonList(tableName),
            "REF " + devBranch + " AS OF '" + Timestamp.from(timeBeforeCommits) + "'"),
        String.format(
            "VALIDATION ERROR: Table '%s.%s' not found", DATAPLANE_PLUGIN_NAME, tableName));
  }

  @Test
  public void selectFromCommitAsOfTimestampThrowsValidationException() throws Exception {
    // Arrange
    final String devBranch = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();

    // Avoid main branch to reduce chance of collisions with other tests
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));

    // Make commits by adding tables (any kind of commit would work)
    final Instant timeBeforeCommits = Instant.now();
    runSQL(createEmptyTableQuery(Collections.singletonList(tableName)));

    // Set up a commit where we know the time before and after
    runSQL(
        insertTableWithValuesQuery(
            Collections.singletonList(tableName),
            Collections.singletonList(FIRST_DEFAULT_VALUE_CLAUSE)));
    runSQL(
        insertTableWithValuesQuery(
            Collections.singletonList(tableName),
            Collections.singletonList(SECOND_DEFAULT_VALUE_CLAUSE)));
    final String hash = getCommitHashForBranch(devBranch);
    // Act
    assertQueryThrowsExpectedError(
        selectStarQueryWithSpecifier(
            Collections.singletonList(tableName),
            "COMMIT " + quoted(hash) + " AS OF '" + Timestamp.from(timeBeforeCommits) + "'"),
        String.format("Reference type 'Commit' cannot be used with AS OF syntax."));
  }

  @Test
  public void selectFromViewAtBranchAsOfTimestamp() throws Exception {
    // Arrange
    final String devBranch = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final String viewName = generateUniqueViewName();

    // Avoid main branch to reduce chance of collisions with other tests
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));

    // Make commits by adding tables (any kind of commit would work)
    runSQL(createEmptyTableQuery(Collections.singletonList(tableName)));
    runSQL(
        createViewQuery(Collections.singletonList(viewName), Collections.singletonList(tableName)));

    // Set up a commit where we know the time before and after
    runSQL(
        insertTableWithValuesQuery(
            Collections.singletonList(tableName),
            Collections.singletonList(FIRST_DEFAULT_VALUE_CLAUSE)));
    final Instant timeInBetweenCommits = Instant.now();
    runSQL(
        insertTableWithValuesQuery(
            Collections.singletonList(tableName),
            Collections.singletonList(SECOND_DEFAULT_VALUE_CLAUSE)));

    // Act
    assertThat(
            runSqlWithResults(
                selectStarQueryWithSpecifier(
                    Collections.singletonList(viewName),
                    "BRANCH "
                        + devBranch
                        + " AS OF '"
                        + Timestamp.from(timeInBetweenCommits)
                        + "'")))
        .hasSize(1);
  }

  @Test
  public void testPathContextSetToOneFolder() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    runSQL(createEmptyTableQuery(table1Path));
    runSQL(insertTableQuery(table1Path));
    runSQL(useContextQuery(Arrays.asList(DATAPLANE_PLUGIN_NAME, table1Path.get(0))));
    assertTableAtBranchHasExpectedNumRows(table1Path, DEFAULT_BRANCH_NAME, 3);
  }

  @Test
  public void testPathContextSetToOneFolderInNonDefaultContext() throws Exception {
    final String table1 = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();

    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    final List<String> table1Path = tablePathWithFolders(table1);
    runSQL(createEmptyTableQuery(table1Path));
    runSQL(insertTableQuery(table1Path));
    runSQL(useContextQuery(Arrays.asList(DATAPLANE_PLUGIN_NAME, table1Path.get(0))));
    assertTableAtBranchHasExpectedNumRows(table1Path, devBranch, 3);
  }

  @Test
  public void testPathContextSetToOneFolderInWrongContext() throws Exception {
    // Arrange
    final String table1 = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();

    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    final List<String> table1Path = tablePathWithFolders(table1);
    runSQL(createEmptyTableQuery(table1Path));
    runSQL(insertTableQuery(table1Path));
    runSQL(useContextQuery(Arrays.asList(DATAPLANE_PLUGIN_NAME, table1Path.get(0))));

    // Act and Assert
    assertQueryThrowsExpectedError(
        selectCountAtBranchQuery(table1Path, DEFAULT_BRANCH_NAME, DEFAULT_COUNT_COLUMN),
        String.format(
            "Table '%s.%s' not found", DATAPLANE_PLUGIN_NAME, joinedTableKey(table1Path)));
  }

  @Test
  public void testPathContextSetToMultipleFolder() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    runSQL(createEmptyTableQuery(table1Path));
    runSQL(insertTableQuery(table1Path));
    runSQL(
        useContextQuery(
            Arrays.asList(DATAPLANE_PLUGIN_NAME, table1Path.get(0), table1Path.get(1))));
    assertTableAtBranchHasExpectedNumRows(table1Path, DEFAULT_BRANCH_NAME, 3);
  }

  @Test
  public void testPathContextSetToNonExistentFolder() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    runSQL(createEmptyTableQuery(table1Path));
    runSQL(insertTableQuery(table1Path));
    assertQueryThrowsExpectedError(
        useContextQuery(Arrays.asList(DATAPLANE_PLUGIN_NAME, "foo")),
        String.format(
            "VALIDATION ERROR: Schema [%s.%s] is not valid with respect to either root schema or current default schema.",
            DATAPLANE_PLUGIN_NAME, "foo"));
  }

  @Test
  public void testPathContextSetToFolderWithTimeTravelQuery() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    runSQL(createEmptyTableQuery(table1Path));
    long mtime1 =
        getMtimeForTable(
            table1Path,
            new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME),
            this);
    final TimestampString ts1 = TimestampString.fromMillisSinceEpoch(mtime1);
    runSQL(insertTableQuery(table1Path));
    // Verify with select
    runSQL(useContextQuery(Arrays.asList(DATAPLANE_PLUGIN_NAME, table1Path.get(0))));
    assertTableHasExpectedNumRows(table1Path, 3);
    assertSQLReturnsExpectedNumRows(
        selectCountQueryWithSpecifier(table1Path, DEFAULT_COUNT_COLUMN, "TIMESTAMP '" + ts1 + "'"),
        DEFAULT_COUNT_COLUMN,
        0);
  }

  @Test
  public void testPathContextSetToFolderWithTimeTravelQueryWrongContext() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    final String devBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    runSQL(createEmptyTableQuery(table1Path));
    long mtime1 =
        getMtimeForTable(
            table1Path, new TableVersionContext(TableVersionType.BRANCH, devBranch), this);
    final TimestampString ts1 = TimestampString.fromMillisSinceEpoch(mtime1);
    runSQL(insertTableQuery(table1Path));
    // Verify with select
    runSQL(useContextQuery(Arrays.asList(DATAPLANE_PLUGIN_NAME, table1Path.get(0))));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    assertQueryThrowsExpectedError(
        selectCountQueryWithSpecifier(table1Path, DEFAULT_COUNT_COLUMN, "TIMESTAMP '" + ts1 + "'"),
        String.format(
            "VALIDATION ERROR: Table '%s.%s' not found",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(table1Path)));
  }
}

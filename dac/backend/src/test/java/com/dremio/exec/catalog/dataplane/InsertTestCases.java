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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateFolderPath;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertSelectQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.mergeBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useCommitQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertIcebergFilesExistAtSubPath;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.TableVersionType;
import com.dremio.exec.catalog.VersionContext;

/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedInsertTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */
public class InsertTestCases {
  private ITDataplanePluginTestSetup base;

  InsertTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  @Test
  public void insertIntoEmpty() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act
    base.runSQL(insertTableQuery(tablePath));

    // Assert
    base.assertTableHasExpectedNumRows(tablePath, 3);

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void insertSelect() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    base.assertTableHasExpectedNumRows(tablePath, 5);

    // Act
    base.runSQL(insertSelectQuery(tablePath, 3));

    // Assert
    // Verify number of rows with select
    base.assertTableHasExpectedNumRows(tablePath, 8);

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void insertWithCommitSet() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    String commitHashBranch = base.getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    base.runSQL(useCommitQuery(commitHashBranch));

    // Act and Assert
    base.assertQueryThrowsExpectedError(insertTableQuery(tablePath),
      String.format("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
        ResolvedVersionContext.DETACHED_REF_NAME));
  }

  // Verify insert creates underlying iceberg files in the right locations
  @Test
  public void insertSelectVerifyFolders() throws Exception {
    // Arrange
    // Create a hierarchy of 2 folders to form key of TABLE
    final List<String> tablePath = Arrays.asList("if1", "if2", generateUniqueTableName());
    final String tableKey = joinedTableKey(tablePath);
    final String createTableQuery = String.format(
      "CREATE TABLE %s.%s %s",
      DATAPLANE_PLUGIN_NAME,
      tableKey,
      "(nation_key int, region_key int)");

    // Create empty
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableQuery);
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(tablePath, 0, 1, 1, 0);

    // Do 2 separate Inserts so there are multiple data files.
    // Insert 1
    base.runSQL(insertSelectQuery(tablePath, 2));
    base.assertTableHasExpectedNumRows(tablePath, 2);
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(tablePath, 1, 2, 2, 1);

    // Insert 2
    base.runSQL(insertSelectQuery(tablePath, 3));
    // Verify number of rows with select
    base.assertTableHasExpectedNumRows(tablePath, 5);

    // Assert
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(tablePath, 2, 3, 3, 2);

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void insertInDiffBranchesAndConflicts() throws Exception {
    // Arrange
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);
    final String devBranchName = generateUniqueBranchName();

    // Set context to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(mainTablePath));
    base.assertTableHasExpectedNumRows(mainTablePath, 0);

    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // insert into table on main branch
    base.runSQL(insertTableQuery(mainTablePath));
    base.assertTableHasExpectedNumRows(mainTablePath, 3);
    long mtime1 = base.getMtimeForTable(mainTablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // switch to branch dev
    base.runSQL(useBranchQuery(devBranchName));

    // insert into table on dev branch so there will be conflicts
    base.runSQL(insertTableQuery(mainTablePath));
    base.assertTableHasExpectedNumRows(mainTablePath, 3);
    long mtime2 = base.getMtimeForTable(mainTablePath, new TableVersionContext(TableVersionType.BRANCH, devBranchName), base);
    // switch to branch dev
    // Act and Assert
    base.assertQueryThrowsExpectedError(mergeBranchQuery(devBranchName, DEFAULT_BRANCH_NAME),
      String.format(("VALIDATION ERROR: Merge branch %s into branch %s failed due to commit conflict on source %s"),
        devBranchName, DEFAULT_BRANCH_NAME, DATAPLANE_PLUGIN_NAME));
    assertThat(mtime2 > mtime1).isTrue();
    // Drop tables
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(mainTablePath));
  }

  @Test
  public void insertInDiffBranchesAndMerge() throws Exception {
    // Arrange
    final List<String> shareFolderPath = generateFolderPath(generateUniqueFolderName());
    final String mainTableName = generateUniqueTableName();
    final String devTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);
    final List<String> devTablePath = tablePathWithFolders(devTableName);
    final String devBranchName = generateUniqueBranchName();

    // Creating an arbitrary commit to Nessie to make a common ancestor between two branches otherwise
    // those are un-related branches
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(shareFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Set context to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(mainTablePath));
    base.assertTableHasExpectedNumRows(mainTablePath, 0);

    // Insert into table main
    base.runSQL(insertTableQuery(mainTablePath));
    base.assertTableHasExpectedNumRows(mainTablePath, 3);


    // switch to branch dev
    base.runSQL(useBranchQuery(devBranchName));
    // Check that table does not exist in Nessie in branch dev (since it was branched off before create table)
    base.assertQueryThrowsExpectedError(selectCountQuery(mainTablePath, DEFAULT_COUNT_COLUMN),
      String.format("VALIDATION ERROR: Object '%s' not found within '%s",
        mainTablePath.get(0),
        DATAPLANE_PLUGIN_NAME));
    createFolders(devTablePath, VersionContext.ofBranch(devBranchName));
    base.runSQL(createEmptyTableQuery(devTablePath));
    base.assertTableHasExpectedNumRows(devTablePath, 0);

    // Insert into table dev
    base.runSQL(insertTableQuery(devTablePath));
    base.assertTableHasExpectedNumRows(devTablePath, 3);

    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Check that dev table cannot be seen in branch main
    base.assertQueryThrowsExpectedError(selectCountQuery(devTablePath, DEFAULT_COUNT_COLUMN),
      String.format("VALIDATION ERROR: Object '%s' not found within '%s",
        devTablePath.get(0),
        DATAPLANE_PLUGIN_NAME));

    // Act
    base.runSQL(mergeBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Assert and checking records in both tables
    // Table must now be visible in main.
    base.assertTableHasExpectedNumRows(devTablePath, 3);
    base.assertTableHasExpectedNumRows(mainTablePath, 3);

    // Drop tables
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(mainTablePath));
    base.runSQL(dropTableQuery(devTablePath));
  }

  /**
   * Ctas in main branch
   * Insert in dev branch
   * Compare row counts in each branch
   * Merge branch to main branch and compare row count again
   */
  @Test
  public void insertAndCtasInDifferentBranches() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 5);
    long mtime1 = base.getMtimeForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Create dev branch
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    // Switch to dev
    base.runSQL(useBranchQuery(devBranch));
    // Insert rows
    base.runSQL(insertSelectQuery(tablePath, 2));
    // Verify number of rows.
    base.assertTableHasExpectedNumRows(tablePath, 7);
    // Switch back to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Verify number of rows
    base.assertTableHasExpectedNumRows(tablePath, 5);

    // Act
    // Merge dev to main
    base.runSQL(mergeBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    long mtime2 = base.getMtimeForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Assert
    base.assertTableHasExpectedNumRows(tablePath, 7);
    assertThat(mtime2 > mtime1).isTrue();
    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  /**
   * The inserts should write data files relative to the table base location, and agnostic of the source configuration.
   * Create a table, insert some records
   * Create a different source with a dummy bucket path as root location
   * Make further inserts, operation should succeed
   * Verify the records
   */
  @Test
  public void insertAgnosticOfSourceBucket() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act
    base.runSQL(insertTableQuery(tablePath));
    base.runWithAlternateSourcePath(insertTableQuery(tablePath));

    // Assert rows from both inserts
    base.assertTableHasExpectedNumRows(tablePath, 6);
    base.assertAllFilesAreInBaseBucket(tablePath);

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void insertInDifferentTablesWithSameName() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Create table with this name in the main branch, insert records
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    base.runSQL(insertSelectQuery(tablePath,5));

    // Create table with this name in the dev branch, different source path, insert records
    base.runSQL(useBranchQuery(devBranch));
    createFolders(tablePath, VersionContext.ofBranch(devBranch));
    base.runWithAlternateSourcePath(createTableAsQuery(tablePath, 5));
    base.runSQL(insertSelectQuery(tablePath, 5));

    // Act: Assert the paths are correct in each branch
    base.assertAllFilesInAlternativeBucket(tablePath); // dev branch

    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.assertAllFilesAreInBaseBucket(tablePath);

    // cleanup
    base.runSQL(useBranchQuery(devBranch));
    base.runSQL(dropTableQuery(tablePath));

    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(tablePath));
    base.runSQL(dropBranchQuery(devBranch));
  }
}

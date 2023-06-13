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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableIfExistsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertIcebergTableExistsAtSubPath;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieDoesNotHaveTable;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasCommitForTable;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasTable;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.projectnessie.model.Operation;

import com.dremio.exec.catalog.VersionContext;

/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedDropTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */
public class DropTestCases {
  private ITDataplanePluginTestSetup base;

  DropTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  @Test
  public void dropTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act
    base.runSQL(dropTableQuery(tablePath));
    // TODO Check for correct message

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Delete.class, DEFAULT_BRANCH_NAME, base);
    assertNessieDoesNotHaveTable(tablePath, DEFAULT_BRANCH_NAME, base);
    // TODO For now, we aren't doing filesystem cleanup, so this check is correct. Might change in the future.
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void dropNonExistentTable() {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act and Assert
    base.assertQueryThrowsExpectedError(dropTableQuery(tablePath),
      "does not exist");
  }

  @Test
  public void dropIfExistsNonExistentTable() throws Exception {
    // Arrange
    // Expect no error for non existent table
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    base.runSQL(dropTableIfExistsQuery(tablePath));

    // Assert
    // No exception
  }

  @Test
  public void dropIfExistsExistingTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act
    base.runSQL(dropTableIfExistsQuery(tablePath));

    // Assert
    // No exception
  }

  @Test
  public void dropExistingTableTwice() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    // Ensure contents
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertIcebergTableExistsAtSubPath(tablePath);

    // First drop
    base.runSQL(dropTableQuery(tablePath));
    // Assert removal of key from Nessie
    assertNessieDoesNotHaveTable(tablePath, DEFAULT_BRANCH_NAME, base);
    // Contents must still exist
    assertIcebergTableExistsAtSubPath(tablePath);

    // Act
    // Try second drop
    base.assertQueryThrowsExpectedError(dropTableQuery(tablePath),
      "does not exist");

    // Assert
    // Contents must still exist
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void dropSameTableNameDifferentBranches() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranchName = generateUniqueBranchName();

    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.assertTableHasExpectedNumRows(tablePath, 0);

    // Switch to branch dev
    base.runSQL(useBranchQuery(devBranchName));
    // Now try to create the same table in that branch - should succeed.
    createFolders(tablePath, VersionContext.ofBranch(devBranchName));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.assertTableHasExpectedNumRows(tablePath, 0);

    // Act
    base.runSQL(dropTableQuery(tablePath));
    // ensure it's dropped in dev branch
    base.assertQueryThrowsExpectedError(selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
      String.format("Object '%s' not found within '%s'", tablePath.get(0), DATAPLANE_PLUGIN_NAME));
    // Switch back to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Assert
    // Ensure table is still there
    base.assertTableHasExpectedNumRows(tablePath, 0);

    base.runSQL(dropTableQuery(tablePath));
    // Now ensure it's gone in main too
    base.assertQueryThrowsExpectedError(selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
      String.format("Object '%s' not found within '%s'", tablePath.get(0), DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void dropTableInNonBranchVersionContext() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String tag = generateUniqueTagName();
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    base.runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    base.runSQL(useTagQuery(tag));

    // Act and Assert
    base.assertQueryThrowsExpectedError(dropTableQuery(tablePath),
            String.format("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
                    tag));
  }

}

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

import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.deleteAllQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.deleteAllQueryWithoutContext;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useContextQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.dremio.exec.catalog.VersionContext;

/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedDeleteTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */
public class DeleteTestCases {
  private ITDataplanePluginTestSetup base;

  DeleteTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  @Test
  public void deleteAll() throws Exception {
    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 3);
    // Create dev branch
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    // Switch to dev
    base.runSQL(useBranchQuery(devBranch));

    // Act
    base.runSQL(deleteAllQuery(tablePath));

    // Assert
    base.assertTableHasExpectedNumRows(tablePath, 0);
    // Check that main context still has the table
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.assertTableHasExpectedNumRows(tablePath, 3);

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void deleteAllWithContext() throws Exception {
    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 3);
    // Create dev branch
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    // Switch to dev
    base.runSQL(useBranchQuery(devBranch));

    // Act
    base.runSQL(useContextQuery());
    base.runSQL(deleteAllQueryWithoutContext(tablePath));

    // Assert
    base.assertTableHasExpectedNumRows(tablePath, 0);
    // Check that main context still has the table
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.assertTableHasExpectedNumRows(tablePath, 3);

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void deleteAllInATag() throws Exception {
    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    String tag = generateUniqueTagName();
    final String devBranch = generateUniqueBranchName();
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 3);
    // Create a tag to mark it
    base.runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));

    // Switch to the tag
    base.runSQL(useTagQuery(tag));

    base.assertQueryThrowsExpectedError(deleteAllQuery(tablePath), "DDL and DML operations are only supported for branches - not on tags or commits");

    // Check that main context still has the table
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.assertTableHasExpectedNumRows(tablePath, 3);

    //cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void deleteAgnosticOfSourceBucket() throws Exception {
    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));

    // Act
    base.runWithAlternateSourcePath(deleteAllQuery(tablePath));

    // Assert
    base.assertTableHasExpectedNumRows(tablePath, 0);

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void deleteWithTagSet() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));
    final String tag = generateUniqueTagName();
    // Act and Assert
    base.runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    base.runSQL(useTagQuery(tag));
    base.assertQueryThrowsExpectedError(deleteAllQuery(tablePath),
            String.format("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
                    tag));
  }
}

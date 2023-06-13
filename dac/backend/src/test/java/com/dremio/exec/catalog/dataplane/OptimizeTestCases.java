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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.OptimizeMode.REWRITE_ALL;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.OptimizeMode.REWRITE_DATA;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.OptimizeMode.REWRITE_MANIFESTS;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertSelectQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.optimizeTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static com.dremio.exec.planner.OptimizeOutputSchema.NEW_DATA_FILES_COUNT;
import static com.dremio.exec.planner.OptimizeOutputSchema.OPTIMIZE_OUTPUT_SUMMARY;
import static com.dremio.exec.planner.OptimizeOutputSchema.REWRITTEN_DATA_FILE_COUNT;
import static com.dremio.exec.planner.OptimizeOutputSchema.REWRITTEN_DELETE_FILE_COUNT;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.dremio.exec.catalog.VersionContext;


/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedOptimizeTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */
public class OptimizeTestCases {
  private ITDataplanePluginTestSetup base;

  OptimizeTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  @Test
  public void optimizeNewTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act
    base.runSQL(insertTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));

    // Verify
    base.testBuilder()
      .sqlQuery(optimizeTableQuery(tablePath, REWRITE_ALL))
      .unOrdered()
      .baselineColumns(REWRITTEN_DATA_FILE_COUNT, REWRITTEN_DELETE_FILE_COUNT, NEW_DATA_FILES_COUNT)
      .baselineValues(2L, 0L, 1L).go();

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void optimizeDataFilesOnly() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act
    base.runSQL(insertTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));

    // Verify
    base.testBuilder()
      .sqlQuery(optimizeTableQuery(tablePath, REWRITE_DATA))
      .unOrdered()
      .baselineColumns(REWRITTEN_DATA_FILE_COUNT, REWRITTEN_DELETE_FILE_COUNT, NEW_DATA_FILES_COUNT)
      .baselineValues(2L, 0L, 1L).go();

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void optimizeManifestsOnly() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act
    base.runSQL(insertTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));

    // Verify
    base.testBuilder()
      .sqlQuery(optimizeTableQuery(tablePath, REWRITE_MANIFESTS))
      .unOrdered()
      .baselineColumns(OPTIMIZE_OUTPUT_SUMMARY)
      .baselineValues("Optimize table successful").go();

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void optimizeAgnosticOfSourceBucket() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act
    base.runSQL(insertTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));

    // Verify
    base.runWithAlternateSourcePath(optimizeTableQuery(tablePath, REWRITE_ALL));
    base.assertAllFilesAreInBaseBucket(tablePath);
    base.assertTableHasExpectedNumOfDataFiles(tablePath, 1);

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void optimizeInDifferentBranches() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Prepare data
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    base.runSQL(insertSelectQuery(tablePath, 2));

    // Create dev branch and switch to that
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(devBranch));

    // Insert and optimize in dev branch
    base.runSQL(insertSelectQuery(tablePath, 2));
    base.testBuilder()
      .sqlQuery(optimizeTableQuery(tablePath, REWRITE_ALL))
      .unOrdered()
      .baselineColumns(REWRITTEN_DATA_FILE_COUNT, REWRITTEN_DELETE_FILE_COUNT, NEW_DATA_FILES_COUNT)
      .baselineValues(3L, 0L, 1L).go();

    // Optimize in main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.testBuilder()
      .sqlQuery(optimizeTableQuery(tablePath, REWRITE_ALL))
      .unOrdered()
      .baselineColumns(REWRITTEN_DATA_FILE_COUNT, REWRITTEN_DELETE_FILE_COUNT, NEW_DATA_FILES_COUNT)
      .baselineValues(2L, 0L, 1L).go();

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }
}

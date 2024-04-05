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

import static com.dremio.exec.catalog.dataplane.test.DataplaneStorage.BucketSelection.PRIMARY_BUCKET;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.OptimizeMode.REWRITE_ALL;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.OptimizeMode.REWRITE_DATA;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.OptimizeMode.REWRITE_MANIFESTS;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertSelectQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.optimizeTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.planner.OptimizeOutputSchema.NEW_DATA_FILES_COUNT;
import static com.dremio.exec.planner.OptimizeOutputSchema.OPTIMIZE_OUTPUT_SUMMARY;
import static com.dremio.exec.planner.OptimizeOutputSchema.REWRITTEN_DATA_FILE_COUNT;
import static com.dremio.exec.planner.OptimizeOutputSchema.REWRITTEN_DELETE_FILE_COUNT;

import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginOptimize extends ITDataplanePluginTestSetup {

  @Test
  public void optimizeNewTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(insertTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    // Verify
    testBuilder()
        .sqlQuery(optimizeTableQuery(tablePath, REWRITE_ALL))
        .unOrdered()
        .baselineColumns(
            REWRITTEN_DATA_FILE_COUNT, REWRITTEN_DELETE_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(2L, 0L, 1L)
        .go();

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void optimizeDataFilesOnly() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(insertTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    // Verify
    testBuilder()
        .sqlQuery(optimizeTableQuery(tablePath, REWRITE_DATA))
        .unOrdered()
        .baselineColumns(
            REWRITTEN_DATA_FILE_COUNT, REWRITTEN_DELETE_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(2L, 0L, 1L)
        .go();

    assertCommitLogTail(
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)),
        String.format("OPTIMIZE REWRITE DATA on TABLE %s", joinedTableKey(tablePath)));

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void optimizeManifestsOnly() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(insertTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    // Verify
    testBuilder()
        .sqlQuery(optimizeTableQuery(tablePath, REWRITE_MANIFESTS))
        .unOrdered()
        .baselineColumns(OPTIMIZE_OUTPUT_SUMMARY)
        .baselineValues("Optimize table successful")
        .go();

    assertCommitLogTail(
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)),
        String.format("OPTIMIZE REWRITE MANIFESTS on TABLE %s", joinedTableKey(tablePath)));

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void optimizeAgnosticOfSourceBucket() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(insertTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    // Verify
    runWithAlternateSourcePath(optimizeTableQuery(tablePath, REWRITE_ALL));
    assertAllFilesAreInBucket(tablePath, PRIMARY_BUCKET);
    assertTableHasExpectedNumOfDataFiles(tablePath, 1);

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void optimizeInDifferentBranches() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Prepare data
    runSQL(createTableAsQuery(tablePath, 5));
    runSQL(insertSelectQuery(tablePath, 2));

    // Create dev branch and switch to that
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));

    // Insert and optimize in dev branch
    runSQL(insertSelectQuery(tablePath, 2));
    testBuilder()
        .sqlQuery(optimizeTableQuery(tablePath, REWRITE_ALL))
        .unOrdered()
        .baselineColumns(
            REWRITTEN_DATA_FILE_COUNT, REWRITTEN_DELETE_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(3L, 0L, 1L)
        .go();

    assertCommitLogTail(
        VersionContext.ofBranch(DEFAULT_BRANCH_NAME),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)));

    assertCommitLogTail(
        VersionContext.ofBranch(devBranch),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)), // on branch
        String.format("OPTIMIZE REWRITE DATA on TABLE %s", joinedTableKey(tablePath)),
        String.format("OPTIMIZE REWRITE MANIFESTS on TABLE %s", joinedTableKey(tablePath)));

    // Optimize in main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    testBuilder()
        .sqlQuery(optimizeTableQuery(tablePath, REWRITE_ALL))
        .unOrdered()
        .baselineColumns(
            REWRITTEN_DATA_FILE_COUNT, REWRITTEN_DELETE_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(2L, 0L, 1L)
        .go();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }
}

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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.rollbackTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.dremio.exec.catalog.VersionContext;

/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedRollbackTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */
public class RollbackTestCases {
  private ITDataplanePluginTestSetup base;

  RollbackTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  @Test
  public void rollbackToTimestamp() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));

    // Verify select results
    base.assertTableHasExpectedNumRows(tablePath, 3);
    base.assertTableHasExpectedNumOfSnapshots(tablePath, 2);

    // Timestamp to rollback
    final long timestampMillis = System.currentTimeMillis();

    // Add an extra row and increase one more snapshot
    base.runSQL(insertTableWithValuesQuery(tablePath,
      Collections.singletonList("(4, CAST('fourth row' AS VARCHAR(65536)), CAST(4000 AS DECIMAL(38,3)))")));

    // Verify select results again
    base.assertTableHasExpectedNumRows(tablePath, 4);
    base.assertTableHasExpectedNumOfSnapshots(tablePath, 3);

    // Run rollback query
    base.testBuilder()
      .sqlQuery(rollbackTableQuery(tablePath, timestampMillis))
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, String.format("Table [%s.%s] rollbacked", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)))
      .go();

    // Rollback changes the number of rows, but not affect number of snapshots.
    base.assertTableHasExpectedNumRows(tablePath, 3);
    base.assertTableHasExpectedNumOfSnapshots(tablePath, 3);

    // Verify select results after rollback
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("id", "name", "distance")
      .baselineValues(1, "first row", new BigDecimal("1000.000"))
      .baselineValues(2, "second row", new BigDecimal("2000.000"))
      .baselineValues(3, "third row", new BigDecimal("3000.000"))
      .go();

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void rollbackInDifferentBranches() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));

    // Verify select results in main branch
    base.assertTableHasExpectedNumRows(tablePath, 3);
    base.assertTableHasExpectedNumOfSnapshots(tablePath, 2);

    // Timestamp to rollback
    final long timestampMillis = System.currentTimeMillis();

    // Create dev branch and switch to that
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(devBranch));

    // Add row into dev branch and increase one more snapshot
    base.runSQL(insertTableWithValuesQuery(tablePath,
      Collections.singletonList("(4, CAST('fourth row' AS VARCHAR(65536)), CAST(4000 AS DECIMAL(38,3)))")));

    // Verify select results in dev branch
    base.assertTableHasExpectedNumRows(tablePath, 4);
    base.assertTableHasExpectedNumOfSnapshots(tablePath, 3);

    // Run rollback query in Dev branch
    base.testBuilder()
      .sqlQuery(rollbackTableQuery(tablePath, timestampMillis))
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, String.format("Table [%s.%s] rollbacked", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)))
      .go();

    // Verify select results after rollback
    base.assertTableHasExpectedNumRows(tablePath, 3);
    base.assertTableHasExpectedNumOfSnapshots(tablePath, 3);

    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("id", "name", "distance")
      .baselineValues(1, "first row", new BigDecimal("1000.000"))
      .baselineValues(2, "second row", new BigDecimal("2000.000"))
      .baselineValues(3, "third row", new BigDecimal("3000.000"))
      .go();

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }
}

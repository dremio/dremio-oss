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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.rollbackTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;

import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginRollback extends ITDataplanePluginTestSetup {

  @Test
  public void rollbackToTimestamp() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    // Verify select results
    assertTableHasExpectedNumRows(tablePath, 3);
    assertTableHasExpectedNumOfSnapshots(tablePath, 2);

    // Timestamp to rollback
    final long timestampMillis = System.currentTimeMillis();

    // Add an extra row and increase one more snapshot
    runSQL(
        insertTableWithValuesQuery(
            tablePath,
            Collections.singletonList(
                "(4, CAST('fourth row' AS VARCHAR(65536)), CAST(4000 AS DECIMAL(38,3)))")));

    // Verify select results again
    assertTableHasExpectedNumRows(tablePath, 4);
    assertTableHasExpectedNumOfSnapshots(tablePath, 3);

    // Run rollback query
    testBuilder()
        .sqlQuery(rollbackTableQuery(tablePath, timestampMillis))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(
            true,
            String.format(
                "Table [%s.%s] rollbacked", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)))
        .go();

    // Rollback changes the number of rows, but not affect number of snapshots.
    assertTableHasExpectedNumRows(tablePath, 3);
    assertTableHasExpectedNumOfSnapshots(tablePath, 3);

    // Verify select results after rollback
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("id", "name", "distance")
        .baselineValues(1, "first row", new BigDecimal("1000.000"))
        .baselineValues(2, "second row", new BigDecimal("2000.000"))
        .baselineValues(3, "third row", new BigDecimal("3000.000"))
        .go();

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void rollbackInDifferentBranches() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    // Verify select results in main branch
    assertTableHasExpectedNumRows(tablePath, 3);
    assertTableHasExpectedNumOfSnapshots(tablePath, 2);

    // Timestamp to rollback
    final long timestampMillis = System.currentTimeMillis();

    // Create dev branch and switch to that
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));

    // Add row into dev branch and increase one more snapshot
    runSQL(
        insertTableWithValuesQuery(
            tablePath,
            Collections.singletonList(
                "(4, CAST('fourth row' AS VARCHAR(65536)), CAST(4000 AS DECIMAL(38,3)))")));

    // Verify select results in dev branch
    assertTableHasExpectedNumRows(tablePath, 4);
    assertTableHasExpectedNumOfSnapshots(tablePath, 3);

    // Run rollback query in Dev branch
    testBuilder()
        .sqlQuery(rollbackTableQuery(tablePath, timestampMillis))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(
            true,
            String.format(
                "Table [%s.%s] rollbacked", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)))
        .go();

    // Verify select results after rollback
    assertTableHasExpectedNumRows(tablePath, 3);
    assertTableHasExpectedNumOfSnapshots(tablePath, 3);

    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("id", "name", "distance")
        .baselineValues(1, "first row", new BigDecimal("1000.000"))
        .baselineValues(2, "second row", new BigDecimal("2000.000"))
        .baselineValues(3, "third row", new BigDecimal("3000.000"))
        .go();

    // Cleanup on main
    runSQL(dropTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    assertCommitLogTail(
        VersionContext.ofBranch(DEFAULT_BRANCH_NAME),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)),
        String.format("DROP TABLE %s", joinedTableKey(tablePath)));

    assertCommitLogTail(
        VersionContext.ofBranch(devBranch),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)), // on branch
        String.format("ROLLBACK on TABLE %s", joinedTableKey(tablePath)) // on branch
        // NO DROP on branch
        );
  }
}

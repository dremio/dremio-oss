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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableAddColumnsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableWithColDefsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.descTableAtBranch;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.descTableAtBranchColumn;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeTable;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeTableAtBranch;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeTableAtBranchColumn;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeTableAtCommit;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeTableAtSnapshot;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeTableAtTimeStamp;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeTableTableAtBranch;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeTableTableAtBranchColumn;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.planner.sql.handlers.SqlHandlerUtil.getTimestampFromMillis;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginDescribe extends ITDataplanePluginTestSetup {

  @Test
  public void testDescribeTableBranch() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Arrays.asList("col2 int", "col3 int", "col4 varchar");
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Change table in main branch.
    runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));

    // Assert
    // Use DESCRIBE <TABLE> (not using AT syntax)
    assertThat(runSqlWithResults(describeTable(tablePath)))
        .anyMatch(row -> (row.get(0).equals("col1")))
        .anyMatch(row -> (row.get(0).equals("col2")))
        .anyMatch(row -> (row.get(0).equals("col3")))
        .anyMatch(row -> (row.get(0).equals("col4")))
        .hasSize(4);

    // Use DESC <TABLE> AT BRANCH main COLUMN
    assertThat(runSqlWithResults(descTableAtBranchColumn(tablePath, DEFAULT_BRANCH_NAME, "col1")))
        .anyMatch(row -> (row.get(0).equals("col1")))
        .hasSize(1);

    // Use DESCRIBE <TABLE> AT BRANCH main COLUMN
    assertThat(
            runSqlWithResults(describeTableAtBranchColumn(tablePath, DEFAULT_BRANCH_NAME, "col1")))
        .anyMatch(row -> (row.get(0).equals("col1")))
        .hasSize(1);

    // Use DESCRIBE TABLE <TABLE> AT BRANCH main COLUMN
    assertThat(
            runSqlWithResults(
                describeTableTableAtBranchColumn(tablePath, DEFAULT_BRANCH_NAME, "col1")))
        .anyMatch(row -> (row.get(0).equals("col1")))
        .hasSize(1);

    // Use DESCRIBE <TABLE> AT BRANCH ... (using AT syntax)
    assertThat(runSqlWithResults(describeTableAtBranch(tablePath, devBranch)))
        .anyMatch(row -> (row.get(0).equals("col1")))
        .hasSize(1);

    assertThat(runSqlWithResults(descTableAtBranch(tablePath, devBranch)))
        .anyMatch(row -> (row.get(0).equals("col1")))
        .hasSize(1);

    assertThat(runSqlWithResults(describeTableTableAtBranch(tablePath, devBranch)))
        .anyMatch(row -> (row.get(0).equals("col1")))
        .hasSize(1);
  }

  @Test
  public void testDescribeTableWrongBranchThrows() throws Exception {
    final String tableName = generateUniqueTableName();
    final String nonExistentBranch = "foo";
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));

    assertQueryThrowsExpectedError(
        describeTableTableAtBranch(tablePath, nonExistentBranch),
        "VALIDATION ERROR: Unknown table");

    assertQueryThrowsExpectedError(
        descTableAtBranch(tablePath, nonExistentBranch), "VALIDATION ERROR: Unknown table");

    assertQueryThrowsExpectedError(
        describeTableAtBranch(tablePath, nonExistentBranch), "VALIDATION ERROR: Unknown table");
  }

  @Test
  public void testDescribeTableCommit() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String commitHash = getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    assertThat(runSqlWithResults(describeTableAtCommit(tablePath, commitHash)))
        .anyMatch(row -> (row.get(0).equals("id")))
        .anyMatch(row -> (row.get(0).equals("name")))
        .anyMatch(row -> (row.get(0).equals("distance")))
        .hasSize(3);
  }

  @Test
  public void testDescribeTableWrongCommitThrows() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String nonExistentCommitHash = "abcdef1234511111";

    assertQueryThrowsExpectedError(
        describeTableAtCommit(tablePath, nonExistentCommitHash), "VALIDATION ERROR");
  }

  @Test
  public void testDescribeTableSnapshot() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tablePathWithSource =
        DataplaneTestDefines.tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);

    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(tablePathWithSource)
            .tableVersionContext(
                new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME))
            .build();

    final String snapshotId =
        String.valueOf(
            getCatalog()
                .getTable(catalogEntityKey)
                .getDatasetConfig()
                .getPhysicalDataset()
                .getIcebergMetadata()
                .getSnapshotId());
    // Assert
    assertThat(runSqlWithResults(describeTableAtSnapshot(tablePath, snapshotId)))
        .anyMatch(row -> (row.get(0).equals("id")))
        .anyMatch(row -> (row.get(0).equals("name")))
        .anyMatch(row -> (row.get(0).equals("distance")))
        .hasSize(3);
  }

  @Test
  public void testDescribeTableWrongSnapshotThrows() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String nonExistentSnapshotId = "123123123";

    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    assertQueryThrowsExpectedError(
        describeTableAtSnapshot(tablePath, nonExistentSnapshotId), "VALIDATION ERROR");
  }

  @Test
  public void testDescribeTableTimestamp() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    TableVersionContext contextTimestamp =
        new TableVersionContext(
            TableVersionType.TIMESTAMP,
            Timestamp.valueOf(getTimestampFromMillis(System.currentTimeMillis())).getTime());
    String timestamp = contextTimestamp.toSql();

    assertThat(runSqlWithResults(describeTableAtTimeStamp(tablePath, timestamp)))
        .anyMatch(row -> (row.get(0).equals("id")))
        .anyMatch(row -> (row.get(0).equals("name")))
        .anyMatch(row -> (row.get(0).equals("distance")))
        .hasSize(3);
  }

  @Test
  public void testDescribeTableWrongTimestampThrows() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    TableVersionContext wrongTimeStamp =
        new TableVersionContext(
            TableVersionType.TIMESTAMP,
            Timestamp.valueOf(getTimestampFromMillis(11111111)).getTime());
    String wrongTimestamp = wrongTimeStamp.toSql();

    assertQueryThrowsExpectedError(
        describeTableAtTimeStamp(tablePath, wrongTimestamp), "VALIDATION ERROR");
  }
}

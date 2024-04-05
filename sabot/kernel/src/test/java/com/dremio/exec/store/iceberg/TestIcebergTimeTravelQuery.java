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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_TIME_TRAVEL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.ArrowDsUtil;
import com.dremio.TestBuilder;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.UserExceptionAssert;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.util.TimestampString;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HistoryEntry;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test class for Iceberg time travel query */
public class TestIcebergTimeTravelQuery extends BaseIcebergTable {

  private static long firstTimestampMs;
  private static TimestampString firstTimestamp;
  private static long firstSnapshotId;

  private static long secondTimestampMs;
  private static TimestampString secondTimestamp;
  private static long secondSnapshotId;
  private static Catalog catalog;

  @BeforeClass
  public static void initTable() throws Exception {
    final CatalogServiceImpl catalogService =
        (CatalogServiceImpl) getSabotContext().getCatalogService();
    catalog = catalogService.getSystemUserCatalog();
    createIcebergTable();
    setSystemOption(ENABLE_ICEBERG_TIME_TRAVEL, "true");
    final BaseTable table = new BaseTable(ops, tableName);
    final List<HistoryEntry> entries = table.history();
    Assert.assertEquals(2, entries.size());

    firstSnapshotId = entries.get(0).snapshotId();
    firstTimestampMs = entries.get(0).timestampMillis();
    firstTimestamp = TimestampString.fromMillisSinceEpoch(firstTimestampMs);

    secondSnapshotId = entries.get(1).snapshotId();
    secondTimestampMs = entries.get(1).timestampMillis();
    secondTimestamp = TimestampString.fromMillisSinceEpoch(secondTimestampMs);
  }

  @Test
  public void atFirstSnapshotId() throws Exception {
    expectFirstSnapshot(
        "SELECT * FROM dfs_hadoop.\"%s\" AT SNAPSHOT '%d'", tableFolder.toPath(), firstSnapshotId);
  }

  @Test
  public void atSecondSnapshotId() throws Exception {
    test("use dfs_test");
    expectSecondSnapshot(
        "SELECT * FROM dfs_hadoop.\"%s\" AT SNAPSHOT '%d'", tableFolder.toPath(), secondSnapshotId);
  }

  @Test
  public void atIncorrectSnapshotId() {
    UserExceptionAssert.assertThatThrownBy(
            () ->
                test("SELECT * FROM dfs_hadoop.\"%s\" AT SNAPSHOT '%d'", tableFolder.toPath(), 345))
        .hasMessageContaining("the provided snapshot ID '%d' is invalid", 345)
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.VALIDATION);
  }

  @Test
  public void atBranch() {
    UserExceptionAssert.assertThatThrownBy(
            () -> test("SELECT * FROM dfs_hadoop.\"%s\" AT BRANCH foo", tableFolder.toPath(), 345))
        .hasMessageContaining("does not support AT BRANCH/TAG/COMMIT specification")
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.VALIDATION);
  }

  @Test
  public void atTag() {
    UserExceptionAssert.assertThatThrownBy(
            () -> test("SELECT * FROM dfs_hadoop.\"%s\" AT TAG foo", tableFolder.toPath(), 345))
        .hasMessageContaining("does not support AT BRANCH/TAG/COMMIT specification")
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.VALIDATION);
  }

  @Test
  public void atCommit() {
    UserExceptionAssert.assertThatThrownBy(
            () -> test("SELECT * FROM dfs_hadoop.\"%s\" AT COMMIT foo", tableFolder.toPath(), 345))
        .hasMessageContaining("does not support AT BRANCH/TAG/COMMIT specification")
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.VALIDATION);
  }

  @Test
  public void incorrectName() {
    UserExceptionAssert.assertThatThrownBy(
            () -> test("SELECT * FROM dfs_hadoop.blah AT SNAPSHOT '%d'", 345))
        .hasMessageContaining("not found")
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.VALIDATION);
  }

  @Test
  public void atFirstTimestamp() throws Exception {
    expectFirstSnapshot(
        "SELECT * FROM dfs_hadoop.\"%s\" AT TIMESTAMP '%s'", tableFolder.toPath(), firstTimestamp);
    expectFirstSnapshot(
        "SELECT * FROM dfs_hadoop.\"%s\" AT TIMESTAMP '%s'",
        tableFolder.toPath(), TimestampString.fromMillisSinceEpoch(secondTimestampMs - 2));
  }

  @Test
  public void atSecondTimestamp() throws Exception {
    expectSecondSnapshot(
        "SELECT * FROM dfs_hadoop.\"%s\" AT TIMESTAMP '%s'", tableFolder.toPath(), secondTimestamp);
    expectSecondSnapshot(
        "SELECT * FROM dfs_hadoop.\"%s\" AT TIMESTAMP '%s'",
        tableFolder.toPath(), TimestampString.fromMillisSinceEpoch(secondTimestampMs + 2));
  }

  @Test
  public void atIncorrectTimestamp1() {
    UserExceptionAssert.assertThatThrownBy(
            () ->
                test(
                    "SELECT * FROM dfs_hadoop.\"%s\" AT TIMESTAMP '%s'",
                    tableFolder.toPath(),
                    TimestampString.fromMillisSinceEpoch(firstTimestampMs - 2_000)))
        .hasMessageContaining("out of range")
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.VALIDATION);
  }

  @Test
  public void atIncorrectTimestamp2() {
    UserExceptionAssert.assertThatThrownBy(
            () ->
                test(
                    "SELECT * FROM dfs_hadoop.\"%s\" AT TIMESTAMP '%s'",
                    tableFolder.toPath(),
                    TimestampString.fromMillisSinceEpoch(System.currentTimeMillis() + 10_000_000)))
        .hasMessageContaining("out of range")
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.VALIDATION);
  }

  @Test
  public void withUnion() throws Exception {
    String sql =
        String.format(
            "select 'snap1' as snap, t1.* from dfs_hadoop.\"%s\" at snapshot '%d' t1\n"
                + "union all\n"
                + "select 'snap2' as snap, t2.* from dfs_hadoop.\"%s\" at snapshot '%d' t2",
            tableFolder.toPath(), firstSnapshotId, tableFolder.toPath(), secondSnapshotId);
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("snap", "col1", "col2", "col3", "col4", "col5", "col6")
        .baselineValues(
            "snap1",
            TestBuilder.listOf(2, 3, 4),
            TestBuilder.mapOf("x", 1, "y", 2L),
            TestBuilder.listOf(2.3f, 3.4f, 4.5f),
            TestBuilder.mapOf("x", 1.2f, "y", 2L),
            ArrowDsUtil.listOfDecimals(Arrays.asList("21.456", "3.123", "41.456")),
            TestBuilder.mapOf("x", new BigDecimal("14.500"), "y", 2L))
        .baselineValues(
            "snap2",
            TestBuilder.listOf(2, 3, 4),
            TestBuilder.mapOf("x", 1, "y", 2L),
            TestBuilder.listOf(2.3f, 3.4f, 4.5f),
            TestBuilder.mapOf("x", 1.2f, "y", 2L),
            ArrowDsUtil.listOfDecimals(Arrays.asList("21.456", "3.123", "41.456")),
            TestBuilder.mapOf("x", new BigDecimal("14.500"), "y", 2L))
        .baselineValues(
            "snap2",
            TestBuilder.listOf(2, 3, 4),
            TestBuilder.mapOf("x", 1, "y", 2L),
            TestBuilder.listOf(2.3f, 3.4f, 4.5f),
            TestBuilder.mapOf("x", 1.2f, "y", 2L),
            ArrowDsUtil.listOfDecimals(Arrays.asList("21.456", "3.123", "41.456")),
            TestBuilder.mapOf("x", new BigDecimal("14.500"), "y", 2L))
        .go();
  }

  private void expectFirstSnapshot(String query, Object... args) throws Exception {
    testBuilder()
        .sqlQuery(query, args)
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4", "col5", "col6")
        .baselineValues(
            TestBuilder.listOf(2, 3, 4),
            TestBuilder.mapOf("x", 1, "y", 2L),
            TestBuilder.listOf(2.3f, 3.4f, 4.5f),
            TestBuilder.mapOf("x", 1.2f, "y", 2L),
            ArrowDsUtil.listOfDecimals(Arrays.asList("21.456", "3.123", "41.456")),
            TestBuilder.mapOf("x", new BigDecimal("14.500"), "y", 2L))
        .build()
        .run();
  }

  private void expectSecondSnapshot(String query, Object... args) throws Exception {
    testBuilder()
        .sqlQuery(query, args)
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4", "col5", "col6")
        .baselineValues(
            TestBuilder.listOf(2, 3, 4),
            TestBuilder.mapOf("x", 1, "y", 2L),
            TestBuilder.listOf(2.3f, 3.4f, 4.5f),
            TestBuilder.mapOf("x", 1.2f, "y", 2L),
            ArrowDsUtil.listOfDecimals(Arrays.asList("21.456", "3.123", "41.456")),
            TestBuilder.mapOf("x", new BigDecimal("14.500"), "y", 2L))
        .baselineValues(
            TestBuilder.listOf(2, 3, 4),
            TestBuilder.mapOf("x", 1, "y", 2L),
            TestBuilder.listOf(2.3f, 3.4f, 4.5f),
            TestBuilder.mapOf("x", 1.2f, "y", 2L),
            ArrowDsUtil.listOfDecimals(Arrays.asList("21.456", "3.123", "41.456")),
            TestBuilder.mapOf("x", new BigDecimal("14.500"), "y", 2L))
        .build()
        .run();
  }

  @Test
  public void testVersionedDatasetIdWithSnapshot() throws JsonProcessingException {
    String query =
        String.format(
            "SELECT * FROM dfs_hadoop.\"%s\" AT SNAPSHOT '%d'",
            tableFolder.toPath(), firstSnapshotId);
    List<String> keyPath = Arrays.asList("dfs_hadoop", tableFolder.toString());
    DremioTable snapshotTable =
        catalog.getTableSnapshotForQuery(
            CatalogEntityKey.newBuilder()
                .keyComponents(keyPath)
                .tableVersionContext(
                    new TableVersionContext(
                        TableVersionType.SNAPSHOT_ID, String.valueOf(firstSnapshotId)))
                .build());
    VersionedDatasetId versionedDatasetId =
        VersionedDatasetId.fromString(snapshotTable.getDatasetConfig().getId().getId());
    DremioTable tableFromDatasetId = catalog.getTable(versionedDatasetId.asString());
    assertThat(snapshotTable.getDatasetConfig().equals(tableFromDatasetId.getDatasetConfig()))
        .isTrue();
  }

  @Test
  public void testVersionedDatasetIdWithTimestamp() throws JsonProcessingException {
    String query =
        String.format(
            "SELECT * FROM dfs_hadoop.\"%s\" AT TIMESTAMP '%s'",
            tableFolder.toPath(), secondTimestamp);
    List<String> keyPath = Arrays.asList("dfs_hadoop", tableFolder.toString());
    DremioTable timestampTable =
        catalog.getTableSnapshotForQuery(
            CatalogEntityKey.newBuilder()
                .keyComponents(keyPath)
                .tableVersionContext(
                    new TableVersionContext(TableVersionType.TIMESTAMP, secondTimestampMs))
                .build());
    VersionedDatasetId versionedDatasetId =
        VersionedDatasetId.fromString(timestampTable.getDatasetConfig().getId().getId());
    DremioTable tableFromDatasetId = catalog.getTable(versionedDatasetId.asString());
    assertThat(timestampTable.getDatasetConfig().equals(tableFromDatasetId.getDatasetConfig()))
        .isTrue();
  }

  @Test
  public void testGetTableForNonexistentTable() {
    String nonExistingTable =
        "NonExistentTable" + TimestampString.fromMillisSinceEpoch(System.currentTimeMillis());
    String query =
        String.format(
            "SELECT * FROM dfs_hadoop.\"%s\" AT SNAPSHOT '%d'", nonExistingTable, firstSnapshotId);
    List<String> keyPath = Arrays.asList("dfs_hadoop", nonExistingTable);

    String expectedErrorMsg =
        String.format("Table 'dfs_hadoop.\"%s\"' not found", nonExistingTable);
    assertThatThrownBy(
            () ->
                catalog.getTableSnapshotForQuery(
                    CatalogEntityKey.newBuilder()
                        .keyComponents(keyPath)
                        .tableVersionContext(
                            new TableVersionContext(
                                TableVersionType.SNAPSHOT_ID, String.valueOf(firstSnapshotId)))
                        .build()))
        .isInstanceOf(UserException.class)
        .hasMessageContaining(expectedErrorMsg);

    TableVersionContext timeTravelVersion =
        new TableVersionContext(TableVersionType.SNAPSHOT_ID, String.format("%d", firstSnapshotId));
    VersionedDatasetId versionedDatasetId =
        VersionedDatasetId.newBuilder()
            .setTableKey(keyPath)
            .setContentId(null)
            .setTableVersionContext(timeTravelVersion)
            .build();

    assertThat(catalog.getTable(versionedDatasetId.asString())).isNull();
  }
}

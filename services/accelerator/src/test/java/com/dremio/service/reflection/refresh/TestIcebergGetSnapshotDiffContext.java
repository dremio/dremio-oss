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
package com.dremio.service.reflection.refresh;

import static com.dremio.exec.planner.sql.handlers.SqlHandlerUtil.getTimestampFromMillis;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_SNAPSHOT_BASED_INCREMENTAL_MAX_UNIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.TableMetadataVerifyAppendOnlyResult;
import com.dremio.exec.catalog.TableMetadataVerifyResult;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.EntityId;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class TestIcebergGetSnapshotDiffContext extends BaseTestQuery {
  class TestLogAppender extends AppenderBase<ILoggingEvent> {
    private ArrayList<ILoggingEvent> loggingEvents = new ArrayList<>();

    @Override
    protected void append(ILoggingEvent eventObject) {
      loggingEvents.add(eventObject);
    }

    ILoggingEvent getLastLoggedEvent() {
      if (loggingEvents.isEmpty()) {
        return null;
      }
      return loggingEvents.get(loggingEvents.size() - 1);
    }
  }

  private static CatalogService catalogService;
  private static EntityExplorer catalog;
  private static final String TEST_TABLE_NAME = "iceberg_get_snapshot_diff_context_test_table";
  private static NamespaceKey testTableNamespaceKey;
  private static CatalogEntityKey testTableEntityKey;

  private Table testTable;
  private TestLogAppender testLogAppender = new TestLogAppender();

  private CatalogService mockedCatalogService = mock(CatalogService.class);
  private Catalog mockedCatalog = mock(Catalog.class);
  private TableMetadata mockedTableMetadata = mock(TableMetadata.class);
  private DatasetConfig mockedDatasetConfig = mock(DatasetConfig.class);
  private DremioTable mockedDremioTable = mock(DremioTable.class);
  private EntityId mockedEntityId = mock(EntityId.class);
  private TableMetadataVerifyResult mockedMetadataVerifyResult =
      mock(TableMetadataVerifyResult.class);
  private TableMetadataVerifyAppendOnlyResult mockedMetadataVerifyAppendOnlyResult =
      mock(TableMetadataVerifyAppendOnlyResult.class);
  private OptionManager mockedOptionManager = mock(OptionManager.class);

  private RelNode mockedStrippedPlan = mock(RelNode.class);

  @BeforeClass
  public static void init() throws Exception {
    catalogService = getCatalogService();
    catalog = CatalogUtil.getSystemCatalogForReflections(catalogService);
    List<String> keyPath = Arrays.asList(TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME);
    testTableNamespaceKey = new NamespaceKey(keyPath);
    testTableEntityKey = CatalogEntityKey.fromNamespaceKey(testTableNamespaceKey);
  }

  @Before
  public void before() throws Exception {
    // Create table
    String createCommandSql =
        String.format(
            "create table %s.%s(c1 int, c2 varchar, c3 double)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME);
    runSQL(createCommandSql);
    File tableFolder = new File(getDfsTestTmpSchemaLocation(), TEST_TABLE_NAME);
    testTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);

    // Logger logger = (Logger) LoggerFactory.getLogger(RefreshDecisionMaker.class);
    Logger logger =
        (Logger)
            LoggerFactory.getLogger("com.dremio.service.reflection.refresh.RefreshDecisionMaker");
    logger.addAppender(testLogAppender);
    logger.setLevel(Level.TRACE);
    testLogAppender.start();

    doReturn(mockedCatalog).when(mockedCatalogService).getCatalog(any());
    doReturn(mockedDremioTable).when(mockedCatalog).getTableSnapshot(any());
    doReturn(mockedTableMetadata).when(mockedDremioTable).getDataset();
    doReturn(testTableNamespaceKey).when(mockedTableMetadata).getName();
    doReturn(mockedDatasetConfig).when(mockedTableMetadata).getDatasetConfig();
    doReturn(mockedEntityId).when(mockedDatasetConfig).getId();
    doReturn("abc123").when(mockedEntityId).getId();
    doReturn(Optional.of(mockedMetadataVerifyAppendOnlyResult))
        .when(mockedCatalog)
        .verifyTableMetadata(any(), any());
    doReturn(50L)
        .when(mockedOptionManager)
        .getOption(REFLECTION_SNAPSHOT_BASED_INCREMENTAL_MAX_UNIONS);
  }

  @After
  public void after() throws Exception {
    // Drop table
    runSQL(String.format("DROP TABLE %s.%s", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), TEST_TABLE_NAME));
  }

  private void loadTable() {
    File tableFolder = new File(getDfsTestTmpSchemaLocation(), TEST_TABLE_NAME);
    testTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
  }

  protected Snapshot getCurrentSnapshot() {
    loadTable();
    return testTable.currentSnapshot();
  }

  private void insertOneRecord() throws Exception {
    String insertCommandSql =
        String.format("insert into %s.%s VALUES(1,'a', 2.0)", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME);
    runSQL(insertCommandSql);
  }

  private void insertTwoRecords() throws Exception {
    String insertCommandSql =
        String.format(
            "insert into %s.%s VALUES(1,'a', 2.0),(2,'b', 3.0)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME);
    runSQL(insertCommandSql);
  }

  @Test
  public void testInvalidMetadataVerifyResultNullOptional() throws Exception {
    // Optional<MetadataVerifyResult> is null
    doReturn(null).when(mockedCatalog).verifyTableMetadata(any(), any());

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            mockedCatalogService,
            mockedTableMetadata,
            "456",
            "123",
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);
    assertThrows(IllegalStateException.class, decisionMaker::getDecision);
  }

  @Test
  public void testInvalidMetadataVerifyResultNullResult() throws Exception {
    // MetadataVerifyResult is null
    doReturn(Optional.ofNullable(null)).when(mockedCatalog).verifyTableMetadata(any(), any());

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            mockedCatalogService,
            mockedTableMetadata,
            "456",
            "123",
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();
    assertNull(snapshotDiffContext);

    assertEquals(
        String.format(
            "Fail to verify the changes between snapshots %s and %s for base table %s.",
            "123", "456", testTableNamespaceKey.toString()),
        decisionMaker.getFullRefreshReason());
  }

  @Test
  public void testInvalidMetadataVerifyResultNotInstanceOf() throws Exception {
    // MetadataVerifyResult is not instance of TableMetadataVerifyAppendOnlyResult
    doReturn(Optional.of(mockedMetadataVerifyResult))
        .when(mockedCatalog)
        .verifyTableMetadata(any(), any());

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            mockedCatalogService,
            mockedTableMetadata,
            "456",
            "123",
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertNull(snapshotDiffContext);
    assertEquals(
        String.format(
            "Fail to verify the changes between snapshots %s and %s for base table %s.",
            "123", "456", testTableNamespaceKey.toString()),
        decisionMaker.getFullRefreshReason());
  }

  @Test
  public void testInvalidMetadataVerifyResultNullResultCode() throws Exception {
    // MetadataVerifyResult has null result code
    doReturn(null).when(mockedMetadataVerifyAppendOnlyResult).getResultCode();
    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            mockedCatalogService,
            mockedTableMetadata,
            "456",
            "123",
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    assertThrows(
        IllegalStateException.class,
        () -> {
          decisionMaker.getDecision();
        });
  }

  @Test
  public void testInvalidMetadataVerifyResultNullSnapshotRanges() throws Exception {
    // MetadataVerifyResult has null snapshot ranges
    doReturn(TableMetadataVerifyAppendOnlyResult.ResultCode.APPEND_ONLY)
        .when(mockedMetadataVerifyAppendOnlyResult)
        .getResultCode();
    doReturn(null).when(mockedMetadataVerifyAppendOnlyResult).getSnapshotRanges();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            mockedCatalogService,
            mockedTableMetadata,
            "456",
            "123",
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    assertThrows(
        IllegalStateException.class,
        () -> {
          decisionMaker.getDecision();
        });
  }

  @Test
  public void testInvalidMetadataVerifyResultEmptySnapshotRanges() throws Exception {
    // MetadataVerifyResult has empty snapshot ranges
    doReturn(TableMetadataVerifyAppendOnlyResult.ResultCode.APPEND_ONLY)
        .when(mockedMetadataVerifyAppendOnlyResult)
        .getResultCode();
    doReturn(Collections.emptyList())
        .when(mockedMetadataVerifyAppendOnlyResult)
        .getSnapshotRanges();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            mockedCatalogService,
            mockedTableMetadata,
            "456",
            "123",
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    assertThrows(
        IllegalStateException.class,
        () -> {
          decisionMaker.getDecision();
        });
  }

  /**
   * Base table: S0 Reflection refresh: last refresh base table state: -1 (invalid), current base
   * table state: S0 Expected SnapshotDiffContex: null
   */
  @Test
  public void testInvalidBeginSnapshot() throws Exception {
    Snapshot snapshot = getCurrentSnapshot();
    TableMetadata tableMetadata = catalog.getTableSnapshot(testTableEntityKey).getDataset();

    // Invalid lastRefreshBaseTableSnapshotId
    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            tableMetadata,
            Long.toString(snapshot.snapshotId()),
            "-1",
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();
    assertNull(snapshotDiffContext);

    assertEquals(
        String.format(
            "Invalid begin snapshot %s for base table %s. The snapshot might have expired.",
            -1, testTableNamespaceKey),
        decisionMaker.getFullRefreshReason());
  }

  /**
   * Base table: S0 Reflection refresh: last refresh base table state: S0, current base table state:
   * -1 (invalid) Expected SnapshotDiffContex: null
   */
  @Test
  public void testInvalidEndSnapshot() throws Exception {
    Snapshot snapshot = getCurrentSnapshot();
    TableMetadata tableMetadata = catalog.getTableSnapshot(testTableEntityKey).getDataset();

    // Invalid baseTableSnapshotId
    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            tableMetadata,
            "-1", // Invalid baseTableSnapshotId
            Long.toString(snapshot.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        String.format("Invalid end snapshot %s for base table %s.", -1, testTableNamespaceKey),
        decisionMaker.getFullRefreshReason());
  }

  /**
   * Base table: S0 -> Append -> S1, Expire S0 Reflection refresh: last refresh base table state: S0
   * (expired), current base table state: S1 Expected SnapshotDiffContex: null
   */
  @Test
  public void testVacuumLastRefreshSnapshotExpired() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    Snapshot snapshot1 = getCurrentSnapshot();

    runSQL(
        String.format(
            "VACUUM TABLE %s.%s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
            TEMP_SCHEMA_HADOOP,
            TEST_TABLE_NAME,
            getTimestampFromMillis(snapshot1.timestampMillis())));
    loadTable();

    assertNull(testTable.snapshot(snapshot0.snapshotId()));
    assertNotNull(testTable.snapshot(snapshot1.snapshotId()));

    TableMetadata snapshot1TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot1TableMetadata,
            Long.toString(snapshot1.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertNull(snapshotDiffContext);
    assertEquals(
        String.format(
            "Invalid begin snapshot %s for base table %s. The snapshot might have expired.",
            snapshot0.snapshotId(), testTableNamespaceKey),
        decisionMaker.getFullRefreshReason());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2, Expire S0 Reflection refresh: last refresh base
   * table state: S1, current base table state: S2 Expected SnapshotDiffContex:
   * {FilterApplyOptions.FILTER_DATA_FILES, [(S1, S2)]}
   */
  @Test
  public void testVacuumLastRefreshSnapshotNotExpired() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    Snapshot snapshot1 = getCurrentSnapshot();
    insertOneRecord();
    Snapshot snapshot2 = getCurrentSnapshot();

    runSQL(
        String.format(
            "VACUUM TABLE %s.%s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
            TEMP_SCHEMA_HADOOP,
            TEST_TABLE_NAME,
            getTimestampFromMillis(snapshot1.timestampMillis())));
    loadTable();

    assertNull(testTable.snapshot(snapshot0.snapshotId()));
    assertNotNull(testTable.snapshot(snapshot1.snapshotId()));
    assertNotNull(testTable.snapshot(snapshot2.snapshotId()));

    TableMetadata snapshot2TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot2TableMetadata,
            Long.toString(snapshot2.snapshotId()),
            Long.toString(snapshot1.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(1, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot1.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
    assertEquals(
        snapshot2TableMetadata, snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata());
  }

  /**
   * Base table: S0 -> Append -> S1, Rollback to S0 Reflection refresh: last refresh base table
   * state: S1 (no longer current), current base table state: S0 Expected SnapshotDiffContex: null
   */
  @Test
  public void testRollbackToBeforeLastRefreshSnapshot() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    Snapshot snapshot1 = getCurrentSnapshot();

    runSQL(
        String.format(
            "ROLLBACK TABLE %s.%s TO SNAPSHOT '%s'",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME, snapshot0.snapshotId()));
    loadTable();

    assertEquals(snapshot0, testTable.currentSnapshot());
    assertFalse(
        SnapshotUtil.isAncestorOf(
            testTable, testTable.currentSnapshot().snapshotId(), snapshot1.snapshotId()));

    TableMetadata snapshot0TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot0TableMetadata,
            Long.toString(snapshot0.snapshotId()),
            Long.toString(snapshot1.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertNull(snapshotDiffContext);
    assertEquals(
        String.format(
            "Snapshot %s is not ancestor of snapshot %s for base table %s. Table might have been rolled back.",
            snapshot1.snapshotId(), snapshot0.snapshotId(), testTableNamespaceKey),
        decisionMaker.getFullRefreshReason());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2, Rollback to S1 Reflection refresh: last refresh
   * base table state: S0, current base table state: S1 Expected SnapshotDiffContex:
   * {FilterApplyOptions.FILTER_DATA_FILES, [(S0, S1)]}
   */
  @Test
  public void testRollbackToAfterLastRefreshSnapshot() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    Snapshot snapshot1 = getCurrentSnapshot();
    insertOneRecord();

    runSQL(
        String.format(
            "ROLLBACK TABLE %s.%s TO SNAPSHOT '%s'",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME, snapshot1.snapshotId()));
    loadTable();

    assertEquals(snapshot1, testTable.currentSnapshot());
    assertTrue(
        SnapshotUtil.isAncestorOf(
            testTable, testTable.currentSnapshot().snapshotId(), snapshot0.snapshotId()));

    TableMetadata snapshot1TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot1TableMetadata,
            Long.toString(snapshot1.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(1, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot0.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
    assertEquals(
        snapshot1TableMetadata, snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2, Rollback to S1 Reflection refresh: last refresh
   * base table state: S1, current base table state: S1 Expected SnapshotDiffContex:
   * {FilterApplyOptions.FILTER_DATA_FILES, [(S1, S1)]}
   */
  @Test
  public void testNoChangeRollbackToLastRefreshSnapshot() throws Exception {
    insertOneRecord();
    Snapshot snapshot1 = getCurrentSnapshot();
    insertOneRecord();

    runSQL(
        String.format(
            "ROLLBACK TABLE %s.%s TO SNAPSHOT '%s'",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME, snapshot1.snapshotId()));
    loadTable();

    assertEquals(snapshot1, testTable.currentSnapshot());
    assertTrue(
        SnapshotUtil.isAncestorOf(
            testTable, testTable.currentSnapshot().snapshotId(), snapshot1.snapshotId()));

    TableMetadata snapshot1TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot1TableMetadata,
            Long.toString(snapshot1.snapshotId()),
            Long.toString(snapshot1.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(1, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot1TableMetadata,
        snapshotDiffContext.getIntervals().get(0).getBeginningTableMetadata());
    assertEquals(
        snapshot1TableMetadata, snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata());
  }

  /**
   * Base table: S0 Reflection refresh: last refresh base table state: S0, current base table state:
   * S0 Expected SnapshotDiffContex: {FilterApplyOptions.FILTER_DATA_FILES, [(S0, S0)]}
   */
  @Test
  public void testNoChange() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();

    TableMetadata snapshot0TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot0TableMetadata,
            Long.toString(snapshot0.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(1, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot0TableMetadata,
        snapshotDiffContext.getIntervals().get(0).getBeginningTableMetadata());
    assertEquals(
        snapshot0TableMetadata, snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Update -> S2 Reflection refresh: last refresh base table
   * state: S0, current base table state: S2 Expected SnapshotDiffContex: null
   */
  @Test
  public void testNotAppendOnlyUpdate() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();

    runSQL(String.format("UPDATE %s.%s SET c2='abc'", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot2 = getCurrentSnapshot();

    TableMetadata snapshot2TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot2TableMetadata,
            Long.toString(snapshot2.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertNull(snapshotDiffContext);

    assertEquals(
        String.format(
            "Changes between snapshots %s and %s on base table %s were not append-only."
                + " Cannot do Append Only Incremental Refresh.",
            snapshot0.snapshotId(), snapshot2.snapshotId(), testTableNamespaceKey),
        decisionMaker.getFullRefreshReason());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Delete -> S2 Reflection refresh: last refresh base table
   * state: S0, current base table state: S2 Expected SnapshotDiffContex: null
   */
  @Test
  public void testNotAppendOnlyDelete() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertTwoRecords();

    runSQL(String.format("DELETE FROM %s.%s WHERE c1=1", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot2 = getCurrentSnapshot();

    TableMetadata snapshot2TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot2TableMetadata,
            Long.toString(snapshot2.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertNull(snapshotDiffContext);

    assertEquals(
        String.format(
            "Changes between snapshots %s and %s on base table %s were not append-only."
                + " Cannot do Append Only Incremental Refresh.",
            snapshot0.snapshotId(), snapshot2.snapshotId(), testTableNamespaceKey),
        decisionMaker.getFullRefreshReason());
  }

  /**
   * Base table: S0 -> Append -> S1 Reflection refresh: last refresh base table state: S0, current
   * base table state: S1 Expected SnapshotDiffContex: {FilterApplyOptions.FILTER_DATA_FILES, [(S0,
   * S1)]}
   */
  @Test
  public void testAppendOnlyOneAppend() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    Snapshot snapshot1 = getCurrentSnapshot();

    TableMetadata snapshot1TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot1TableMetadata,
            Long.toString(snapshot1.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(1, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot0.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
    assertEquals(
        snapshot1TableMetadata, snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 Reflection refresh: last refresh base table
   * state: S0, current base table state: S2 Expected SnapshotDiffContex:
   * {FilterApplyOptions.FILTER_DATA_FILES, [(S0, S2)]}
   */
  @Test
  public void testAppendOnlyMultiAppends() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    insertOneRecord();
    Snapshot snapshot2 = getCurrentSnapshot();

    TableMetadata snapshot2TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot2TableMetadata,
            Long.toString(snapshot2.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(1, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot0.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
    assertEquals(
        snapshot2TableMetadata, snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 Reflection refresh: last
   * refresh base table state: S0, current base table state: S3 Expected SnapshotDiffContex:
   * {FilterApplyOptions.FILTER_DATA_FILES, [(S0, S2)]}
   */
  @Test
  public void testAppendOnlyOneCompact() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    insertOneRecord();
    Snapshot snapshot2 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot3 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());

    TableMetadata snapshot3TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot3TableMetadata,
            Long.toString(snapshot3.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(1, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot0.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot2.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getEndingTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Append -> S4 -> Compact ->
   * S5 Reflection refresh: last refresh base table state: S0, current base table state: S5 Expected
   * SnapshotDiffContex: {FilterApplyOptions.FILTER_DATA_FILES, [(S3, S4), (S0, S2)]}
   */
  @Test
  public void testAppendOnlyMultiCompacts() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    insertOneRecord();
    Snapshot snapshot2 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot3 = getCurrentSnapshot();

    insertOneRecord();
    Snapshot snapshot4 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot5 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());
    assertEquals(DataOperations.REPLACE, snapshot5.operation());

    TableMetadata snapshot5TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot5TableMetadata,
            Long.toString(snapshot5.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(2, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot3.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot4.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getEndingTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot0.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(1)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot2.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(1)
            .getEndingTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Append -> S4 -> Compact ->
   * S5 -> Append -> S6 Reflection refresh: last refresh base table state: S0, current base table
   * state: S6 Expected SnapshotDiffContex: {FilterApplyOptions.FILTER_DATA_FILES, [(S5, S6), (S3,
   * S4), (S0, S2)]}
   */
  @Test
  public void testAppendOnlyMultiCompactsBaseTableLastOperationIsAppend() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    insertOneRecord();
    Snapshot snapshot2 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot3 = getCurrentSnapshot();

    insertOneRecord();
    Snapshot snapshot4 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot5 = getCurrentSnapshot();

    insertOneRecord();
    Snapshot snapshot6 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());
    assertEquals(DataOperations.REPLACE, snapshot5.operation());

    TableMetadata snapshot6TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot6TableMetadata,
            Long.toString(snapshot6.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(3, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot5.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot6TableMetadata, snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata());

    assertEquals(
        snapshot3.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(1)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot4.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(1)
            .getEndingTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot0.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(2)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot2.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(2)
            .getEndingTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Append -> S4 -> Compact ->
   * S5 -> Append -> S6 Reflection refresh: last refresh base table state: S3, current base table
   * state: S6 Expected SnapshotDiffContex: {FilterApplyOptions.FILTER_DATA_FILES, [(S5, S6), (S3,
   * S4)]}
   */
  @Test
  public void testAppendOnlyMultiCompactsLastRefreshBaseTableSnapshotIsCompact() throws Exception {
    insertOneRecord();
    insertOneRecord();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot3 = getCurrentSnapshot();

    insertOneRecord();
    Snapshot snapshot4 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot5 = getCurrentSnapshot();

    insertOneRecord();
    Snapshot snapshot6 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());
    assertEquals(DataOperations.REPLACE, snapshot5.operation());

    TableMetadata snapshot6TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot6TableMetadata,
            Long.toString(snapshot6.snapshotId()),
            Long.toString(snapshot3.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(2, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot5.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot6TableMetadata, snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata());

    assertEquals(
        snapshot3.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(1)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot4.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(1)
            .getEndingTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Compact -> S4 Reflection
   * refresh: last refresh base table state: S0, current base table state: S4 Expected
   * SnapshotDiffContex: {FilterApplyOptions.FILTER_DATA_FILES, [(S0, S2)]}
   */
  @Test
  public void testAppendOnlyAdjacentCompacts() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    insertOneRecord();
    Snapshot snapshot2 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot3 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE MANIFESTS", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot4 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());
    assertEquals(DataOperations.REPLACE, snapshot4.operation());

    TableMetadata snapshot4TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot4TableMetadata,
            Long.toString(snapshot4.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(1, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot0.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot2.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getEndingTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Compact -> S4 -> Append ->
   * S5 Reflection refresh: last refresh base table state: S0, current base table state: S5 Expected
   * SnapshotDiffContex: {FilterApplyOptions.FILTER_DATA_FILES, [(S4, S5), (S0, S2)]}
   */
  @Test
  public void testAppendOnlyAdjacentCompactsBaseTableLastOperationIsAppend() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    insertOneRecord();
    Snapshot snapshot2 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot3 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE MANIFESTS", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot4 = getCurrentSnapshot();

    insertOneRecord();
    Snapshot snapshot5 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());
    assertEquals(DataOperations.REPLACE, snapshot4.operation());

    TableMetadata snapshot5TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot5TableMetadata,
            Long.toString(snapshot5.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(2, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot4.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot5TableMetadata, snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata());

    assertEquals(
        snapshot0.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(1)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot2.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(1)
            .getEndingTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Compact -> S4 -> Append ->
   * S5 Reflection refresh: last refresh base table state: S3, current base table state: S5 Expected
   * SnapshotDiffContex: {FilterApplyOptions.FILTER_DATA_FILES, [(S4, S5)]}
   */
  @Test
  public void testAppendOnlyAdjacentCompactsLastRefreshBaseTableSnapshotIsCompact()
      throws Exception {
    insertOneRecord();
    insertOneRecord();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot3 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE MANIFESTS", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot4 = getCurrentSnapshot();

    insertOneRecord();
    Snapshot snapshot5 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());
    assertEquals(DataOperations.REPLACE, snapshot4.operation());

    TableMetadata snapshot5TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot5TableMetadata,
            Long.toString(snapshot5.snapshotId()),
            Long.toString(snapshot3.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(1, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot4.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot5TableMetadata, snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Compact -> S4 Reflection
   * refresh: last refresh base table state: S2, current base table state: S4 Expected
   * SnapshotDiffContex: {FilterApplyOptions.FILTER_DATA_FILES, [(S2, S2)]}
   */
  @Test
  public void testNoChangeAllCompactOperations() throws Exception {
    insertOneRecord();
    insertOneRecord();
    Snapshot snapshot2 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot3 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE MANIFESTS", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot4 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());
    assertEquals(DataOperations.REPLACE, snapshot4.operation());

    TableMetadata snapshot4TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot4TableMetadata,
            Long.toString(snapshot4.snapshotId()),
            Long.toString(snapshot2.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(1, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot2.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot2.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getEndingTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Compact -> S4 Reflection
   * refresh: last refresh base table state: S3, current base table state: S4 Expected
   * SnapshotDiffContex: {FilterApplyOptions.FILTER_DATA_FILES, [(S3, S3)]}
   */
  @Test
  public void testNoChangeAllCompactOperationsLastRefreshBaseTableSnapshotIsCompact()
      throws Exception {
    insertOneRecord();
    insertOneRecord();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot3 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE MANIFESTS", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot4 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());
    assertEquals(DataOperations.REPLACE, snapshot4.operation());

    TableMetadata snapshot4TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot4TableMetadata,
            Long.toString(snapshot4.snapshotId()),
            Long.toString(snapshot3.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertEquals(
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES,
        snapshotDiffContext.getFilterApplyOptions());
    assertEquals(1, snapshotDiffContext.getIntervals().size());

    assertEquals(
        snapshot3.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getBeginningTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());

    assertEquals(
        snapshot3.snapshotId(),
        snapshotDiffContext
            .getIntervals()
            .get(0)
            .getEndingTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .longValue());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Append -> S4 -> Compact ->
   * S5 -> Update -> S6 Reflection refresh: last refresh base table state: S0, current base table
   * state: S6 Expected SnapshotDiffContex: null
   */
  @Test
  public void testNotAppendOnlyWithAppendUpdateCompact() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    insertOneRecord();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot3 = getCurrentSnapshot();

    insertOneRecord();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot5 = getCurrentSnapshot();

    runSQL(String.format("UPDATE %s.%s SET c2='abc'", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot6 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());
    assertEquals(DataOperations.REPLACE, snapshot5.operation());

    TableMetadata snapshot6TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot6TableMetadata,
            Long.toString(snapshot6.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();

    assertNull(snapshotDiffContext);

    assertEquals(
        String.format(
            "Changes between snapshots %s and %s on base table %s were not append-only."
                + " Cannot do Append Only Incremental Refresh.",
            snapshot0.snapshotId(), snapshot6.snapshotId(), testTableNamespaceKey),
        decisionMaker.getFullRefreshReason());
  }

  /**
   * Base table: S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Append -> S4 -> Compact ->
   * Append -> S5 Reflection refresh: last refresh base table state: S0, current base table state:
   * S5 REFLECTION_SNAPSHOT_BASED_INCREMENTAL_MAX_UNIONS: 1 Expected SnapshotDiffContex: null
   */
  @Test
  public void testAppendOnlyMultiCompactsExceedLimit() throws Exception {

    doReturn(1L)
        .when(mockedOptionManager)
        .getOption(REFLECTION_SNAPSHOT_BASED_INCREMENTAL_MAX_UNIONS);

    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    insertOneRecord();
    Snapshot snapshot2 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot3 = getCurrentSnapshot();

    insertOneRecord();
    Snapshot snapshot4 = getCurrentSnapshot();

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE DATA USING BIN_PACK (MIN_INPUT_FILES=1)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));

    insertOneRecord();
    Snapshot snapshot5 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());
    assertEquals(DataOperations.APPEND, snapshot5.operation());

    TableMetadata snapshot5TableMetadata =
        catalog.getTableSnapshot(testTableEntityKey).getDataset();

    SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
        new SnapshotBasedIncrementalRefreshDecisionMaker(
            catalogService,
            snapshot5TableMetadata,
            Long.toString(snapshot5.snapshotId()),
            Long.toString(snapshot0.snapshotId()),
            mockedOptionManager,
            null,
            null,
            null,
            null,
            mockedStrippedPlan);

    SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();
    assertNull(snapshotDiffContext);
    assertEquals(
        String.format(
            "Number of unions required for append-only ranges between snapshots %s and %s on base table %s exceed system limit (required:2, limit:1). Cannot do Append Only Incremental Refresh.",
            Long.toString(snapshot0.snapshotId()),
            Long.toString(snapshot5.snapshotId()),
            testTableNamespaceKey.toString()),
        decisionMaker.getFullRefreshReason());
  }
}

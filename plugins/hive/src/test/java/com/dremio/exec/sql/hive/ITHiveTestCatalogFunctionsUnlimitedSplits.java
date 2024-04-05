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
package com.dremio.exec.sql.hive;

import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.fsDelete;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.TableMetadataVerifyAppendOnlyRequest;
import com.dremio.exec.catalog.TableMetadataVerifyAppendOnlyResult;
import com.dremio.exec.catalog.TableMetadataVerifyResult;
import com.dremio.exec.hive.LazyDataGeneratingHiveTestBase;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;

/**
 * Test following catalog functions with unlimited splits
 * - getTableSnapshot(): used for getting TableMetadata for a given snapshot ID
 * - verifyTableMetadata(): used for verifying if changes between two snapshots were append-only
 */
public class ITHiveTestCatalogFunctionsUnlimitedSplits extends LazyDataGeneratingHiveTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ITHiveTestCatalogFunctionsUnlimitedSplits.class);
  protected static final String HIVE = "hive.";
  protected static final String ALTER_PDS_REFRESH = "ALTER PDS %s REFRESH METADATA";
  private static final String FORGET_DATASET = "ALTER TABLE %s FORGET METADATA";
  private static final String REFRESH_DONE = "Metadata for table '%s' refreshed.";

  private static FileSystem fs;
  protected static String finalIcebergMetadataLocation;

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(250, TimeUnit.SECONDS);

  @BeforeClass
  public static void generateHiveWithoutData() throws Exception {
    BaseTestQuery.setupDefaultTestCluster();
    LazyDataGeneratingHiveTestBase.generateHiveWithoutData();
    finalIcebergMetadataLocation = getDfsTestTmpSchemaLocation();
    fs = setupLocalFS();
    setSystemOption(REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL, "true");
  }

  @AfterClass
  public static void close() throws Exception {
    resetSystemOption(REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL.getOptionName());
  }

  @After
  public void tearDown() throws IOException {
    fsDelete(fs, new Path(finalIcebergMetadataLocation));
  }

  /*
   * Test getTableSnapshot() with MetadataRequestOptions.useMetadataTable option as false.
   */
  @Test
  public void testGetTableSnapshotNotUseMetadataTable() throws Exception {
    final String tableName = "incrrefresh_v2_test_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING)");
      final String insertCmd = "INSERT INTO " + tableName + " VALUES(1, 'a')";
      dataGenerator.executeDDL(insertCmd);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();

      EntityExplorer entityExplorer = CatalogUtil.getSystemCatalogForReflections(getSabotContext().getCatalogService());

      CatalogEntityKey catalogEntityKey = CatalogEntityKey.newBuilder()
        .keyComponents(ImmutableList.of("hive", tableName))
        .tableVersionContext(new TableVersionContext(
          TableVersionType.SNAPSHOT_ID,
          Long.toString(icebergTable.currentSnapshot().snapshotId())))
        .build();
      assertThatThrownBy(() -> {
        entityExplorer.getTableSnapshot(catalogEntityKey);
      }).isInstanceOf(UserException.class)
        .hasMessageContaining("Time travel is not supported on table");
    } finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  /*
   * Test getTableSnapshot() with MetadataRequestOptions.useMetadataTable option as true.
   */
  @Test
  public void testGetTableSnapshotUseMetadataTable() throws Exception {
    final String tableName = "incrrefresh_v2_test_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING)");
      final String insertCmd = "INSERT INTO " + tableName + " VALUES(1, 'a')";
      dataGenerator.executeDDL(insertCmd);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      Snapshot snapshot1 = icebergTable.currentSnapshot();

      final String insertCmd2 = "INSERT INTO " + tableName + " VALUES(2, 'b')";
      dataGenerator.executeDDL(insertCmd2);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      EntityExplorer entityExplorer = getSabotContext().getCatalogService().getCatalog(MetadataRequestOptions.newBuilder()
        .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
        .setCheckValidity(false)
        .setNeverPromote(true)
        .setUseInternalMetadataTable(true)
        .build());

      CatalogEntityKey catalogEntityKey = CatalogEntityKey.newBuilder()
        .keyComponents(ImmutableList.of("hive", tableName))
        .tableVersionContext(new TableVersionContext(
          TableVersionType.SNAPSHOT_ID,
          Long.toString(snapshot1.snapshotId())))
        .build();
      DremioTable table = entityExplorer.getTableSnapshot(catalogEntityKey);

      assertFalse(DatasetHelper.isIcebergDataset(table.getDatasetConfig()));
      assertTrue(DatasetHelper.isInternalIcebergTable(table.getDatasetConfig()));
      IcebergMetadata icebergMetadata = table.getDatasetConfig().getPhysicalDataset().getIcebergMetadata();
      assertEquals(snapshot1.snapshotId(), icebergMetadata.getSnapshotId().longValue());
      assertTrue(icebergMetadata.getMetadataFileLocation().contains(finalIcebergMetadataLocation));
      assertEquals(icebergTable.name(), icebergMetadata.getTableUuid());
    } finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  /*
   * Test verifyTableMetadata() with MetadataRequestOptions.useMetadataTable option as false.
   */
  @Test
  public void testVerifyTableMetadataNotUseMetadataTable() throws Exception {
    final String tableName = "incrrefresh_v2_test_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING)");
      final String insertCmd = "INSERT INTO " + tableName + " VALUES(1, 'a')";
      dataGenerator.executeDDL(insertCmd);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      Snapshot snapshot1 = icebergTable.currentSnapshot();

      final String insertCmd2 = "INSERT INTO " + tableName + " VALUES(2, 'b')";
      dataGenerator.executeDDL(insertCmd2);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();
      Snapshot snapshot2 = icebergTable.currentSnapshot();

      EntityExplorer entityExplorer = getSabotContext().getCatalogService().getCatalog(MetadataRequestOptions.newBuilder()
        .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
        .setCheckValidity(false)
        .setNeverPromote(true)
        .build());
      NamespaceKey key = new NamespaceKey(ImmutableList.of("hive", tableName));

      Optional<TableMetadataVerifyResult> result = entityExplorer.verifyTableMetadata(
        CatalogEntityKey.newBuilder()
          .keyComponents(key.getPathComponents())
          .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
          .build(),
        new TableMetadataVerifyAppendOnlyRequest(Long.toString(snapshot1.snapshotId()), Long.toString(snapshot2.snapshotId())));

      assertNotNull(result);
      assertFalse(result.isPresent());
    } finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  /*
   * Test verifyTableMetadata() with MetadataRequestOptions.useMetadataTable option as true, changes were append-only.
   */
  @Test
  public void testVerifyTableMetadataAppendOnly() throws Exception {
    final String tableName = "incrrefresh_v2_test_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING)");
      final String insertCmd = "INSERT INTO " + tableName + " VALUES(1, 'a')";
      dataGenerator.executeDDL(insertCmd);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      Snapshot snapshot1 = icebergTable.currentSnapshot();

      final String insertCmd2 = "INSERT INTO " + tableName + " VALUES(2, 'b')";
      dataGenerator.executeDDL(insertCmd2);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();
      Snapshot snapshot2 = icebergTable.currentSnapshot();

      EntityExplorer entityExplorer = getSabotContext().getCatalogService().getCatalog(MetadataRequestOptions.newBuilder()
        .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
        .setCheckValidity(false)
        .setNeverPromote(true)
        .setUseInternalMetadataTable(true)
        .build());
      NamespaceKey key = new NamespaceKey(ImmutableList.of("hive", tableName));

      Optional<TableMetadataVerifyResult> result = entityExplorer.verifyTableMetadata(
        CatalogEntityKey.newBuilder()
          .keyComponents(key.getPathComponents())
          .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
          .build(),
        new TableMetadataVerifyAppendOnlyRequest(Long.toString(snapshot1.snapshotId()), Long.toString(snapshot2.snapshotId())));

      assertNotNull(result);
      assertTrue(result.isPresent());
      TableMetadataVerifyAppendOnlyResult appendOnlyResult = (TableMetadataVerifyAppendOnlyResult) result.get();
      assertEquals(TableMetadataVerifyAppendOnlyResult.ResultCode.APPEND_ONLY, appendOnlyResult.getResultCode());
      assertEquals(1, appendOnlyResult.getSnapshotRanges().size());
      assertEquals(Long.toString(snapshot1.snapshotId()), appendOnlyResult.getSnapshotRanges().get(0).getLeft());
      assertEquals(Long.toString(snapshot2.snapshotId()), appendOnlyResult.getSnapshotRanges().get(0).getRight());
    } finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  /*
   * Test verifyTableMetadata() with MetadataRequestOptions.useMetadataTable option as true, changes were not append-only.
   */
  @Test
  public void testVerifyTableMetadataNotAppendOnly() throws Exception {
    final String tableName = "incrrefresh_v2_test_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(id INT) PARTITIONED BY (year INT, month STRING)");
      final String insertCmd1 = "INSERT INTO " + tableName + " PARTITION(year=2020, month='Feb') VALUES(1)";
      dataGenerator.executeDDL(insertCmd1);

      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      Snapshot snapshot1 = icebergTable.currentSnapshot();

      final String insertCmd2 = "INSERT INTO " + tableName + " PARTITION(year=2021, month='Jan') VALUES(2)";
      dataGenerator.executeDDL(insertCmd2);

      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();
      Snapshot snapshot2 = icebergTable.currentSnapshot();

      final String dropCmd1 = "ALTER TABLE " + tableName + " DROP PARTITION(year=2020, month='Feb')";
      dataGenerator.executeDDL(dropCmd1);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();
      Snapshot snapshot3 = icebergTable.currentSnapshot();

      EntityExplorer entityExplorer = getSabotContext().getCatalogService().getCatalog(MetadataRequestOptions.newBuilder()
        .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
        .setCheckValidity(false)
        .setNeverPromote(true)
        .setUseInternalMetadataTable(true)
        .build());
      NamespaceKey key = new NamespaceKey(ImmutableList.of("hive", tableName));

      // Verify (snapshot1, snapshot2)
      Optional<TableMetadataVerifyResult> result = entityExplorer.verifyTableMetadata(
        CatalogEntityKey.newBuilder()
          .keyComponents(key.getPathComponents())
          .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
          .build(),
        new TableMetadataVerifyAppendOnlyRequest(Long.toString(snapshot1.snapshotId()), Long.toString(snapshot2.snapshotId())));

      assertNotNull(result);
      assertTrue(result.isPresent());
      TableMetadataVerifyAppendOnlyResult appendOnlyResult = (TableMetadataVerifyAppendOnlyResult) result.get();
      assertEquals(TableMetadataVerifyAppendOnlyResult.ResultCode.APPEND_ONLY, appendOnlyResult.getResultCode());
      assertEquals(1, appendOnlyResult.getSnapshotRanges().size());
      assertEquals(Long.toString(snapshot1.snapshotId()), appendOnlyResult.getSnapshotRanges().get(0).getLeft());
      assertEquals(Long.toString(snapshot2.snapshotId()), appendOnlyResult.getSnapshotRanges().get(0).getRight());

      // Verify (snapshot1, snapshot3)
      result = entityExplorer.verifyTableMetadata(
        CatalogEntityKey.newBuilder()
          .keyComponents(key.getPathComponents())
          .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
          .build(),
        new TableMetadataVerifyAppendOnlyRequest(Long.toString(snapshot1.snapshotId()), Long.toString(snapshot3.snapshotId())));

      assertNotNull(result);
      assertTrue(result.isPresent());
      appendOnlyResult = (TableMetadataVerifyAppendOnlyResult) result.get();
      assertEquals(TableMetadataVerifyAppendOnlyResult.ResultCode.NOT_APPEND_ONLY, appendOnlyResult.getResultCode());
      assertEquals(0, appendOnlyResult.getSnapshotRanges().size());

      // Verify (snapshot2, snapshot3)
      result = entityExplorer.verifyTableMetadata(
        CatalogEntityKey.newBuilder()
          .keyComponents(key.getPathComponents())
          .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
          .build(),
        new TableMetadataVerifyAppendOnlyRequest(Long.toString(snapshot2.snapshotId()), Long.toString(snapshot3.snapshotId())));

      assertNotNull(result);
      assertTrue(result.isPresent());
      appendOnlyResult = (TableMetadataVerifyAppendOnlyResult) result.get();
      assertEquals(TableMetadataVerifyAppendOnlyResult.ResultCode.NOT_APPEND_ONLY, appendOnlyResult.getResultCode());
      assertEquals(0, appendOnlyResult.getSnapshotRanges().size());
    } finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  /*
   * Test verifyTableMetadata() with MetadataRequestOptions.useMetadataTable option as true, changes were data modifying.
   */
  @Test
  public void testVerifyTableMetadataDataModifying() throws Exception {
    final String tableName = "incrrefresh_v2_test_" + getFileFormatLowerCase();
    try {
      createTable(tableName, "(col1 INT, col2 STRING)");
      final String insertCmd = "INSERT INTO " + tableName + " VALUES(1, 'a')";
      dataGenerator.executeDDL(insertCmd);
      runFullRefresh(tableName);

      Table icebergTable = getIcebergTable();
      Snapshot snapshot1 = icebergTable.currentSnapshot();

      final String insertCmd2 = "INSERT INTO " + tableName + " VALUES(2, 'b')";
      dataGenerator.executeDDL(insertCmd2);
      runFullRefresh(tableName);

      //Refresh the same iceberg table again
      icebergTable.refresh();
      Snapshot snapshot2 = icebergTable.currentSnapshot();

      EntityExplorer entityExplorer = getSabotContext().getCatalogService().getCatalog(MetadataRequestOptions.newBuilder()
        .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
        .setCheckValidity(false)
        .setNeverPromote(true)
        .setUseInternalMetadataTable(true)
        .build());
      NamespaceKey key = new NamespaceKey(ImmutableList.of("hive", tableName));

      Optional<TableMetadataVerifyResult> result = entityExplorer.verifyTableMetadata(
        CatalogEntityKey.newBuilder()
          .keyComponents(key.getPathComponents())
          .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
          .build(),
        new TableMetadataVerifyAppendOnlyRequest(Long.toString(snapshot1.snapshotId()), Long.toString(snapshot2.snapshotId())));

      assertNotNull(result);
      assertTrue(result.isPresent());
      TableMetadataVerifyAppendOnlyResult appendOnlyResult = (TableMetadataVerifyAppendOnlyResult) result.get();
      assertEquals(TableMetadataVerifyAppendOnlyResult.ResultCode.APPEND_ONLY, appendOnlyResult.getResultCode());
      assertEquals(1, appendOnlyResult.getSnapshotRanges().size());
      assertEquals(Long.toString(snapshot1.snapshotId()), appendOnlyResult.getSnapshotRanges().get(0).getLeft());
      assertEquals(Long.toString(snapshot2.snapshotId()), appendOnlyResult.getSnapshotRanges().get(0).getRight());
    } finally {
      forgetMetadata(tableName);
      dropTable(tableName);
    }
  }

  protected String getFileFormat() {
    return "PARQUET";
  }

  protected String getFileFormatLowerCase() {
    return getFileFormat().toLowerCase(Locale.ROOT);
  }

  protected void createTable(String tableName, String queryDef) throws IOException {
    String query = "CREATE TABLE IF NOT EXISTS " + tableName + queryDef + " STORED AS " + getFileFormat();
    System.out.println("query = " + query);
    dataGenerator.executeDDL(query);
  }

  protected void dropTable(String tableName) throws IOException {
    dataGenerator.executeDDL("DROP TABLE IF EXISTS " + tableName);
  }

  protected Table getIcebergTable() {
    return RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);
  }

  protected void forgetMetadata(String tableName) throws Exception {
    runSQL(String.format(FORGET_DATASET, HIVE + tableName));
  }

  protected void runFullRefresh(String tableName) throws Exception {
    runRefresh(String.format(ALTER_PDS_REFRESH, HIVE + tableName), String.format(REFRESH_DONE, HIVE + tableName));
  }

  protected void runPartialRefresh(String tableName, String partitionColumnFilters) throws Exception {
    runRefresh(String.format(ALTER_PDS_REFRESH + " FOR PARTITIONS %s", HIVE + tableName, partitionColumnFilters), String.format(REFRESH_DONE, HIVE + tableName));
  }

  private void runRefresh(String query, String expectedMsg) throws Exception {
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, expectedMsg)
      .build()
      .run();
  }
}

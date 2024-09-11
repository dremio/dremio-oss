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

import static com.dremio.common.utils.PathUtils.parseFullPath;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addRows;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.dremio.BaseTestQuery;
import com.dremio.DremioTestWrapper;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.connector.metadata.DatasetVerifyAppendOnlyResult;
import com.dremio.connector.metadata.options.MetadataVerifyRequest;
import com.dremio.connector.metadata.options.VerifyAppendOnlyRequest;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.TableMetadataVerifyAppendOnlyRequest;
import com.dremio.exec.catalog.TableMetadataVerifyAppendOnlyResult;
import com.dremio.exec.catalog.TableMetadataVerifyRequest;
import com.dremio.exec.catalog.TableMetadataVerifyResult;
import com.dremio.exec.planner.sql.DmlQueryTestUtils;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestIcebergMetadataVerifyProcessors extends BaseTestQuery {
  private static Table test_iceberg_table;
  private static final String TEST_TABLE_NAME = "iceberg_metadata_verify_processors_test_table";

  @Before
  public void before() throws Exception {
    // Create table
    String createCommandSql =
        String.format(
            "create table %s.%s(c1 int, c2 varchar, c3 double)",
            TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME);
    runSQL(createCommandSql);
    File tableFolder = new File(getDfsTestTmpSchemaLocation(), TEST_TABLE_NAME);
    test_iceberg_table = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
  }

  @After
  public void after() throws Exception {
    // Drop table
    runSQL(String.format("DROP TABLE %s.%s", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), TEST_TABLE_NAME));
  }

  private static void loadTable() {
    File tableFolder = new File(getDfsTestTmpSchemaLocation(), TEST_TABLE_NAME);
    test_iceberg_table = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
  }

  protected Snapshot getCurrentSnapshot() {
    loadTable();
    return test_iceberg_table.currentSnapshot();
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

  /**
   * Test metadata verify by calling {@link EntityExplorer#verifyTableMetadata(CatalogEntityKey,
   * TableMetadataVerifyRequest)} Test: S0 -> Append -> S1 Expected ResultCode: APPEND_ONLY Expected
   * snapshot ranges: [(S0, S1)]
   */
  @Test
  public void testCatalogVerifyTableMetadata() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    Snapshot snapshot1 = getCurrentSnapshot();
    assertNotEquals(snapshot0, snapshot1);

    List<String> keyPath = Arrays.asList(TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME);

    EntityExplorer entityExplorer = CatalogUtil.getSystemCatalogForReflections(getCatalogService());
    Optional<TableMetadataVerifyResult> result =
        entityExplorer.verifyTableMetadata(
            CatalogEntityKey.newBuilder()
                .keyComponents(keyPath)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build(),
            new TableMetadataVerifyAppendOnlyRequest(
                Long.toString(snapshot0.snapshotId()), Long.toString(snapshot1.snapshotId())));

    assertNotNull(result);
    assertTrue(result.isPresent());

    TableMetadataVerifyAppendOnlyResult appendOnlyResult =
        (TableMetadataVerifyAppendOnlyResult) result.get();

    assertEquals(
        TableMetadataVerifyAppendOnlyResult.ResultCode.APPEND_ONLY,
        appendOnlyResult.getResultCode());
    assertEquals(1, appendOnlyResult.getSnapshotRanges().size());
    assertEquals(
        Long.toString(snapshot0.snapshotId()),
        appendOnlyResult.getSnapshotRanges().get(0).getLeft());
    assertEquals(
        Long.toString(snapshot1.snapshotId()),
        appendOnlyResult.getSnapshotRanges().get(0).getRight());
  }

  /**
   * A Generic test method supporting iceberg table on different source (e.g. Hive)
   *
   * <p>Test metadata verify by calling {@link EntityExplorer#verifyTableMetadata(CatalogEntityKey,
   * TableMetadataVerifyRequest)} Test: S0 -> Append -> S1 Expected ResultCode: APPEND_ONLY Expected
   * snapshot ranges: [(S0, S1)]
   */
  public static void testCatalogVerifyTableMetadata(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      // Insert row to increase snapshots
      addRows(table, 1);

      List<QueryDataBatch> queryBatch =
          testSqlWithResults(
              String.format(
                  "SELECT snapshot_id FROM table(table_snapshot('%s')) order by committed_at",
                  table.fqn));
      RecordBatchLoader loader = new RecordBatchLoader(allocator);
      List<Map<String, Object>> records = new ArrayList<>();
      DremioTestWrapper.addToMaterializedResults(records, queryBatch, loader);

      String snapshot0 = ((Long) records.get(records.size() - 2).get("`snapshot_id`")).toString();
      String snapshot1 = ((Long) records.get(records.size() - 1).get("`snapshot_id`")).toString();

      EntityExplorer entityExplorer =
          CatalogUtil.getSystemCatalogForReflections(getCatalogService());
      Optional<TableMetadataVerifyResult> result =
          entityExplorer.verifyTableMetadata(
              CatalogEntityKey.newBuilder()
                  .keyComponents(parseFullPath(table.fqn))
                  .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                  .build(),
              new TableMetadataVerifyAppendOnlyRequest(snapshot0, snapshot1));

      assertNotNull(result);
      assertTrue(result.isPresent());

      TableMetadataVerifyAppendOnlyResult appendOnlyResult =
          (TableMetadataVerifyAppendOnlyResult) result.get();

      assertEquals(
          TableMetadataVerifyAppendOnlyResult.ResultCode.APPEND_ONLY,
          appendOnlyResult.getResultCode());
      assertEquals(1, appendOnlyResult.getSnapshotRanges().size());
      assertEquals(snapshot0, appendOnlyResult.getSnapshotRanges().get(0).getLeft());
      assertEquals(snapshot1, appendOnlyResult.getSnapshotRanges().get(0).getRight());
    }
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#verify(MetadataVerifyRequest, Table)} directly Test: S0 ->
   * Append -> Append -> S1 Expected ResultCode: APPEND_ONLY Expected snapshot ranges: [(S0, S1)]
   */
  @Test
  public void testMetadataVerifyOption() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    insertTwoRecords();
    Snapshot snapshot1 = getCurrentSnapshot();
    assertNotEquals(snapshot0, snapshot1);

    MetadataVerifyRequest metadataVerifyRequest =
        new VerifyAppendOnlyRequest(
            Long.toString(snapshot0.snapshotId()), Long.toString(snapshot1.snapshotId()));
    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.verify(metadataVerifyRequest, test_iceberg_table).get();

    assertEquals(DatasetVerifyAppendOnlyResult.ResultCode.APPEND_ONLY, result.getResultCode());
    assertEquals(1, result.getSnapshotRanges().size());
    assertEquals(
        Long.toString(snapshot0.snapshotId()), result.getSnapshotRanges().get(0).getLeft());
    assertEquals(
        Long.toString(snapshot1.snapshotId()), result.getSnapshotRanges().get(0).getRight());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * Invalid begin snapshot Expected ResultCode: INVALID_BEGIN_SNAPSHOT
   */
  @Test
  public void testInvalidBeginSnapshot() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    Snapshot snapshot1 = getCurrentSnapshot();
    assertNotEquals(snapshot0, snapshot1);

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, 123L, snapshot1.snapshotId());

    assertEquals(
        DatasetVerifyAppendOnlyResult.ResultCode.INVALID_BEGIN_SNAPSHOT, result.getResultCode());
    assertEquals(0, result.getSnapshotRanges().size());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * Invalid begin snapshot Expected ResultCode: INVALID_END_SNAPSHOT
   */
  @Test
  public void testInvalidEndSnapshot() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    Snapshot snapshot1 = getCurrentSnapshot();
    assertNotEquals(snapshot0, snapshot1);

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, snapshot0.snapshotId(), 123L);

    assertEquals(
        DatasetVerifyAppendOnlyResult.ResultCode.INVALID_END_SNAPSHOT, result.getResultCode());
    assertEquals(0, result.getSnapshotRanges().size());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * Begin snapshot is not ancestor of end snapshot Expected ResultCode: NOT_ANCESTOR
   */
  @Test
  public void testNotAncestor() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    Snapshot snapshot1 = getCurrentSnapshot();
    assertNotEquals(snapshot0, snapshot1);

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, snapshot1.snapshotId(), snapshot0.snapshotId());

    assertEquals(DatasetVerifyAppendOnlyResult.ResultCode.NOT_ANCESTOR, result.getResultCode());
    assertEquals(0, result.getSnapshotRanges().size());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * S0 -> Append -> S1 -> Update -> S2 Expected ResultCode: NOT_APPEND_ONLY
   */
  @Test
  public void testNotAppendOnly() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    runSQL(String.format("UPDATE %s.%s SET c2='abc'", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot2 = getCurrentSnapshot();

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, snapshot0.snapshotId(), snapshot2.snapshotId());

    assertEquals(DatasetVerifyAppendOnlyResult.ResultCode.NOT_APPEND_ONLY, result.getResultCode());
    assertEquals(0, result.getSnapshotRanges().size());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * Begin snapshot is same as end snapshot Expected ResultCode: APPEND_ONLY Expected snapshot
   * ranges: [(S1, S1)]
   */
  @Test
  public void testSameSnapshots() throws Exception {
    Snapshot snapshot0 = getCurrentSnapshot();
    insertOneRecord();
    Snapshot snapshot1 = getCurrentSnapshot();
    assertNotEquals(snapshot0, snapshot1);

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, snapshot1.snapshotId(), snapshot1.snapshotId());

    assertEquals(DatasetVerifyAppendOnlyResult.ResultCode.APPEND_ONLY, result.getResultCode());
    assertEquals(1, result.getSnapshotRanges().size());
    assertEquals(
        Long.toString(snapshot1.snapshotId()), result.getSnapshotRanges().get(0).getLeft());
    assertEquals(
        Long.toString(snapshot1.snapshotId()), result.getSnapshotRanges().get(0).getRight());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 Expected ResultCode: APPEND_ONLY Expected
   * snapshot ranges: [(S0, S2)]
   */
  @Test
  public void testWithOneCompact() throws Exception {
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

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, snapshot0.snapshotId(), snapshot3.snapshotId());

    assertEquals(DatasetVerifyAppendOnlyResult.ResultCode.APPEND_ONLY, result.getResultCode());
    assertEquals(1, result.getSnapshotRanges().size());
    assertEquals(
        Long.toString(snapshot0.snapshotId()), result.getSnapshotRanges().get(0).getLeft());
    assertEquals(
        Long.toString(snapshot2.snapshotId()), result.getSnapshotRanges().get(0).getRight());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Append -> S4 -> Compact -> S5 Expected
   * ResultCode: APPEND_ONLY Expected snapshot ranges: [(S3, S4), (S0, S2)]
   */
  @Test
  public void testWithMultiCompact() throws Exception {
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

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, snapshot0.snapshotId(), snapshot5.snapshotId());

    assertEquals(DatasetVerifyAppendOnlyResult.ResultCode.APPEND_ONLY, result.getResultCode());
    assertEquals(2, result.getSnapshotRanges().size());
    assertEquals(
        Long.toString(snapshot3.snapshotId()), result.getSnapshotRanges().get(0).getLeft());
    assertEquals(
        Long.toString(snapshot4.snapshotId()), result.getSnapshotRanges().get(0).getRight());
    assertEquals(
        Long.toString(snapshot0.snapshotId()), result.getSnapshotRanges().get(1).getLeft());
    assertEquals(
        Long.toString(snapshot2.snapshotId()), result.getSnapshotRanges().get(1).getRight());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Append -> S4 -> Compact -> S5 -> Append
   * -> S6 Expected ResultCode: APPEND_ONLY Expected snapshot ranges: [(S5, S6), (S3, S4), (S0, S2)]
   */
  @Test
  public void testWithMultiCompactAndAppend() throws Exception {
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

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, snapshot0.snapshotId(), snapshot6.snapshotId());

    assertEquals(DatasetVerifyAppendOnlyResult.ResultCode.APPEND_ONLY, result.getResultCode());
    assertEquals(3, result.getSnapshotRanges().size());
    assertEquals(
        Long.toString(snapshot5.snapshotId()), result.getSnapshotRanges().get(0).getLeft());
    assertEquals(
        Long.toString(snapshot6.snapshotId()), result.getSnapshotRanges().get(0).getRight());
    assertEquals(
        Long.toString(snapshot3.snapshotId()), result.getSnapshotRanges().get(1).getLeft());
    assertEquals(
        Long.toString(snapshot4.snapshotId()), result.getSnapshotRanges().get(1).getRight());
    assertEquals(
        Long.toString(snapshot0.snapshotId()), result.getSnapshotRanges().get(2).getLeft());
    assertEquals(
        Long.toString(snapshot2.snapshotId()), result.getSnapshotRanges().get(2).getRight());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Compact -> S4 -> Append -> S5 Expected
   * ResultCode: APPEND_ONLY Expected snapshot ranges: [(S4, S5), (S0, S2)]
   */
  @Test
  public void testWithAdjacentCompacts() throws Exception {
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

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, snapshot0.snapshotId(), snapshot5.snapshotId());

    assertEquals(DatasetVerifyAppendOnlyResult.ResultCode.APPEND_ONLY, result.getResultCode());
    assertEquals(2, result.getSnapshotRanges().size());
    assertEquals(
        Long.toString(snapshot4.snapshotId()), result.getSnapshotRanges().get(0).getLeft());
    assertEquals(
        Long.toString(snapshot5.snapshotId()), result.getSnapshotRanges().get(0).getRight());
    assertEquals(
        Long.toString(snapshot0.snapshotId()), result.getSnapshotRanges().get(1).getLeft());
    assertEquals(
        Long.toString(snapshot2.snapshotId()), result.getSnapshotRanges().get(1).getRight());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * -> Compact -> S3 -> Compact -> S4 Expected ResultCode: APPEND_ONLY Expected snapshot ranges:
   * [(S3, S3)]
   */
  @Test
  public void testAllAdjacentCompacts() throws Exception {
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

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, snapshot3.snapshotId(), snapshot4.snapshotId());

    assertEquals(DatasetVerifyAppendOnlyResult.ResultCode.APPEND_ONLY, result.getResultCode());
    assertEquals(1, result.getSnapshotRanges().size());
    assertEquals(
        Long.toString(snapshot3.snapshotId()), result.getSnapshotRanges().get(0).getLeft());
    assertEquals(
        Long.toString(snapshot3.snapshotId()), result.getSnapshotRanges().get(0).getRight());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * -> Append -> S2 -> Compact -> S3 -> Compact -> S4 Expected ResultCode: APPEND_ONLY Expected
   * snapshot ranges: [(S2, S2)]
   */
  @Test
  public void testAppendAndAllAdjacentCompacts() throws Exception {
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

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, snapshot2.snapshotId(), snapshot4.snapshotId());

    assertEquals(DatasetVerifyAppendOnlyResult.ResultCode.APPEND_ONLY, result.getResultCode());
    assertEquals(1, result.getSnapshotRanges().size());
    assertEquals(
        Long.toString(snapshot2.snapshotId()), result.getSnapshotRanges().get(0).getLeft());
    assertEquals(
        Long.toString(snapshot2.snapshotId()), result.getSnapshotRanges().get(0).getRight());
  }

  /**
   * Test metadata verify by calling {@link
   * IcebergMetadataVerifyProcessors#isAppendOnlyBetweenSnapshots(Table, Long, Long)} directly Test:
   * S0 -> Append -> S1 -> Append -> S2 -> Compact -> S3 -> Update -> S4 -> Compact -> S5 -> Append
   * -> S6 Expected ResultCode: NOT_APPEND_ONLY
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

    runSQL(String.format("UPDATE %s.%s SET c2='abc'", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));

    runSQL(
        String.format(
            "OPTIMIZE TABLE %s.%s REWRITE MANIFESTS", TEMP_SCHEMA_HADOOP, TEST_TABLE_NAME));
    Snapshot snapshot5 = getCurrentSnapshot();

    insertOneRecord();
    Snapshot snapshot6 = getCurrentSnapshot();

    assertEquals(DataOperations.REPLACE, snapshot3.operation());
    assertEquals(DataOperations.REPLACE, snapshot5.operation());

    DatasetVerifyAppendOnlyResult result =
        (DatasetVerifyAppendOnlyResult)
            IcebergMetadataVerifyProcessors.isAppendOnlyBetweenSnapshots(
                test_iceberg_table, snapshot0.snapshotId(), snapshot6.snapshotId());

    assertEquals(DatasetVerifyAppendOnlyResult.ResultCode.NOT_APPEND_ONLY, result.getResultCode());
    assertEquals(0, result.getSnapshotRanges().size());
  }
}

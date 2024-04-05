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
package com.dremio.exec.planner.sql;

import static com.dremio.exec.planner.VacuumOutputSchema.DELETED_FILES_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETED_FILES_SIZE_MB;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_DATA_FILE_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_EQUALITY_DELETE_FILES_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_MANIFEST_FILES_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_MANIFEST_LISTS_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_PARTITION_STATS_FILES_COUNT;
import static com.dremio.exec.planner.VacuumOutputSchema.DELETE_POSITION_DELETE_FILES_COUNT;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.EMPTY_PATHS;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.PARTITION_COLUMN_ONE_INDEX_SET;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addQuotes;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addRows;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicNonPartitionedAndPartitionedTables;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createEmptyTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createStockIcebergTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.loadTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testMalformedDmlQueries;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testQueryValidateStatusSummary;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.verifyCountSnapshotQuery;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.verifyData;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.waitUntilAfter;
import static com.dremio.exec.planner.sql.handlers.SqlHandlerUtil.getTimestampFromMillis;
import static com.dremio.exec.store.iceberg.IcebergUtils.getPartitionStatsFiles;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.dremio.TestBuilder;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.io.file.Path;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatsFileLocations;
import org.apache.iceberg.PartitionStatsMetadata;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.junit.Assert;

/**
 * Vacuum tests.
 *
 * <p>Note: Add tests used across all platforms here.
 */
public class VacuumTests extends ITDmlQueryBase {
  public static void testMalformedVacuumQueries(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      testMalformedDmlQueries(
          new Object[] {table.fqn, "'2022-10-22 18:24:30'", "'3'"},
          "VACUUM",
          "VACUUM TABLE",
          "VACUUM TABLE %s",
          "VACUUM TABLE %s EXPIRE",
          "VACUUM TABLE %s EXPIRE SNAPSHOT",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS %s",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS %s =",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN =",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = %s RETAIN_LAST",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN RETAIN_LAST",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN %s RETAIN_LAST",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = %s RETAIN_LAST =",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST =",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST '3'",
          "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST = '3'");
    }
  }

  public static void testSimpleExpireOlderThanRetainLastUsingEqual(
      BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      Thread.sleep(100);
      final long timestampMillisToExpire = System.currentTimeMillis();
      // Insert more rows to increase snapshots
      DmlQueryTestUtils.Table table2 = addRows(table, 1);
      table2 = addRows(table2, 1);
      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = '%s' RETAIN_LAST = 1",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 2L, 0L});

      // Data not changed.
      verifyData(allocator, table2, table2.originalData);
    }
  }

  public static void testSimpleExpireOlderThan(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      Thread.sleep(100);
      final long timestampMillisToExpire = System.currentTimeMillis();
      // Insert more rows to increase snapshots
      DmlQueryTestUtils.Table table2 = addRows(table, 1);
      table2 = addRows(table2, 1);
      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 2L, 0L});

      // Data not changed.
      verifyData(allocator, table2, table2.originalData);
    }
  }

  public static void testExpireOlderThan(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName =
          table.name.startsWith("\"")
              ? table.name.substring(1, table.name.length() - 1)
              : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long timestampMillisToExpire =
          waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());
      Assert.assertEquals("Should have two snapshots", 2, Iterables.size(icebergTable.snapshots()));
      Assert.assertEquals("Should have two history entries", 2, icebergTable.history().size());
      final Snapshot firstSnapshot = Iterables.getFirst(icebergTable.snapshots(), null);
      Assert.assertNotNull("Should get first snapshot", firstSnapshot);
      final Snapshot secondSnapshot = Iterables.getLast(icebergTable.snapshots());

      // Insert more rows to increase snapshots
      DmlQueryTestUtils.Table table2 = addRows(table, 1);
      table2 = addRows(table2, 1);
      Table updatedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long expectedSnapshotId = updatedTable.currentSnapshot().snapshotId();
      Assert.assertEquals(
          "Should have four snapshots", 4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals("Should have four history entries", 4, updatedTable.history().size());

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 2L, 0L});

      // Data not changed.
      verifyData(allocator, table2, table2.originalData);

      Table vacuumedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(
          "Expire should keep last two snapshots", 2, Iterables.size(vacuumedTable.snapshots()));
      Assert.assertEquals(
          "Expire should keep last two history entries ", 2, vacuumedTable.history().size());
      Assert.assertEquals(
          "Expire should not change current snapshot",
          expectedSnapshotId,
          vacuumedTable.currentSnapshot().snapshotId());
      Assert.assertNull(
          "Expire should remove the oldest snapshot",
          vacuumedTable.snapshot(firstSnapshot.snapshotId()));
      Assert.assertNull(
          "Expire should remove the second oldest snapshot",
          vacuumedTable.snapshot(secondSnapshot.snapshotId()));
    }
  }

  public static void testExpireRetainLast(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName =
          table.name.startsWith("\"")
              ? table.name.substring(1, table.name.length() - 1)
              : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals("Should have two snapshots", 2, Iterables.size(icebergTable.snapshots()));
      Assert.assertEquals("Should have two history entries", 2, icebergTable.history().size());
      final Snapshot firstSnapshot = Iterables.getFirst(icebergTable.snapshots(), null);
      Assert.assertNotNull("Should get first snapshot", firstSnapshot);
      final Snapshot secondSnapshot = Iterables.getLast(icebergTable.snapshots());

      // Insert more rows to increase snapshots
      DmlQueryTestUtils.Table table2 = addRows(table, 1);
      table2 = addRows(table2, 1);
      Table updatedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long expectedSnapshotId = updatedTable.currentSnapshot().snapshotId();
      Assert.assertEquals(
          "Should have four snapshots", 4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals("Should have four history entries", 4, updatedTable.history().size());

      // No snapshots are dated back to default 5 days ago, and no snapshots are expired, even claim
      // to retain last 2.
      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST %s",
          new Object[] {table.fqn, "2"},
          table,
          new Long[] {0L, 0L, 0L, 0L, 0L, 0L});

      // Data not changed.
      verifyData(allocator, table2, table2.originalData);

      Table vacuumedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(
          "Expire should keep last four snapshots", 4, Iterables.size(vacuumedTable.snapshots()));
      Assert.assertEquals(
          "Expire should keep last four history entries ", 4, vacuumedTable.history().size());
      Assert.assertEquals(
          "Expire should not change current snapshot",
          expectedSnapshotId,
          vacuumedTable.currentSnapshot().snapshotId());
      Assert.assertNotNull(
          "Expire should not remove the oldest snapshot",
          vacuumedTable.snapshot(firstSnapshot.snapshotId()));
      Assert.assertNotNull(
          "Expire should not remove the second oldest snapshot",
          vacuumedTable.snapshot(secondSnapshot.snapshotId()));
    }
  }

  public static void testRetainLastWithExpireOlderThan(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName =
          table.name.startsWith("\"")
              ? table.name.substring(1, table.name.length() - 1)
              : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals("Should have two snapshots", 2, Iterables.size(icebergTable.snapshots()));
      Assert.assertEquals("Should have two history entries", 2, icebergTable.history().size());
      final Snapshot firstSnapshot = Iterables.getFirst(icebergTable.snapshots(), null);
      Assert.assertNotNull("Should get first snapshot", firstSnapshot);
      final Snapshot secondSnapshot = Iterables.getLast(icebergTable.snapshots());

      // Insert more rows to increase snapshots
      DmlQueryTestUtils.Table table2 = addRows(table, 1);
      table2 = addRows(table2, 1);
      Table updatedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long timestampMillisToExpire =
          waitUntilAfter(updatedTable.currentSnapshot().timestampMillis());
      Assert.assertEquals(
          "Should have four snapshots", 4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals("Should have four history entries", 4, updatedTable.history().size());
      final long expectedSnapshotId = updatedTable.currentSnapshot().snapshotId();

      // Use the latest snapshot's timestamp for OLDER_THAN. But, it still needs to keep two
      // snapshots as RETAIN_LAST is set.
      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s' RETAIN_LAST %s",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire), "2"},
          table,
          new Long[] {0L, 0L, 0L, 0L, 2L, 0L});

      // Data not changed.
      verifyData(allocator, table2, table2.originalData);

      Table vacuumedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(
          "Expire should keep last two snapshots", 2, Iterables.size(vacuumedTable.snapshots()));
      Assert.assertEquals(
          "Expire should keep last two history entries ", 2, vacuumedTable.history().size());
      Assert.assertEquals(
          "Expire should not change current snapshot",
          expectedSnapshotId,
          vacuumedTable.currentSnapshot().snapshotId());
      Assert.assertNull(
          "Expire should remove the oldest snapshot",
          vacuumedTable.snapshot(firstSnapshot.snapshotId()));
      Assert.assertNull(
          "Expire should remove the second oldest snapshot",
          vacuumedTable.snapshot(secondSnapshot.snapshotId()));
    }
  }

  public static void testExpireDataFilesCleanup(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName =
          table.name.startsWith("\"")
              ? table.name.substring(1, table.name.length() - 1)
              : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long rollbackToSnapshotId = icebergTable.currentSnapshot().snapshotId();

      // Collect all data files, which should be cleared by expire.
      Set<String> dataFilesToClean = Sets.newHashSet();
      addRows(table, 1);
      icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      dataFilesToClean.addAll(
          collectDataFilesFromSnapshot(icebergTable.currentSnapshot(), icebergTable.io()));
      addRows(table, 1);
      icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      dataFilesToClean.addAll(
          collectDataFilesFromSnapshot(icebergTable.currentSnapshot(), icebergTable.io()));
      addRows(table, 1);
      icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      dataFilesToClean.addAll(
          collectDataFilesFromSnapshot(icebergTable.currentSnapshot(), icebergTable.io()));

      final long timestampMillisToExpire =
          waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());

      testQueryValidateStatusSummary(
          allocator,
          "ROLLBACK TABLE %s TO SNAPSHOT '%s'",
          new Object[] {table.fqn, rollbackToSnapshotId},
          table,
          true,
          String.format("Table [%s] rollbacked", table.fqn),
          null);

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {3L, 0L, 0L, 3L, 4L, 0L});

      // Data not changed.
      verifyData(allocator, table, table.originalData);

      Table vacuumedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final Set<String> filesAfterVacuum = collectDataFilesFromTable(vacuumedTable);
      for (String file : dataFilesToClean) {
        Assert.assertFalse("Expire should clean the file", filesAfterVacuum.contains(file));
        File filePath = new File(Path.getContainerSpecificRelativePath(Path.of(file)));
        Assert.assertFalse(filePath.exists());
      }
    }
  }

  public static void testExpireOlderThanWithRollback(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName =
          table.name.startsWith("\"")
              ? table.name.substring(1, table.name.length() - 1)
              : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals("Should have two snapshots", 2, Iterables.size(icebergTable.snapshots()));
      Assert.assertEquals("Should have two history entries", 2, icebergTable.history().size());
      final Snapshot firstSnapshot = Iterables.getFirst(icebergTable.snapshots(), null);
      Assert.assertNotNull("Should get first snapshot", firstSnapshot);
      final Snapshot secondSnapshot = Iterables.getLast(icebergTable.snapshots());
      final long rollbackToSnapshotId = secondSnapshot.snapshotId();

      // Insert more rows to increase snapshots
      addRows(table, 1);
      final Snapshot thirdSnapshot =
          getIcebergTable(tableFolder, IcebergCatalogType.HADOOP).currentSnapshot();
      addRows(table, 1);
      final Snapshot fourthSnapshot =
          getIcebergTable(tableFolder, IcebergCatalogType.HADOOP).currentSnapshot();
      final long timestampMillisToExpire = waitUntilAfter(fourthSnapshot.timestampMillis());

      testQueryValidateStatusSummary(
          allocator,
          "ROLLBACK TABLE %s TO SNAPSHOT '%s'",
          new Object[] {table.fqn, rollbackToSnapshotId},
          table,
          true,
          String.format("Table [%s] rollbacked", table.fqn),
          null);

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s' RETAIN_LAST 2",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {2L, 0L, 0L, 2L, 2L, 0L});

      // Data not changed.
      verifyData(allocator, table, table.originalData);

      Table vacuumedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(
          "Expire should keep last 2 snapshot", 2, Iterables.size(vacuumedTable.snapshots()));
      Assert.assertEquals(
          "Expire should not change current snapshot",
          rollbackToSnapshotId,
          vacuumedTable.currentSnapshot().snapshotId());
      Assert.assertNotNull(
          "Expire should keep the first snapshot, current",
          vacuumedTable.snapshot(firstSnapshot.snapshotId()));
      Assert.assertNotNull(
          "Expire should keep the second snapshot, current",
          vacuumedTable.snapshot(secondSnapshot.snapshotId()));
      Assert.assertNull(
          "Expire should remove the orphaned snapshot",
          vacuumedTable.snapshot(thirdSnapshot.snapshotId()));
      Assert.assertNull(
          "Expire should remove the orphaned snapshot",
          vacuumedTable.snapshot(fourthSnapshot.snapshotId()));
    }
  }

  public static void testExpireOnTableWithPartitions(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Tables tables =
        createBasicNonPartitionedAndPartitionedTables(
            source, 2, 3, PARTITION_COLUMN_ONE_INDEX_SET)) {
      Assert.assertEquals("Should have two tables", 2, tables.tables.length);
      // Second table has partitions
      DmlQueryTestUtils.Table table = tables.tables[1];
      Table icebergTable = loadIcebergTable(table);
      Assert.assertEquals("Should have two snapshots", 2, Iterables.size(icebergTable.snapshots()));
      Assert.assertEquals("Should have two history entries", 2, icebergTable.history().size());
      final Snapshot firstSnapshot = Iterables.getFirst(icebergTable.snapshots(), null);
      Assert.assertNotNull("Should get first snapshot", firstSnapshot);
      final Snapshot secondSnapshot = Iterables.getLast(icebergTable.snapshots());
      final long rollbackToSnapshotId = secondSnapshot.snapshotId();

      // Insert more rows to increase snapshots and partition files
      addRows(table, 2);
      addRows(table, 2);

      final long timestampMillisToExpire = System.currentTimeMillis();

      testQueryValidateStatusSummary(
          allocator,
          "ROLLBACK TABLE %s TO SNAPSHOT '%s'",
          new Object[] {table.fqn, rollbackToSnapshotId},
          table,
          true,
          String.format("Table [%s] rollbacked", table.fqn),
          null);

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s' RETAIN_LAST 2",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {4L, 0L, 0L, 2L, 2L, 4L});

      Table icebergTable2 = loadIcebergTable(table);
      Assert.assertEquals(
          "Should have two snapshots", 2, Iterables.size(icebergTable2.snapshots()));
    }
  }

  public static void testExpireOnEmptyTableNoSnapshots(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(source, 0, 2, "modes_isolation")) {
      Table icebergTable = loadTable(table);
      Assert.assertNull(icebergTable.currentSnapshot());
      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s' RETAIN_LAST 1",
          new Object[] {table.fqn, getTimestampFromMillis(System.currentTimeMillis())},
          table,
          new Long[] {0L, 0L, 0L, 0L, 0L, 0L});

      icebergTable.refresh();
      Assert.assertNull(icebergTable.currentSnapshot());
    }
  }

  public static void testRetainZeroSnapshots(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName =
          table.name.startsWith("\"")
              ? table.name.substring(1, table.name.length() - 1)
              : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long timestampMillisToExpire =
          waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());

      UserExceptionAssert.assertThatThrownBy(
              () ->
                  test(
                      "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s' RETAIN_LAST 0",
                      table.fqn, getTimestampFromMillis(timestampMillisToExpire)))
          .hasErrorType(ErrorType.UNSUPPORTED_OPERATION)
          .hasMessageContaining("Minimum number of snapshots to retain can be 1");
    }
  }

  public static void testInvalidTimestampLiteral(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      UserExceptionAssert.assertThatThrownBy(
              () -> test("VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '2022-09-01 abc'", table.fqn))
          .hasErrorType(ErrorType.PARSE)
          .hasMessageContaining("Literal '2022-09-01 abc' cannot be casted to TIMESTAMP");
    }
  }

  public static void testEmptyTimestamp(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      UserExceptionAssert.assertThatThrownBy(
              () -> test("VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN ''", table.fqn))
          .hasErrorType(ErrorType.PARSE)
          .hasMessageContaining("Literal '' cannot be casted to TIMESTAMP");
    }
  }

  public static void testExpireDatasetRefreshed(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName =
          table.name.startsWith("\"")
              ? table.name.substring(1, table.name.length() - 1)
              : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long timestampMillisToExpire =
          waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());
      verifyCountSnapshotQuery(allocator, table.fqn, 2L);
      // Insert more rows to increase snapshots
      newSnapshots(table, 2);
      verifyCountSnapshotQuery(allocator, table.fqn, 4L);

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 2L, 0L});

      // The count table_snapshot query result should be refreshed and only 2 are left.
      verifyCountSnapshotQuery(allocator, table.fqn, 2L);
    }
  }

  public static void testUnparseSqlVacuum(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName =
          table.name.startsWith("\"")
              ? table.name.substring(1, table.name.length() - 1)
              : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long timestampMillisToExpire =
          waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());

      final String vacuumQuery =
          String.format(
              "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s' RETAIN_LAST 1",
              table.fqn, getTimestampFromMillis(timestampMillisToExpire));

      final String expected =
          String.format(
              "VACUUM TABLE %s EXPIRE SNAPSHOTS \"OLDER_THAN\" '%s' \"RETAIN_LAST\" 1",
              "\"" + source + "\"." + addQuotes(tableName),
              getTimestampFromMillis(timestampMillisToExpire));
      parseAndValidateSqlNode(vacuumQuery, expected);
    }
  }

  public static void testExpireOnTableOneSnapshot(BufferAllocator allocator, String source)
      throws Exception {
    // Table has only one snapshot. Don't need to run expire snapshots query.
    try (DmlQueryTestUtils.Table table = createEmptyTable(source, EMPTY_PATHS, "tableName", 1)) {
      final long timestampMillisToExpire = System.currentTimeMillis();

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 0L, 0L});
    }
  }

  public static void testRetainMoreSnapshots(BufferAllocator allocator, String source)
      throws Exception {
    // Table has less snapshot than the retained number. Don't need to run expire snapshots query.
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST 5",
          new Object[] {table.fqn},
          table,
          new Long[] {0L, 0L, 0L, 0L, 0L, 0L});
    }
  }

  public static void testRetainAllSnapshots(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      verifyCountSnapshotQuery(allocator, table.fqn, 2L);
      // Insert more rows to increase snapshots
      newSnapshots(table, 5);

      verifyCountSnapshotQuery(allocator, table.fqn, 7L);

      // No snapshots are dated back to default 5 days ago, and no snapshots are expired, even claim
      // to retain last 2.
      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST %s",
          new Object[] {table.fqn, "2"},
          table,
          new Long[] {0L, 0L, 0L, 0L, 0L, 0L});
    }
  }

  public static void testGCDisabled(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      newSnapshots(table, 5);
      verifyCountSnapshotQuery(allocator, table.fqn, 7L);

      Table icebergTable = loadIcebergTable(table);
      icebergTable
          .updateProperties()
          .set(TableProperties.GC_ENABLED, Boolean.FALSE.toString())
          .commit();
      runSQL(String.format("ALTER TABLE %s REFRESH METADATA FORCE UPDATE", table.fqn));

      final long timestampMillisToExpire = System.currentTimeMillis();

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 0L, 0L});

      verifyCountSnapshotQuery(allocator, table.fqn, 7L);
      assertDoesNotThrow(
          () ->
              runSQL(
                  String.format(
                      "SELECT * FROM %s LIMIT 1", table.fqn))); // ensure select query goes through
    }
  }

  public static void testMinSnapshotsTablePropOverride(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      newSnapshots(table, 5);
      verifyCountSnapshotQuery(allocator, table.fqn, 7L);

      Table icebergTable = loadIcebergTable(table);
      icebergTable.updateProperties().set(TableProperties.MIN_SNAPSHOTS_TO_KEEP, "4").commit();
      runSQL(String.format("ALTER TABLE %s REFRESH METADATA FORCE UPDATE", table.fqn));

      final long timestampMillisToExpire = System.currentTimeMillis();

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 3L, 0L});

      verifyCountSnapshotQuery(allocator, table.fqn, 4L); // Only 4 retained
      assertDoesNotThrow(
          () ->
              runSQL(
                  String.format(
                      "SELECT * FROM %s LIMIT 1", table.fqn))); // ensure select query goes through
    }
  }

  public static void testMinSnapshotsAggressiveTablePropOverride(
      BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      newSnapshots(table, 5);
      verifyCountSnapshotQuery(allocator, table.fqn, 7L);

      Table icebergTable = loadIcebergTable(table);
      icebergTable.updateProperties().set(TableProperties.MIN_SNAPSHOTS_TO_KEEP, "2").commit();
      runSQL(String.format("ALTER TABLE %s REFRESH METADATA FORCE UPDATE", table.fqn));

      final long timestampMillisToExpire = System.currentTimeMillis();

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s' RETAIN_LAST 4",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 3L, 0L});

      verifyCountSnapshotQuery(allocator, table.fqn, 4L); // Only 4 retained
      assertDoesNotThrow(
          () ->
              runSQL(
                  String.format(
                      "SELECT * FROM %s LIMIT 1", table.fqn))); // ensure select query goes through
    }
  }

  public static void testSnapshotAgeTablePropOverride(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      newSnapshots(table, 5);
      verifyCountSnapshotQuery(allocator, table.fqn, 7L);

      Table icebergTable = loadIcebergTable(table);
      icebergTable
          .updateProperties()
          .set(TableProperties.MAX_SNAPSHOT_AGE_MS, "3600000") // an hour
          .commit();
      runSQL(String.format("ALTER TABLE %s REFRESH METADATA FORCE UPDATE", table.fqn));

      final long timestampMillisToExpire = System.currentTimeMillis();

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 0L, 0L});

      verifyCountSnapshotQuery(
          allocator, table.fqn, 7L); // All are retained because table level property expects 1hour
    }
  }

  public static void testSnapshotAgeAggressiveTablePropOverride(
      BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      newSnapshots(table, 3);
      final long timestampMillisToExpire = System.currentTimeMillis();
      newSnapshots(table, 3);

      verifyCountSnapshotQuery(allocator, table.fqn, 8L);

      waitUntilAfter(System.currentTimeMillis() + 1);
      Table icebergTable = loadIcebergTable(table);
      icebergTable.updateProperties().set(TableProperties.MAX_SNAPSHOT_AGE_MS, "1").commit();
      runSQL(String.format("ALTER TABLE %s REFRESH METADATA FORCE UPDATE", table.fqn));

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 5L, 0L});

      verifyCountSnapshotQuery(
          allocator, table.fqn, 3L); // All are retained because table level property expects 1hour
    }
  }

  public static void testExpireSnapshotsWithTensOfSnapshots(
      BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      // Verify number of snapshots
      verifyCountSnapshotQuery(allocator, table.fqn, 2L);

      // Insert no less than 50 snapshots.
      DmlQueryTestUtils.Table table2;
      long timestampMillisToExpire = System.currentTimeMillis();
      for (int i = 0; i < 60; i++) {
        table2 = addRows(table, 1);
        if (49 == i) {
          timestampMillisToExpire = System.currentTimeMillis();
        }
      }
      verifyCountSnapshotQuery(allocator, table.fqn, 62L);
      runSQL(
          String.format(
              "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
              table.fqn, getTimestampFromMillis(timestampMillisToExpire)));
      verifyCountSnapshotQuery(allocator, table.fqn, 10L);
    }
  }

  public static void testExpireSnapshotsOnNonMainBranch(String source, BufferAllocator allocator)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(source, 0, 2, "nonMainBranch")) {
      Table icebergTable = loadTable(table);
      Assert.assertEquals(0, Iterables.size(icebergTable.snapshots()));

      appendManifestFiles(icebergTable, "dev");
      appendManifestFiles(icebergTable, "dev");
      appendManifestFiles(icebergTable, "dev");
      Assert.assertEquals(
          "Two snapshots in dev branch", 3, Iterables.size(icebergTable.snapshots()));
      // Committing to 'dev' branch does not make the table have valid current snapshot.
      Assert.assertNull(icebergTable.currentSnapshot());

      long timestampMillisToExpire = waitUntilAfter(System.currentTimeMillis());
      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 2L, 0L});
    }
  }

  public static void testExpireSnapshotsOnDifferentBranches(
      String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(source, 0, 2, "twoBranches")) {
      Table icebergTable = loadTable(table);
      Assert.assertEquals(0, Iterables.size(icebergTable.snapshots()));

      appendManifestFiles(icebergTable, "dev");
      appendManifestFiles(icebergTable, "dev");
      Assert.assertEquals(
          "One snapshot in dev branch", 2, Iterables.size(icebergTable.snapshots()));
      // Committing to 'dev' branch does not make the table have valid current snapshot.
      Assert.assertNull(icebergTable.currentSnapshot());
      appendManifestFiles(icebergTable, "main");
      long timestampMillisToExpire = waitUntilAfter(System.currentTimeMillis());
      appendManifestFiles(icebergTable, "main");
      Assert.assertEquals(4, Iterables.size(icebergTable.snapshots()));

      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 2L, 0L});

      icebergTable.refresh();
      Assert.assertEquals(2, Iterables.size(icebergTable.snapshots()));
    }
  }

  /** Test cases for Remove orphan files */
  public static void testMalformedVacuumRemoveOrphanFileQueries(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      testMalformedDmlQueries(
          new Object[] {table.fqn, "'2022-10-22 18:24:30'", "'file:///path'"},
          "VACUUM TABLE %s REMOVE",
          "VACUUM TABLE %s REMOVE ORPHAN",
          "VACUUM TABLE %s REMOVE ORPHAN FILE",
          "VACUUM TABLE %s REMOVE ORPHAN FILES %s",
          "VACUUM TABLE %s REMOVE ORPHAN FILES %s =",
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN",
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN =",
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN = %s LOCATION",
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN LOCATION",
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN %s LOCATION",
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN = %s LOCATION =",
          "VACUUM TABLE %s REMOVE ORPHAN FILES LOCATION",
          "VACUUM TABLE %s REMOVE ORPHAN FILES LOCATION =",
          "VACUUM TABLE %s REMOVE ORPHAN FILES LOCATION 3",
          "VACUUM TABLE %s REMOVE ORPHAN FILES LOCATION = 3");
    }
  }

  public static void testSimpleRemoveOrphanFiles(BufferAllocator allocator, String source)
      throws Exception {
    // Don't add any orphan files into table location. Thus, no files should be removed.
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      Thread.sleep(100);
      verifyCountSnapshotQuery(allocator, table.fqn, 2L);

      // Add more row and increase snapshot
      DmlQueryTestUtils.Table table2 = addRows(table, 1);
      final long timestampMillisToExpire = System.currentTimeMillis();
      validateRemoveOrphanFilesOutputResult(
          allocator,
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          new Long[] {0L, 0L});

      // Don't modify table
      verifyCountSnapshotQuery(allocator, table.fqn, 3L);
      // Data not changed.
      verifyData(allocator, table2, table2.originalData);
    }
  }

  public static void testSimpleRemoveOrphanFilesUsingEqual(BufferAllocator allocator, String source)
      throws Exception {
    // Don't add any orphan files into table location. Thus, no files should be removed.
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      Thread.sleep(100);
      verifyCountSnapshotQuery(allocator, table.fqn, 2L);

      Table icebergTable = loadIcebergTable(table);
      String tableLocation =
          Path.getContainerSpecificRelativePath(Path.of(icebergTable.location()));

      final long timestampMillisToExpire = System.currentTimeMillis();
      validateRemoveOrphanFilesOutputResult(
          allocator,
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN = '%s' LOCATION '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire), tableLocation},
          new Long[] {0L, 0L});
    }
  }

  public static void testRemoveOrphanFilesNotDeleteValidFiles(
      BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Tables tables =
        createBasicNonPartitionedAndPartitionedTables(
            source, 2, 3, PARTITION_COLUMN_ONE_INDEX_SET)) {
      Assert.assertEquals("Should have two tables", 2, tables.tables.length);

      // First table doesn't have partitions
      DmlQueryTestUtils.Table table = tables.tables[0];
      // Add more row and increase snapshot
      addRows(table, 1);
      Table icebergTable = loadIcebergTable(table);
      Assert.assertEquals(
          "Should have three snapshots", 3, Iterables.size(icebergTable.snapshots()));

      // Collect files before running remove orphan files
      final Set<String> tableFiles = collectAllFilesFromTable(icebergTable);
      Assert.assertEquals("Should have 11 files", 11, tableFiles.size());

      final long timestampMillisToExpire = System.currentTimeMillis();
      validateRemoveOrphanFilesOutputResult(
          allocator,
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          new Long[] {0L, 0L});

      // Remove orphan files should NOT delete valid files
      for (String file : tableFiles) {
        File filePath = new File(Path.getContainerSpecificRelativePath(Path.of(file)));
        Assert.assertTrue("File should not be removed", filePath.exists());
      }
    }
  }

  public static void testRemoveOrphanFilesDeleteOrphanFiles(
      BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {

      // Add row
      DmlQueryTestUtils.Table table2 = addRows(table, 1);

      Table icebergTable = loadIcebergTable(table);
      String tableLocation =
          Path.getContainerSpecificRelativePath(Path.of(icebergTable.location()));

      // Copy file: "iceberg/orphan_files/orphan.json" into to table folder as orphan files
      copyFromJar("iceberg/orphan_files", java.nio.file.Paths.get(tableLocation + "/orphanfiles"));

      File orphanFilePath = new File(tableLocation + "/orphanfiles/orphan.json");

      Assert.assertTrue("File should exist", orphanFilePath.exists());
      Thread.sleep(10);
      final long timestampMillisToExpire = System.currentTimeMillis();
      validateRemoveOrphanFilesOutputResult(
          allocator,
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          new Long[] {1L, 0L});

      // Copied orphan files should be deleted
      Assert.assertFalse("File should be removed", orphanFilePath.exists());

      // Don't modify table
      verifyCountSnapshotQuery(allocator, table.fqn, 3L);
      // Data not changed.
      verifyData(allocator, table2, table2.originalData);
    }
  }

  public static void testRemoveOrphanFilesWithLocationClause(
      BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {

      // Add row
      DmlQueryTestUtils.Table table2 = addRows(table, 1);

      Table icebergTable = loadIcebergTable(table);
      String tableLocation =
          Path.getContainerSpecificRelativePath(Path.of(icebergTable.location()));

      // Create two sub folders inside table location and put each folder with one orphan file.
      // Then, use one sub folder location as location clause to remove orphan files in that folder.

      // Copy file: "iceberg/orphan_files/orphan.json" into to table folder as orphan files
      copyFromJar("iceberg/orphan_files", java.nio.file.Paths.get(tableLocation + "/folder1"));
      File orphanFilePath1 = new File(tableLocation + "/folder1/orphan.json");
      Assert.assertTrue("File should exist", orphanFilePath1.exists());

      copyFromJar("iceberg/orphan_files", java.nio.file.Paths.get(tableLocation + "/folder2"));
      File orphanFilePath2 = new File(tableLocation + "/folder2/orphan.json");
      Assert.assertTrue("File should exist", orphanFilePath2.exists());

      Thread.sleep(10);
      final long timestampMillisToExpire = System.currentTimeMillis();

      runSQL(
          String.format(
              "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN '%s' LOCATION '%s'",
              table.fqn,
              getTimestampFromMillis(timestampMillisToExpire),
              tableLocation + "/folder1"));

      // Copied orphan file in 'folder1' should be deleted
      Assert.assertFalse("File should be removed", orphanFilePath1.exists());

      // Copied orphan file in 'folder2' should not be deleted
      Assert.assertTrue("File should not be removed", orphanFilePath2.exists());

      // Don't modify table
      verifyCountSnapshotQuery(allocator, table.fqn, 3L);
      // Data not changed.
      verifyData(allocator, table2, table2.originalData);
    }
  }

  public static void testRemoveOrphanFilesInvalidTimestampLiteral(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      UserExceptionAssert.assertThatThrownBy(
              () ->
                  test(
                      "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN '2022-09-01 abc'", table.fqn))
          .hasErrorType(ErrorType.PARSE)
          .hasMessageContaining("Literal '2022-09-01 abc' cannot be casted to TIMESTAMP");
    }
  }

  public static void testUnparseRemoveOrphanFilesQuery(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName =
          table.name.startsWith("\"")
              ? table.name.substring(1, table.name.length() - 1)
              : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long timestampMillisToExpire =
          waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());

      final String vacuumQuery =
          String.format(
              "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN '%s' LOCATION 'file:///fakepath'",
              table.fqn, getTimestampFromMillis(timestampMillisToExpire));

      final String expected =
          String.format(
              "VACUUM TABLE %s REMOVE ORPHAN FILES \"OLDER_THAN\" '%s' \"LOCATION\" 'file:///fakepath'",
              "\"" + source + "\"." + addQuotes(tableName),
              getTimestampFromMillis(timestampMillisToExpire));
      parseAndValidateSqlNode(vacuumQuery, expected);
    }
  }

  public static void testVacuumOnExternallyCreatedTable(BufferAllocator allocator, String source)
      throws Exception {
    // Table is not created using DREMIO SQL. This table is basically empty and only has a
    // version-hint file and
    // a metadata entry file. No manifest list files, manifest files and data files.
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(source, 1, 1, "t1")) {
      final long timestampMillisToExpire = waitUntilAfter(System.currentTimeMillis() + 1);
      validateOutputResult(
          allocator,
          "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = '%s' RETAIN_LAST = 1",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          table,
          new Long[] {0L, 0L, 0L, 0L, 0L, 0L});

      validateRemoveOrphanFilesOutputResult(
          allocator,
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          new Long[] {0L, 0L});

      assertDoesNotThrow(() -> runSQL(String.format("SELECT * FROM %s", table.fqn)));
    }
  }

  public static void testRemoveOrphanFilesHybridlyGeneratedTable(
      BufferAllocator allocator, String source) throws Exception {
    // Table is not created using DREMIO SQL. But, we use DREMIO SQL to insert rows. Thus, the table
    // includes hybrid
    // style paths, e.g., 'file:///path' and '/path'.
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(source, 1, 1, "t1")) {
      final long timestampMillisToExpire = waitUntilAfter(System.currentTimeMillis() + 1);
      DmlQueryTestUtils.Table table2 = addRows(table, 1);
      table2 = addRows(table2, 1);

      validateRemoveOrphanFilesOutputResult(
          allocator,
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          new Long[] {0L, 0L});

      // Don't modify table snapshots
      verifyCountSnapshotQuery(allocator, table.fqn, 2L);
      // Data not changed.
      verifyData(allocator, table2, table2.originalData);
    }
  }

  private static DmlQueryTestUtils.Table newSnapshots(
      DmlQueryTestUtils.Table table, int noOfSnapshots) throws Exception {
    for (int i = 0; i < noOfSnapshots; i++) {
      table = addRows(table, 1);
    }
    return table;
  }

  private static Table loadIcebergTable(DmlQueryTestUtils.Table table) {
    String tableName =
        table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
    File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
    return getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
  }

  private static Set<String> collectAllFilesFromTable(Table icebergTable) {
    Set<String> files = Sets.newHashSet();
    if (icebergTable == null) {
      return files;
    }

    // Snapshots files
    Iterator<Snapshot> iterator = icebergTable.snapshots().iterator();
    while (iterator.hasNext()) {
      Snapshot snapshot = iterator.next();
      files.addAll(collectAllFilesFromSnapshot(snapshot, icebergTable.io()));
    }

    File metadataFolder = new File(icebergTable.location(), "metadata");
    // Metadata files
    int numSnapshots = Iterables.size(icebergTable.snapshots());
    for (int i = 1; i <= numSnapshots; i++) {
      String metadataFile =
          metadataFolder.getPath() + Path.SEPARATOR + String.format("v%d.metadata.json", i);
      files.add(metadataFile);
    }
    files.add(metadataFolder.getPath() + Path.SEPARATOR + "version-hint.text");

    return files;
  }

  private static Set<String> collectDataFilesFromTable(Table icebergTable) {
    Set<String> files = Sets.newHashSet();
    if (icebergTable == null) {
      return files;
    }

    Iterator<Snapshot> iterator = icebergTable.snapshots().iterator();
    while (iterator.hasNext()) {
      Snapshot snapshot = iterator.next();
      files.addAll(collectDataFilesFromSnapshot(snapshot, icebergTable.io()));
    }

    return files;
  }

  private static Set<String> collectAllFilesFromSnapshot(Snapshot snapshot, FileIO io) {
    Set<String> files = Sets.newHashSet();
    files.addAll(pathSet(snapshot.addedDataFiles(io)));
    files.add(snapshot.manifestListLocation());
    files.addAll(manifestPaths(snapshot.allManifests(io)));
    files.addAll(partitionStatsPaths(snapshot.partitionStatsMetadata(), io));
    return files;
  }

  private static Set<String> collectDataFilesFromSnapshot(Snapshot snapshot, FileIO io) {
    return pathSet(snapshot.addedDataFiles(io));
  }

  private static Set<String> pathSet(Iterable<DataFile> files) {
    return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
  }

  private static Set<String> manifestPaths(Iterable<ManifestFile> files) {
    return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
  }

  private static Set<String> partitionStatsPaths(
      PartitionStatsMetadata partitionStatsMetadata, FileIO io) {
    Set<String> partitionStatsFiles = Sets.newHashSet();
    if (partitionStatsMetadata != null) {
      String partitionStatsMetadataLocation = partitionStatsMetadata.metadataFileLocation();
      PartitionStatsFileLocations partitionStatsLocations =
          getPartitionStatsFiles(io, partitionStatsMetadataLocation);
      if (partitionStatsLocations != null) {
        // Partition stats have metadata file and partition files.
        partitionStatsFiles.add(partitionStatsMetadataLocation);
        partitionStatsFiles.addAll(
            partitionStatsLocations.all().values().stream().collect(Collectors.toList()));
      }
    }

    return partitionStatsFiles;
  }

  private static void validateOutputResult(
      BufferAllocator allocator,
      String query,
      Object[] args,
      DmlQueryTestUtils.Table table,
      Long[] results)
      throws Exception {
    Assert.assertEquals(6, results.length);
    new TestBuilder(allocator)
        .sqlQuery(query, args)
        .unOrdered()
        .baselineColumns(
            DELETE_DATA_FILE_COUNT,
            DELETE_POSITION_DELETE_FILES_COUNT,
            DELETE_EQUALITY_DELETE_FILES_COUNT,
            DELETE_MANIFEST_FILES_COUNT,
            DELETE_MANIFEST_LISTS_COUNT,
            DELETE_PARTITION_STATS_FILES_COUNT)
        .baselineValues(results[0], results[1], results[2], results[3], results[4], results[5])
        .go();
  }

  private static void validateRemoveOrphanFilesOutputResult(
      BufferAllocator allocator, String query, Object[] args, Long[] results) throws Exception {
    Assert.assertEquals(2, results.length);
    new TestBuilder(allocator)
        .sqlQuery(query, args)
        .unOrdered()
        .baselineColumns(DELETED_FILES_COUNT, DELETED_FILES_SIZE_MB)
        .baselineValues(results[0], results[1])
        .go();
  }

  private static Table appendManifestFiles(Table icebergTable, String branch) throws IOException {
    List<ManifestFile> manifestFiles = new ArrayList<>(5);
    for (int i = 0; i < 5; i++) {
      ManifestFile manifestFile = writeManifestFile(icebergTable, 10);
      manifestFiles.add(manifestFile);
    }
    AppendFiles append = icebergTable.newFastAppend().toBranch(branch);
    manifestFiles.forEach(append::appendManifest);
    append.commit();
    icebergTable.refresh();
    return icebergTable;
  }
}

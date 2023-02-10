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

import static com.dremio.BaseTestQuery.getDfsTestTmpSchemaLocation;
import static com.dremio.BaseTestQuery.getIcebergTable;
import static com.dremio.BaseTestQuery.test;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addQuotes;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addRows;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testMalformedDmlQueries;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testQueryValidateStatusSummary;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.verifyCountSnapshotQuery;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.waitUntilAfter;
import static com.dremio.exec.planner.sql.handlers.SqlHandlerUtil.getTimestampFromMillis;

import java.io.File;
import java.util.Iterator;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.junit.Assert;

import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.io.file.Path;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Vacuum tests.
 *
 * Note: Add tests used across all platforms here.
 */
public class VacuumTests extends ITDmlQueryBase {
  public static void testMalformedVacuumQueries(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      testMalformedDmlQueries(new Object[]{table.fqn, "'2022-10-22 18:24:30'", "'3'"},
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
        "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = %s RETAIN_LAST =",
        "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST",
        "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST =",
        "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST '3'",
        "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST = '3'"
      );
    }
  }

  public static void testSimpleExpireOlderThan(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      Thread.sleep(100);
      final long timestampMillisToExpire = System.currentTimeMillis();
      // Insert more rows to increase snapshots
      DmlQueryTestUtils.Table table2 = addRows(table, 1);
      table2 = addRows(table2, 1);
      testQueryValidateStatusSummary(allocator,
        "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = '%s'", new Object[]{table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
        table,
        true,
        String.format("Table [%s] vacuumed", table.fqn),
        ArrayUtils.subarray(table2.originalData, 0, table2.originalData.length));
    }
  }

  public static void testExpireOlderThan(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long timestampMillisToExpire = waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());
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
      Assert.assertEquals("Should have four snapshots", 4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals("Should have four history entries", 4, updatedTable.history().size());

      testQueryValidateStatusSummary(allocator,
        "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = '%s'", new Object[]{table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
        table,
        true,
        String.format("Table [%s] vacuumed", table.fqn),
        ArrayUtils.subarray(table2.originalData, 0, table2.originalData.length));

      Table vacuumedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals("Expire should keep last two snapshots", 2, Iterables.size(vacuumedTable.snapshots()));
      Assert.assertEquals("Expire should keep last two history entries ", 2, vacuumedTable.history().size());
      Assert.assertEquals("Expire should not change current snapshot", expectedSnapshotId, vacuumedTable.currentSnapshot().snapshotId());
      Assert.assertNull("Expire should remove the oldest snapshot", vacuumedTable.snapshot(firstSnapshot.snapshotId()));
      Assert.assertNull("Expire should remove the second oldest snapshot", vacuumedTable.snapshot(secondSnapshot.snapshotId()));
    }
  }

  public static void testExpireRetainLast(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
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
      Assert.assertEquals("Should have four snapshots", 4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals("Should have four history entries", 4, updatedTable.history().size());

      // No snapshots are dated back to default 5 days ago, and no snapshots are expired, even claim to retain last 2.
      testQueryValidateStatusSummary(allocator,
        "VACUUM TABLE %s EXPIRE SNAPSHOTS RETAIN_LAST = %s", new Object[]{table.fqn, "2"},
        table,
        true,
        String.format("Table [%s] vacuumed", table.fqn),
        ArrayUtils.subarray(table2.originalData, 0, table2.originalData.length));

      Table vacuumedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals("Expire should keep last four snapshots", 4, Iterables.size(vacuumedTable.snapshots()));
      Assert.assertEquals("Expire should keep last four history entries ", 4, vacuumedTable.history().size());
      Assert.assertEquals("Expire should not change current snapshot", expectedSnapshotId, vacuumedTable.currentSnapshot().snapshotId());
      Assert.assertNotNull("Expire should not remove the oldest snapshot", vacuumedTable.snapshot(firstSnapshot.snapshotId()));
      Assert.assertNotNull("Expire should not remove the second oldest snapshot", vacuumedTable.snapshot(secondSnapshot.snapshotId()));
    }
  }

  public static void testRetainLastWithExpireOlderThan(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
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
      final long timestampMillisToExpire = waitUntilAfter(updatedTable.currentSnapshot().timestampMillis());
      Assert.assertEquals("Should have four snapshots", 4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals("Should have four history entries", 4, updatedTable.history().size());
      final long expectedSnapshotId = updatedTable.currentSnapshot().snapshotId();

      // Use the latest snapshot's timestamp for OLDER_THAN. But, it still needs to keep two snapshots as RETAIN_LAST is set.
      testQueryValidateStatusSummary(allocator,
        "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = '%s' RETAIN_LAST = %s",
        new Object[]{table.fqn, getTimestampFromMillis(timestampMillisToExpire), "2"},
        table,
        true,
        String.format("Table [%s] vacuumed", table.fqn),
        ArrayUtils.subarray(table2.originalData, 0, table2.originalData.length));

      Table vacuumedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals("Expire should keep last two snapshots", 2, Iterables.size(vacuumedTable.snapshots()));
      Assert.assertEquals("Expire should keep last two history entries ", 2, vacuumedTable.history().size());
      Assert.assertEquals("Expire should not change current snapshot", expectedSnapshotId, vacuumedTable.currentSnapshot().snapshotId());
      Assert.assertNull("Expire should remove the oldest snapshot", vacuumedTable.snapshot(firstSnapshot.snapshotId()));
      Assert.assertNull("Expire should remove the second oldest snapshot", vacuumedTable.snapshot(secondSnapshot.snapshotId()));
    }
  }

  public static void testExpireDataFilesCleanup(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long rollbackToSnapshotId = icebergTable.currentSnapshot().snapshotId();

      // Collect all data files, which should be cleared by expire.
      Set<String> dataFilesToClean = Sets.newHashSet();
      addRows(table, 1);
      icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      dataFilesToClean.addAll(collectDataFilesFromSnapshot(icebergTable.currentSnapshot(), icebergTable.io()));
      addRows(table, 1);
      icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      dataFilesToClean.addAll(collectDataFilesFromSnapshot(icebergTable.currentSnapshot(), icebergTable.io()));
      addRows(table, 1);
      icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      dataFilesToClean.addAll(collectDataFilesFromSnapshot(icebergTable.currentSnapshot(), icebergTable.io()));

      final long timestampMillisToExpire = waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());

      testQueryValidateStatusSummary(allocator,
        "ROLLBACK TABLE %s TO SNAPSHOT '%s'", new Object[]{table.fqn, rollbackToSnapshotId},
        table,
        true,
        String.format("Table [%s] rollbacked", table.fqn),
        null);

      testQueryValidateStatusSummary(allocator,
        "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = '%s'", new Object[]{table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
        table,
        true,
        String.format("Table [%s] vacuumed", table.fqn),
        null);

      Table vacuumedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final Set<String> filesAfterVacuum = collectDataFilesFromTable(vacuumedTable);
      for (String file : dataFilesToClean) {
        Assert.assertFalse("Expire should clean the file", filesAfterVacuum.contains(file));
        File filePath = new File(Path.getContainerSpecificRelativePath(Path.of(file)));
        Assert.assertFalse(filePath.exists());
      }
    }
  }

  public static void testExpireOlderThanWithRollback(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
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
      final Snapshot thirdSnapshot = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP).currentSnapshot();
      addRows(table, 1);
      final Snapshot fourthSnapshot = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP).currentSnapshot();
      final long timestampMillisToExpire = waitUntilAfter(secondSnapshot.timestampMillis());

      testQueryValidateStatusSummary(allocator,
        "ROLLBACK TABLE %s TO SNAPSHOT '%s'", new Object[]{table.fqn, rollbackToSnapshotId},
        table,
        true,
        String.format("Table [%s] rollbacked", table.fqn),
        null);

      testQueryValidateStatusSummary(allocator,
        "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = '%s' RETAIN_LAST = 2", new Object[]{table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
        table,
        true,
        String.format("Table [%s] vacuumed", table.fqn),
        null);

      Table vacuumedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals("Expire should keep last 2 snapshot", 2, Iterables.size(vacuumedTable.snapshots()));
      Assert.assertEquals("Expire should not change current snapshot", rollbackToSnapshotId, vacuumedTable.currentSnapshot().snapshotId());
      Assert.assertNotNull("Expire should keep the first snapshot, current", vacuumedTable.snapshot(firstSnapshot.snapshotId()));
      Assert.assertNotNull("Expire should keep the second snapshot, current", vacuumedTable.snapshot(secondSnapshot.snapshotId()));
      Assert.assertNull("Expire should remove the orphaned snapshot", vacuumedTable.snapshot(thirdSnapshot.snapshotId()));
      Assert.assertNull("Expire should remove the orphaned snapshot", vacuumedTable.snapshot(fourthSnapshot.snapshotId()));
    }
  }

  public static void testRetainZeroSnapshots(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long timestampMillisToExpire = waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());

      UserExceptionAssert.assertThatThrownBy(() ->
          test("VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = '%s' RETAIN_LAST = 0", table.fqn, getTimestampFromMillis(timestampMillisToExpire)))
        .hasErrorType(ErrorType.UNSUPPORTED_OPERATION)
        .hasMessageContaining("Minimum number of snapshots to retain can be 1");
    }
  }

  public static void testInvalidTimestampLiteral(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      UserExceptionAssert.assertThatThrownBy(() ->
          test("VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = '2022-09-01 abc'", table.fqn))
        .hasErrorType(ErrorType.PARSE)
        .hasMessageContaining("Literal '2022-09-01 abc' cannot be casted to TIMESTAMP");
    }
  }

  public static void testEmptyTimestamp(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      UserExceptionAssert.assertThatThrownBy(() ->
          test("VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = ''", table.fqn))
        .hasErrorType(ErrorType.PARSE)
        .hasMessageContaining("Literal '' cannot be casted to TIMESTAMP");
    }
  }

  public static void testExpireDatasetRefreshed(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long timestampMillisToExpire = waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());
      verifyCountSnapshotQuery(allocator, table.fqn, 2L);
      // Insert more rows to increase snapshots
      addRows(table, 1);
      addRows(table, 1);
      verifyCountSnapshotQuery(allocator, table.fqn, 4L);

      testQueryValidateStatusSummary(allocator,
        "VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = '%s'", new Object[]{table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
        table,
        true,
        String.format("Table [%s] vacuumed", table.fqn),
        null);

      // The count table_snapshot query result should be refreshed and only 2 are left.
      verifyCountSnapshotQuery(allocator, table.fqn, 2L);
    }
  }

  public static void testUnparseSqlVacuum(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long timestampMillisToExpire = waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());

      final String vacuumQuery = String.format("VACUUM TABLE %s EXPIRE SNAPSHOTS OLDER_THAN = '%s' RETAIN_LAST = 1",
        table.fqn, getTimestampFromMillis(timestampMillisToExpire));


      final String expected = String.format("VACUUM TABLE %s EXPIRE SNAPSHOTS \"OLDER_THAN\" = '%s' \"RETAIN_LAST\" = 1",
        "\"" + source + "\"." + addQuotes(tableName), getTimestampFromMillis(timestampMillisToExpire));
      parseAndValidateSqlNode(vacuumQuery, expected);
    }
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

  private static Set<String> collectDataFilesFromSnapshot(Snapshot snapshot, FileIO io) {
    return pathSet(snapshot.addedDataFiles(io));
  }

  private static Set<String> pathSet(Iterable<DataFile> files) {
    return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
  }
}

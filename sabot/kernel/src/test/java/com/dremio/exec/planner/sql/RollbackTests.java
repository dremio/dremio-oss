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
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.waitUntilAfter;
import static com.dremio.exec.planner.sql.handlers.SqlHandlerUtil.getTimestampFromMillis;

import java.io.File;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.Table;
import org.junit.Assert;

import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.Iterables;

/**
 * Rollback tests.
 *
 * Note: Add tests used across all platforms here.
 */
public class RollbackTests extends ITDmlQueryBase {

  public static void testMalformedRollbackQueries(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      testMalformedDmlQueries(new Object[]{table.fqn, table.name},
        "ROLLBACK",
        "ROLLBACK TABLE",
        "ROLLBACK TABLE %s",
        "ROLLBACK TABLE %s TO",
        "ROLLBACK TABLE %s TO %s",
        "ROLLBACK TABLE %s TO SNAPSHOT",
        "ROLLBACK TABLE %s TO SNAPSHOT SNAPSHOT",
        "ROLLBACK TABLE %s TO TIMESTAMP",
        "ROLLBACK TABLE %s TO TIMESTAMP TIMESTAMP"
      );
    }
  }

  public static void testSimpleRollbackToTimestamp(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      Thread.sleep(100);
      final long timestampMillisToExpire = System.currentTimeMillis();

      // Insert more rows to increase snapshots
      addRows(table, 1);
      addRows(table, 1);

      final String timestampStr = getTimestampFromMillis(timestampMillisToExpire);
      testQueryValidateStatusSummary(allocator,
        "ROLLBACK TABLE %s TO TIMESTAMP '%s'", new Object[]{table.fqn, timestampStr},
        table,
        true,
        String.format("Table [%s] rollbacked", table.fqn),
        ArrayUtils.subarray(table.originalData, 0, table.originalData.length));
    }
  }

  public static void testRollbackToSnapshot(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(2, Iterables.size(icebergTable.snapshots()));
      Assert.assertEquals(2, icebergTable.history().size());
      final long rollbackToSnapshotId = icebergTable.history().get(1).snapshotId();

      // Insert more rows to increase snapshots
      addRows(table, 1);
      addRows(table, 1);
      Table updatedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals(4, updatedTable.history().size());

      testQueryValidateStatusSummary(allocator,
        "ROLLBACK TABLE %s TO SNAPSHOT '%s'", new Object[]{table.fqn, rollbackToSnapshotId},
        table,
        true,
        String.format("Table [%s] rollbacked", table.fqn),
        ArrayUtils.subarray(table.originalData, 0, table.originalData.length));

      Table rollbackedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals(5, rollbackedTable.history().size());
      Assert.assertEquals(rollbackToSnapshotId, rollbackedTable.currentSnapshot().snapshotId());
    }
  }

  public static void testRollbackToTimestamp(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(2, Iterables.size(icebergTable.snapshots()));
      Assert.assertEquals(2, icebergTable.history().size());
      final long rollbackToTimestamp = waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());
      final long rollbackToSnapshotId = icebergTable.history().get(1).snapshotId();
      // Insert more rows to increase snapshots
      addRows(table, 1);
      addRows(table, 1);
      Table updatedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals(4, updatedTable.history().size());

      final String timestampStr = getTimestampFromMillis(rollbackToTimestamp);
      testQueryValidateStatusSummary(allocator,
        "ROLLBACK TABLE %s TO TIMESTAMP '%s'", new Object[]{table.fqn, timestampStr},
        table,
        true,
        String.format("Table [%s] rollbacked", table.fqn),
        ArrayUtils.subarray(table.originalData, 0, table.originalData.length));

      Table rollbackedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals(5, rollbackedTable.history().size());
      Assert.assertEquals(rollbackToSnapshotId, rollbackedTable.currentSnapshot().snapshotId());
    }
  }

  public static void testRollbackToTimestampMatchSnapshot(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(2, Iterables.size(icebergTable.snapshots()));
      Assert.assertEquals(2, icebergTable.history().size());
      final long rollbackToTimestamp = icebergTable.history().get(1).timestampMillis();
      final long rollbackToSnapshotId = icebergTable.history().get(1).snapshotId();
      // Insert more rows to increase snapshots
      addRows(table, 1);
      addRows(table, 1);
      Table updatedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals(4, updatedTable.history().size());

      final String timestampStr = getTimestampFromMillis(rollbackToTimestamp);
      testQueryValidateStatusSummary(allocator,
        "ROLLBACK TABLE %s TO TIMESTAMP '%s'", new Object[]{table.fqn, timestampStr},
        table,
        true,
        String.format("Table [%s] rollbacked", table.fqn),
        ArrayUtils.subarray(table.originalData, 0, table.originalData.length));

      Table rollbackedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals(5, rollbackedTable.history().size());
      Assert.assertEquals(rollbackToSnapshotId, rollbackedTable.currentSnapshot().snapshotId());
    }
  }

  public static void testRollbackToCurrentSnapshot(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long rollbackToSnapshotId = icebergTable.currentSnapshot().snapshotId();

      UserExceptionAssert.assertThatThrownBy(() -> test("ROLLBACK TABLE %s TO SNAPSHOT '%s'", table.fqn, rollbackToSnapshotId))
        .hasErrorType(ErrorType.UNSUPPORTED_OPERATION)
        .hasMessageContaining("Cannot rollback table to current snapshot");

      final long rollbackToTimestamp = icebergTable.currentSnapshot().timestampMillis();
      final String timestampStr = getTimestampFromMillis(rollbackToTimestamp);
      UserExceptionAssert.assertThatThrownBy(() -> test("ROLLBACK TABLE %s TO TIMESTAMP '%s'", table.fqn, timestampStr))
        .hasErrorType(ErrorType.UNSUPPORTED_OPERATION)
        .hasMessageContaining("Cannot rollback table to current snapshot");
    }
  }


  public static void testRollbackWithPartialTablePath(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(2, icebergTable.history().size());
      final long rollbackToSnapshotId = icebergTable.history().get(1).snapshotId();

      // Insert more rows to increase snapshots
      addRows(table, 1);
      addRows(table, 1);
      Table updatedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(4, updatedTable.history().size());

      testQueryValidateStatusSummary(allocator,
        "ROLLBACK TABLE %s TO SNAPSHOT '%s'", new Object[]{table.name, rollbackToSnapshotId},
        table,
        true,
        String.format("Table [%s] rollbacked", table.fqn),
        ArrayUtils.subarray(table.originalData, 0, table.originalData.length));

      Table rollbackedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(5, rollbackedTable.history().size());
      Assert.assertEquals(rollbackToSnapshotId, rollbackedTable.currentSnapshot().snapshotId());
    }
  }

  public static void testRollbackToInvalidSnapshot(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(2, icebergTable.history().size());

      UserExceptionAssert.assertThatThrownBy(() -> test("ROLLBACK TABLE %s TO SNAPSHOT '%s'", table.fqn, 1111))
        .hasErrorType(ErrorType.UNSUPPORTED_OPERATION)
        .hasMessageContaining("Cannot rollback table to unknown snapshot ID 1111");

      // Iceberg table should not be updated.
      Table icebergTable2 = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(2, icebergTable2.history().size());
    }
  }

  public static void testRollbackToTimestampBeforeFirstSnapshot(String source) throws Exception {
    final String timestampStr = getTimestampFromMillis(System.currentTimeMillis());
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(2, icebergTable.history().size());
      UserExceptionAssert.assertThatThrownBy(() -> test("ROLLBACK TABLE %s TO TIMESTAMP '%s'", table.fqn, timestampStr))
        .hasErrorType(ErrorType.UNSUPPORTED_OPERATION)
        .hasMessageContaining("Cannot rollback table, no valid snapshot older than");

      // Iceberg table should not be updated.
      Table icebergTable2 = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(2, icebergTable2.history().size());
    }
  }

  public static void testInvalidSnapshotIdLiteral(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String invalidSnapshotId = "thb111";
      UserExceptionAssert.assertThatThrownBy(() -> test("ROLLBACK TABLE %s TO SNAPSHOT '%s'", table.fqn, invalidSnapshotId))
        .hasErrorType(ErrorType.PARSE)
        .hasMessageContaining(String.format("Literal %s is an invalid snapshot id", invalidSnapshotId));
    }
  }

  public static void testInvalidTimestampLiteral(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String invalidTimestamp = "2022-09-01 abc";
      UserExceptionAssert.assertThatThrownBy(() -> test("ROLLBACK TABLE %s TO TIMESTAMP '%s'", table.fqn, invalidTimestamp))
        .hasErrorType(ErrorType.PARSE)
        .hasMessageContaining(String.format("Literal '%s' cannot be casted to TIMESTAMP", invalidTimestamp));
    }
  }

  public static void testRollbackToNonAncestorSnapshot(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(2, icebergTable.history().size());
      Assert.assertEquals(2, Iterables.size(icebergTable.snapshots()));
      final long secondSnapshotId = icebergTable.history().get(1).snapshotId();

      // Insert more rows to increase snapshots
      addRows(table, 1);
      addRows(table, 1);
      Table updatedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals(4, updatedTable.history().size());

      final long fourthSnapshotId = updatedTable.currentSnapshot().snapshotId();

      // First, rollback to the second snapshot and make the four snapshot invalid in the ancestor list.
      testQueryValidateStatusSummary(allocator,
        "ROLLBACK TABLE %s TO SNAPSHOT '%s'", new Object[]{table.fqn, secondSnapshotId},
        table,
        true,
        String.format("Table [%s] rollbacked", table.fqn),
        ArrayUtils.subarray(table.originalData, 0, table.originalData.length));

      Table rollbackedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals(5, rollbackedTable.history().size());

      // Second, rollback to the fourth snapshot, which is not in the ancestor list.
      UserExceptionAssert.assertThatThrownBy(() -> test("ROLLBACK TABLE %s TO SNAPSHOT '%s'", table.fqn, fourthSnapshotId))
        .hasErrorType(ErrorType.UNSUPPORTED_OPERATION)
        .hasMessageContaining(String.format("Cannot rollback table to snapshot ID %s", fourthSnapshotId));

      rollbackedTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(4, Iterables.size(updatedTable.snapshots()));
      Assert.assertEquals(5, rollbackedTable.history().size());
    }
  }

  public static void testRollbackUseInvalidTablePath(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(2, icebergTable.history().size());
      final long rollbackToSnapshotId = icebergTable.history().get(1).snapshotId();

      String nonExistenceTablePath = "src";
      UserExceptionAssert.assertThatThrownBy(() -> test("ROLLBACK TABLE %s TO SNAPSHOT '%s'",  nonExistenceTablePath, rollbackToSnapshotId))
        .hasErrorType(ErrorType.VALIDATION)
        .hasMessageContaining(String.format("Table [%s.%s] does not exist.", source, nonExistenceTablePath));

      // Iceberg table should not be updated.
      Table icebergTable2 = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      Assert.assertEquals(2, icebergTable2.history().size());
    }
  }

  public static void testUnparseSqlRollbackTable(String source) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source,2, 1)) {
      String tableName = table.name.startsWith("\"") ? table.name.substring(1, table.name.length() - 1) : table.name;
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      final long rollbackToSnapshotId = icebergTable.currentSnapshot().snapshotId();
      final long timestampMillisToExpire = waitUntilAfter(icebergTable.currentSnapshot().timestampMillis());

      final String rollbackSnapshotQuery = String.format("ROLLBACK TABLE %s TO SNAPSHOT '%s'",
        table.fqn, rollbackToSnapshotId);
      final String expectedSnapshot = String.format("ROLLBACK TABLE %s TO SNAPSHOT '%s'",
        "\"" + source + "\"" + "." + addQuotes(tableName), rollbackToSnapshotId);
      parseAndValidateSqlNode(rollbackSnapshotQuery, expectedSnapshot);

      final String rollbackTimestampQuery = String.format("ROLLBACK TABLE %s TO TIMESTAMP '%s'",
        table.fqn, getTimestampFromMillis(timestampMillisToExpire));
      final String expectedTimestamp = String.format("ROLLBACK TABLE %s TO TIMESTAMP '%s'",
        "\"" + source + "\"" + "." + addQuotes(tableName), getTimestampFromMillis(timestampMillisToExpire));
      parseAndValidateSqlNode(rollbackTimestampQuery, expectedTimestamp);
    }
  }
}

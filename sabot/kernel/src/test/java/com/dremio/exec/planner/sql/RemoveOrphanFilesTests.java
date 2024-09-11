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

import static com.dremio.exec.planner.sql.DmlQueryTestUtils.PARTITION_COLUMN_ONE_INDEX_SET;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addQuotes;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addRows;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicNonPartitionedAndPartitionedTables;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createStockIcebergTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.loadTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testMalformedDmlQueries;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.verifyCountSnapshotQuery;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.verifyData;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.waitUntilAfter;
import static com.dremio.exec.planner.sql.handlers.SqlHandlerUtil.getTimestampFromMillis;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.io.file.Path;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.Iterables;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.junit.Assert;

/**
 * Vacuum Remove Orphan Files tests.
 *
 * <p>Note: Add tests used across all platforms here.
 */
public class RemoveOrphanFilesTests extends TestVacuumBase {
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

  public static void testRemoveOrphanFilesWithPartitions(BufferAllocator allocator, String source)
      throws Exception {
    try (DmlQueryTestUtils.Tables tables =
        createBasicNonPartitionedAndPartitionedTables(
            source, 2, 3, PARTITION_COLUMN_ONE_INDEX_SET)) {
      Assert.assertEquals("Should have two tables", 2, tables.tables.length);
      // Second table has partitions
      DmlQueryTestUtils.Table table = tables.tables[1];

      // Add row
      DmlQueryTestUtils.Table table2 = addRows(table, 1);

      Table icebergTable = loadIcebergTable(table);
      Assert.assertEquals(
          "Should have three snapshots", 3, Iterables.size(icebergTable.snapshots()));
      Assert.assertNotNull(icebergTable.currentSnapshot().partitionStatsMetadata());

      Set<String> partitionStatsPaths =
          partitionStatsPaths(
              icebergTable.currentSnapshot().partitionStatsMetadata(), icebergTable.io());
      Assert.assertEquals("Should have two partition stats files", 2, partitionStatsPaths.size());

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

  public static void testRemoveOrphanFilesUsingDefaultMinFileAge(
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

      // Remove Orphan Files query uses the default : MIN_FILE_AGE_MS_DEFAULT (5 days), to determine
      // orphan files to clean. As all files in this table's location are newly-added, the query
      // will not delete any files.
      validateRemoveOrphanFilesOutputResult(
          allocator,
          "VACUUM TABLE %s REMOVE ORPHAN FILES",
          new Object[] {table.fqn},
          new Long[] {0L, 0L}); // No file should be deleted.

      validateRemoveOrphanFilesOutputResult(
          allocator,
          "VACUUM TABLE %s REMOVE ORPHAN FILES LOCATION '%s'",
          new Object[] {table.fqn, tableLocation},
          new Long[] {0L, 0L}); // No file should be deleted.

      // Copied orphan files should be deleted
      Assert.assertTrue("File should exist", orphanFilePath.exists());

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
      Table icebergTable = loadIcebergTable(table);
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

  public static void testRemoveOrphanFilesHybridlyGeneratedTable(
      BufferAllocator allocator, String source) throws Exception {
    // Table is not created using DREMIO SQL. But, we use DREMIO SQL to insert rows. Thus, the table
    // includes hybrid style paths, e.g., 'file:///path' and '/path'.
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(source, 0, 2, "modes_isolation")) {
      Table icebergTable = loadTable(table);
      DmlQueryTestUtils.Table table2 = addRows(table, 1);
      addRows(table2, 1);

      // Verify the number of the table's snapshots
      verifyCountSnapshotQuery(allocator, table.fqn, 2L);

      String tableLocation =
          Path.getContainerSpecificRelativePath(Path.of(icebergTable.location()));

      // Copy a DataFile object and commit its path without scheme info into Iceberg table.

      icebergTable.refresh();
      Iterable<DataFile> dataFilesIter =
          icebergTable.currentSnapshot().addedDataFiles(icebergTable.io());
      DataFile dataFile = dataFilesIter.iterator().next();
      String datafilePath = dataFile.path().toString();

      File destinationFolder = new File(tableLocation + "/" + UUID.randomUUID());
      destinationFolder.mkdir();
      java.nio.file.Path destinationDatafile =
          Paths.get(destinationFolder.toPath().toString(), "0000.parquet");
      Files.copy(Paths.get(new URI(datafilePath).getPath()), destinationDatafile, REPLACE_EXISTING);

      Assert.assertTrue(
          "New parquet file should be created", destinationDatafile.toFile().exists());
      String destinationDatafileStr = destinationDatafile.toString();
      Assert.assertTrue(
          "New parquet file path should not contain scheme info",
          !destinationDatafileStr.startsWith("file:/"));

      DataFiles.Builder dataFileBuilder =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(destinationDatafileStr)
              .withFileSizeInBytes(dataFile.fileSizeInBytes())
              .withRecordCount(dataFile.recordCount())
              .withFormat(FileFormat.PARQUET);

      icebergTable.newAppend().appendFile(dataFileBuilder.build()).commit();

      runSQL(String.format("ALTER TABLE %s REFRESH METADATA FORCE UPDATE", table.fqn));
      // Verify the number of the table's snapshots
      verifyCountSnapshotQuery(allocator, table.fqn, 3L);

      icebergTable.refresh();
      List<ManifestFile> manifestFiles =
          icebergTable.currentSnapshot().dataManifests(icebergTable.io());
      Assert.assertEquals(3, manifestFiles.size());
      String manifestFilePath = manifestFiles.get(0).path().toString();
      Assert.assertTrue(
          "New manifest file path should not contain scheme info",
          !manifestFilePath.startsWith("file:/"));

      final long timestampMillisToExpire = waitUntilAfter(System.currentTimeMillis() + 1);
      validateRemoveOrphanFilesOutputResult(
          allocator,
          "VACUUM TABLE %s REMOVE ORPHAN FILES OLDER_THAN '%s'",
          new Object[] {table.fqn, getTimestampFromMillis(timestampMillisToExpire)},
          new Long[] {0L, 0L});

      runSQL(String.format("ALTER TABLE %s REFRESH METADATA FORCE UPDATE", table.fqn));
      // Don't modify table snapshots
      verifyCountSnapshotQuery(allocator, table.fqn, 3L);

      Assert.assertTrue(
          "The new created parquet file should not be deleted",
          destinationDatafile.toFile().exists());
      Assert.assertTrue(
          "The new created manifest file should not be deleted",
          Paths.get(manifestFilePath).toFile().exists());
    }
  }
}

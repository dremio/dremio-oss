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

import static com.dremio.exec.planner.OptimizeOutputSchema.NEW_DATA_FILES_COUNT;
import static com.dremio.exec.planner.OptimizeOutputSchema.OPTIMIZE_OUTPUT_SUMMARY;
import static com.dremio.exec.planner.OptimizeOutputSchema.REWRITTEN_DATA_FILE_COUNT;
import static com.dremio.exec.planner.OptimizeOutputSchema.REWRITTEN_DELETE_FILE_COUNT;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createStockIcebergTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.insertRows;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.loadTable;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.util.Text;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import com.dremio.TestBuilder;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.TestTools;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class OptimizeTests extends ITDmlQueryBase {

  public static void testOnUnPartitioned(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 6)) {
      assertFileCount(table.fqn, 6L, allocator);

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s", table.fqn), 6L, 0L, 1L);

      assertFileCount(table.fqn, 1L, allocator);
    }
  }

  public static void testOptimizeDataFilesUnPartitioned(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 6)) {
      assertFileCount(table.fqn, 6L, allocator);

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s REWRITE DATA", table.fqn), 6L, 0L, 1L);

      assertFileCount(table.fqn, 1L, allocator);
    }
  }

  public static void testOptimizeManifestsOnlyUnPartitioned(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 6)) {
      assertFileCount(table.fqn, 6L, allocator);
      assertManifestCount(table.fqn, 6L, allocator);

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s REWRITE MANIFESTS", table.fqn).unOrdered()
        .baselineColumns(OPTIMIZE_OUTPUT_SUMMARY)
        .baselineValues("Optimize table successful").go();

      assertFileCount(table.fqn, 6L, allocator);
      assertManifestCount(table.fqn, 1L, allocator);
    }
  }

  public static void testOnPartitioned(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createPartitionedTestTable(source, 3)) {
      assertFileCount(table.fqn, 9L, allocator); // 3 files per partition

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s", table.fqn),9L, 0L, 3L);

      assertFileCount(table.fqn, 3L, allocator); // should be left with 1 file per partition
    }
  }

  public static void testOptimizeManifestsOnlyPartitioned(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createPartitionedTestTable(source, 3)) {
      assertFileCount(table.fqn, 9L, allocator); // 3 files per partition
      assertManifestCount(table.fqn, 3L, allocator);

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s REWRITE MANIFESTS", table.fqn).unOrdered()
        .baselineColumns(OPTIMIZE_OUTPUT_SUMMARY)
        .baselineValues("Optimize table successful").go();

      assertManifestCount(table.fqn, 1L, allocator);
      assertFileCount(table.fqn, 9L, allocator); // unchanged
    }
  }

  public static void testOptimizeDataOnPartitioned(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createPartitionedTestTable(source, 3)) {
      assertFileCount(table.fqn, 9L, allocator); // 3 files per partition

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s REWRITE DATA", table.fqn),9L, 0L, 3L);

      assertFileCount(table.fqn, 3L, allocator); // should be left with 1 file per partition
    }
  }

  public static void testOnUnpartitionedMinInputFilesCriteria(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 5)) {
      // NOOP because min input files > small file count
      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (MIN_INPUT_FILES = 6)", table.fqn), 0L, 0L, 0L);

      assertFileCount(table.fqn, 5L, allocator);

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (MIN_INPUT_FILES = 5)", table.fqn),5L, 0L, 1L );

      assertFileCount(table.fqn, 1L, allocator);
    }
  }

  public static void testOnPartitionedMinInputFilesCriteria(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createPartitionedTestTable(source, 3)) {
      // NOOP because min input files > small file count from all partitions
      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (MIN_INPUT_FILES = 10)", table.fqn),0L, 0L, 0L );

      assertFileCount(table.fqn, 9, allocator);

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (MIN_INPUT_FILES = 9)", table.fqn),9L, 0L, 3L );

      assertFileCount(table.fqn, 3, allocator);
    }
  }

  public static void testOnUnpartitionedMinFileSizeCriteria(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 5)) {
      assertFileCount(table.fqn, 5L, allocator);

      // NOOP because files are likely to be larger than 20 bytes
      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (MIN_FILE_SIZE_MB = 0)", table.fqn), 0L, 0L, 0L);

      assertFileCount(table.fqn, 5L, allocator);

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (MIN_FILE_SIZE_MB = 1)", table.fqn),5L, 0L, 1L);

      assertFileCount(table.fqn, 1L, allocator);
    }
  }

  public static void testWithTargetFileSizeAlreadyOptimal(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 0)) {
      org.apache.iceberg.Table loadedTable = DmlQueryTestUtils.loadTable(table);
      for (int commitId = 0; commitId < 5; commitId++) {
        loadedTable.newAppend().appendFile(
          DataFiles.builder(loadedTable.spec())
            .withPath("/data/fake.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(20 * 1024 * 1024L)
            .withRecordCount(1_000_000)
            .build()
        ).commit();
      }
      refreshTable(table, allocator);
      assertFileCount(table.fqn, 5L, allocator);

      long avgFileSize = getAvgFileSize(table.fqn, allocator);

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (TARGET_FILE_SIZE_MB = %d)", table.fqn, avgFileSize),0L, 0L, 0L);

      assertFileCount(table.fqn, 5L, allocator);
    }
  }

  public static void testWithMixedSizes(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 0)) {
      org.apache.iceberg.Table loadedTable = DmlQueryTestUtils.loadTable(table);
      loadedTable.newAppend().appendFile(
        DataFiles.builder(loadedTable.spec())
          .withPath("/data/fake.parquet")
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(2_033_219)
          .withRecordCount(1_000_000)
          .build()
      ).appendFile(
        DataFiles.builder(loadedTable.spec())
          .withPath(TestTools.getWorkingPath() + "/src/test/resources/iceberg/root_pointer/f1.parquet")
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(929)
          .withRecordCount(1_000)
          .build()
      ).appendFile(
        DataFiles.builder(loadedTable.spec())
          .withPath(TestTools.getWorkingPath() + "/src/test/resources/iceberg/root_pointer/f2.parquet")
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(929)
          .withRecordCount(1_000)
          .build()
      ).commit();
      refreshTable(table, allocator);
      assertFileCount(table.fqn, 3L, allocator);

      // Only two small files (f1 and f2) are considered for rewrite but not the already optimal one (map_float_type).
      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (TARGET_FILE_SIZE_MB=%d, MIN_FILE_SIZE_MB=%d, MAX_FILE_SIZE_MB=%d, MIN_INPUT_FILES=%d)",
        table.fqn, 2L, 1L, 3L, 2L),2L, 0L, 1L );

      assertFileCount(table.fqn, 2L, allocator);
    }
  }

  public static void testWithSingleFilePartitions(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createPartitionedTestTable(source, 3)) {
      assertFileCount(table.fqn, 9L, allocator); // 3 files per partition

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (MIN_INPUT_FILES=%d)", table.fqn, 2),9L, 0L, 3L);

      assertFileCount(table.fqn, 3L, allocator); // should be left with 1 file per partition

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (MIN_INPUT_FILES=%d)", table.fqn, 2),0L, 0L, 0L); // should be NOOP

      assertFileCount(table.fqn, 3L, allocator);
    }
  }

  public static void testWithSingleFilePartitionsAndEvolvedSpec(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 0)) {
      runSQL(String.format("INSERT INTO %s VALUES (1, 1), (1, 1), (1, 1)", table.fqn));
      runSQL(String.format("ALTER TABLE %s ADD PARTITION FIELD %s", table.fqn, table.columns[0]));
      runSQL(String.format("INSERT INTO %s VALUES (1, 1), (2, 2)", table.fqn));
      runSQL(String.format("INSERT INTO %s VALUES (2, 2)", table.fqn));
      // Partition column_0=1 will have 3 records from previous version of spec and 1 record from recent insert.

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (MIN_INPUT_FILES=%d)", table.fqn, 1),4L, 0L, 2L);

      List<QueryDataBatch> results = testSqlWithResults(String.format("SELECT OPERATION, SUMMARY FROM TABLE(TABLE_SNAPSHOT('%s')) ORDER BY COMMITTED_AT DESC LIMIT 2", table.fqn));
      RecordBatchLoader loader = new RecordBatchLoader(getSabotContext().getAllocator());
      QueryDataBatch data = results.get(0);
      loader.load(data.getHeader().getDef(), data.getData());

      VarCharVector operationVector = loader.getValueAccessorById(VarCharVector.class,
        loader.getValueVectorId(SchemaPath.getCompoundPath("operation")).getFieldIds()).getValueVector();
      assertThat(operationVector.getObject(0).toString()).isEqualTo("replace");
      assertThat(operationVector.getObject(1).toString()).isEqualTo("replace");

      Map<String, String> rewriteDataSnapshotSummary = fetchSummary(loader, 1);
      String addedRecords = rewriteDataSnapshotSummary.get("added-records");
      String deletedRecords = rewriteDataSnapshotSummary.get("deleted-records");
      assertThat(addedRecords).isEqualTo(deletedRecords).isEqualTo("6");
    }
  }

  public static void testUnsupportedScenarios(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createPartitionedTestTable(source, 3)) {
      assertThatThrownBy(() -> runSQL(String.format("OPTIMIZE TABLE %s REWRITE DATA USING SORT", table.fqn)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageStartingWith("PARSE ERROR");

      assertThatThrownBy(() -> runSQL(String.format("OPTIMIZE TABLE %s USING SORT", table.fqn)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageStartingWith("PARSE ERROR");
    }
  }

  public static void testEvolvedPartitions(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 6)) {
      assertFileCount(table.fqn, 6L, allocator);

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s", table.fqn),6L, 0L, 1L);

      assertFileCount(table.fqn, 1L, allocator);

      runSQL(String.format("ALTER TABLE %s ADD partition FIELD %s", table.fqn, table.columns[1]));

      testOptimizeCommand(allocator, String.format("OPTIMIZE TABLE %s (MIN_INPUT_FILES=%d)", table.fqn, 1), 1L, 0L, 6L);

      assertFileCount(table.fqn, 6L, allocator);
    }
  }

  public static void testOptimizeLargeManifests(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(source, 0, 2, "large_manifests_table")) {
      Table icebergTable = loadTable(table);
      icebergTable.updateProperties().set(MANIFEST_TARGET_SIZE_BYTES, "1000").commit(); // 1KB
      icebergTable.newFastAppend().appendManifest(writeManifestFile(icebergTable, 10)).commit();

      runSQL(String.format("OPTIMIZE TABLE %s REWRITE MANIFESTS", table.fqn));

      icebergTable.refresh();
      Snapshot replacedSnapshot = icebergTable.currentSnapshot();

      assertThat(replacedSnapshot.operation()).isEqualTo("replace");
      assertThat(Integer.parseInt(replacedSnapshot.summary().get("manifests-created"))).isGreaterThan(1);
      assertThat(replacedSnapshot.summary().get("manifests-replaced")).isEqualTo("1");
    }
  }

  public static void testOptimizeManifestsModesIsolations(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 6)) {

      long snapshot1 = latestSnapshotId(table);
      Set<String> snapshot1DataFiles = dataFilePaths(table);
      Set<String> snapshot1Manifests = manifestFilePaths(table);

      runSQL(String.format("OPTIMIZE TABLE %s REWRITE MANIFESTS", table.fqn));

      long snapshot2 = latestSnapshotId(table);
      Set<String> snapshot2DataFiles = dataFilePaths(table);
      Set<String> snapshot2ManifestFiles = manifestFilePaths(table);

      assertThat(snapshot1).isNotEqualTo(snapshot2);
      assertThat(snapshot1DataFiles).isEqualTo(snapshot2DataFiles);
      assertThat(snapshot1Manifests).isNotEqualTo(snapshot2ManifestFiles);

      insertCommits(table, 6);
      long snapshot3 = latestSnapshotId(table);
      Set<String> snapshot3DataFiles = dataFilePaths(table);
      runSQL(String.format("OPTIMIZE TABLE %s REWRITE DATA", table.fqn));

      long snapshot4 = latestSnapshotId(table);
      Set<String> snapshot4DataFiles = dataFilePaths(table);

      assertThat(snapshot3).isNotEqualTo(snapshot4);
      assertThat(snapshot3DataFiles).isNotEqualTo(snapshot4DataFiles);
    }
  }

  public static void testOptimizeManifestsWithOptimalSize(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(source, 0, 2, "modes_isolation")) {
      Table icebergTable = loadTable(table);
      List<ManifestFile> manifestFiles = new ArrayList<>(5);
      for (int i = 0; i < 5; i++) {
        ManifestFile manifestFile = writeManifestFile(icebergTable, 10);
        manifestFiles.add(manifestFile);
      }
      AppendFiles append = icebergTable.newFastAppend();
      manifestFiles.forEach(append::appendManifest);
      append.commit();
      icebergTable.refresh();

      long avgManifestSize = (long) manifestFiles.stream().mapToLong(ManifestFile::length).average().getAsDouble();
      icebergTable.updateProperties().set(MANIFEST_TARGET_SIZE_BYTES, String.valueOf(avgManifestSize)).commit();
      Snapshot snapshot1 = icebergTable.currentSnapshot();

      runSQL(String.format("OPTIMIZE TABLE %s REWRITE MANIFESTS", table.fqn));
      icebergTable.refresh();
      Snapshot snapshot2 = icebergTable.currentSnapshot();

      assertThat(snapshot1.snapshotId()).isEqualTo(snapshot2.snapshotId()); // NOOP

      assertNoOrphanManifests(icebergTable, snapshot1);
    }
  }

  public static void testOptimizeOnEmptyTableNoSnapshots(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(source, 0, 2, "modes_isolation")) {
      Table icebergTable = loadTable(table);

      runSQL(String.format("OPTIMIZE TABLE %s REWRITE MANIFESTS", table.fqn));
      icebergTable.refresh();

      assertThat(icebergTable.currentSnapshot()).isNull();

      runSQL(String.format("OPTIMIZE TABLE %s REWRITE DATA (MIN_INPUT_FILES=1)", table.fqn));
      icebergTable.refresh();

      assertThat(icebergTable.currentSnapshot()).isNull();

      runSQL(String.format("OPTIMIZE TABLE %s (MIN_INPUT_FILES=1)", table.fqn));
      icebergTable.refresh();

      assertThat(icebergTable.currentSnapshot()).isNull();
    }
  }

  public static void testOptimizeOnEmptyTableHollowSnapshot(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 2, 0)) {

      runSQL(String.format("OPTIMIZE TABLE %s REWRITE MANIFESTS", table.fqn));
      runSQL(String.format("OPTIMIZE TABLE %s REWRITE DATA (MIN_INPUT_FILES=1)", table.fqn));
      runSQL(String.format("OPTIMIZE TABLE %s (MIN_INPUT_FILES=1)", table.fqn));

      new TestBuilder(allocator).sqlQuery("SELECT count(*) as CNT FROM TABLE(TABLE_SNAPSHOT('%s')) where operation='replace'", table.fqn).unOrdered()
        .baselineColumns("CNT")
        .baselineValues(0L).go(); // No replace snapshot
    }
  }

  public static void testOptimizeNoopOnResidualDataManifests(String source, BufferAllocator allocator) throws Exception {
    final int noOfInputManifests = 10;
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(source, 0, 2, "modes_isolation")) {
      Table icebergTable = loadTable(table);
      List<ManifestFile> manifestFiles = new ArrayList<>(5);
      for (int i = 0; i < noOfInputManifests; i++) {
        ManifestFile manifestFile = writeManifestFile(icebergTable, 1000);
        manifestFiles.add(manifestFile);
      }
      AppendFiles append = icebergTable.newFastAppend();
      manifestFiles.forEach(append::appendManifest);
      append.commit();
      icebergTable.refresh();

      long totalManifestSize = manifestFiles.stream().mapToLong(ManifestFile::length).sum();
      long targetManifestSize = (long) (totalManifestSize * 0.3);
      icebergTable.updateProperties().set(MANIFEST_TARGET_SIZE_BYTES, String.valueOf(targetManifestSize)).commit();
      Snapshot initialSnapshot = icebergTable.currentSnapshot();

      runSQL(String.format("OPTIMIZE TABLE %s REWRITE MANIFESTS", table.fqn));
      icebergTable.refresh();
      Snapshot replacedSnapshot = icebergTable.currentSnapshot();

      // Manifests optimized into multiple output manifests, last one residual is expected to be outside the optimal range.
      assertThat(initialSnapshot.snapshotId()).isNotEqualTo(replacedSnapshot.snapshotId());
      assertThat(Integer.parseInt(replacedSnapshot.summary().get("manifests-created"))).isGreaterThan(1);
      assertThat(replacedSnapshot.summary().get("manifests-replaced")).isEqualTo(String.valueOf(noOfInputManifests));

      // Assert at-least one file outside the range
      assertThat(replacedSnapshot.allManifests(icebergTable.io()).stream()
        .anyMatch(m -> isNotInOptimalSizeRange(m, targetManifestSize))).isTrue();

      runSQL(String.format("OPTIMIZE TABLE %s REWRITE MANIFESTS", table.fqn));
      icebergTable.refresh();
      Snapshot noopSnapshot = icebergTable.currentSnapshot();
      assertThat(replacedSnapshot.snapshotId()).isEqualTo(noopSnapshot.snapshotId());

      // Ensure no orphan manifest files are left over in the directory
      assertNoOrphanManifests(icebergTable, initialSnapshot, replacedSnapshot);
    }
  }

  public static void testRewriteManifestsForEvolvedPartitionSpec(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(source, 3, 5)) {
      runSQL(String.format("ALTER TABLE %s ADD PARTITION FIELD identity(%s)", table.fqn, table.columns[0]));
      insertCommits(table, 2);

      runSQL(String.format("ALTER TABLE %s ADD PARTITION FIELD truncate(2, %s)", table.fqn, table.columns[1]));
      insertCommits(table, 2);

      runSQL(String.format("ALTER TABLE %s ADD PARTITION FIELD identity(%s)", table.fqn, table.columns[2]));
      insertCommits(table, 2);

      long snapshot1 = latestSnapshotId(table);
      runSQL(String.format("OPTIMIZE TABLE %s REWRITE MANIFESTS", table.fqn));

      long replaceSnapshot = latestSnapshotId(table);

      assertThat(snapshot1).isNotEqualTo(replaceSnapshot);

      new TestBuilder(allocator)
        .sqlQuery("SELECT partition_spec_id FROM TABLE(table_manifests('%s'))", table.fqn)
        .unOrdered()
        .baselineColumns("partition_spec_id")
        .baselineRecords(ImmutableList.of(
          ImmutableMap.of("`partition_spec_id`", 0),
          ImmutableMap.of("`partition_spec_id`", 1),
          ImmutableMap.of("`partition_spec_id`", 2),
          ImmutableMap.of("`partition_spec_id`", 3)
        )).go();
    }
  }

  public static void assertNoOrphanManifests(Table icebergTable, Snapshot... snapshots) throws IOException {
    Set<String> allManifestPaths = new HashSet<>();
    for (Snapshot snapshot : snapshots) {
      snapshot.allManifests(icebergTable.io()).forEach(m -> allManifestPaths.add(m.path()));
    }

    try (Stream<java.nio.file.Path> filePathStream = Files.walk(Paths.get(icebergTable.location()))) {
      Set<String> allAvroInTableDir = filePathStream.filter(p -> p.endsWith("avro"))
        .map(p -> p.toString()).collect(Collectors.toSet());

      assertThat(allManifestPaths).containsAll(allAvroInTableDir);
    }
  }

  private static boolean isNotInOptimalSizeRange(ManifestFile manifestFile, long manifestTargetSizeBytes) {
    long minManifestFileSize = (long) (manifestTargetSizeBytes * 0.75);
    long maxManifestFileSize = (long) (manifestTargetSizeBytes * 1.8);

    return manifestFile.length() < minManifestFileSize || manifestFile.length() > maxManifestFileSize;
  }

  private static long getAvgFileSize(String tableFqn, BufferAllocator allocator) throws Exception {
    List<QueryDataBatch> res = testSqlWithResults(String.format("SELECT avg(file_size_in_bytes)/1048576 as avg_file_size_mb FROM TABLE(table_files('%s'))", tableFqn));

    RecordBatchLoader loader = new RecordBatchLoader(allocator);
    QueryDataBatch result = res.get(0);
    loader.load(result.getHeader().getDef(), result.getData());
    Preconditions.checkState(loader.getRecordCount() == 1);
    long fileSize = -1L;
    for (VectorWrapper<?> vw : loader) {
      if (vw.getValueVector().getField().getName().equals("avg_file_size_mb")) {
        fileSize = (long) ((Float8Vector) vw.getValueVector()).get(0);
        break;
      }
    }

    loader.clear();
    result.release();
    return fileSize;
  }

  private static void refreshTable(DmlQueryTestUtils.Table table, BufferAllocator allocator) throws Exception {
    new TestBuilder(allocator).sqlQuery("ALTER TABLE %s REFRESH METADATA FORCE UPDATE", table.fqn).unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, String.format("Metadata for table '%s' refreshed.", table.fqn.replaceAll("\"", ""))).go();
  }

  public static void assertFileCount(String tableFqn, long expectedFileCount, BufferAllocator allocator) throws Exception {
    new TestBuilder(allocator).sqlQuery("SELECT COUNT(*) AS FILE_COUNT FROM TABLE(table_files('%s'))", tableFqn).unOrdered()
      .baselineColumns("FILE_COUNT")
      .baselineValues(expectedFileCount).go();
  }

  private static void assertManifestCount(String tableFqn, long expectedFileCount, BufferAllocator allocator) throws Exception {
    new TestBuilder(allocator).sqlQuery("SELECT COUNT(*) AS FILE_COUNT FROM TABLE(table_manifests('%s'))", tableFqn).unOrdered()
      .baselineColumns("FILE_COUNT")
      .baselineValues(expectedFileCount).go();
  }

  private static DmlQueryTestUtils.Table createPartitionedTestTable(String source, int noOfInsertCommitsPerPartition) throws Exception {
    DmlQueryTestUtils.Table table = createBasicTable(source, 0, 2, 0, ImmutableSet.of(1));
    insertCommits(table, noOfInsertCommitsPerPartition);
    return table;
  }

  private static DmlQueryTestUtils.Table createTestTable(String source, int noOfInsertCommits) throws Exception {
    DmlQueryTestUtils.Table table = createBasicTable(source, 2, 0);
    insertCommits(table, noOfInsertCommits);
    return table;
  }

  private static Map<String, String> fetchSummary(RecordBatchLoader loader, int index) {
    ListVector summaryVector = loader.getValueAccessorById(ListVector.class,
      loader.getValueVectorId(SchemaPath.getCompoundPath("summary")).getFieldIds()).getValueVector();

    Map<String, String> result = new HashMap<>();
    List<?> summary = summaryVector.getObject(index);

    for (Object entry : summary) {
      Map<String, Text> entryMap = (Map<String, Text>) entry;
      result.put(entryMap.get("key").toString(), entryMap.get("value").toString());
    }

    return result;
  }

  private static long latestSnapshotId(DmlQueryTestUtils.Table table) throws Exception {
    List<QueryDataBatch> results = testSqlWithResults(String.format("SELECT SNAPSHOT_ID FROM TABLE(TABLE_SNAPSHOT('%s')) ORDER BY COMMITTED_AT DESC LIMIT 1", table.fqn));
    RecordBatchLoader loader = new RecordBatchLoader(getSabotContext().getAllocator());
    QueryDataBatch data = results.get(0);
    loader.load(data.getHeader().getDef(), data.getData());

    BigIntVector snapshotIdVector = loader.getValueAccessorById(BigIntVector.class, loader.getValueVectorId(
      SchemaPath.getCompoundPath("SNAPSHOT_ID")).getFieldIds()).getValueVector();

    return snapshotIdVector.get(0);
  }

  private static Set<String> dataFilePaths(DmlQueryTestUtils.Table table) throws Exception {
    List<QueryDataBatch> results = testSqlWithResults(String.format("SELECT FILE_PATH FROM TABLE(TABLE_FILES('%s'))", table.fqn));
    RecordBatchLoader loader = new RecordBatchLoader(getSabotContext().getAllocator());
    QueryDataBatch data = results.get(0);
    loader.load(data.getHeader().getDef(), data.getData());

    VarCharVector filePathVector = loader.getValueAccessorById(VarCharVector.class, loader.getValueVectorId(
      SchemaPath.getCompoundPath("FILE_PATH")).getFieldIds()).getValueVector();

    Set<String> filePaths = new HashSet<>(filePathVector.getValueCount());
    for (int i = 0; i < filePathVector.getValueCount(); i++) {
      filePaths.add(filePathVector.getObject(i).toString());
    }
    return filePaths;
  }

  private static Set<String> manifestFilePaths(DmlQueryTestUtils.Table table) throws Exception {
    List<QueryDataBatch> results = testSqlWithResults(String.format("SELECT PATH FROM TABLE(TABLE_MANIFESTS('%s'))", table.fqn));
    RecordBatchLoader loader = new RecordBatchLoader(getSabotContext().getAllocator());
    QueryDataBatch data = results.get(0);
    loader.load(data.getHeader().getDef(), data.getData());

    VarCharVector filePathVector = loader.getValueAccessorById(VarCharVector.class, loader.getValueVectorId(
      SchemaPath.getCompoundPath("PATH")).getFieldIds()).getValueVector();

    Set<String> filePaths = new HashSet<>(filePathVector.getValueCount());
    for (int i = 0; i < filePathVector.getValueCount(); i++) {
      filePaths.add(filePathVector.getObject(i).toString());
    }
    return filePaths;
  }

  public static void insertCommits(DmlQueryTestUtils.Table table, int noOfInsertCommits) throws Exception {
    for (int commitId = 0; commitId < noOfInsertCommits; commitId++) {
      // Same number of rows per commit. If it's partitioned table, rows will get distributed.
      insertRows(table, noOfInsertCommits);
    }
  }

  public static void testOptimizeCommand(BufferAllocator allocator, String optimizeQuery, long expectedRDFC, long expectedRDelFC, long expectedNDFC) throws Exception {
    new TestBuilder(allocator)
      .sqlQuery(optimizeQuery)
      .unOrdered()
      .baselineColumns(REWRITTEN_DATA_FILE_COUNT, REWRITTEN_DELETE_FILE_COUNT, NEW_DATA_FILES_COUNT)
      .baselineValues(expectedRDFC,expectedRDelFC,expectedNDFC)
      .build()
      .run();
  }
}

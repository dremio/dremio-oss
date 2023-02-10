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
import static com.dremio.exec.planner.OptimizeOutputSchema.REWRITTEN_DATA_FILE_COUNT;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.insertRows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

public class OptimizeTests extends BaseTestQuery {

  public static void testOnUnPartitioned(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 6)) {
      assertFileCount(table.fqn, 6L, allocator);

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s", table.fqn).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(6L, 1L).go();

      assertFileCount(table.fqn, 1L, allocator);
    }
  }

  public static void testOnPartitioned(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createPartitionedTestTable(source, 3)) {
      assertFileCount(table.fqn, 9L, allocator); // 3 files per partition

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s", table.fqn).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(9L, 3L).go();

      assertFileCount(table.fqn, 3L, allocator); // should be left with 1 file per partition
    }
  }

  public static void testOnUnpartitionedMinInputFilesCriteria(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 5)) {
      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (MIN_INPUT_FILES = 6)", table.fqn).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(0L, 0L).go(); // NOOP because min input files > small file count

      assertFileCount(table.fqn, 5L, allocator);

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (MIN_INPUT_FILES = 5)", table.fqn).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(5L, 1L).go();

      assertFileCount(table.fqn, 1L, allocator);
    }
  }

  public static void testOnPartitionedMinInputFilesCriteria(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createPartitionedTestTable(source, 3)) {
      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (MIN_INPUT_FILES = 10)", table.fqn).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(0L, 0L).go(); // NOOP because min input files > small file count from all partitions

      assertFileCount(table.fqn, 9, allocator);

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (MIN_INPUT_FILES = 9)", table.fqn).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(9L, 3L).go();

      assertFileCount(table.fqn, 3, allocator);
    }
  }

  public static void testOnUnpartitionedMinFileSizeCriteria(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 5)) {
      assertFileCount(table.fqn, 5L, allocator);

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (MIN_FILE_SIZE_MB = 0)", table.fqn).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(0L, 0L).go(); // NOOP because files are likely to be larger than 20 bytes

      assertFileCount(table.fqn, 5L, allocator);

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (MIN_FILE_SIZE_MB = 1)", table.fqn).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(5L, 1L).go();

      assertFileCount(table.fqn, 1L, allocator);
    }
  }

  public static void testWithTargetFileSizeAlreadyOptimal(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source, 0)) {
      org.apache.iceberg.Table loadedTable = DmlQueryTestUtils.loadTable(table, allocator);
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

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (TARGET_FILE_SIZE_MB = %d)", table.fqn, avgFileSize)
        .unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(0L, 0L).go();

      assertFileCount(table.fqn, 5L, allocator);
    }
  }

  public static void testWithMixedSizes(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source,0)) {
      org.apache.iceberg.Table loadedTable = DmlQueryTestUtils.loadTable(table, allocator);
      loadedTable.newAppend().appendFile(
        DataFiles.builder(loadedTable.spec())
          .withPath("/data/fake.parquet")
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(2_033_219)
          .withRecordCount(1_000_000)
          .build()
      ).appendFile(
        DataFiles.builder(loadedTable.spec())
          .withPath(TestTools.getWorkingPath()+"/src/test/resources/iceberg/root_pointer/f1.parquet")
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(929)
          .withRecordCount(1_000)
          .build()
      ).appendFile(
        DataFiles.builder(loadedTable.spec())
          .withPath(TestTools.getWorkingPath()+"/src/test/resources/iceberg/root_pointer/f2.parquet")
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(929)
          .withRecordCount(1_000)
          .build()
      ).commit();
      refreshTable(table, allocator);
      assertFileCount(table.fqn, 3L, allocator);

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (TARGET_FILE_SIZE_MB=%d, MIN_FILE_SIZE_MB=%d, MAX_FILE_SIZE_MB=%d, MIN_INPUT_FILES=%d)",
          table.fqn, 2L, 1L, 3L, 2L)
        .unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(2L, 1L).go(); // Only two small files (f1 and f2) are considered for rewrite but not the already optimal one (map_float_type).

      assertFileCount(table.fqn, 2L, allocator);
    }
  }

  public static void testWithSingleFilePartitions(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createPartitionedTestTable(source, 3)) {
      assertFileCount(table.fqn, 9L, allocator); // 3 files per partition

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (MIN_INPUT_FILES=%d)", table.fqn, 2).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(9L, 3L).go();

      assertFileCount(table.fqn, 3L, allocator); // should be left with 1 file per partition

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (MIN_INPUT_FILES=%d)", table.fqn, 2).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(0L, 0L).go(); // should be NOOP
      assertFileCount(table.fqn, 3L, allocator);
    }
  }

  public static void testWithSingleFilePartitionsAndEvolvedSpec(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source,0)) {
      runSQL(String.format("INSERT INTO %s VALUES (1, 1), (1, 1), (1, 1)", table.fqn));
      runSQL(String.format("ALTER TABLE %s ADD PARTITION FIELD %s", table.fqn, table.columns[0]));
      runSQL(String.format("INSERT INTO %s VALUES (1, 1), (2, 2)", table.fqn));
      runSQL(String.format("INSERT INTO %s VALUES (2, 2)", table.fqn));
      // Partition column_0=1 will have 3 records from previous version of spec and 1 record from recent insert.

      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (MIN_INPUT_FILES=%d)", table.fqn, 1).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(4L, 2L).go();

      Snapshot snapshot = DmlQueryTestUtils.loadTable(table, allocator).currentSnapshot();
      assertThat(snapshot.operation()).isEqualTo("replace");
      String addedRecords = snapshot.summary().get("added-records");
      String deletedRecords = snapshot.summary().get("deleted-records");

      assertThat(addedRecords).isEqualTo(deletedRecords).isEqualTo("6");
    }
  }

  public static void testUnsupportedScenarios(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createPartitionedTestTable(source, 3)) {
      assertThatThrownBy(() -> runSQL(String.format("OPTIMIZE TABLE %s WHERE %s=0", table.fqn, table.columns[1])))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("PARSE ERROR: Failure parsing the query.");

      assertThatThrownBy(() -> runSQL(String.format("OPTIMIZE TABLE %s REWRITE MANIFESTS", table.fqn)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("PARSE ERROR: Failure parsing the query.");

      assertThatThrownBy(() -> runSQL(String.format("OPTIMIZE TABLE %s REWRITE DATA USING SORT", table.fqn)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("PARSE ERROR: Failure parsing the query.");

      assertThatThrownBy(() -> runSQL(String.format("OPTIMIZE TABLE %s USING SORT", table.fqn)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("PARSE ERROR: Failure parsing the query.");
    }
  }

  public static void testEvolvedPartitions(String source, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createTestTable(source,6)) {
      assertFileCount(table.fqn, 6L, allocator);
      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s", table.fqn).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(6L, 1L).go();
      assertFileCount(table.fqn, 1L, allocator);

      runSQL(String.format("ALTER TABLE %s ADD partition FIELD %s", table.fqn, table.columns[1]));
      new TestBuilder(allocator).sqlQuery("OPTIMIZE TABLE %s (MIN_INPUT_FILES=%d)", table.fqn, 1).unOrdered()
        .baselineColumns(REWRITTEN_DATA_FILE_COUNT, NEW_DATA_FILES_COUNT)
        .baselineValues(1L, 6L).go();

      assertFileCount(table.fqn, 6L, allocator);
    }
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

  private static void assertFileCount(String tableFqn, long expectedFileCount, BufferAllocator allocator) throws Exception {
    new TestBuilder(allocator).sqlQuery("SELECT COUNT(*) AS FILE_COUNT FROM TABLE(table_files('%s'))", tableFqn).unOrdered()
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

  private static void insertCommits(DmlQueryTestUtils.Table table, int noOfInsertCommits) throws Exception {
    for (int commitId = 0; commitId < noOfInsertCommits; commitId++) {
      // Same number of rows per commit. If it's partitioned table, rows will get distributed.
      insertRows(table, noOfInsertCommits);
    }
  }
}

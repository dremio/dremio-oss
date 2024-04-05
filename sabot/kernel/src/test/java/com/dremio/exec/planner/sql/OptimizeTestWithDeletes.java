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

import static org.apache.iceberg.Transactions.createTableTransaction;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.types.Types;

public class OptimizeTestWithDeletes extends BaseTestQuery {
  private static FileSystem fs;
  private static final String SOURCE_TABLE_PATH = "iceberg/v2/multi_rowgroup_orders_with_deletes";
  private static final String SETUP_BASE_PATH =
      "/tmp/iceberg-test-tables/v2/multi_rowgroup_orders_with_deletes";

  private static final Schema ICEBERG_SCHEMA =
      new Schema(
          required(1, "order_id", Types.IntegerType.get()),
          required(2, "order_year", Types.IntegerType.get()),
          required(3, "order_date", Types.TimestampType.withoutZone()),
          required(4, "source_id", Types.IntegerType.get()),
          required(5, "product_name", Types.StringType.get()),
          required(6, "amount", Types.DoubleType.get()));

  private static final PartitionSpec UNPARTITIONED_SPEC = PartitionSpec.unpartitioned();

  private static final PartitionSpec PARTITIONED_SPEC =
      PartitionSpec.builderFor(ICEBERG_SCHEMA).identity("order_year").build();
  private static final DataFile DF1 = unpartitionedDataFile("2021/2021-00.parquet", 21393);
  private static final DataFile DF2 = unpartitionedDataFile("2021/2021-01.parquet", 21412);
  private static final DataFile DF3 = unpartitionedDataFile("2021/2021-02.parquet", 21444);
  private static final DataFile DF4 = unpartitionedDataFile("2019/2019-00.parquet", 21361);
  private static final DeleteFile DEL_F1 =
      unpartitionedDeleteFile("2021/delete-2021-00.parquet", 27919);
  private static final DeleteFile DEL_F2 =
      unpartitionedDeleteFile("2021/delete-2021-01.parquet", 3841);

  public static void setup() throws Exception {
    fs = setupLocalFS();
    setupTableData();
  }

  private static void setupTableData() throws Exception {
    Path path = new Path(SETUP_BASE_PATH);
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    fs.mkdirs(path);
    copyFromJar(SOURCE_TABLE_PATH + "/data", Paths.get(SETUP_BASE_PATH + "/data"));
  }

  private static TableInfo setupUnpartitionedV2Table(
      List<DataFile> unlinkedDataFiles,
      List<DataFile> linkedDataFiles,
      List<DeleteFile> deleteFiles)
      throws Exception {
    // Setup metadata in a temporary location, so table path doesn't conflict across test-cases
    String metadataPath =
        Files.createTempDirectory(Paths.get(SETUP_BASE_PATH), "optimizeV2")
            .toAbsolutePath()
            .toString();
    final String[] tablePathComponents = StringUtils.split(metadataPath, '/');
    final String tableName = tablePathComponents[tablePathComponents.length - 1];
    final String tableFqn =
        Arrays.stream(tablePathComponents, 2, tablePathComponents.length)
            .collect(Collectors.joining("\".\"", "dfs_hadoop_mutable.\"", "\""));

    String tablePath = "file://" + metadataPath;
    final TableMetadata metadata =
        TableMetadata.newTableMetadata(
            ICEBERG_SCHEMA, UNPARTITIONED_SPEC, tablePath, ImmutableMap.of("format-version", "2"));
    TableOperations ops = new TestHadoopTableOperations(new Path(tablePath), fs.getConf());

    Transaction createTableTransaction = createTableTransaction(tableName, ops, metadata);
    createTableTransaction.commitTransaction();

    if (!unlinkedDataFiles.isEmpty()) {
      Transaction appendTransaction = Transactions.newTransaction(tableName, ops);
      AppendFiles appendFiles = appendTransaction.newAppend();
      unlinkedDataFiles.forEach(appendFiles::appendFile);
      appendFiles.commit();
      appendTransaction.commitTransaction();
    }

    Transaction rowDeltaTransaction = Transactions.newTransaction(tableName, ops);
    RowDelta rowDelta = rowDeltaTransaction.newRowDelta();
    linkedDataFiles.forEach(rowDelta::addRows);
    deleteFiles.forEach(rowDelta::addDeletes);
    rowDelta.commit();
    rowDeltaTransaction.commitTransaction();

    refresh(tableFqn);
    return new TableInfo(tableFqn, metadataPath, ops);
  }

  public static TableInfo setupPartitionedV2Table() throws Exception {
    // Setup metadata in a temporary location, so table path doesn't conflict across test-cases
    String metadataPath =
        Files.createTempDirectory(Paths.get(SETUP_BASE_PATH), "optimizeV2Partitioned")
            .toAbsolutePath()
            .toString();
    final String[] tablePathComponents = StringUtils.split(metadataPath, '/');
    final String tableName = tablePathComponents[tablePathComponents.length - 1];
    final String tableFqn =
        Arrays.stream(tablePathComponents, 2, tablePathComponents.length)
            .collect(Collectors.joining("\".\"", "dfs_hadoop_mutable.\"", "\""));

    String tablePath = "file://" + metadataPath;
    final TableMetadata metadata =
        TableMetadata.newTableMetadata(
            ICEBERG_SCHEMA, PARTITIONED_SPEC, tablePath, ImmutableMap.of("format-version", "2"));
    TableOperations ops = new TestHadoopTableOperations(new Path(tablePath), fs.getConf());

    Transaction createTableTransaction = createTableTransaction(tableName, ops, metadata);
    createTableTransaction.commitTransaction();

    // Use same data files and delete files as in the source
    Transaction appendTransaction = Transactions.newTransaction(tableName, ops);
    AppendFiles appendFiles = appendTransaction.newAppend();
    appendFiles.appendFile(partitionedDataFile("2019-02.parquet", 21358, 2019));
    appendFiles.appendFile(partitionedDataFile("2020-02.parquet", 21438, 2020));
    appendFiles.appendFile(partitionedDataFile("2019-00.parquet", 21361, 2019));
    appendFiles.appendFile(partitionedDataFile("2019-01.parquet", 21434, 2019));
    appendFiles.commit();
    appendTransaction.commitTransaction();

    Transaction rowDeltaTransaction1 = Transactions.newTransaction(tableName, ops);
    RowDelta rowDelta1 = rowDeltaTransaction1.newRowDelta();
    rowDelta1.addRows(partitionedDataFile("2020-00.parquet", 21386, 2020));
    rowDelta1.addRows(partitionedDataFile("2020-01.parquet", 21430, 2020));
    rowDelta1.addDeletes(partitionedDeleteFile("delete-2020-00.parquet", 16289, 500, 2020));
    rowDelta1.commit();
    rowDeltaTransaction1.commitTransaction();

    Transaction rowDeltaTransaction2 = Transactions.newTransaction(tableName, ops);
    RowDelta rowDelta2 = rowDeltaTransaction2.newRowDelta();
    rowDelta2.addRows(partitionedDataFile("2021-00.parquet", 21393, 2021));
    rowDelta2.addRows(partitionedDataFile("2021-01.parquet", 21412, 2021));
    rowDelta2.addRows(partitionedDataFile("2021-02.parquet", 21444, 2021));
    rowDelta2.addDeletes(partitionedDeleteFile("delete-2021-02.parquet", 16283, 500, 2021));
    rowDelta2.addDeletes(partitionedDeleteFile("delete-2021-01.parquet", 3841, 30, 2021));
    rowDelta2.addDeletes(partitionedDeleteFile("delete-2021-00.parquet", 27919, 900, 2021));
    rowDelta2.commit();
    rowDeltaTransaction2.commitTransaction();

    refresh(tableFqn);
    return new TableInfo(tableFqn, metadataPath, ops);
  }

  private static void assertOptimize(
      BufferAllocator allocator,
      String table,
      String options,
      long expectedRewrittenDataFiles,
      long expectedRewrittenDeleteFiles,
      long expectedNewDataFiles)
      throws Exception {
    new TestBuilder(allocator)
        .sqlQuery(String.format("optimize table %s (%s)", table, options))
        .unOrdered()
        .baselineColumns(
            "rewritten_data_files_count", "rewritten_delete_files_count", "new_data_files_count")
        .baselineValues(
            expectedRewrittenDataFiles, expectedRewrittenDeleteFiles, expectedNewDataFiles)
        .build()
        .run();
  }

  private static <T> List<T> l(T... t) {
    return Arrays.asList(t);
  }

  public static void testV2OptimizePartitioned(BufferAllocator allocator) throws Exception {
    TableInfo tableInfo = setupPartitionedV2Table();
    assertOptimize(allocator, tableInfo.getTableName(), "MIN_INPUT_FILES=1", 9L, 4L, 3L);
    fs.delete(new Path(tableInfo.getMetadataLocation()), true);
  }

  public static void testV2OptimizeDeleteLinkedFilesOnlyPartitioned(BufferAllocator allocator)
      throws Exception {
    TableInfo tableInfo = setupPartitionedV2Table();
    assertOptimize(
        allocator, tableInfo.getTableName(), "MIN_INPUT_FILES=1, MIN_FILE_SIZE_MB=0", 5L, 4L, 2L);
    fs.delete(new Path(tableInfo.getMetadataLocation()), true);
  }

  public static void testV2OptimizeUnpartitioned(BufferAllocator allocator) throws Exception {
    TableInfo tableInfo =
        setupUnpartitionedV2Table(Collections.EMPTY_LIST, l(DF1, DF2, DF3), l(DEL_F1));
    assertOptimize(allocator, tableInfo.getTableName(), "MIN_INPUT_FILES=2", 3L, 1L, 1L);
    fs.delete(new Path(tableInfo.getMetadataLocation()), true);
  }

  public static void testV2OptimizeMinInputFiles(BufferAllocator allocator) throws Exception {
    TableInfo tableInfo =
        setupUnpartitionedV2Table(Collections.EMPTY_LIST, l(DF1, DF2, DF3, DF4), l(DEL_F1));
    assertOptimize(allocator, tableInfo.getTableName(), "MIN_INPUT_FILES=5", 4L, 1L, 1L);
    fs.delete(new Path(tableInfo.getMetadataLocation()), true);
  }

  public static void testV2OptimizeDeleteLinkedFilesOnlyUnpartitioned(BufferAllocator allocator)
      throws Exception {
    TableInfo tableInfo = setupUnpartitionedV2Table(l(DF4), l(DF1, DF2, DF3), l(DEL_F1));
    assertOptimize(
        allocator, tableInfo.getTableName(), "MIN_INPUT_FILES=1, MIN_FILE_SIZE_MB=0", 3L, 1L, 1L);
    fs.delete(new Path(tableInfo.getMetadataLocation()), true);
  }

  public static void testV2OptimizeMultipleDeleteFiles(BufferAllocator allocator) throws Exception {
    TableInfo tableInfo = setupUnpartitionedV2Table(l(DF4), l(DF1), l(DEL_F1, DEL_F2));
    assertOptimize(
        allocator, tableInfo.getTableName(), "MIN_INPUT_FILES=1, MIN_FILE_SIZE_MB=0", 1L, 2L, 1L);
    fs.delete(new Path(tableInfo.getMetadataLocation()), true);
  }

  public static void testV2OptimizePartitionEvolution(BufferAllocator allocator) throws Exception {
    TableInfo tableInfo = setupUnpartitionedV2Table(l(DF4), l(DF1, DF2, DF3), l(DEL_F1));
    runSQL(
        String.format("alter table %s add partition field order_year", tableInfo.getTableName()));
    assertOptimize(
        allocator, tableInfo.getTableName(), "MIN_INPUT_FILES=1, MIN_FILE_SIZE_MB=0", 4L, 1L, 2L);

    new TestBuilder(allocator)
        .sqlQuery(
            String.format(
                "SELECT \"partition\" FROM TABLE(TABLE_FILES('%s'))", tableInfo.getTableName()))
        .unOrdered()
        .baselineColumns("partition")
        .baselineValues("{order_year=2019}")
        .baselineValues("{order_year=2021}")
        .build()
        .run();
    fs.delete(new Path(tableInfo.getMetadataLocation()), true);
  }

  public static void testV2OptimizeUpdateSequenceNumber(BufferAllocator allocator)
      throws Exception {
    final String metadataPath =
        Files.createTempDirectory(Paths.get(SETUP_BASE_PATH), "optimizeV2")
            .toAbsolutePath()
            .toString();
    final String[] tablePathComponents = StringUtils.split(metadataPath, '/');
    final String tableName = tablePathComponents[tablePathComponents.length - 1];
    final String tableFqn =
        Arrays.stream(tablePathComponents, 2, tablePathComponents.length)
            .collect(Collectors.joining("\".\"", "dfs_hadoop_mutable.\"", "\""));

    String tablePath = "file://" + metadataPath;
    final TableMetadata metadata =
        TableMetadata.newTableMetadata(
            ICEBERG_SCHEMA, UNPARTITIONED_SPEC, tablePath, ImmutableMap.of("format-version", "2"));
    TableOperations ops = new TestHadoopTableOperations(new Path(tablePath), fs.getConf());

    Transaction createTableTransaction = createTableTransaction(tableName, ops, metadata);
    createTableTransaction.commitTransaction();

    Transaction rowDeltaTransaction = Transactions.newTransaction(tableName, ops);
    RowDelta rowDelta = rowDeltaTransaction.newRowDelta();
    l(DF1, DF2, DF3).forEach(rowDelta::addRows);
    l(DEL_F1).forEach(rowDelta::addDeletes);
    rowDelta.commit();
    rowDeltaTransaction.commitTransaction();

    // Rewrite DF1
    Long snapshotId = ops.current().currentSnapshot().snapshotId();
    Transaction transaction = Transactions.newTransaction(tableName, ops);
    transaction
        .newRewrite()
        .rewriteFiles(ImmutableSet.of(DF1), ImmutableSet.of(DF1))
        .validateFromSnapshot(snapshotId)
        .commit();
    transaction.commitTransaction();

    refresh(tableFqn);
    assertOptimize(allocator, tableFqn, "MIN_INPUT_FILES=2, MIN_FILE_SIZE_MB=0", 2L, 1L, 1L);

    new TestBuilder(allocator)
        .sqlQuery(
            String.format(
                "SELECT file_path FROM TABLE(TABLE_FILES('%s')) ORDER BY record_count LIMIT 1",
                tableFqn))
        .ordered()
        .baselineColumns("file_path")
        .baselineValues(DF1.path().toString())
        .build()
        .run();
    fs.delete(new Path(metadataPath), true);
  }

  public static void testV2OptimizeUpdateSequenceNumberWithDeleteLink(BufferAllocator allocator)
      throws Exception {
    final String metadataPath =
        Files.createTempDirectory(Paths.get(SETUP_BASE_PATH), "optimizeV2")
            .toAbsolutePath()
            .toString();
    final String[] tablePathComponents = StringUtils.split(metadataPath, '/');
    final String tableName = tablePathComponents[tablePathComponents.length - 1];
    final String tableFqn =
        Arrays.stream(tablePathComponents, 2, tablePathComponents.length)
            .collect(Collectors.joining("\".\"", "dfs_hadoop_mutable.\"", "\""));

    String tablePath = "file://" + metadataPath;
    final TableMetadata metadata =
        TableMetadata.newTableMetadata(
            ICEBERG_SCHEMA, UNPARTITIONED_SPEC, tablePath, ImmutableMap.of("format-version", "2"));
    TableOperations ops = new TestHadoopTableOperations(new Path(tablePath), fs.getConf());

    Transaction createTableTransaction = createTableTransaction(tableName, ops, metadata);
    createTableTransaction.commitTransaction();

    Transaction transaction = Transactions.newTransaction(tableName, ops);
    transaction.newAppend().appendFile(DF1).commit();
    transaction.commitTransaction();

    transaction = Transactions.newTransaction(tableName, ops);
    transaction.newRowDelta().addRows(DF2).addDeletes(DEL_F1).commit();
    transaction.commitTransaction();

    Long snapshotId = ops.current().currentSnapshot().snapshotId();
    transaction = Transactions.newTransaction(tableName, ops);
    transaction
        .newRewrite()
        .rewriteFiles(ImmutableSet.of(DF1), ImmutableSet.of(DF1))
        .validateFromSnapshot(snapshotId)
        .commit();
    transaction.commitTransaction();

    transaction = Transactions.newTransaction(tableName, ops);
    transaction.newRowDelta().addDeletes(DEL_F2).commit();
    transaction.commitTransaction();

    refresh(tableFqn);
    assertOptimize(allocator, tableFqn, "MIN_INPUT_FILES=2, MIN_FILE_SIZE_MB=0", 2L, 2L, 1L);

    // delf1 had 300 deletes linked to df1 (1000 rows) and df2 (1000 rows) each, while delf2 has 10
    // deletes for both.
    // Final count should be 1680 - 990 from df1 [10 rows from delf2 removed] and 690 from df2 [300
    // rows removed from delf1 and 10 rows from delf2].
    new TestBuilder(allocator)
        .sqlQuery(String.format("SELECT record_count FROM TABLE(TABLE_FILES('%s'))", tableFqn))
        .unOrdered()
        .baselineColumns("record_count")
        .baselineValues(1680L)
        .build()
        .run();
    fs.delete(new Path(metadataPath), true);
  }

  public static void testV2OptimizeForIdentityPartitions(BufferAllocator allocator)
      throws Exception {
    TableInfo tableInfo = setupPartitionedV2Table();
    new TestBuilder(allocator)
        .sqlQuery(
            String.format(
                "optimize table %s FOR PARTITIONS order_year=2021 (MIN_INPUT_FILES=1)",
                tableInfo.getTableName()))
        .unOrdered()
        .baselineColumns(
            "rewritten_data_files_count", "rewritten_delete_files_count", "new_data_files_count")
        .baselineValues(3L, 3L, 1L)
        .build()
        .run();
    fs.delete(new Path(tableInfo.getMetadataLocation()), true);
  }

  public static void testV2OptimizeForIdentityPartitionsWithExpression(BufferAllocator allocator)
      throws Exception {
    TableInfo tableInfo = setupPartitionedV2Table();
    new TestBuilder(allocator)
        .sqlQuery(
            String.format(
                "optimize table %s FOR PARTITIONS order_year%%2=0 (MIN_INPUT_FILES=1, MIN_FILE_SIZE_MB=0)",
                tableInfo.getTableName()))
        .unOrdered()
        .baselineColumns(
            "rewritten_data_files_count", "rewritten_delete_files_count", "new_data_files_count")
        .baselineValues(2L, 1L, 1L)
        .build()
        .run();
    fs.delete(new Path(tableInfo.getMetadataLocation()), true);
  }

  public static void testV2OptimizeEmptyPartition(BufferAllocator allocator) throws Exception {
    TableInfo tableInfo = setupPartitionedV2Table();
    runSQL(String.format("DELETE FROM %s WHERE order_year=2021", tableInfo.getTableName()));
    new TestBuilder(allocator)
        .sqlQuery(
            String.format(
                "SELECT * FROM TABLE(TABLE_FILES('%s')) WHERE 'partition'='{order_year=2021}'",
                tableInfo.getTableName()))
        .unOrdered()
        .expectsEmptyResultSet()
        .go();
    new TestBuilder(allocator)
        .sqlQuery(
            String.format(
                "optimize table %s FOR PARTITIONS order_year=2021 (MIN_INPUT_FILES=1)",
                tableInfo.getTableName()))
        .unOrdered()
        .baselineColumns(
            "rewritten_data_files_count", "rewritten_delete_files_count", "new_data_files_count")
        .baselineValues(0L, 0L, 0L)
        .build()
        .run();
    fs.delete(new Path(tableInfo.getMetadataLocation()), true);
  }

  private static DataFile unpartitionedDataFile(String relativePath, long size) {
    return DataFiles.builder(UNPARTITIONED_SPEC)
        .withFileSizeInBytes(size)
        .withRecordCount(1)
        .withPath(String.format("%s/data/%s", SETUP_BASE_PATH, relativePath))
        .build();
  }

  private static DeleteFile unpartitionedDeleteFile(String relativePath, long size) {
    return FileMetadata.deleteFileBuilder(UNPARTITIONED_SPEC)
        .ofPositionDeletes()
        .withFileSizeInBytes(size)
        .withRecordCount(1)
        .withPath(String.format("%s/data/%s", SETUP_BASE_PATH, relativePath))
        .build();
  }

  private static DataFile partitionedDataFile(
      String relativePath, long size, int partitionOrderYr) {
    IcebergPartitionData icebergPartitionData =
        new IcebergPartitionData(PARTITIONED_SPEC.partitionType());
    icebergPartitionData.set(0, partitionOrderYr);

    return DataFiles.builder(PARTITIONED_SPEC)
        .withFileSizeInBytes(size)
        .withRecordCount(1000L)
        .withPath(String.format("%s/data/%d/%s", SETUP_BASE_PATH, partitionOrderYr, relativePath))
        .withPartition(icebergPartitionData)
        .build();
  }

  private static DeleteFile partitionedDeleteFile(
      String relativePath, long size, long records, int partitionOrderYr) {
    IcebergPartitionData icebergPartitionData =
        new IcebergPartitionData(PARTITIONED_SPEC.partitionType());
    icebergPartitionData.set(0, partitionOrderYr);

    return FileMetadata.deleteFileBuilder(PARTITIONED_SPEC)
        .ofPositionDeletes()
        .withFileSizeInBytes(size)
        .withRecordCount(records)
        .withPath(String.format("%s/data/%d/%s", SETUP_BASE_PATH, partitionOrderYr, relativePath))
        .withPartition(icebergPartitionData)
        .build();
  }

  public static class TableInfo {
    private final String tableName;
    private final String metadataLocation;
    private final TableOperations tableOps;

    TableInfo(String tableName, String metadataLocation, TableOperations tableOps) {
      this.tableName = tableName;
      this.metadataLocation = metadataLocation;
      this.tableOps = tableOps;
    }

    public String getTableName() {
      return tableName;
    }

    public String getMetadataLocation() {
      return metadataLocation;
    }

    public TableOperations getTableOps() {
      return tableOps;
    }
  }
}

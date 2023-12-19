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
package com.dremio.exec.physical.impl.writer;

import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testDmlQuery;
import static com.dremio.sabot.op.sender.partition.vectorized.AdaptiveVectorizedPartitionSenderOperator.calculateDop;
import static com.dremio.test.scaffolding.ScaffoldingRel.TYPE_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.BaseTestQuery;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.AdaptiveHashExchangePrel;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ValuesPrel;
import com.dremio.exec.planner.physical.visitor.InsertLocalExchangeVisitor;
import com.dremio.exec.planner.sql.DmlQueryTestUtils;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.Path;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class WriterTests extends BaseTestQuery {
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  private static final int MAX_DOP = 10;

  private static final int INITIAL_DOP = 1;

  private static final int DEFAULT_TOP_N_PARTITIONS = 5;

  private BufferAllocator rootAllocator;
  private BufferAllocator allocator;

  @BeforeClass
  public static void setUp() throws Exception {
    setSessionOption(ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_DML.getOptionName(), "true");
    setSessionOption(ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_OPTIMIZE.getOptionName(), "true");
    setSessionOption(ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_PARTITIONED_TABLE_WRITES.getOptionName(), "true");
    setSessionOption(ExecConstants.ADAPTIVE_HASH.getOptionName(), "true");
  }

  @AfterClass
  public static void cleanup() throws Exception {
    resetSystemOption(ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_DML.getOptionName());
    resetSystemOption(ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_OPTIMIZE.getOptionName());
    resetSystemOption(ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_PARTITIONED_TABLE_WRITES.getOptionName());
    resetSystemOption(ExecConstants.ADAPTIVE_HASH.getOptionName());
  }

  @Before
  @Override
  public void initAllocators() {
    rootAllocator = RootAllocatorFactory.newRoot(DEFAULT_SABOT_CONFIG);
    allocator = rootAllocator.newChildAllocator(testName.getMethodName(), 0, rootAllocator.getLimit());

    // for first-round writing. Many small files will ge generated with following settings
    setSessionOption(ExecConstants.PARQUET_MIN_RECORDS_FOR_FLUSH_VALIDATOR.getOptionName(), "100");
  }

  @After
  public void cleanUpTest() {
    resetSystemOption(ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR.getOptionName());
    resetSystemOption(ExecConstants.SMALL_PARQUET_BLOCK_SIZE_RATIO.getOptionName());
    resetSystemOption(ExecConstants.PARQUET_MIN_RECORDS_FOR_FLUSH_VALIDATOR.getOptionName());
    resetSystemOption(ExecConstants.TARGET_COMBINED_SMALL_PARQUET_BLOCK_SIZE_VALIDATOR.getOptionName());
  }

  private static long getAvgFileSize(String tableFqn, BufferAllocator allocator) throws Exception {
    List<QueryDataBatch> res = testRunAndReturn(UserBitShared.QueryType.SQL, String.format("SELECT avg(file_size_in_bytes) as avg_file_size_mb FROM TABLE(table_files('%s'))", tableFqn));

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

  private static int getParquetFileCount(Path dataFileFolder) {
    return FileUtils.listFiles(new File(dataFileFolder.toURI().getPath()), new String[]{ "parquet"}, true).size();
  }

  private void verifyIcebergTable(DmlQueryTestUtils.Table table,
                                  Object[][] expectedData,
                                  long expectedParquetFileCount,
                                  long expectedDataFileCount,
                                  long avgExpectedGeneratedDataFileSizeLowerBound) throws Exception {
    Thread.sleep(1001);

    // test written data are correct
    DmlQueryTestUtils.testSelectQuery(allocator, table, expectedData);

    Set<String> dataFiles = getDataFilePaths(table);
    Path dataFileFolder = Path.of(dataFiles.iterator().next()).getParent();

    // verify total generated parquet files in data folder
    assertEquals(expectedParquetFileCount, getParquetFileCount(dataFileFolder));

    // check generated data file count
    assertEquals(expectedDataFileCount, dataFiles.size());

    // check data file size
    long avgFileSize = getAvgFileSize(table.fqn, allocator);
    assertTrue(avgFileSize > avgExpectedGeneratedDataFileSizeLowerBound);

  }

  private void testCombiningSmallFiles(Long smallFileThreshold,
                                                             Long firstRoundWritingTargetFileSize,
                                                             Long secondRoundWritingTargetFileSize,
                                                             Integer partitionCount,
                                                             int columnCount,
                                                             int rowCount,
                                                             long expectedParquetFileCount,
                                                             long expectedDataFileCount,
                                                             long avgExpectedGeneratedDataFileSizeLowerBound) throws Exception {
    setSessionOption(ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR.getOptionName(), firstRoundWritingTargetFileSize.toString());
    setSessionOption(ExecConstants.SMALL_PARQUET_BLOCK_SIZE_RATIO.getOptionName(),
      String.valueOf(smallFileThreshold / (double)firstRoundWritingTargetFileSize));
    setSessionOption(ExecConstants.TARGET_COMBINED_SMALL_PARQUET_BLOCK_SIZE_VALIDATOR.getOptionName(), secondRoundWritingTargetFileSize.toString());
    try (DmlQueryTestUtils.Table table = createBasicTable(SOURCE,columnCount, rowCount)) {
      Object[][] expectedData = table.originalData;

      // verify Insert writing results
      verifyIcebergTable(table, expectedData, expectedParquetFileCount, expectedDataFileCount, avgExpectedGeneratedDataFileSizeLowerBound);

      if (partitionCount != null) {
        // update all rows, it is still an unpartitioned table
        Object[][] updatedData = new Object[rowCount][columnCount];
        for (int row = 0; row < table.originalData.length; row++) {
          updatedData[row][0] = row % partitionCount;
          for (int col = 1; col < table.originalData[0].length; col++) {
            updatedData[row][col] = table.originalData[row][col];
          }
        }
        expectedData = updatedData;
        testDmlQuery(allocator, "UPDATE %s SET id = mod(id, %s) ", new Object[]{table.fqn, partitionCount}, table, rowCount, expectedData);

        // add partitioned table
        runSQL(String.format("ALTER TABLE %s add PARTITION FIELD %s", table.fqn, "id"));
      }

      // Update all rows, verify Update writing results
      testDmlQuery(allocator, "UPDATE %s SET id = id", new Object[]{table.fqn}, table, rowCount, expectedData);
      verifyIcebergTable(table, expectedData, expectedParquetFileCount, expectedDataFileCount, avgExpectedGeneratedDataFileSizeLowerBound);
    }
  }

  @Test
  public void testCombiningSmallFilesForUnpartitionedAndSinglePartitionedTableNoSmallFiles() throws Exception {
    // first round writing target size = 2000 bytes
    // real parquet file size from first round ~ 2368 bytes
    // small file threshold = 1000, which is smaller than real parquet file size from first round. Second-round writing is not supposed to be triggered
    // data files in Iceberg table are from first-round writing (10)

    // unparitioned table
    testCombiningSmallFiles(1000L, 2000L, 10000L, null,
      3, 1000, 10, 10L, 2000);

    // single partitioned table
    testCombiningSmallFiles(1000L, 2000L, 10000L, 1,
      3, 1000, 10, 10L, 1500);
  }

  @Test
  public void testCombiningSmallFilesForUnpartitionedAndSinglePartitionedTableMoreSmallFiles() throws Exception {
    // first round writing target size = 4000 bytes
    // real parquet file size from first round ~ 3374 bytes
    // small file threshold = 4000, which is bigger than real parquet file size from first round. Second-round writing is triggered
    // files generated from first-round writing: 6
    // files generated from second-round writing: 10, because the target file size on the second-round writing is 2000, which is smaller than the first-round writting
    // data files in Iceberg table are from second-round writing (10)

    // unparitioned table
    testCombiningSmallFiles(4000L, 4000L, 2000L, null,
      3, 1000, 10, 10L, 2000);

    // single partitioned table
    testCombiningSmallFiles(4000L, 4000L, 2000L, 1,
      3, 1000, 10, 10L, 1000);
  }

  @Test
  public void testCombiningSmallFilesForUnpartitionedAndSinglePartitionedTableLessSmallFiles() throws Exception {
    // first round writing target size = 2000 bytes
    // real parquet file size from first round ~ 2372 bytes
    // small file threshold = 3000, which is bigger than real parquet file size from first round. Second-round writing is triggered
    // files generated from first-round writing: 10
    // files generated from second-round writing: 6, because the target file size on the second-round writing is 4000, which is bigger than the first-round writing. Less data files are geneated.
    // data files in Iceberg table are from second-round writing (6)

    // unparitioned table
    testCombiningSmallFiles(3000L, 2000L, 4000L, null,
      3, 1000, 6, 6L, 3000);

    // single partitioned table
    testCombiningSmallFiles(3000L, 2000L, 4000L, 1,
      3, 1000, 6, 6L, 2000);
  }

  @Test
  public void testCombiningSmallFilesForUnpartitionedAndSinglePartitionedTableCombinedAllSmallFiles() throws Exception {
    // first round writing target size = 2000 bytes
    // real parquet file size from first round ~ 2372 bytes
    // small file threshold = 3000, which is bigger than real parquet file size from first round. Second-round writing is triggered
    // files generated from first-round writing: 10
    // files generated from second-round writing: 1, because the target file size on the second-round writing is 20000, which is bigger than the first-round writing. All small files from the first-round writing are combined into the single file
    // data files in Iceberg table are from second-round writing (1)

    // unparitioned table
    testCombiningSmallFiles(3000L, 2000L, 30000L,null,
      3, 1000, 1, 1L, 10000);

    // single partitioned table
    testCombiningSmallFiles(3000L, 2000L, 30000L,1,
      3, 1000, 1, 1L, 5000);
  }

  @Test
  public void testCombiningSmallFilesForUnpartitionedAndSinglePartitionedTableCombinedAllSmallFilesDml() throws Exception {
    // first round writing target size = 2000 bytes
    // real parquet file size from first round ~ 2372 bytes
    // small file threshold = 3000, which is bigger than real parquet file size from first round. Second-round writing is triggered
    // files generated from first-round writing: 10
    // files generated from second-round writing: 1, because the target file size on the second-round writing is 20000, which is bigger than the first-round writing. All small files from the first-round writing are combined into the single file
    // data files in Iceberg table are from second-round writing (1)

    // unparitioned table
    testCombiningSmallFiles(3000L, 2000L, 30000L, null,
      3, 1000, 1, 1L, 10000);

    // single partitioned table
    testCombiningSmallFiles(3000L, 2000L, 30000L, 1,
      3, 1000, 1, 1L, 5000);
  }

  @Test
  public void testWithParquetAutoCorrectDatesFlag() throws Exception {
    try {
      setSessionOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES, "true");

      // unparitioned table
      testCombiningSmallFiles(3000L, 2000L, 30000L, null,
        3, 1000, 1, 1L, 10000);

    } finally {
      resetSystemOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES);
    }
  }

  private static Set<String> getDataFilePaths(DmlQueryTestUtils.Table table) throws Exception {
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

  private void testAdaptiveHash(List<Pair<Long, List<ExecProtos.HashDistributionValueCount>>> participantsPartitionCounters,
                                int topNPartitions,
                                int expectedDop) {
    List<ExecProtos.HashDistributionValueCounts> partitionValueCountsList = new ArrayList<>();
    for (Pair<Long, List<ExecProtos.HashDistributionValueCount>> partitionValueCountsPair : participantsPartitionCounters) {
      ExecProtos.HashDistributionValueCounts.Builder partitionValueCountsBuilder = ExecProtos.HashDistributionValueCounts.newBuilder();
      partitionValueCountsPair.right.stream().forEach(p -> partitionValueCountsBuilder.addHashDistributionValueCounts(p));
      partitionValueCountsBuilder.setTotalSeenRecords(partitionValueCountsPair.left);
      partitionValueCountsBuilder.setUniqueValueCount(partitionValueCountsPair.right.size());
      partitionValueCountsList.add(partitionValueCountsBuilder.build());
    }
    testAdaptiveHash(partitionValueCountsList, topNPartitions, expectedDop);
  }

  private void testAdaptiveHash(Collection<ExecProtos.HashDistributionValueCount> participantsPartitionCounters,
                                int seenRecords,
                                int uniquePartitionValues,
                                int expectedDop) {
    ExecProtos.HashDistributionValueCounts.Builder partitionValueCountsBuilder = ExecProtos.HashDistributionValueCounts.newBuilder();
    participantsPartitionCounters.stream().forEach(p -> partitionValueCountsBuilder.addHashDistributionValueCounts(p));
    partitionValueCountsBuilder.setTotalSeenRecords(seenRecords);
    partitionValueCountsBuilder.setUniqueValueCount(uniquePartitionValues);
    testAdaptiveHash(ImmutableList.of(partitionValueCountsBuilder.build()), DEFAULT_TOP_N_PARTITIONS, expectedDop);
  }

  private void testAdaptiveHash(Collection<ExecProtos.HashDistributionValueCounts> participantsPartitionCounters,
                                int topNPartitions,
                                int expectedDop) {
    int actual = calculateDop(participantsPartitionCounters, topNPartitions, 0.8, MAX_DOP, INITIAL_DOP);
    assertEquals(expectedDop, actual);
  }

  @Test
  public void testAdaptiveHashSingleFragMultiPartitionMaxDop() {
    List<ExecProtos.HashDistributionValueCount> participantsPartitionCounters =
      ImmutableList.of(
        ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(1).setCount(1000L).build(),
        ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(2).setCount(10L).build(),
        ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(3).setCount(10L).build(),
        ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(4).setCount(10L).build(),
        ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(5).setCount(10L).build()
      );

    // number of partitions > MAX_PARTITIONS (1)
    // rows with top 5 partition values > 80%
    // dop = 1
    testAdaptiveHash(participantsPartitionCounters, 1060, 5,  INITIAL_DOP);
  }

  @Test
  public void testAdaptiveHashSingleFragManyPartitionSingleDop() {
    List<ExecProtos.HashDistributionValueCount> participantsPartitionCounters =
      ImmutableList.of(
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(1).setCount(1000L).build(),
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(2).setCount(10L).build(),
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(3).setCount(10L).build(),
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(4).setCount(10L).build(),
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(5).setCount(10L).build(),
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(6).setCount(10L).build(),
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(7).setCount(10L).build()
        );

    // number of partitions > MAX_PARTITIONS (5)
    // dop = 1
    testAdaptiveHash(participantsPartitionCounters, 1060, 7,  INITIAL_DOP);
  }

  @Test
  public void testAdaptiveHashSingleFragMultiPartitionSingleDop() {
    List<ExecProtos.HashDistributionValueCount> participantsPartitionCounters =
      ImmutableList.of(
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(1).setCount(10L).build(),
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(2).setCount(10L).build(),
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(3).setCount(10L).build(),
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(4).setCount(10L).build(),
          ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(5).setCount(10L).build()
        );

    // rows with top 5 partition values < 80%
    // dop = 1
    testAdaptiveHash(participantsPartitionCounters, 70, 7, INITIAL_DOP);
  }

  @Test
  public void testAdaptiveHashSinglePartition() {
    List<Pair<Long, List<ExecProtos.HashDistributionValueCount>>> participantsPartitionCounters =
      ImmutableList.of(
        new Pair(100L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(1).setCount(100L).build())),
        new Pair(200L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(1).setCount(200L).build())),
        new Pair(100L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(1).setCount(100L).build())),
        new Pair(10L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(1).setCount(10L).build()))
      );
    // number of partitions <= MAX_PARTITIONS (5)
    // rows with top 5 partition values > 80%
    // max_dop
    testAdaptiveHash(participantsPartitionCounters, DEFAULT_TOP_N_PARTITIONS, MAX_DOP);
  }

  @Test
  public void testAdaptiveHashThreePartitionsMaxDop() {
    List<Pair<Long, List<ExecProtos.HashDistributionValueCount>>> participantsPartitionCounters =
      ImmutableList.of(
        new Pair(100L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(1).setCount(100L).build())),
        new Pair(200L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(2).setCount(200L).build())),
        new Pair(100L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(3).setCount(100L).build())),
        new Pair(10L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(1).setCount(10L).build()))
      );
    // number of partitions > MAX_PARTITIONS (1)
    // rows with top 5 partition values > 80%
    // dop = 1
    testAdaptiveHash(participantsPartitionCounters, DEFAULT_TOP_N_PARTITIONS, INITIAL_DOP);
  }

  @Test
  public void testAdaptiveHashThreeEvenPartitionsSingleDop() {
    List<Pair<Long, List<ExecProtos.HashDistributionValueCount>>> participantsPartitionCounters =
      ImmutableList.of(
        new Pair(100L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(1).setCount(100L).build())),
        new Pair(200L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(2).setCount(200L).build())),
        new Pair(100L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(3).setCount(100L).build())),
        new Pair(10L, ImmutableList.of(ExecProtos.HashDistributionValueCount.newBuilder().setHashDistributionKey(1).setCount(10L).build()))
      );
    // rows with top 2 partition values (75%) < 80%
    // dop = 1
    testAdaptiveHash(participantsPartitionCounters, 2, INITIAL_DOP);
  }

  @Test
  public void testAdaptiveHashPrelWithInsertLocalExchangeVisitor() {
    SabotContext context = getSabotContext();

    InsertLocalExchangeVisitor insertLocalExchangeVisitor = new InsertLocalExchangeVisitor(
      true, 0, false,
      context.getClusterResourceInformation(), context.getOptionManager());

    RelOptTable.ToRelContext toRelContext = mock(RelOptTable.ToRelContext.class, Mockito.RETURNS_DEEP_STUBS);
    when(toRelContext.getCluster().getTypeFactory()).thenReturn(SqlTypeFactoryImpl.INSTANCE);
    when(toRelContext.getCluster().getPlanner().getContext().unwrap(PlannerSettings.class))
      .thenReturn(mock(PlannerSettings.class));
    when(toRelContext.getCluster().getMetadataQuery().getRowCount(Mockito.any())).thenReturn(1d);

    ValuesPrel relNode = new ValuesPrel(
      toRelContext.getCluster(),
      RelTraitSet.createEmpty().plus(Prel.PHYSICAL),
      SqlTypeFactoryImpl.INSTANCE.createStructType(ImmutableList.of(
        Maps.immutableEntry("id", TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)))),
      null, 1L);

    DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
    AdaptiveHashExchangePrel adaptiveHashPrel = new AdaptiveHashExchangePrel(relNode.getCluster(), relNode.getTraitSet(),
      relNode, ImmutableList.of(distributionField));

    Prel newPrel = adaptiveHashPrel.accept(insertLocalExchangeVisitor, null);
    assertTrue(newPrel instanceof AdaptiveHashExchangePrel);
  }
}

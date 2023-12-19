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
package com.dremio.exec.store.parquet;

import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rb;
import static com.dremio.sabot.RecordSet.rs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.iceberg.FileContent;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.iceberg.deletes.RowLevelDeleteFilterFactory.DeleteFileInfo;
import com.dremio.exec.util.BloomFilter;
import com.dremio.sabot.Generator;
import com.dremio.sabot.RecordBatchValidator;
import com.dremio.sabot.RecordBatchValidatorDefaultImpl;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.RecordSet.Record;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import io.protostuff.ByteString;

public class TestParquetScanTableFunctionWithPositionalDeletes extends BaseTestParquetScanTableFunction {

  private static final List<SchemaPath> COLUMNS = ImmutableList.of(
      SchemaPath.getSimplePath("order_id"),
      SchemaPath.getSimplePath("order_year"));
  private static final BatchSchema OUTPUT_SCHEMA = IcebergTestTables.V2_ORDERS_SCHEMA.maskAndReorder(COLUMNS);
  private static final List<String> PARTITION_COLUMNS = ImmutableList.of("order_year");
  private static final ByteString EXTENDED_PROPS = getExtendedProperties(OUTPUT_SCHEMA);
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_2019 =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(PartitionProtobuf.PartitionValue.newBuilder()
              .setColumn("order_year")
              .setIntValue(2019)
              .build())
          .build();
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_2020 =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(PartitionProtobuf.PartitionValue.newBuilder()
              .setColumn("order_year")
              .setIntValue(2020)
              .build())
          .build();
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_2021 =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(PartitionProtobuf.PartitionValue.newBuilder()
              .setColumn("order_year")
              .setIntValue(2021)
              .build())
          .build();

  // contains order_id 0..999
  private static final String DATA_2019_00 = "2019/2019-00.parquet";
  // contains order_id 1000..1999
  private static final String DATA_2019_01 = "2019/2019-01.parquet";
  // contains order_id 2000..2999
  private static final String DATA_2019_02 = "2019/2019-02.parquet";

  // contains order_id 3000..3999
  private static final String DATA_2020_00 = "2020/2020-00.parquet";
  // contains order_id 4000..4999
  private static final String DATA_2020_01 = "2020/2020-01.parquet";
  // contains order_id 5000..5999
  private static final String DATA_2020_02 = "2020/2020-02.parquet";

  // contains order_id 6000..6999
  private static final String DATA_2021_00 = "2021/2021-00.parquet";
  // contains order_id 7000..7999
  private static final String DATA_2021_01 = "2021/2021-01.parquet";
  // contains order_id 8000..8999
  private static final String DATA_2021_02 = "2021/2021-02.parquet";

  // delete rows where order_id >= 3700 && order_id < 4200
  private static final String DELETE_2020_00 = "2020/delete-2020-00.parquet";

  // delete rows where order_id >= 6000 && order_id < 9000 && order_id % 10 < 3
  private static final String DELETE_2021_00 = "2021/delete-2021-00.parquet";
  // delete rows where order_id >= 6000 && order_id < 9000 && order_id % 100 == 5
  private static final String DELETE_2021_01 = "2021/delete-2021-01.parquet";
  // delete rows where order_id >= 6700 && order_id < 7200
  private static final String DELETE_2021_02 = "2021/delete-2021-02.parquet";

  private static final Predicate<Integer> FILTER_DELETE_2020_00 = i -> !(i >= 3700 && i < 4200);
  private static final Predicate<Integer> FILTER_DELETE_2021_00 = i -> !(i >= 6000 && i < 9000 && i % 10 < 3);
  private static final Predicate<Integer> FILTER_DELETE_2021_01 = i -> !(i >= 6000 && i < 9000 && i % 100 == 5);
  private static final Predicate<Integer> FILTER_DELETE_2021_02 = i -> !(i >= 6700 && i < 7200);

  private static final int BATCH_SIZE = 670;

  private static IcebergTestTables.Table table;

  @BeforeClass
  public static void initTables() {
    table = IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
  }

  @AfterClass
  public static void closeTables() throws Exception {
    table.close();
  }

  @Test
  public void testDataFileWithMultipleRowGroups() throws Exception {
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        inputRow(DATA_2021_00, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)));

    Iterator<Integer> orderIds = Stream.iterate(6000, i -> i + 1)
        .limit(1000)
        .filter(FILTER_DELETE_2021_00)
        .filter(FILTER_DELETE_2021_01)
        .filter(FILTER_DELETE_2021_02)
        .iterator();
    RecordSet output = outputRecordSet(orderIds);

    validate(input, output);
  }

  @Test
  public void testWithMultipleDataFilesSharingDeleteFiles() throws Exception {
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        inputRow(DATA_2021_00, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
        inputRow(DATA_2021_01, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
        inputRow(DATA_2021_02, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)));

    Iterator<Integer> orderIds = Stream.iterate(6000, i -> i + 1)
        .limit(3000)
        .filter(FILTER_DELETE_2021_00)
        .filter(FILTER_DELETE_2021_01)
        .filter(FILTER_DELETE_2021_02)
        .iterator();
    RecordSet output = outputRecordSet(orderIds);

    validate(input, output);
  }

  @Test
  public void testWithMultipleUnsortedInputBatches() throws Exception {
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        rb(
            inputRow(DATA_2021_01, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
            inputRow(DATA_2020_01, ImmutableList.of(DELETE_2020_00)),
            inputRow(DATA_2019_02),
            inputRow(DATA_2020_00, ImmutableList.of(DELETE_2020_00)),
            inputRow(DATA_2019_00)),
        rb(
            inputRow(DATA_2021_02, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
            inputRow(DATA_2020_02, ImmutableList.of(DELETE_2020_00)),
            inputRow(DATA_2021_00, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
            inputRow(DATA_2019_01)));

    Iterator<Integer> orderIds = Iterators.concat(
        // input batch 1 - ordered by datafile path
        // DATA_2019_00
        Stream.iterate(0, i -> i + 1)
            .limit(1000)
            .iterator(),
        // DATA_2019_02
        Stream.iterate(2000, i -> i + 1)
            .limit(1000)
            .iterator(),
        // DATA_2020_00
        Stream.iterate(3000, i -> i + 1)
            .limit(1000)
            .filter(FILTER_DELETE_2020_00)
            .iterator(),
        // DATA_2020_01
        Stream.iterate(4000, i -> i + 1)
            .limit(1000)
            .filter(FILTER_DELETE_2020_00)
            .iterator(),
        // DATA_2021_01
        Stream.iterate(7000, i -> i + 1)
            .limit(1000)
            .filter(FILTER_DELETE_2021_00)
            .filter(FILTER_DELETE_2021_01)
            .filter(FILTER_DELETE_2021_02)
            .iterator(),

        // input batch 2 - ordered by datafile path
        // DATA_2019_01
        Stream.iterate(1000, i -> i + 1)
            .limit(1000)
            .iterator(),
        // DATA_2020_02
        Stream.iterate(5000, i -> i + 1)
            .limit(1000)
            .filter(FILTER_DELETE_2020_00)
            .iterator(),
        // DATA_2021_00
        Stream.iterate(6000, i -> i + 1)
            .limit(1000)
            .filter(FILTER_DELETE_2021_00)
            .filter(FILTER_DELETE_2021_01)
            .filter(FILTER_DELETE_2021_02)
            .iterator(),
        // DATA_2021_02
        Stream.iterate(8000, i -> i + 1)
            .limit(1000)
            .filter(FILTER_DELETE_2021_00)
            .filter(FILTER_DELETE_2021_01)
            .filter(FILTER_DELETE_2021_02)
            .iterator());

    RecordSet output = outputRecordSet(orderIds);

    validate(input, output);
  }

  @Test
  public void testWithEmptyBlockSplitExpansion() throws Exception {
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        // this split should expand to only the 1st rowgroup which ends around offset 15200
        inputRow(DATA_2021_00, 0, 10000, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
        // this should expand to nothing
        inputRow(DATA_2021_00, 20000, 10000, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)));

    Iterator<Integer> orderIds = Stream.iterate(6000, i -> i + 1)
        .limit(552)
        .filter(FILTER_DELETE_2021_00)
        .filter(FILTER_DELETE_2021_01)
        .filter(FILTER_DELETE_2021_02)
        .iterator();
    RecordSet output = outputRecordSet(orderIds);

    validate(input, output);
  }

  @Test
  public void testWithNoDeleteFiles() throws Exception {
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        inputRow(DATA_2021_00),
        inputRow(DATA_2021_01),
        inputRow(DATA_2021_02));

    Iterator<Integer> orderIds = Stream.iterate(6000, i -> i + 1)
        .limit(3000)
        .iterator();
    RecordSet output = outputRecordSet(orderIds);

    validate(input, output);
  }

  @Test
  public void testWithRuntimeFilterThatExcludesDataFiles() throws Exception {
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        inputRow(DATA_2021_00, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
        inputRow(DATA_2021_01, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)));

    Iterator<Integer> orderIds = Stream.iterate(6000, i -> i + 1)
        .limit(552)
        .filter(FILTER_DELETE_2021_00)
        .filter(FILTER_DELETE_2021_01)
        .filter(FILTER_DELETE_2021_02)
        .iterator();
    RecordSet output = outputRecordSet(orderIds);
    // create a filter that will exclude all data files being scanned
    OutOfBandMessage msg = createRuntimeFilterForOrderYears(ImmutableList.of(2020));

    // run with the runtime filter, which will be applied immediately after the first rowgroup in DATA_2021_00
    // is processed, resulting in the 2nd rowgroup in DATA_2021_00 and all of DATA_2021_01 being skipped
    validateWithRuntimeFilter(input, new RecordBatchValidatorDefaultImpl(output), msg, 1);
  }

  private Record inputRow(String relativePath) throws Exception {
    return inputRow(relativePath, 0, -1, ImmutableList.of());
  }

  private Record inputRow(String relativePath, List<String> deleteFiles) throws Exception {
    return inputRow(relativePath, 0, -1, deleteFiles);
  }

  private Record inputRow(String relativePath, long offset, long length, List<String> deleteFiles) throws Exception {
    String fullPath = table.getLocation() + "/data/" + relativePath;
    PartitionProtobuf.NormalizedPartitionInfo partitionInfo = getPartitionInfoForDataFile(relativePath);
    List<DeleteFileInfo> deleteFilesWithFullPaths = deleteFiles.stream()
        .map(p -> new DeleteFileInfo(
            table.getLocation() + "/data/" + p,
            FileContent.POSITION_DELETES,
            0L,
            null))
        .collect(Collectors.toList());
    return inputRow(fullPath, offset, length, partitionInfo, deleteFilesWithFullPaths, EXTENDED_PROPS);
  }

  private RecordSet outputRecordSet(Iterator<Integer> expectedOrderIds) {
    List<Record> rows = new ArrayList<>();
    while (expectedOrderIds.hasNext()) {
      int orderId = expectedOrderIds.next();
      int orderYear = orderId < 3000 ? 2019 : orderId < 6000 ? 2020 : 2021;
      rows.add(r(orderId, orderYear));
    }
    return rs(OUTPUT_SCHEMA,
        rows.toArray(new Record[0]));
  }

  private void validate(RecordSet input, RecordSet output) throws Exception {
    TableFunctionPOP pop = getPopForIceberg(table, IcebergTestTables.V2_ORDERS_SCHEMA, COLUMNS, PARTITION_COLUMNS,
        EXTENDED_PROPS);
    try (AutoCloseable closeable = with(ExecConstants.PARQUET_READER_VECTORIZE, false)) {
      validateSingle(pop, TableFunctionOperator.class, input, new RecordBatchValidatorDefaultImpl(output), BATCH_SIZE);
    }
  }

  private void validateWithRuntimeFilter(Generator.Creator input, RecordBatchValidator result, OutOfBandMessage msg,
      int sendMsgAtRecordCount) throws Exception {
    boolean msgSent = false;
    TableFunctionPOP pop = getPopForIceberg(table, IcebergTestTables.V2_ORDERS_SCHEMA, COLUMNS, PARTITION_COLUMNS,
        EXTENDED_PROPS);
    int totalRecords = 0;
    final List<RecordBatchData> data = new ArrayList<>();
    try (
        AutoCloseable closeable = with(ExecConstants.PARQUET_READER_VECTORIZE, false);
        Pair<TableFunctionOperator, OperatorStats> pair =
            newOperatorWithStats(TableFunctionOperator.class, pop, BATCH_SIZE);
        Generator generator = input.toGenerator(getTestAllocator())) {

      TableFunctionOperator op = pair.first;
      OperatorStats stats = pair.second;
      stats.startProcessing();
      final VectorAccessible output = op.setup(generator.getOutput());
      int count;
      while (op.getState() != SingleInputOperator.State.DONE && (count = generator.next(BATCH_SIZE)) != 0) {
        assertState(op, SingleInputOperator.State.CAN_CONSUME);
        op.consumeData(count);
        while (op.getState() == SingleInputOperator.State.CAN_PRODUCE) {
          int recordsOutput = op.outputData();
          totalRecords += recordsOutput;
          if (recordsOutput > 0) {
            data.add(new RecordBatchData(output, getTestAllocator()));
          }
          // send oob message after the batch that exceeds the threshold count
          if (totalRecords >= sendMsgAtRecordCount && !msgSent) {
            op.workOnOOB(msg);
            msgSent = true;
          }
        }
      }

      if (op.getState() == SingleInputOperator.State.CAN_CONSUME) {
        op.noMoreToConsume();
      }

      while (op.getState() == SingleInputOperator.State.CAN_PRODUCE) {
        int recordsOutput = op.outputData();
        totalRecords += recordsOutput;
        if (recordsOutput > 0) {
          data.add(new RecordBatchData(output, getTestAllocator()));
        }
      }

      if (op.getState() == SingleInputOperator.State.CAN_CONSUME) {
        op.noMoreToConsume();
      }

      assertState(op, SingleInputOperator.State.DONE);
      result.checkValid(data);
      stats.stopProcessing();
    } finally {
      AutoCloseables.close(data);
    }
  }

  private PartitionProtobuf.NormalizedPartitionInfo getPartitionInfoForDataFile(String relativePath) {
    if (relativePath.startsWith("2019")) {
      return PARTITION_INFO_2019;
    } else if (relativePath.startsWith("2020")) {
      return PARTITION_INFO_2020;
    } else {
      return PARTITION_INFO_2021;
    }
  }

  private OutOfBandMessage createRuntimeFilterForOrderYears(ImmutableList<Integer> orderYears) {
    try (
        ArrowBuf keyBuf = getTestAllocator().buffer(4);
        BloomFilter bloomFilter = new BloomFilter(getTestAllocator(), "testfilter", 64)) {
      bloomFilter.setup();

      for (int orderYear : orderYears) {
        keyBuf.writerIndex(0);
        keyBuf.writeInt(orderYear);
        bloomFilter.put(keyBuf, 4);
      }

      return createRuntimeFilterMsg(ImmutableList.of("order_year"), bloomFilter);
    }
  }

  private OutOfBandMessage createRuntimeFilterMsg(List<String> partitionCols, BloomFilter bloomFilter) {
    ExecProtos.RuntimeFilter.Builder runtimeFilter = ExecProtos.RuntimeFilter.newBuilder().setProbeScanOperatorId(0).setProbeScanMajorFragmentId(0);
    ExecProtos.CompositeColumnFilter partitionColFilter = ExecProtos.CompositeColumnFilter.newBuilder()
        .setFilterType(ExecProtos.RuntimeFilterType.BLOOM_FILTER)
        .setSizeBytes(bloomFilter.getSizeInBytes())
        .setValueCount(bloomFilter.getNumBitsSet())
        .addAllColumns(partitionCols)
        .build();
    runtimeFilter.setPartitionColumnFilter(partitionColFilter);
    ArrowBuf buf = bloomFilter.getDataBuffer();
    buf.readerIndex(0);
    buf.writerIndex(bloomFilter.getSizeInBytes());
    ArrayList<Integer> bufferLengths = new ArrayList<>();
    bufferLengths.add((int)runtimeFilter.getPartitionColumnFilter().getSizeBytes());

    return new OutOfBandMessage(
        null,
        0,
        ImmutableList.of(0),
        0,
        0,
        0,
        0,
        new OutOfBandMessage.Payload(runtimeFilter.build()),
        new ArrowBuf[] {buf},
        bufferLengths,
        false);
  }
}

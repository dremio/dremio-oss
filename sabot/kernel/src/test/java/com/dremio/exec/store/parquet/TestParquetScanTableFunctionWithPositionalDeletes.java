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

import static com.dremio.sabot.Fixtures.struct;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.tb;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static com.dremio.sabot.Fixtures.tuple;
import static com.dremio.sabot.Fixtures.varCharList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.util.BloomFilter;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Fixtures.DataRow;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.Generator;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;
import io.protostuff.ByteStringUtil;

public class TestParquetScanTableFunctionWithPositionalDeletes extends BaseTestTableFunction {

  private static final List<SchemaPath> COLUMNS = ImmutableList.of(
      SchemaPath.getSimplePath("order_id"),
      SchemaPath.getSimplePath("order_year"));
  private static final List<String> PARTITION_COLUMNS = ImmutableList.of("order_year");
  private static final ByteString EXTENDED_PROPS = getExtendedProperties(COLUMNS);
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
  private static final Fixtures.HeaderRow INPUT_HEADER = th(
      struct(SystemSchemas.SPLIT_IDENTITY, ImmutableList.of(
          SplitIdentity.PATH, SplitIdentity.OFFSET, SplitIdentity.LENGTH, SplitIdentity.FILE_LENGTH)),
      SystemSchemas.SPLIT_INFORMATION,
      SystemSchemas.COL_IDS,
      SystemSchemas.AGG_DELETEFILE_PATHS);

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

  private FileSystem fs;
  @Mock
  private StoragePluginId pluginId;
  @Mock(extraInterfaces = {SupportsIcebergRootPointer.class, SupportsInternalIcebergTable.class})
  private MutablePlugin plugin;

  @BeforeClass
  public static void initTables() {
    table = IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
  }

  @AfterClass
  public static void closeTables() throws Exception {
    table.close();
  }

  @Before
  public void prepareMocks() throws Exception {
    fs = HadoopFileSystem.get(Path.of("/"), new Configuration());
    when(fec.getStoragePlugin(pluginId)).thenReturn(plugin);
    SupportsIcebergRootPointer sirp = (SupportsIcebergRootPointer) plugin;
    when(sirp.createFSWithAsyncOptions(anyString(), anyString(), any())).thenReturn(fs);
    SupportsInternalIcebergTable siit = (SupportsInternalIcebergTable) plugin;
    when(siit.createScanTableFunction(any(), any(), any(), any())).thenAnswer(i ->
        new ParquetScanTableFunction(i.getArgument(0), i.getArgument(1), i.getArgument(2), i.getArgument(3)));
    when(pluginId.getName()).thenReturn("testplugin");
  }

  @Test
  public void testDataFileWithMultipleRowGroups() throws Exception {
    Table input = t(
        INPUT_HEADER,
        inputRow(DATA_2021_00, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)));

    Iterator<Integer> orderIds = Stream.iterate(6000, i -> i + 1)
        .limit(1000)
        .filter(FILTER_DELETE_2021_00)
        .filter(FILTER_DELETE_2021_01)
        .filter(FILTER_DELETE_2021_02)
        .iterator();
    Table output = outputTable(orderIds);

    validate(input, output);
  }

  @Test
  public void testWithMultipleDataFilesSharingDeleteFiles() throws Exception {
    Table input = t(
        INPUT_HEADER,
        inputRow(DATA_2021_00, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
        inputRow(DATA_2021_01, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
        inputRow(DATA_2021_02, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)));

    Iterator<Integer> orderIds = Stream.iterate(6000, i -> i + 1)
        .limit(3000)
        .filter(FILTER_DELETE_2021_00)
        .filter(FILTER_DELETE_2021_01)
        .filter(FILTER_DELETE_2021_02)
        .iterator();
    Table output = outputTable(orderIds);

    validate(input, output);
  }

  @Test
  public void testWithMultipleUnsortedInputBatches() throws Exception {
    Table input = t(
        INPUT_HEADER,
        tb(
            inputRow(DATA_2021_01, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
            inputRow(DATA_2020_01, ImmutableList.of(DELETE_2020_00)),
            inputRow(DATA_2019_02),
            inputRow(DATA_2020_00, ImmutableList.of(DELETE_2020_00)),
            inputRow(DATA_2019_00)),
        tb(
            inputRow(DATA_2021_02, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
            inputRow(DATA_2020_02, ImmutableList.of(DELETE_2020_00)),
            inputRow(DATA_2021_00, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
            inputRow(DATA_2019_01)));

    Iterator<Integer> orderIds = Stream.iterate(0, i -> i + 1)
        .limit(9000)
        .filter(FILTER_DELETE_2020_00)
        .filter(FILTER_DELETE_2021_00)
        .filter(FILTER_DELETE_2021_01)
        .filter(FILTER_DELETE_2021_02)
        .iterator();
    Table output = outputTable(orderIds).orderInsensitive();

    validate(input, output);
  }

  @Test
  public void testWithEmptyBlockSplitExpansion() throws Exception {
    Table input = t(
        INPUT_HEADER,
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
    Table output = outputTable(orderIds);

    validate(input, output);
  }

  @Test
  public void testWithNoDeleteFiles() throws Exception {
    Table input = t(
        INPUT_HEADER,
        inputRow(DATA_2021_00),
        inputRow(DATA_2021_01),
        inputRow(DATA_2021_02));

    Iterator<Integer> orderIds = Stream.iterate(6000, i -> i + 1)
        .limit(3000)
        .iterator();
    Table output = outputTable(orderIds);

    validate(input, output);
  }

  @Test
  public void testWithRuntimeFilterThatExcludesDataFiles() throws Exception {
    Table input = t(
        INPUT_HEADER,
        inputRow(DATA_2021_00, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)),
        inputRow(DATA_2021_01, ImmutableList.of(DELETE_2021_00, DELETE_2021_01, DELETE_2021_02)));

    Iterator<Integer> orderIds = Stream.iterate(6000, i -> i + 1)
        .limit(552)
        .filter(FILTER_DELETE_2021_00)
        .filter(FILTER_DELETE_2021_01)
        .filter(FILTER_DELETE_2021_02)
        .iterator();
    Table output = outputTable(orderIds);
    // create a filter that will exclude all data files being scanned
    OutOfBandMessage msg = createRuntimeFilterForOrderYears(ImmutableList.of(2020));

    // run with the runtime filter, which will be applied immediately after the first rowgroup in DATA_2021_00
    // is processed, resulting in the 2nd rowgroup in DATA_2021_00 and all of DATA_2021_01 being skipped
    validateWithRuntimeFilter(input, output, msg, 1);
  }

  private DataRow inputRow(String relativePath) throws Exception {
    return inputRow(relativePath, 0, -1, ImmutableList.of());
  }

  private DataRow inputRow(String relativePath, List<String> deleteFiles) throws Exception {
    return inputRow(relativePath, 0, -1, deleteFiles);
  }

  private DataRow inputRow(String relativePath, long offset, long length, List<String> deleteFiles) throws Exception {
    Path path = Path.of(table.getLocation() + "/data/" + relativePath);
    long fileSize = fs.getFileAttributes(path).size();
    if (length == -1) {
      length = fileSize;
    }
    PartitionProtobuf.NormalizedPartitionInfo partitionInfo = getPartitionInfoForDataFile(relativePath);
    return tr(
        tuple(path.toString(), 0, fileSize, fileSize),
        createSplitInformation(path.toString(), offset, length, fileSize, 0, partitionInfo),
        EXTENDED_PROPS.toByteArray(),
        varCharList(deleteFiles.stream()
            .map(p -> table.getLocation() + "/data/" + p)
            .toArray(String[]::new)));
  }

  private Table outputTable(Iterator<Integer> expectedOrderIds) {
    List<DataRow> rows = new ArrayList<>();
    while (expectedOrderIds.hasNext()) {
      int orderId = expectedOrderIds.next();
      int orderYear = orderId < 3000 ? 2019 : orderId < 6000 ? 2020 : 2021;
      rows.add(tr(orderId, orderYear));
    }
    return t(
        th("order_id", "order_year"),
        rows.toArray(new DataRow[0]));
  }

  private void validate(Table input, Table output) throws Exception {
    TableFunctionPOP pop = getPop(table, IcebergTestTables.V2_ORDERS_SCHEMA, COLUMNS, PARTITION_COLUMNS,
        EXTENDED_PROPS);
    try (AutoCloseable closeable = with(ExecConstants.PARQUET_READER_VECTORIZE, false)) {
      validateSingle(pop, TableFunctionOperator.class, input, output, BATCH_SIZE);
    }
  }

  private void validateWithRuntimeFilter(Table input, Table result, OutOfBandMessage msg, int sendMsgAtRecordCount)
      throws Exception {
    boolean msgSent = false;
    TableFunctionPOP pop = getPop(table, IcebergTestTables.V2_ORDERS_SCHEMA, COLUMNS, PARTITION_COLUMNS,
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

  private TableFunctionPOP getPop(
      IcebergTestTables.Table table,
      BatchSchema fullSchema,
      List<SchemaPath> columns,
      List<String> partitionColumns,
      ByteString extendedProps) {
    BatchSchema projectedSchema = fullSchema.maskAndReorder(columns);
    return new TableFunctionPOP(
        PROPS,
        null,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.DATA_FILE_SCAN,
            false,
            new TableFunctionContext(
                getFileConfig(table),
                fullSchema,
                projectedSchema,
                ImmutableList.of(Arrays.asList(table.getTableName().split("\\."))),
                null,
                pluginId,
                null,
                columns,
                partitionColumns,
                null,
                extendedProps,
                false,
                false,
                false,
                null)));
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

  private FileConfig getFileConfig(IcebergTestTables.Table table) {
    FileConfig config = new FileConfig();
    config.setLocation(table.getLocation());
    config.setType(FileType.ICEBERG);
    return config;
  }

  private static ByteString getExtendedProperties(List<SchemaPath> columns) {
    IcebergProtobuf.IcebergDatasetXAttr.Builder builder = IcebergProtobuf.IcebergDatasetXAttr.newBuilder();
    for (int i = 0; i < columns.size(); i++) {
      builder.addColumnIds(IcebergProtobuf.IcebergSchemaField.newBuilder()
          .setSchemaPath(columns.get(i).toDotString())
          .setId(i + 1));
    }
    return toProtostuff(builder.build()::writeTo);
  }

  private static ByteString toProtostuff(BytesOutput out) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      out.writeTo(output);
      return ByteStringUtil.wrap(output.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static byte[] createSplitInformation(String path, long offset, long length, long fileSize, long mtime,
      PartitionProtobuf.NormalizedPartitionInfo partitionInfo) throws Exception {
    ParquetProtobuf.ParquetBlockBasedSplitXAttr splitExtended = ParquetProtobuf.ParquetBlockBasedSplitXAttr.newBuilder()
        .setPath(path)
        .setStart(offset)
        .setLength(length)
        .setFileLength(fileSize)
        .setLastModificationTime(mtime)
        .build();

    PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
        .setPartitionId(partitionInfo.getId())
        .setExtendedProperty(splitExtended.toByteString());

    return IcebergSerDe.serializeToByteArray(new SplitAndPartitionInfo(partitionInfo, splitInfo.build()));
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
        false);
  }
}

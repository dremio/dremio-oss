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
import static com.dremio.sabot.RecordSet.rs;
import static com.dremio.sabot.RecordSet.st;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.util.RuntimeFilterTestUtils;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.sabot.RecordBatchValidatorDefaultImpl;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.RecordSet.RsRecord;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Tests for {@link ParquetScanTableFunction} */
public class TestParquetScanTableFunction extends BaseTestParquetScanTableFunction {
  private RuntimeFilterTestUtils utils;

  private BatchSchema FIFTY_ROWGROUPS_SCHEMA =
      BatchSchema.newBuilder()
          .addField(Field.nullable("id", Types.MinorType.BIGINT.getType()))
          .addField(Field.nullable("col_1", Types.MinorType.BIGINT.getType()))
          .addField(Field.nullable("col_2", Types.MinorType.BIGINT.getType()))
          .addField(Field.nullable("col_3", Types.MinorType.BIGINT.getType()))
          .addField(Field.nullable("col_4", Types.MinorType.BIGINT.getType()))
          .build();

  private static final int BATCH_SIZE = 12;

  @Before
  public void setupTest() {
    utils = new RuntimeFilterTestUtils(getTestAllocator());
  }

  @Test
  public void testDataFileSplitWithMultipleRowGroupsAndTrimming() throws Exception {
    String path = FileUtils.getResourceAsFile("/parquet/50-rowgroups.parquet").toURI().toString();

    List<SchemaPath> outputColumns = ImmutableList.of(SchemaPath.getSimplePath("id"));

    // block split containing rowgroups 26 - 50
    // rowgroup 26 starts at offset 30419
    RecordSet input =
        rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA, inputRow(path, 30000, 100000));

    // ids in the file are sequential, with 10 values per rowgroup
    List<RsRecord> ids =
        Stream.iterate(250L, i -> i + 1L).limit(250).map(RecordSet::r).collect(Collectors.toList());
    RecordSet output =
        rs(FIFTY_ROWGROUPS_SCHEMA.maskAndReorder(outputColumns), ids.toArray(new RsRecord[0]));

    // validate with TOTAL_HEAP_OBJ_SIZE set to 50, which in the case of this file, would try to
    // trim down to 5
    // rowgroups in memory
    try (AutoCloseable heapOpt = with(ExecConstants.TOTAL_HEAP_OBJ_SIZE, 50)) {
      validate(FIFTY_ROWGROUPS_SCHEMA, outputColumns, input, output);
    }
  }

  @Test
  public void testWorkOnOOBRuntimeFilter() throws Exception {
    // Send 6 messages. 1/2 are independent filters, 3 is dup of 1 from a different minor frag and
    // should be dropped, 4 comes from
    // a different sender but filter structure is similar to 2/3, 5 comes from same sender as 4 but
    // has one extra column.
    // 6th comes from same sender as 4 but with one less non-partition column.

    // Finally, we expect four runtime filters to be added.

    int sendingMajorFragment1 = 11;
    int sendingMajorFragment2 = 12;

    int sendingMinorFragment1 = 2;
    int sendingMinorFragment2 = 1;

    int sendingOp1 = 101;
    int sendingOp2 = 102;
    int sendingOp3 = 103;

    ArrowBuf bloomFilterBuf = getTestAllocator().buffer(64);
    bloomFilterBuf.setZero(0, bloomFilterBuf.capacity());
    List<String> m1PtCols = Lists.newArrayList("pCol1", "pCol2");
    List<String> m2PtCols = Lists.newArrayList("pCol3", "pCol4");
    List<String> m5PtCols = new ArrayList<>(m2PtCols);
    m5PtCols.add("pCol5"); // Extra partition col, so this won't be considered as a duplicate.

    ValueListFilter m1Vlf1 = utils.prepareNewValueListFilter("npCol1", false, 1, 2, 3);
    ValueListFilter m1Vlf2 = utils.prepareNewValueListFilter("npCol2", false, 11, 12, 13);
    ValueListFilter m2Vlf1 = utils.prepareNewValueListBooleanFilter("npCol3", false, false, true);
    ValueListFilter m2Vlf2 = utils.prepareNewValueListBooleanFilter("npCol4", true, true, false);

    // START: re-use for other messages
    m1Vlf1.buf().getReferenceManager().retain(); // msg3
    m1Vlf2.buf().getReferenceManager().retain(); // msg3
    bloomFilterBuf.getReferenceManager().retain(5); // msg2, msg3, msg4, msg5, msg6
    m2Vlf1.buf().getReferenceManager().retain(3); // re-used by msg4, msg5, msg6
    m2Vlf2.buf().getReferenceManager().retain(2); // re-used by msg4, msg5
    // END
    ParquetScanTableFunction scanOp =
        spy(
            new ParquetScanTableFunction(
                mock(FragmentExecutionContext.class),
                getMockContext(),
                getProps(),
                getTableFunctionConfig()));
    RecordReaderIterator mockedReaderIterator = mock(RecordReaderIterator.class);
    doNothing().when(mockedReaderIterator).addRuntimeFilter(any(RuntimeFilter.class));
    when(scanOp.getRecordReaderIterator()).thenReturn(mockedReaderIterator);

    OutOfBandMessage msg1 =
        utils.newOOB(
            sendingMajorFragment1,
            sendingOp1,
            sendingMinorFragment1,
            m1PtCols,
            bloomFilterBuf,
            m1Vlf1,
            m1Vlf2);
    scanOp.workOnOOB(msg1);
    Arrays.stream(msg1.getBuffers()).forEach(ArrowBuf::close);
    assertEquals(1, scanOp.getRuntimeFilters().size());
    ArgumentCaptor<RuntimeFilter> addedFilter =
        ArgumentCaptor.forClass(com.dremio.exec.store.RuntimeFilter.class);

    OutOfBandMessage msg2 =
        utils.newOOB(
            sendingMajorFragment2,
            sendingOp2,
            sendingMinorFragment1,
            m2PtCols,
            bloomFilterBuf,
            m2Vlf1,
            m2Vlf2);
    scanOp.workOnOOB(msg2);
    Arrays.stream(msg2.getBuffers()).forEach(ArrowBuf::close);
    assertEquals(2, scanOp.getRuntimeFilters().size());

    OutOfBandMessage msg3 =
        utils.newOOB(
            sendingMajorFragment1,
            sendingOp1,
            sendingMinorFragment2,
            m1PtCols,
            bloomFilterBuf,
            m1Vlf1,
            m1Vlf2);
    scanOp.workOnOOB(msg3); // should get skipped
    Arrays.stream(msg3.getBuffers()).forEach(ArrowBuf::close);
    assertEquals(2, scanOp.getRuntimeFilters().size());

    OutOfBandMessage msg4 =
        utils.newOOB(
            sendingMajorFragment2,
            sendingOp3,
            sendingMinorFragment1,
            m2PtCols,
            bloomFilterBuf,
            m2Vlf1,
            m2Vlf2);
    scanOp.workOnOOB(
        msg4); // shouldn't be considered as duplicate because of different sending operator.
    Arrays.stream(msg4.getBuffers()).forEach(ArrowBuf::close);
    assertEquals(3, scanOp.getRuntimeFilters().size());

    OutOfBandMessage msg5 =
        utils.newOOB(
            sendingMajorFragment2,
            sendingOp3,
            sendingMinorFragment1,
            m5PtCols,
            bloomFilterBuf,
            m2Vlf1,
            m2Vlf2);
    scanOp.workOnOOB(msg5);
    Arrays.stream(msg5.getBuffers()).forEach(ArrowBuf::close);
    assertEquals(4, scanOp.getRuntimeFilters().size());

    // With one less non partition col filter - m2vlf2
    OutOfBandMessage msg6 =
        utils.newOOB(
            sendingMajorFragment2,
            sendingOp3,
            sendingMinorFragment1,
            m2PtCols,
            bloomFilterBuf,
            m2Vlf1);
    scanOp.workOnOOB(msg6);
    Arrays.stream(msg6.getBuffers()).forEach(ArrowBuf::close);
    assertEquals(5, scanOp.getRuntimeFilters().size());

    verify(mockedReaderIterator, times(5)).addRuntimeFilter(addedFilter.capture());
    com.dremio.exec.store.RuntimeFilter filter1 = addedFilter.getAllValues().get(0);
    assertEquals(
        Lists.newArrayList("pCol1", "pCol2"), filter1.getPartitionColumnFilter().getColumnsList());
    assertEquals("BLOOM_FILTER", filter1.getPartitionColumnFilter().getFilterType().name());
    assertEquals(2, filter1.getNonPartitionColumnFilters().size());
    assertEquals(
        Lists.newArrayList("npCol1"),
        filter1.getNonPartitionColumnFilters().get(0).getColumnsList());
    assertEquals(
        Lists.newArrayList(1, 2, 3),
        utils.getValues(filter1.getNonPartitionColumnFilters().get(0).getValueList()));
    assertEquals(
        "VALUE_LIST", filter1.getNonPartitionColumnFilters().get(0).getFilterType().name());
    assertEquals(
        Lists.newArrayList("npCol2"),
        filter1.getNonPartitionColumnFilters().get(1).getColumnsList());
    assertEquals(
        Lists.newArrayList(11, 12, 13),
        utils.getValues(filter1.getNonPartitionColumnFilters().get(1).getValueList()));
    assertEquals(
        "VALUE_LIST", filter1.getNonPartitionColumnFilters().get(1).getFilterType().name());

    com.dremio.exec.store.RuntimeFilter filter2 = addedFilter.getAllValues().get(1);
    assertEquals(
        Lists.newArrayList("pCol3", "pCol4"), filter2.getPartitionColumnFilter().getColumnsList());
    assertEquals("BLOOM_FILTER", filter2.getPartitionColumnFilter().getFilterType().name());
    assertEquals(2, filter2.getNonPartitionColumnFilters().size());
    assertEquals(
        Lists.newArrayList("npCol3"),
        filter2.getNonPartitionColumnFilters().get(0).getColumnsList());
    assertEquals(
        "VALUE_LIST", filter2.getNonPartitionColumnFilters().get(0).getFilterType().name());
    ValueListFilter col1Filter = filter2.getNonPartitionColumnFilters().get(0).getValueList();
    assertFalse(col1Filter.isContainsNull());
    assertFalse(col1Filter.isContainsFalse());
    assertTrue(col1Filter.isContainsTrue());
    assertTrue(col1Filter.isBoolField());

    assertEquals(
        Lists.newArrayList("npCol4"),
        filter2.getNonPartitionColumnFilters().get(1).getColumnsList());
    assertEquals(
        "VALUE_LIST", filter2.getNonPartitionColumnFilters().get(1).getFilterType().name());
    ValueListFilter col2Filter = filter2.getNonPartitionColumnFilters().get(1).getValueList();
    assertTrue(col2Filter.isContainsNull());
    assertTrue(col2Filter.isContainsFalse());
    assertFalse(col2Filter.isContainsTrue());
    assertTrue(col2Filter.isBoolField());

    com.dremio.exec.store.RuntimeFilter filter3 = addedFilter.getAllValues().get(2);
    assertEquals(
        Lists.newArrayList("pCol3", "pCol4"), filter3.getPartitionColumnFilter().getColumnsList());
    List<String> f3NonPartitionCols =
        filter3.getNonPartitionColumnFilters().stream()
            .map(f -> f.getColumnsList().get(0))
            .collect(Collectors.toList());
    assertEquals(Lists.newArrayList("npCol3", "npCol4"), f3NonPartitionCols);

    com.dremio.exec.store.RuntimeFilter filter4 = addedFilter.getAllValues().get(3);
    assertEquals(
        Lists.newArrayList("pCol3", "pCol4", "pCol5"),
        filter4.getPartitionColumnFilter().getColumnsList());
    List<String> f4NonPartitionCols =
        filter4.getNonPartitionColumnFilters().stream()
            .map(f -> f.getColumnsList().get(0))
            .collect(Collectors.toList());
    assertEquals(Lists.newArrayList("npCol3", "npCol4"), f4NonPartitionCols);

    com.dremio.exec.store.RuntimeFilter filter5 = addedFilter.getAllValues().get(4);
    assertEquals(
        Lists.newArrayList("pCol3", "pCol4"), filter5.getPartitionColumnFilter().getColumnsList());
    List<String> f5NonPartitionCols =
        filter5.getNonPartitionColumnFilters().stream()
            .map(f -> f.getColumnsList().get(0))
            .collect(Collectors.toList());
    assertEquals(Lists.newArrayList("npCol3"), f5NonPartitionCols);

    AutoCloseables.close(scanOp.getRuntimeFilters());
  }

  @Test
  public void testWorkOnOOBRuntimeFilterInvalidFilterSize() {
    int buildMinorFragment1 = 2;
    int buildMajorFragment1 = 1;
    try (ArrowBuf oobMessageBuf = getTestAllocator().buffer(128)) {
      ExecProtos.RuntimeFilter filter1 = newRuntimeFilter(512, "col1", "col2"); // mismatched size
      OutOfBandMessage msg1 =
          newOOBMessage(filter1, oobMessageBuf, buildMajorFragment1, buildMinorFragment1);
      RecordReader mockReader = mock(RecordReader.class);
      ParquetScanTableFunction scanOp =
          new ParquetScanTableFunction(
              mock(FragmentExecutionContext.class),
              getMockContext(),
              getProps(),
              getTableFunctionConfig());
      scanOp.workOnOOB(msg1);
      Arrays.stream(msg1.getBuffers()).forEach(ArrowBuf::close);
      verify(mockReader, never()).addRuntimeFilter(any(com.dremio.exec.store.RuntimeFilter.class));
    }
  }

  protected RsRecord inputRow(String path, long offset, long length) throws Exception {
    long fileSize = fs.getFileAttributes(Path.of(path)).size();
    if (length == -1) {
      length = fileSize;
    }
    PartitionProtobuf.NormalizedPartitionInfo partitionInfo =
        PartitionProtobuf.NormalizedPartitionInfo.newBuilder().setId("0").build();
    return r(
        st(path, 0L, fileSize, fileSize),
        createSplitInformation(path, offset, length, fileSize, 0, partitionInfo),
        null);
  }

  private void validate(
      BatchSchema fileSchema, List<SchemaPath> columns, RecordSet input, RecordSet output)
      throws Exception {
    TableFunctionPOP pop = getPopForParquet(fileSchema, columns, ImmutableList.of());
    try (AutoCloseable closeable = with(ExecConstants.PARQUET_READER_VECTORIZE, false)) {
      validateSingle(
          pop,
          TableFunctionOperator.class,
          input,
          new RecordBatchValidatorDefaultImpl(output),
          BATCH_SIZE);
    }
  }

  private OperatorContext getMockContext() {
    OperatorContext context = mock(OperatorContext.class);
    OperatorStats stats = mock(OperatorStats.class);
    OptionManager manager = mock(OptionManager.class);
    when(context.getStats()).thenReturn(stats);
    doNothing().when(stats).startProcessing();
    doNothing().when(stats).addLongStat(eq(ScanOperator.Metric.NUM_READERS), eq(1));
    when(context.getOptions()).thenReturn(manager);
    when(manager.getOption(ExecConstants.ENABLE_ROW_LEVEL_RUNTIME_FILTERING)).thenReturn(false);
    return context;
  }

  private OutOfBandMessage newOOBMessage(
      ExecProtos.RuntimeFilter filter,
      ArrowBuf oobMessageBuf,
      int buildMajorFragment,
      int buildMinorFragment) {
    int probeScanId = 2;
    int probeOpId = 131074;
    int buildOpId = 65541;
    ArrowBuf[] bufs = new ArrowBuf[] {oobMessageBuf};
    List<Integer> bufferLengths = new ArrayList<>();
    bufferLengths.add((int) filter.getPartitionColumnFilter().getSizeBytes());
    filter
        .getNonPartitionColumnFilterList()
        .forEach(v -> bufferLengths.add((int) v.getSizeBytes()));
    return new OutOfBandMessage(
        UserBitShared.QueryId.newBuilder().build(),
        probeScanId,
        Lists.newArrayList(0, 3),
        probeOpId,
        buildMajorFragment,
        buildMinorFragment,
        buildOpId,
        new OutOfBandMessage.Payload(filter),
        bufs,
        bufferLengths,
        false);
  }

  private OpProps getProps() {
    OpProps props = mock(OpProps.class);
    when(props.getOperatorId()).thenReturn(123);
    return props;
  }

  private TableFunctionConfig getTableFunctionConfig() {
    TableFunctionContext functionContext = mock(TableFunctionContext.class);
    BatchSchema fullSchema = new BatchSchema(Collections.EMPTY_LIST);
    when(functionContext.getFullSchema()).thenReturn(fullSchema);
    when(functionContext.getColumns()).thenReturn(Collections.EMPTY_LIST);

    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.DATA_FILE_SCAN, false, functionContext);
  }

  private ExecProtos.RuntimeFilter newRuntimeFilter(int sizeBytes, String... cols) {
    int probeScanId = 2;
    int probeOpId = 131074;
    return ExecProtos.RuntimeFilter.newBuilder()
        .setProbeScanMajorFragmentId(probeScanId)
        .setProbeScanOperatorId(probeOpId)
        .setPartitionColumnFilter(
            ExecProtos.CompositeColumnFilter.newBuilder()
                .addAllColumns(Lists.newArrayList(cols))
                .setFilterType(ExecProtos.RuntimeFilterType.BLOOM_FILTER)
                .setSizeBytes(sizeBytes)
                .build())
        .build();
  }
}

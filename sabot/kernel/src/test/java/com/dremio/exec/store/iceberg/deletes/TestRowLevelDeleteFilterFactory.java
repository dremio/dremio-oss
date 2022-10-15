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
package com.dremio.exec.store.iceberg.deletes;

import static com.dremio.sabot.op.scan.ScanOperator.Metric.PARQUET_BYTES_READ;
import static com.dremio.sabot.op.tablefunction.TableFunctionOperator.Metric.NUM_DELETE_FILE_READERS;
import static com.dremio.sabot.op.tablefunction.TableFunctionOperator.Metric.PARQUET_DELETE_FILE_BYTES_READ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SimpleIntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.FileContent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.dremio.exec.store.TestOutputMutator;
import com.dremio.exec.store.iceberg.deletes.RowLevelDeleteFilterFactory.DataFileInfo;
import com.dremio.exec.store.iceberg.deletes.RowLevelDeleteFilterFactory.DeleteFileInfo;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestRowLevelDeleteFilterFactory extends BaseTestOperator {

  private static final int DEFAULT_BATCH_SIZE = 3;
  private static final Path POS_DELETE_FILE_1 = Path.of("pos-delete1");
  private static final Path POS_DELETE_FILE_2 = Path.of("pos-delete2");
  private static final Path POS_DELETE_FILE_3 = Path.of("pos-delete3");
  private static final Path EQ_DELETE_FILE_1 = Path.of("eq-delete1");
  private static final Path EQ_DELETE_FILE_2 = Path.of("eq-delete2");
  private static final Path EQ_DELETE_FILE_3 = Path.of("eq-delete3");
  private static final DeleteFileInfo POS_DELETE_FILE_INFO_1 = new DeleteFileInfo(POS_DELETE_FILE_1.toString(),
      FileContent.POSITION_DELETES, 10, null);
  private static final DeleteFileInfo POS_DELETE_FILE_INFO_2 = new DeleteFileInfo(POS_DELETE_FILE_2.toString(),
      FileContent.POSITION_DELETES, 20, null);
  private static final DeleteFileInfo POS_DELETE_FILE_INFO_3 = new DeleteFileInfo(POS_DELETE_FILE_3.toString(),
      FileContent.POSITION_DELETES, 30, null);
  private static final DeleteFileInfo EQ_DELETE_FILE_INFO_1 = new DeleteFileInfo(EQ_DELETE_FILE_1.toString(),
      FileContent.EQUALITY_DELETES, 10, ImmutableList.of(1));
  private static final DeleteFileInfo EQ_DELETE_FILE_INFO_2 = new DeleteFileInfo(EQ_DELETE_FILE_2.toString(),
      FileContent.EQUALITY_DELETES, 20, ImmutableList.of(1));
  private static final DeleteFileInfo EQ_DELETE_FILE_INFO_3 = new DeleteFileInfo(EQ_DELETE_FILE_3.toString(),
      FileContent.EQUALITY_DELETES, 30, ImmutableList.of(1));
  private static final String DATA_FILE_1 = "file1";
  private static final String DATA_FILE_2 = "file2";
  private static final String DATA_FILE_3 = "file3";
  private static final List<Long> POSITIONS_1 = ImmutableList.of(1L, 2L, 3L);
  private static final List<Long> POSITIONS_2 = ImmutableList.of(4L, 5L, 6L);
  private static final List<Long> POSITIONS_3 = ImmutableList.of(7L, 8L, 9L);
  private static final List<IcebergProtobuf.IcebergSchemaField> ICEBERG_FIELDS = ImmutableList.of(
      IcebergProtobuf.IcebergSchemaField.newBuilder().setSchemaPath("col1").setId(1).build());

  private OperatorContextImpl context;
  private SimpleIntVector deltas;
  private TestOutputMutator mutator;
  private ArrowBuf validityBuf;
  @Mock
  private RowLevelDeleteFileReaderFactory readerFactory;
  @Mock
  private PositionalDeleteFileReader posReader1;
  @Mock
  private PositionalDeleteFileReader posReader2;
  @Mock
  private PositionalDeleteFileReader posReader3;
  @Mock
  private EqualityDeleteFileReader eqReader1;
  @Mock
  private EqualityDeleteFileReader eqReader2;
  @Mock
  private EqualityDeleteFileReader eqReader3;
  @Mock
  private EqualityDeleteHashTable eqTable1;
  @Mock
  private EqualityDeleteHashTable eqTable2;
  @Mock
  private EqualityDeleteHashTable eqTable3;

  @Before
  public void beforeTest() throws Exception {
    context = testContext.getNewOperatorContext(getTestAllocator(), null, DEFAULT_BATCH_SIZE, null);
    testCloseables.add(context);
    deltas = new SimpleIntVector("pos", getAllocator());
    testCloseables.add(deltas);
    deltas.allocateNew(DEFAULT_BATCH_SIZE);
    mutator = new TestOutputMutator(getTestAllocator());
    testCloseables.add(mutator);
    mutator.addField(Field.nullable("col1", Types.MinorType.INT.getType()), IntVector.class);
    validityBuf = getTestAllocator().buffer(1);
    testCloseables.add(validityBuf);

    when(readerFactory.createPositionalDeleteFileReader(any(), eq(POS_DELETE_FILE_1), anyList())).thenAnswer(i -> {
      i.getArgument(0, OperatorContext.class).getStats().addLongStat(PARQUET_BYTES_READ, 30);
      return posReader1;
    });
    when(readerFactory.createPositionalDeleteFileReader(any(), eq(POS_DELETE_FILE_2), anyList())).thenAnswer(i -> {
      i.getArgument(0, OperatorContext.class).getStats().addLongStat(PARQUET_BYTES_READ, 40);
      return posReader2;
    });
    when(readerFactory.createPositionalDeleteFileReader(any(), eq(POS_DELETE_FILE_3), anyList())).thenAnswer(i -> {
      i.getArgument(0, OperatorContext.class).getStats().addLongStat(PARQUET_BYTES_READ, 50);
      return posReader3;
    });
    when(posReader1.createIteratorForDataFile(any())).thenAnswer(i -> createIteratorFromList(POSITIONS_1, posReader1));
    when(posReader2.createIteratorForDataFile(any())).thenAnswer(i -> createIteratorFromList(POSITIONS_2, posReader2));
    when(posReader3.createIteratorForDataFile(any())).thenAnswer(i -> createIteratorFromList(POSITIONS_3, posReader3));

    when(readerFactory.createEqualityDeleteFileReader(any(), eq(EQ_DELETE_FILE_1), anyLong(), anyList(), anyList()))
        .thenAnswer(i -> {
          i.getArgument(0, OperatorContext.class).getStats().addLongStat(PARQUET_BYTES_READ, 30);
          return eqReader1;
        });
    when(readerFactory.createEqualityDeleteFileReader(any(), eq(EQ_DELETE_FILE_2), anyLong(), anyList(), anyList()))
        .thenAnswer(i -> {
          i.getArgument(0, OperatorContext.class).getStats().addLongStat(PARQUET_BYTES_READ, 40);
          return eqReader2;
        });
    when(readerFactory.createEqualityDeleteFileReader(any(), eq(EQ_DELETE_FILE_3), anyLong(), anyList(), anyList()))
        .thenAnswer(i -> {
          i.getArgument(0, OperatorContext.class).getStats().addLongStat(PARQUET_BYTES_READ, 50);
          return eqReader3;
        });

    when(eqReader1.buildHashTable()).thenReturn(eqTable1);
    when(eqReader2.buildHashTable()).thenReturn(eqTable2);
    when(eqReader3.buildHashTable()).thenReturn(eqTable3);
  }

  @Test
  public void testUnknownDataFileReturnsNull() {
    RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory);
    assertThat(factory.createPositionalDeleteFilter(DATA_FILE_1)).isNull();
  }

  @Test
  public void testDataFileWithNoDeleteFilesReturnsNull() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1, ImmutableList.of(), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);

      assertThat(factory.createPositionalDeleteFilter(DATA_FILE_1)).isNull();
      assertThat(factory.createEqualityDeleteFilter(DATA_FILE_1, ICEBERG_FIELDS)).isNull();
    }
  }

  @Test
  public void testCreatePositionalDeleteFilterWithSingleFile() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1, ImmutableList.of(POS_DELETE_FILE_INFO_1), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);
      PositionalDeleteFilter filter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      filter.seek(0);

      // filter rows [ 1 .. 3 ]
      verifyFilter(filter, ImmutableList.of(0, 3, 0));
      filter.release();
    }

    assertThat(context.getStats().getLongStat(NUM_DELETE_FILE_READERS)).isEqualTo(1);
    assertThat(context.getStats().getLongStat(PARQUET_DELETE_FILE_BYTES_READ)).isEqualTo(30);
  }

  @Test
  public void testCreatePositionalDeleteFilterWithMultipleFiles() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_2), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);
      PositionalDeleteFilter filter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      filter.seek(0);

      // filter rows [ 1 .. 6 ]
      verifyFilter(filter, ImmutableList.of(0, 6, 0));
    }

    assertThat(context.getStats().getLongStat(NUM_DELETE_FILE_READERS)).isEqualTo(2);
    assertThat(context.getStats().getLongStat(PARQUET_DELETE_FILE_BYTES_READ)).isEqualTo(70);
  }

  @Test
  public void testCreatePositionalDeleteFilterForMultipleRowGroups() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_2), 3));
      factory.setDataFileInfoForBatch(dataFileInfo);

      // first "row group", grab rows 0..2
      PositionalDeleteFilter filter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      filter.seek(0);
      verifyFilter(filter, 3, ImmutableList.of(0));
      filter.release();

      // second "row group", grab rows 3..5
      filter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      filter.seek(3);
      verifyFilter(filter, 6, ImmutableList.of());
      filter.release();

      // third "row group", grab rows 6..8
      filter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      filter.seek(6);
      verifyFilter(filter, 9, ImmutableList.of(1, 0));
      filter.release();

      assertThat(filter.refCount()).isEqualTo(0);
    }

    assertThat(context.getStats().getLongStat(NUM_DELETE_FILE_READERS)).isEqualTo(2);
    assertThat(context.getStats().getLongStat(PARQUET_DELETE_FILE_BYTES_READ)).isEqualTo(70);
  }

  @Test
  public void testIncrementRowGroupCount() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_2, EQ_DELETE_FILE_INFO_1), 2));
      factory.setDataFileInfoForBatch(dataFileInfo);

      // first "row group"
      PositionalDeleteFilter posFilter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      EqualityDeleteFilter eqFilter = factory.createEqualityDeleteFilter(DATA_FILE_1, ICEBERG_FIELDS);
      posFilter.release();
      eqFilter.release();

      // increment the row group count to add another row group
      assertThat(posFilter.refCount()).isEqualTo(1);
      assertThat(eqFilter.refCount()).isEqualTo(1);
      factory.adjustRowGroupCount(DATA_FILE_1, 1);
      assertThat(posFilter.refCount()).isEqualTo(2);
      assertThat(eqFilter.refCount()).isEqualTo(2);

      // second "row group"
      posFilter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      eqFilter = factory.createEqualityDeleteFilter(DATA_FILE_1, ICEBERG_FIELDS);
      posFilter.release();
      eqFilter.release();

      // third "row group"
      posFilter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      eqFilter = factory.createEqualityDeleteFilter(DATA_FILE_1, ICEBERG_FIELDS);
      posFilter.release();
      eqFilter.release();

      assertThat(posFilter.refCount()).isEqualTo(0);
      assertThat(eqFilter.refCount()).isEqualTo(0);
    }
  }

  @Test
  public void testDecrementRowGroupCountToZero() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = new HashMap<>(ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_2), 1),
          DATA_FILE_2, new DataFileInfo(DATA_FILE_2,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_2), 1),
          DATA_FILE_3, new DataFileInfo(DATA_FILE_3,
              ImmutableList.of(EQ_DELETE_FILE_INFO_1, EQ_DELETE_FILE_INFO_2), 2)));
      factory.setDataFileInfoForBatch(dataFileInfo);

      PositionalDeleteFilter posFilter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      posFilter.seek(0);
      posFilter.release();
      verify(posReader1, times(1)).release();
      verify(posReader2, times(1)).release();

      // test decrement of row group count on data file with single row group which has shared pos delete files
      // with previous data file
      factory.adjustRowGroupCount(DATA_FILE_2, -1);
      verify(posReader1, times(2)).release();
      verify(posReader2, times(2)).release();

      // test decrement of row group count on data file with multiple row groups and an active eq delete filter
      // create filter and do the release for 1st row group
      EqualityDeleteFilter eqFilter = factory.createEqualityDeleteFilter(DATA_FILE_3, ICEBERG_FIELDS);
      eqFilter.release();
      // decrement row group count to 0
      factory.adjustRowGroupCount(DATA_FILE_3, -1);
      assertThat(eqFilter.refCount()).isEqualTo(0);
    }
  }

  @Test
  public void testCreatePositionalDeleteFilterForNonAdjacentRowGroups() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_2), 2));
      factory.setDataFileInfoForBatch(dataFileInfo);

      // first "row group", grab rows 0..2
      PositionalDeleteFilter filter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      filter.seek(0);
      verifyFilter(filter, 3, ImmutableList.of(0));
      filter.release();

      // skip rows 3..5, third "row group", grab rows 6..8
      filter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      filter.seek(6);
      verifyFilter(filter, 9, ImmutableList.of(1, 0));
      filter.release();

      assertThat(filter.refCount()).isEqualTo(0);
    }
  }

  @Test
  public void testCreatePositionalDeleteFilterForMultipleDataFiles() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_2), 1),
          DATA_FILE_2, new DataFileInfo(DATA_FILE_2, ImmutableList.of(POS_DELETE_FILE_INFO_2), 1),
          DATA_FILE_3, new DataFileInfo(DATA_FILE_3, ImmutableList.of(POS_DELETE_FILE_INFO_3), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);

      PositionalDeleteFilter filter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      filter.seek(0);
      // filter rows [ 1 .. 6 ]
      verifyFilter(filter, ImmutableList.of(0, 6, 0));
      filter.release();

      filter = factory.createPositionalDeleteFilter(DATA_FILE_2);
      filter.seek(0);
      // filter rows [ 4 .. 6 ]
      verifyFilter(filter, ImmutableList.of(0, 0, 0));
      verifyFilter(filter, ImmutableList.of(0, 3, 0));
      filter.release();

      filter = factory.createPositionalDeleteFilter(DATA_FILE_3);
      filter.seek(0);
      // filter rows [ 7 .. 9 ]
      verifyFilter(filter, ImmutableList.of(0, 0, 0));
      verifyFilter(filter, ImmutableList.of(0, 0, 0));
      verifyFilter(filter, ImmutableList.of(0, 3, 0));
      filter.release();
    }

    assertThat(context.getStats().getLongStat(NUM_DELETE_FILE_READERS)).isEqualTo(3);
    assertThat(context.getStats().getLongStat(PARQUET_DELETE_FILE_BYTES_READ)).isEqualTo(120);
  }

  @Test
  public void testCreatePositionalDeleteFilterInMultipleBatches() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_2), 1),
          DATA_FILE_2, new DataFileInfo(DATA_FILE_2, ImmutableList.of(POS_DELETE_FILE_INFO_2), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);

      PositionalDeleteFilter filter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      filter.seek(0);
      // filter rows [ 1 .. 6 ]
      verifyFilter(filter, ImmutableList.of(0, 6, 0));
      filter.release();

      filter = factory.createPositionalDeleteFilter(DATA_FILE_2);
      filter.seek(0);
      // filter rows [ 4 .. 6 ]
      verifyFilter(filter, ImmutableList.of(0, 0, 0));
      verifyFilter(filter, ImmutableList.of(0, 3, 0));
      filter.release();

      dataFileInfo = ImmutableMap.of(
          DATA_FILE_3, new DataFileInfo(DATA_FILE_3, ImmutableList.of(POS_DELETE_FILE_INFO_3), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);

      filter = factory.createPositionalDeleteFilter(DATA_FILE_3);
      filter.seek(0);
      // filter rows [ 7 .. 9 ]
      verifyFilter(filter, ImmutableList.of(0, 0, 0));
      verifyFilter(filter, ImmutableList.of(0, 0, 0));
      verifyFilter(filter, ImmutableList.of(0, 3, 0));
      filter.release();
    }

    assertThat(context.getStats().getLongStat(NUM_DELETE_FILE_READERS)).isEqualTo(3);
    assertThat(context.getStats().getLongStat(PARQUET_DELETE_FILE_BYTES_READ)).isEqualTo(120);
  }

  @Test
  public void testPositionalDeleteFileReadersCreatedOnce() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_2), 1),
          DATA_FILE_2, new DataFileInfo(DATA_FILE_2, ImmutableList.of(POS_DELETE_FILE_INFO_2), 1),
          DATA_FILE_3, new DataFileInfo(DATA_FILE_3,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_3), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);

      PositionalDeleteFilter filter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      filter.release();

      filter = factory.createPositionalDeleteFilter(DATA_FILE_2);
      filter.release();

      filter = factory.createPositionalDeleteFilter(DATA_FILE_3);
      filter.release();

      verify(readerFactory, times(1)).createPositionalDeleteFileReader(any(), eq(POS_DELETE_FILE_1), anyList());
      verify(readerFactory, times(1)).createPositionalDeleteFileReader(any(), eq(POS_DELETE_FILE_2), anyList());
      verify(readerFactory, times(1)).createPositionalDeleteFileReader(any(), eq(POS_DELETE_FILE_3), anyList());
    }
  }

  @Test
  public void testFilterFromPreviousBatchCanBeUsedAfterNewBatchAdded() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_2, EQ_DELETE_FILE_INFO_1), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);

      // filter is created and assigned to a prefetched reader but not advanced
      PositionalDeleteFilter posFilter = factory.createPositionalDeleteFilter(DATA_FILE_1);
      EqualityDeleteFilter eqFilter = factory.createEqualityDeleteFilter(DATA_FILE_1, ICEBERG_FIELDS);

      // start a new batch
      dataFileInfo = ImmutableMap.of(
          DATA_FILE_2, new DataFileInfo(DATA_FILE_2, ImmutableList.of(POS_DELETE_FILE_INFO_2), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);

      // now advance previously created filter from old batch
      posFilter.seek(0);
      // filter rows [ 1 .. 6 ]
      verifyFilter(posFilter, ImmutableList.of(0, 6, 0));
      posFilter.release();

      // verify that equality filter is still alive
      assertThat(eqFilter.refCount()).isEqualTo(1);
      eqFilter.release();
    }
  }

  @Test
  public void testPositionalDeleteFiltersCanBeAdvancedLazily() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(POS_DELETE_FILE_INFO_1, POS_DELETE_FILE_INFO_2), 2),
          DATA_FILE_2, new DataFileInfo(DATA_FILE_2, ImmutableList.of(POS_DELETE_FILE_INFO_2), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);

      // create 3 filters - 2 row groups for DATA_FILE_1, 1 row groups for DATA_FILE_2
      PositionalDeleteFilter filter1 = factory.createPositionalDeleteFilter(DATA_FILE_1);
      PositionalDeleteFilter filter2 = factory.createPositionalDeleteFilter(DATA_FILE_1);
      PositionalDeleteFilter filter3 = factory.createPositionalDeleteFilter(DATA_FILE_2);

      filter1.seek(0);
      // fetch 1st 3 rows - filter rows [ 1 .. 2 ]
      verifyFilter(filter1, 3, ImmutableList.of(0));
      filter1.release();

      filter2.seek(3);
      // fetch next 6 rows - filter rows [ 3 .. 6 ]
      verifyFilter(filter2, 9, ImmutableList.of(4, 0));
      filter2.release();

      filter3.seek(0);
      // filter rows [ 4 .. 6 ]
      verifyFilter(filter3, ImmutableList.of(0, 0, 0));
      verifyFilter(filter3, ImmutableList.of(0, 3, 0));
      filter3.release();
    }
  }

  @Test
  public void testCreateEqualityDeleteFilterWithSingleFile() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1, ImmutableList.of(EQ_DELETE_FILE_INFO_1), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);
      EqualityDeleteFilter filter = factory.createEqualityDeleteFilter(DATA_FILE_1, ICEBERG_FIELDS);
      filter.setup(mutator, validityBuf);
      filter.release();

      verify(eqReader1, times(1)).buildHashTable();
      verify(eqReader1, times(1)).close();
      verify(eqTable1, times(1)).close();
    }

    assertThat(context.getStats().getLongStat(NUM_DELETE_FILE_READERS)).isEqualTo(1);
    assertThat(context.getStats().getLongStat(PARQUET_DELETE_FILE_BYTES_READ)).isEqualTo(30);
  }

  @Test
  public void testCreateEqualityDeleteFilterWithMultipleFiles() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(EQ_DELETE_FILE_INFO_1, EQ_DELETE_FILE_INFO_2), 1));
      factory.setDataFileInfoForBatch(dataFileInfo);
      EqualityDeleteFilter filter = factory.createEqualityDeleteFilter(DATA_FILE_1, ICEBERG_FIELDS);
      filter.setup(mutator, validityBuf);
      filter.release();

      verify(eqReader1, times(1)).buildHashTable();
      verify(eqReader2, times(1)).buildHashTable();
      verify(eqReader1, times(1)).close();
      verify(eqReader2, times(1)).close();
      verify(eqTable1, times(1)).close();
      verify(eqTable2, times(1)).close();
    }

    assertThat(context.getStats().getLongStat(NUM_DELETE_FILE_READERS)).isEqualTo(2);
    assertThat(context.getStats().getLongStat(PARQUET_DELETE_FILE_BYTES_READ)).isEqualTo(70);
  }

  @Test
  public void testCreateEqualityDeleteFilterForMultipleRowGroups() throws Exception {
    try (RowLevelDeleteFilterFactory factory = new RowLevelDeleteFilterFactory(context, readerFactory)) {
      Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
          DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
              ImmutableList.of(EQ_DELETE_FILE_INFO_1, EQ_DELETE_FILE_INFO_2), 3));
      factory.setDataFileInfoForBatch(dataFileInfo);

      // first "row group"
      EqualityDeleteFilter filter = factory.createEqualityDeleteFilter(DATA_FILE_1, ICEBERG_FIELDS);
      filter.setup(mutator, validityBuf);
      filter.release();

      // second "row group"
      filter = factory.createEqualityDeleteFilter(DATA_FILE_1, ICEBERG_FIELDS);
      filter.setup(mutator, validityBuf);
      filter.release();

      // third "row group"
      filter = factory.createEqualityDeleteFilter(DATA_FILE_1, ICEBERG_FIELDS);
      filter.setup(mutator, validityBuf);
      filter.release();

      assertThat(filter.refCount()).isEqualTo(0);
      verify(eqReader1, times(1)).buildHashTable();
      verify(eqReader2, times(1)).buildHashTable();
      verify(eqReader1, times(1)).close();
      verify(eqReader2, times(1)).close();
      verify(eqTable1, times(1)).close();
      verify(eqTable2, times(1)).close();
    }

    assertThat(context.getStats().getLongStat(NUM_DELETE_FILE_READERS)).isEqualTo(2);
    assertThat(context.getStats().getLongStat(PARQUET_DELETE_FILE_BYTES_READ)).isEqualTo(70);
  }

  private void verifyFilter(PositionalDeleteFilter filter, List<Integer> expected) {
    verifyFilter(filter, Integer.MAX_VALUE, expected);
  }

  private void verifyFilter(PositionalDeleteFilter filter, long endRowPos, List<Integer> expected) {
    filter.applyToDeltas(endRowPos, DEFAULT_BATCH_SIZE, deltas);

    List<Integer> actual = new ArrayList<>();
    for (int i = 0; i < deltas.getValueCount(); i++) {
      actual.add(deltas.get(i));
    }

    assertThat(actual).isEqualTo(expected);
  }

  private static PositionalDeleteIterator createIteratorFromList(List<Long> list, PositionalDeleteFileReader reader) {
    Iterator<Long> iterator = list.iterator();
    return new PositionalDeleteIterator() {
      @Override
      public void close() throws Exception {
        reader.release();
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Long next() {
        return iterator.next();
      }
    };
  }
}

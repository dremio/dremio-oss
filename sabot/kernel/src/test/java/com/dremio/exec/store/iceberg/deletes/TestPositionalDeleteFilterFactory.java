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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.SimpleIntVector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.dremio.exec.store.iceberg.deletes.PositionalDeleteFilterFactory.DataFileInfo;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestPositionalDeleteFilterFactory extends BaseTestOperator {

  private static final int DEFAULT_BATCH_SIZE = 3;
  private static final Path DELETE_FILE_1 = Path.of("delete1");
  private static final Path DELETE_FILE_2 = Path.of("delete2");
  private static final Path DELETE_FILE_3 = Path.of("delete3");
  private static final String DATA_FILE_1 = "file1";
  private static final String DATA_FILE_2 = "file2";
  private static final String DATA_FILE_3 = "file3";
  private static final List<Long> POSITIONS_1 = ImmutableList.of(1L, 2L, 3L);
  private static final List<Long> POSITIONS_2 = ImmutableList.of(4L, 5L, 6L);
  private static final List<Long> POSITIONS_3 = ImmutableList.of(7L, 8L, 9L);

  private OperatorContextImpl context;
  private SimpleIntVector deltas;
  @Mock
  private PositionalDeleteFileReaderFactory readerFactory;
  @Mock
  private PositionalDeleteFileReader reader1;
  @Mock
  private PositionalDeleteFileReader reader2;
  @Mock
  private PositionalDeleteFileReader reader3;

  @Before
  public void beforeTest() throws Exception {
    context = testContext.getNewOperatorContext(getTestAllocator(), null, DEFAULT_BATCH_SIZE, null);
    testCloseables.add(context);
    deltas = new SimpleIntVector("pos", getAllocator());
    testCloseables.add(deltas);
    deltas.allocateNew(DEFAULT_BATCH_SIZE);

    when(readerFactory.create(eq(context), eq(DELETE_FILE_1), anyList())).thenReturn(reader1);
    when(readerFactory.create(eq(context), eq(DELETE_FILE_2), anyList())).thenReturn(reader2);
    when(readerFactory.create(eq(context), eq(DELETE_FILE_3), anyList())).thenReturn(reader3);
    when(reader1.createIteratorForDataFile(any())).thenAnswer(i -> createIteratorFromList(POSITIONS_1, reader1));
    when(reader2.createIteratorForDataFile(any())).thenAnswer(i -> createIteratorFromList(POSITIONS_2, reader2));
    when(reader3.createIteratorForDataFile(any())).thenAnswer(i -> createIteratorFromList(POSITIONS_3, reader3));
  }

  @Test
  public void testUnknownDataFileReturnsNull() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    assertThat(factory.create(DATA_FILE_1)).isNull();
  }

  @Test
  public void testDataFileWithNoDeleteFilesReturnsNull() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
      DATA_FILE_1, new DataFileInfo(DATA_FILE_1, ImmutableList.of(), 1));
    factory.setDataFileInfoForBatch(dataFileInfo);

    assertThat(factory.create(DATA_FILE_1)).isNull();
  }

  @Test
  public void testCreateFilterWithSingleDeleteFile() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
        DATA_FILE_1, new DataFileInfo(DATA_FILE_1, ImmutableList.of(DELETE_FILE_1.toString()), 1));
    factory.setDataFileInfoForBatch(dataFileInfo);
    PositionalDeleteFilter filter = factory.create(DATA_FILE_1);
    filter.seek(0);

    // filter rows [ 1 .. 3 ]
    verifyFilter(filter, ImmutableList.of(0, 3, 0));
  }

  @Test
  public void testCreateFilterWithMultipleDeleteFile() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
        DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_2.toString()), 1));
    factory.setDataFileInfoForBatch(dataFileInfo);
    PositionalDeleteFilter filter = factory.create(DATA_FILE_1);
    filter.seek(0);

    // filter rows [ 1 .. 6 ]
    verifyFilter(filter, ImmutableList.of(0, 6, 0));
  }

  @Test
  public void testCreateFilterForMultipleRowGroups() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
        DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_2.toString()), 3));
    factory.setDataFileInfoForBatch(dataFileInfo);

    // first "row group", grab rows 0..2
    PositionalDeleteFilter filter = factory.create(DATA_FILE_1);
    filter.seek(0);
    verifyFilter(filter, 3, ImmutableList.of(0));
    filter.release();

    // second "row group", grab rows 3..5
    filter = factory.create(DATA_FILE_1);
    filter.seek(3);
    verifyFilter(filter, 6, ImmutableList.of());
    filter.release();

    // third "row group", grab rows 6..8
    filter = factory.create(DATA_FILE_1);
    filter.seek(6);
    verifyFilter(filter, 9, ImmutableList.of(1, 0));
    filter.release();

    assertThat(filter.refCount()).isEqualTo(0);
  }

  @Test
  public void testIncrementRowGroupCount() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
        DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_2.toString()), 2));
    factory.setDataFileInfoForBatch(dataFileInfo);

    // first "row group", grab rows 0..2
    PositionalDeleteFilter filter = factory.create(DATA_FILE_1);
    filter.seek(0);
    verifyFilter(filter, 3, ImmutableList.of(0));
    filter.release();

    // increment the row group count to add another row group
    assertThat(filter.refCount()).isEqualTo(1);
    factory.adjustRowGroupCount(DATA_FILE_1, 1);
    assertThat(filter.refCount()).isEqualTo(2);

    // second "row group", grab rows 3..5
    filter = factory.create(DATA_FILE_1);
    filter.seek(3);
    verifyFilter(filter, 6, ImmutableList.of());
    filter.release();

    // third "row group", grab rows 6..8
    filter = factory.create(DATA_FILE_1);
    filter.seek(6);
    verifyFilter(filter, 9, ImmutableList.of(1, 0));
    filter.release();

    assertThat(filter.refCount()).isEqualTo(0);
  }

  @Test
  public void testDecrementRowGroupCountToZero()  {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = new HashMap<>(ImmutableMap.of(
        DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_2.toString()), 1),
        DATA_FILE_2, new DataFileInfo(DATA_FILE_2,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_2.toString()), 1)));
    factory.setDataFileInfoForBatch(dataFileInfo);

    PositionalDeleteFilter filter = factory.create(DATA_FILE_1);
    filter.seek(0);
    filter.release();
    verify(reader1, times(1)).release();
    verify(reader2, times(1)).release();

    factory.adjustRowGroupCount(DATA_FILE_2, -1);
    verify(reader1, times(2)).release();
    verify(reader2, times(2)).release();
  }

  @Test
  public void testCreateFilterForNonAdjacentRowGroups() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
        DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_2.toString()), 2));
    factory.setDataFileInfoForBatch(dataFileInfo);

    // first "row group", grab rows 0..2
    PositionalDeleteFilter filter = factory.create(DATA_FILE_1);
    filter.seek(0);
    verifyFilter(filter, 3, ImmutableList.of(0));
    filter.release();

    // skip rows 3..5, third "row group", grab rows 6..8
    filter = factory.create(DATA_FILE_1);
    filter.seek(6);
    verifyFilter(filter, 9, ImmutableList.of(1, 0));
    filter.release();

    assertThat(filter.refCount()).isEqualTo(0);
  }

  @Test
  public void testCreateFilterForMultipleDataFiles() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
        DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_2.toString()), 1),
        DATA_FILE_2, new DataFileInfo(DATA_FILE_2, ImmutableList.of(DELETE_FILE_2.toString()), 1),
        DATA_FILE_3, new DataFileInfo(DATA_FILE_3, ImmutableList.of(DELETE_FILE_3.toString()), 1));
    factory.setDataFileInfoForBatch(dataFileInfo);

    PositionalDeleteFilter filter = factory.create(DATA_FILE_1);
    filter.seek(0);
    // filter rows [ 1 .. 6 ]
    verifyFilter(filter, ImmutableList.of(0, 6, 0));
    filter.release();

    filter = factory.create(DATA_FILE_2);
    filter.seek(0);
    // filter rows [ 4 .. 6 ]
    verifyFilter(filter, ImmutableList.of(0, 0, 0));
    verifyFilter(filter, ImmutableList.of(0, 3, 0));
    filter.release();

    filter = factory.create(DATA_FILE_3);
    filter.seek(0);
    // filter rows [ 7 .. 9 ]
    verifyFilter(filter, ImmutableList.of(0, 0, 0));
    verifyFilter(filter, ImmutableList.of(0, 0, 0));
    verifyFilter(filter, ImmutableList.of(0, 3, 0));
    filter.release();
  }

  @Test
  public void testCreateFilterInMultipleBatches() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
        DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_2.toString()), 1),
        DATA_FILE_2, new DataFileInfo(DATA_FILE_2, ImmutableList.of(DELETE_FILE_2.toString()), 1));
    factory.setDataFileInfoForBatch(dataFileInfo);

    PositionalDeleteFilter filter = factory.create(DATA_FILE_1);
    filter.seek(0);
    // filter rows [ 1 .. 6 ]
    verifyFilter(filter, ImmutableList.of(0, 6, 0));
    filter.release();

    filter = factory.create(DATA_FILE_2);
    filter.seek(0);
    // filter rows [ 4 .. 6 ]
    verifyFilter(filter, ImmutableList.of(0, 0, 0));
    verifyFilter(filter, ImmutableList.of(0, 3, 0));
    filter.release();

    dataFileInfo = ImmutableMap.of(
        DATA_FILE_3, new DataFileInfo(DATA_FILE_3, ImmutableList.of(DELETE_FILE_3.toString()), 1));
    factory.setDataFileInfoForBatch(dataFileInfo);

    filter = factory.create(DATA_FILE_3);
    filter.seek(0);
    // filter rows [ 7 .. 9 ]
    verifyFilter(filter, ImmutableList.of(0, 0, 0));
    verifyFilter(filter, ImmutableList.of(0, 0, 0));
    verifyFilter(filter, ImmutableList.of(0, 3, 0));
    filter.release();
  }


  @Test
  public void testDeleteFileReadersCreatedOnce() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
        DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_2.toString()), 1),
        DATA_FILE_2, new DataFileInfo(DATA_FILE_2, ImmutableList.of(DELETE_FILE_2.toString()), 1),
        DATA_FILE_3, new DataFileInfo(DATA_FILE_3,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_3.toString()), 1));
    factory.setDataFileInfoForBatch(dataFileInfo);

    PositionalDeleteFilter filter = factory.create(DATA_FILE_1);
    filter.release();

    filter = factory.create(DATA_FILE_2);
    filter.release();

    filter = factory.create(DATA_FILE_3);
    filter.release();

    verify(readerFactory, times(1)).create(eq(context), eq(DELETE_FILE_1), anyList());
    verify(readerFactory, times(1)).create(eq(context), eq(DELETE_FILE_2), anyList());
    verify(readerFactory, times(1)).create(eq(context), eq(DELETE_FILE_3), anyList());
  }

  @Test
  public void testFilterFromPreviousBatchCanBeUsedAfterNewBatchAdded() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
        DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_2.toString()), 1));
    factory.setDataFileInfoForBatch(dataFileInfo);

    // filter is created and assigned to a prefetched reader but not advanced
    PositionalDeleteFilter filter = factory.create(DATA_FILE_1);

    // start a new batch
    dataFileInfo = ImmutableMap.of(
        DATA_FILE_2, new DataFileInfo(DATA_FILE_2, ImmutableList.of(DELETE_FILE_2.toString()), 1));
    factory.setDataFileInfoForBatch(dataFileInfo);

    // now advance previously created filter from old batch
    filter.seek(0);
    // filter rows [ 1 .. 6 ]
    verifyFilter(filter, ImmutableList.of(0, 6, 0));
    filter.release();
  }

  @Test
  public void testFiltersCanBeAdvancedLazily() {
    PositionalDeleteFilterFactory factory = new PositionalDeleteFilterFactory(context, readerFactory);
    Map<String, DataFileInfo> dataFileInfo = ImmutableMap.of(
        DATA_FILE_1, new DataFileInfo(DATA_FILE_1,
            ImmutableList.of(DELETE_FILE_1.toString(), DELETE_FILE_2.toString()), 2),
        DATA_FILE_2, new DataFileInfo(DATA_FILE_2, ImmutableList.of(DELETE_FILE_2.toString()), 1));
    factory.setDataFileInfoForBatch(dataFileInfo);

    // create 3 filters - 2 row groups for DATA_FILE_1, 1 row groups for DATA_FILE_2
    PositionalDeleteFilter filter1 = factory.create(DATA_FILE_1);
    PositionalDeleteFilter filter2 = factory.create(DATA_FILE_1);
    PositionalDeleteFilter filter3 = factory.create(DATA_FILE_2);

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

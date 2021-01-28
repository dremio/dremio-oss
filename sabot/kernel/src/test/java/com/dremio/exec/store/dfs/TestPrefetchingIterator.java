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

package com.dremio.exec.store.dfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.store.CompositeColumnFilter;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.util.BloomFilter;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.test.AllocatorRule;

/**
 * Tests for {@link PrefetchingIterator}
 */
public class TestPrefetchingIterator {
    private final static String TEST_NAME = "TestPrefetchingIterator";

    private BufferAllocator testAllocator;

    @Rule
    public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

    @Before
    public void setupBeforeTest() {
        testAllocator = allocatorRule.newAllocator("test-prefetching-iterator", 0, Long.MAX_VALUE);
    }

    @After
    public void cleanupAfterTest() {
        testAllocator.close();
    }

    @Test
    public void testIteratorWithoutFilter() throws Exception {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        List<SplitReaderCreatorTest> creators = getMockSplitReaderCreators(10, 1);
        PrefetchingIterator<SplitReaderCreatorTest> it = new PrefetchingIterator<>(getCtx(), readerConfig, creators, 1);

        MutableParquetMetadata prevFooter = null;
        InputStreamProvider inputStreamProvider = null;
        for (int i = 0; i < creators.size(); i++) {
            SplitReaderCreatorTest insertedCreator = creators.get(i);
            assertTrue(it.hasNext());
            assertEquals(insertedCreator.createRecordReader(null), it.next());
            insertedCreator.createInputStreamProvider(inputStreamProvider, prevFooter);
            prevFooter = insertedCreator.getFooter();
            inputStreamProvider = insertedCreator.getInputStreamProvider();
        }

        assertFalse(it.hasNext());
    }

    private void verifyPrefetched(int startIndex, int numPrefetch, List<SplitReaderCreatorTest> creators) throws Exception {
      int i = 0;
      while ((startIndex + i) < creators.size()) {
        SplitReaderCreatorTest creator = creators.get(startIndex + i);
        assertTrue(creator.prefetched);
        i++;
        if (i >= numPrefetch) {
          break;
        }
      }
    }

    private void testIteratorWithoutFilterPrefetchN(int numPrefetch) throws Exception {
      CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
      List<SplitReaderCreatorTest> creators = getMockSplitReaderCreators(10, numPrefetch);
      PrefetchingIterator<SplitReaderCreatorTest> it = new PrefetchingIterator<>(getCtx(), readerConfig, creators, numPrefetch);

      MutableParquetMetadata prevFooter = null;
      InputStreamProvider inputStreamProvider = null;
      for (int i = 0; i < creators.size(); i++) {
        SplitReaderCreatorTest insertedCreator = creators.get(i);
        assertTrue(it.hasNext());
        assertEquals(insertedCreator.createRecordReader(null), it.next());
        insertedCreator.createInputStreamProvider(inputStreamProvider, prevFooter);
        prevFooter = insertedCreator.getFooter();
        inputStreamProvider = insertedCreator.getInputStreamProvider();
        verifyPrefetched(i, numPrefetch, creators);
      }

      assertFalse(it.hasNext());
    }

    @Test
    public void testIteratorWithoutFilterPrefetchAllN() throws Exception {
      for(int i = 1; i < 10; i++) {
        testIteratorWithoutFilterPrefetchN(i);
      }
    }

    @Test
    public void testIteratorWithFilterAddedInBetween() {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
                .thenReturn(getNonMatchingNameValuePairs());

        List<SplitReaderCreatorTest> creators = getMockSplitReaderCreators(10, 1);
        OperatorContext ctx = getCtx();
        PrefetchingIterator<SplitReaderCreatorTest> it = new PrefetchingIterator<>(ctx, readerConfig, creators, 1);

        InputStreamProvider inputStreamProvider = null;
        MutableParquetMetadata prevFooter = null;
        SplitReaderCreatorTest lastCreator = null;
        for (int i = 0; i < 5; i++) {
            SplitReaderCreatorTest insertedCreator = creators.get(i);
            assertTrue(it.hasNext());
            assertEquals(insertedCreator.createRecordReader(null), it.next());
            insertedCreator.createInputStreamProvider(inputStreamProvider, prevFooter);
            prevFooter = insertedCreator.getFooter();
            inputStreamProvider = insertedCreator.getInputStreamProvider();
        }

        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());

            it.addRuntimeFilter(filter);
            assertTrue(it.hasNext()); // next was already pre-initialized
            it.next();
            lastCreator = creators.get(5);

            assertFalse(it.hasNext());
            assertTrue(lastCreator.next == null);
            assertEquals(4L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void verifyAllClosed(List<SplitReaderCreatorTest> creators) throws Exception {
      for(SplitReaderCreatorTest creator : creators) {
        assertTrue(creator.closed);
      }
    }

    private void testIteratorWithFilterAddedInBetweenPrefetch(int numPrefetch) throws Exception {
      CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
      when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
        .thenReturn(getNonMatchingNameValuePairs());

      List<SplitReaderCreatorTest> creators = getMockSplitReaderCreators(10, numPrefetch);
      OperatorContext ctx = getCtx();
      PrefetchingIterator<SplitReaderCreatorTest> it = new PrefetchingIterator<>(ctx, readerConfig, creators, numPrefetch);

      InputStreamProvider inputStreamProvider = null;
      MutableParquetMetadata prevFooter = null;
      SplitReaderCreatorTest lastCreator = null;
      for (int i = 0; i < 5; i++) {
        SplitReaderCreatorTest insertedCreator = creators.get(i);
        assertTrue(it.hasNext());
        assertEquals(insertedCreator.createRecordReader(null), it.next());
        insertedCreator.createInputStreamProvider(inputStreamProvider, prevFooter);
        prevFooter = insertedCreator.getFooter();
        inputStreamProvider = insertedCreator.getInputStreamProvider();
        insertedCreator.close();
      }

      try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
        RuntimeFilter filter = prepareRuntimeFilter();
        closer.add(filter.getPartitionColumnFilter().getBloomFilter());

        it.addRuntimeFilter(filter);
        assertTrue(it.hasNext()); // next was already pre-initialized
        it.next();
        lastCreator = creators.get(5);

        assertFalse(it.hasNext());
        assertTrue(lastCreator.next == null);
        assertEquals(4L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        lastCreator.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      it.close();
      verifyAllClosed(creators);
    }

  @Test
  public void testIteratorWithFilterAddedInBetweenPrefetchAll() throws Exception {
      for(int i = 1; i < 10; i++) {
        testIteratorWithFilterAddedInBetweenPrefetch(i);
      }
  }

  @Test
    public void testIteratorWithFilterNothingSkipped() {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
                .thenReturn(getMatchingNameValuePairs());

        List<SplitReaderCreatorTest> creators = getMockSplitReaderCreators(10, 1);
        OperatorContext ctx = getCtx();
        PrefetchingIterator<SplitReaderCreatorTest> it = new PrefetchingIterator<>(ctx, readerConfig, creators, 1);
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());

            it.addRuntimeFilter(filter);

            InputStreamProvider inputStreamProvider = null;
            MutableParquetMetadata prevFooter = null;
            for (int i = 0; i < creators.size(); i++) {
                SplitReaderCreatorTest insertedCreator = creators.get(i);
                assertTrue(it.hasNext());
                assertEquals(insertedCreator.createRecordReader(null), it.next());
                insertedCreator.createInputStreamProvider(inputStreamProvider, prevFooter);
                prevFooter = insertedCreator.getFooter();
                inputStreamProvider = insertedCreator.getInputStreamProvider();
            }
            assertEquals(0L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIteratorWithFilterAllSkipped() throws Exception {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
                .thenReturn(getNonMatchingNameValuePairs());

        List<SplitReaderCreatorTest> creators = getMockSplitReaderCreators(10, 1);
        OperatorContext ctx = getCtx();
        PrefetchingIterator<SplitReaderCreatorTest> it = new PrefetchingIterator<>(ctx, readerConfig, creators, 1);
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());

            it.addRuntimeFilter(filter);
            assertTrue(it.hasNext());
            assertEquals(creators.get(0).createRecordReader(null), it.next());
            assertFalse(it.hasNext());

            assertEquals(9L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIteratorWithFilterSomeSkipped() {
        Predicate<Integer> isSelectedSplit = i -> i==1 || i==3 || i==9;
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        List<SplitReaderCreatorTest> creators = getMockSplitReaderCreators(10, 1);
        List<SplitReaderCreatorTest> selectedCreators = new ArrayList<>();

        for (int i = 0; i < creators.size(); i++) {
            SplitAndPartitionInfo split = ((GetSplitAndPartitionInfo)creators.get(i)).getSplit();
            if (isSelectedSplit.test(i)) {
                when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), eq(split)))
                        .thenReturn(getMatchingNameValuePairs());
                selectedCreators.add(creators.get(i));
            } else {
                when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), eq(split)))
                        .thenReturn(getNonMatchingNameValuePairs());
            }
        }


        OperatorContext ctx = getCtx();
        PrefetchingIterator<SplitReaderCreatorTest> it = new PrefetchingIterator<>(ctx, readerConfig, creators, 1);
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());

            it.addRuntimeFilter(filter);
            it.next();

            MutableParquetMetadata prevFooter = creators.get(0).getFooter();
            InputStreamProvider inputStreamProvider = creators.get(0).getInputStreamProvider();
            for (int i = 0; i < selectedCreators.size(); i++) {
                SplitReaderCreatorTest insertedCreator = selectedCreators.get(i);
                assertTrue(it.hasNext());
                assertEquals(insertedCreator.createRecordReader(null), it.next());
                insertedCreator.createInputStreamProvider(inputStreamProvider, prevFooter);
                prevFooter = insertedCreator.getFooter();
                inputStreamProvider = insertedCreator.getInputStreamProvider();
                if (i < selectedCreators.size() - 1) {
                  assertEquals(insertedCreator.next, selectedCreators.get(i + 1));
                } else {
                  assertEquals(insertedCreator.next, null);
                }
            }
            assertEquals(6L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void testIteratorWithFilterSomeSkippedPrefetch(int numPrefetch) throws Exception {
      Predicate<Integer> isSelectedSplit = i -> i==1 || i==3 || i==9;
      CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
      List<SplitReaderCreatorTest> creators = getMockSplitReaderCreators(10, numPrefetch);
      List<SplitReaderCreatorTest> selectedCreators = new ArrayList<>();

      for (int i = 0; i < creators.size(); i++) {
        SplitAndPartitionInfo split = ((GetSplitAndPartitionInfo)creators.get(i)).getSplit();
        if (isSelectedSplit.test(i)) {
          when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), eq(split)))
            .thenReturn(getMatchingNameValuePairs());
          selectedCreators.add(creators.get(i));
        } else {
          when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), eq(split)))
            .thenReturn(getNonMatchingNameValuePairs());
        }
      }


      OperatorContext ctx = getCtx();
      PrefetchingIterator<SplitReaderCreatorTest> it = new PrefetchingIterator<>(ctx, readerConfig, creators, numPrefetch);
      try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
        RuntimeFilter filter = prepareRuntimeFilter();
        closer.add(filter.getPartitionColumnFilter().getBloomFilter());

        it.addRuntimeFilter(filter);
        it.next();

        MutableParquetMetadata prevFooter = creators.get(0).getFooter();
        InputStreamProvider inputStreamProvider = creators.get(0).getInputStreamProvider();
        for (int i = 0; i < selectedCreators.size(); i++) {
          SplitReaderCreatorTest insertedCreator = selectedCreators.get(i);
          assertTrue(it.hasNext());
          assertEquals(insertedCreator.createRecordReader(null), it.next());
          insertedCreator.createInputStreamProvider(inputStreamProvider, prevFooter);
          prevFooter = insertedCreator.getFooter();
          inputStreamProvider = insertedCreator.getInputStreamProvider();
          if (i < selectedCreators.size() - 1) {
            assertEquals(insertedCreator.next, selectedCreators.get(i + 1));
          } else {
            assertEquals(insertedCreator.next, null);
          }
        }
        assertEquals(6L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      it.close();
      verifyAllClosed(creators);
    }

    @Test
    public void testIteratorWithFilterSomeSkippedPrefetchAll() throws Exception {
      for(int i = 1; i < 10; i++) {
        testIteratorWithFilterSomeSkippedPrefetch(i);
      }
    }

  @Test
  public void testMultipleFilters() {
    Predicate<Integer> isSelectedSplit1 = i -> i==1 || i==2 || i==3 || i==9;
    Predicate<Integer> isSelectedSplit2 = i -> i==3 || i==5 || i==9;

    CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        List<SplitReaderCreatorTest> creators = getMockSplitReaderCreators(10, 1);
        List<SplitReaderCreatorTest> selectedCreators = new ArrayList<>();
        final String secondColumnName = "partitionCol2";

        for (int i = 0; i < creators.size(); i++) {
            SplitAndPartitionInfo split = ((GetSplitAndPartitionInfo)creators.get(i)).getSplit();
            List<NameValuePair<?>> nvp = new ArrayList<>();
            if (isSelectedSplit1.and(isSelectedSplit2).test(i)) {
                selectedCreators.add(creators.get(i));
                nvp.addAll(getMatchingNameValuePairs());
                nvp.addAll(getMatchingNameValuePairs1(secondColumnName));

            } else if (isSelectedSplit1.test(i)) {
                nvp.addAll(getMatchingNameValuePairs());
                nvp.addAll(getNonMatchingNameValuePairs1(secondColumnName));
            } else if (isSelectedSplit2.test(i)) {
                nvp.addAll(getNonMatchingNameValuePairs());
                nvp.addAll(getMatchingNameValuePairs1(secondColumnName));
            } else {
                nvp.addAll(getNonMatchingNameValuePairs());
                nvp.addAll(getNonMatchingNameValuePairs1(secondColumnName));
            }
            when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), eq(split)))
                    .thenReturn(nvp);
        }


        OperatorContext ctx = getCtx();
        PrefetchingIterator<SplitReaderCreatorTest> it = new PrefetchingIterator<>(ctx, readerConfig, creators, 1);
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter1 = prepareRuntimeFilter();
            closer.add(filter1.getPartitionColumnFilter().getBloomFilter());
            it.addRuntimeFilter(filter1);

            RuntimeFilter filter2 = prepareRuntimeFilter(secondColumnName, 1, 2);
            closer.add(filter2.getPartitionColumnFilter().getBloomFilter());
            it.addRuntimeFilter(filter2);

            it.next();

            InputStreamProvider inputStreamProvider = creators.get(0).getInputStreamProvider();
            MutableParquetMetadata prevFooter = creators.get(0).getFooter();
            for (int i = 0; i < selectedCreators.size(); i++) {
                SplitReaderCreatorTest insertedCreator = selectedCreators.get(i);
                assertTrue(it.hasNext());
                assertEquals(insertedCreator.createRecordReader(null), it.next());
                insertedCreator.createInputStreamProvider(inputStreamProvider, prevFooter);
                prevFooter = insertedCreator.getFooter();
                inputStreamProvider = insertedCreator.getInputStreamProvider();
            }
            assertEquals(7L , ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIteratorEmpty() {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
                .thenReturn(getMatchingNameValuePairs());

        List<SplitReaderCreatorTest> creators = Collections.EMPTY_LIST;
        OperatorContext ctx = getCtx();
        PrefetchingIterator<SplitReaderCreatorTest> it = new PrefetchingIterator<>(ctx, readerConfig, creators, 1);
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());

            it.addRuntimeFilter(filter);
            assertFalse(it.hasNext());
            assertEquals(0L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private RuntimeFilter prepareRuntimeFilter() throws Exception {
        return prepareRuntimeFilter("partitionColumn", 1);
    }

    private RuntimeFilter prepareRuntimeFilter(String colName, int... value) throws Exception {
        try (AutoCloseables.RollbackCloseable rollbackCloseable = new AutoCloseables.RollbackCloseable();
             ArrowBuf keyBuf = testAllocator.buffer(5)) {
            BloomFilter bloomFilter = new BloomFilter(testAllocator, TEST_NAME, 512);
            rollbackCloseable.add(bloomFilter);
            bloomFilter.setup();

            keyBuf.setByte(0, 1);
            for (int v : value) {
                keyBuf.setInt(1, v);
                bloomFilter.put(keyBuf, 5);
            }

            rollbackCloseable.commit();

            CompositeColumnFilter partitionColumnFilter = new CompositeColumnFilter.Builder()
                    .setColumnsList(Collections.singletonList(colName))
                    .setBloomFilter(bloomFilter)
                    .setFilterType(CompositeColumnFilter.RuntimeFilterType.BLOOM_FILTER)
                    .build();
            return new RuntimeFilter(partitionColumnFilter, null, "");
        }
    }

    private List<NameValuePair<?>> getMatchingNameValuePairs() {
        return Collections.singletonList(new ConstantColumnPopulators.IntNameValuePair("partitionColumn", 1));
    }

    private List<NameValuePair<?>> getMatchingNameValuePairs1(String colName) {
        return Collections.singletonList(new ConstantColumnPopulators.IntNameValuePair(colName, 2));
    }

    private List<NameValuePair<?>> getNonMatchingNameValuePairs() {
        return Collections.singletonList(new ConstantColumnPopulators.IntNameValuePair("partitionColumn", 100));
    }

    private List<NameValuePair<?>> getNonMatchingNameValuePairs1(String colName) {
        return Collections.singletonList(new ConstantColumnPopulators.IntNameValuePair(colName, 100));
    }

    private List<SplitReaderCreatorTest> getMockSplitReaderCreators(int size, int numPrefetch) {
        final List<SplitReaderCreatorTest> readerCreators = new ArrayList<>(size);
        SplitReaderCreatorTest prev = null;

        for (int i = 0; i < size; i++) {
          SplitReaderCreatorTest current = new SplitReaderCreatorTest(i, numPrefetch);
          if (prev != null) {
            prev.setNext(current);
          }
          readerCreators.add(current);
          prev = current;
        }
        return readerCreators;
    }

    private OperatorContext getCtx() {
        OpProfileDef prof = new OpProfileDef(1, 1, 1);
        final OperatorStats stats = new OperatorStats(prof, testAllocator);

        OperatorContext ctx = mock(OperatorContext.class);
        when(ctx.getStats()).thenReturn(stats);

        when(ctx.getAllocator()).thenReturn(testAllocator);
        return ctx;
    }

    class SplitReaderCreatorTest extends SplitReaderCreator implements GetSplitAndPartitionInfo {
      final int idx;
      final int numPrefetch;
      final SplitAndPartitionInfo split;
      final RecordReader recordReader;
      final InputStreamProvider inputStreamProvider;
      final MutableParquetMetadata footer;
      boolean prefetched = false;
      boolean closed = false;

      SplitReaderCreatorTest(int idx, int numPrefetch) {
        this.idx = idx;
        this.numPrefetch = numPrefetch;
        split = mock(SplitAndPartitionInfo.class);
        recordReader = mock(RecordReader.class);
        inputStreamProvider = mock(InputStreamProvider.class);
        footer = mock(MutableParquetMetadata.class);
        PartitionProtobuf.NormalizedPartitionInfo partitionInfo = PartitionProtobuf
          .NormalizedPartitionInfo.newBuilder().build();
        when(split.getPartitionInfo()).thenReturn(partitionInfo);
      }

      @Override
      public Path getPath() {
        return Path.of("path" + idx);
      }

      @Override
      public MutableParquetMetadata getFooter() {
        return footer;
      }

      @Override
      public InputStreamProvider getInputStreamProvider() {
        return inputStreamProvider;
      }

      @Override
      public SplitAndPartitionInfo getSplit() {
        return split;
      }

      @Override
      public RecordReader createRecordReader(MutableParquetMetadata footer) {
        int n = 0;
        SplitReaderCreator nextCreator = next;
        while ((n < numPrefetch) && (nextCreator != null)) {
          nextCreator.createInputStreamProvider(null, null);
          nextCreator = nextCreator.next;
          n++;
        }
        return recordReader;
      }

      @Override
      public void createInputStreamProvider(InputStreamProvider lastInputStreamProvider, MutableParquetMetadata lastFooter) {
        prefetched = true;
      }

      @Override
      public void close() throws Exception {
        closed = true;
      }

      @Override
      public void addRowGroupsToRead(Set<Integer> rowGroupList) {

      }
    }
}

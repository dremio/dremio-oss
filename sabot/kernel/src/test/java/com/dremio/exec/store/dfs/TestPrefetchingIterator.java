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

import static com.dremio.exec.ExecConstants.DISABLED_GANDIVA_FUNCTIONS;
import static com.dremio.exec.ExecConstants.NUM_SPLITS_TO_PREFETCH;
import static com.dremio.exec.ExecConstants.PARQUET_CACHED_ENTITY_SET_FILE_SIZE;
import static com.dremio.exec.ExecConstants.PREFETCH_READER;
import static com.dremio.exec.ExecConstants.QUERY_EXEC_OPTION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.MessageType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.CompositeColumnFilter;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.InputStreamProviderFactory;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.exec.store.parquet.ParquetSplitReaderCreatorIterator;
import com.dremio.exec.store.parquet.ParquetSubScan;
import com.dremio.exec.store.parquet.SplitReaderCreatorIterator;
import com.dremio.exec.util.BloomFilter;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.test.AllocatorRule;
import com.google.protobuf.ByteString;

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
        List<SplitReaderCreator> creators = getMockSplitReaderCreators(10, 1);

        PrefetchingIterator it = new PrefetchingIterator(createSplitReaderCreatorIterator(creators));

        MutableParquetMetadata prevFooter = null;
        InputStreamProvider inputStreamProvider = null;
        for (int i = 0; i < creators.size(); i++) {
            SplitReaderCreator insertedCreator = creators.get(i);
            assertTrue(it.hasNext());
            assertEquals(insertedCreator.createRecordReader(null), it.next());
            insertedCreator.createInputStreamProvider(inputStreamProvider, prevFooter);
            prevFooter = insertedCreator.getFooter();
            inputStreamProvider = insertedCreator.getInputStreamProvider();
        }

        assertFalse(it.hasNext());
    }

    private void verifyPrefetched(int startIndex, int numPrefetch, List<SplitReaderCreator> creators) throws Exception {
      int i = 0;
      while ((startIndex + i) < creators.size()) {
        SplitReaderCreatorTest creator = (SplitReaderCreatorTest) creators.get(startIndex + i);
        assertTrue(creator.prefetched);
        i++;
        if (i >= numPrefetch) {
          break;
        }
      }
    }

    private void testIteratorWithoutFilterPrefetchN(int numPrefetch) throws Exception {
      CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
      List<SplitReaderCreator> creators = getMockSplitReaderCreators(10, numPrefetch);
      PrefetchingIterator it = new PrefetchingIterator(createSplitReaderCreatorIterator(creators));

      MutableParquetMetadata prevFooter = null;
      InputStreamProvider inputStreamProvider = null;
      for (int i = 0; i < creators.size(); i++) {
        SplitReaderCreator insertedCreator = creators.get(i);
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

    private void testIteratorWithFilterAddedInBetween(boolean fromRowGroupSplits) throws Exception {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
                .thenReturn(getNonMatchingNameValuePairs());

        OperatorContext ctx = getCtx();
        SplitReaderCreatorIterator creators = createSplitReaderCreator(ctx, fromRowGroupSplits, true, 1, 10, readerConfig);
        PrefetchingIterator it = new PrefetchingIterator(creators);

        for (int i = 0; i < 5; i++) {
            assertTrue(it.hasNext());
            it.next();
        }

        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());

            it.addRuntimeFilter(filter);
            assertTrue(it.hasNext()); // next was already pre-initialized
            RecordReader recordReader = it.next();
            assertEquals(0, recordReader.next());

            assertFalse(it.hasNext());
            assertEquals(5L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIteratorWithFilterAddedInBetween() throws Exception {
      testIteratorWithFilterAddedInBetween(true);
      testIteratorWithFilterAddedInBetween(false);
    }

    private void verifyAllClosed(List<SplitReaderCreator> creators) throws Exception {
      for(SplitReaderCreator creator : creators) {
        assertTrue(((SplitReaderCreatorTest) creator).closed);
      }
    }

    private void testIteratorWithFilterAddedInBetweenPrefetch(boolean fromRowGroupSplits, int numPrefetch) throws Exception {
      CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
      when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
        .thenReturn(getNonMatchingNameValuePairs());

      OperatorContext ctx = getCtx();
      SplitReaderCreatorIterator creators = createSplitReaderCreator(ctx, fromRowGroupSplits, true, numPrefetch, 10, readerConfig);
      PrefetchingIterator it = new PrefetchingIterator(creators);

      for (int i = 0; i < 5; i++) {
        assertTrue(it.hasNext());
        it.next();
      }

      try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
        RuntimeFilter filter = prepareRuntimeFilter();
        closer.add(filter.getPartitionColumnFilter().getBloomFilter());

        it.addRuntimeFilter(filter);
        assertTrue(it.hasNext()); // next was already pre-initialized
        it.next();

        assertFalse(it.hasNext());
        assertEquals(5L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      it.close();
    }

  @Test
  public void testIteratorWithFilterAddedInBetweenPrefetchAll() throws Exception {
    for(int i = 1; i < 10; i++) {
      testIteratorWithFilterAddedInBetweenPrefetch(true, i);
    }

    for(int i = 1; i < 10; i++) {
      testIteratorWithFilterAddedInBetweenPrefetch(false, i);
    }
  }

  @Test
    public void testIteratorWithFilterNothingSkipped() {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
                .thenReturn(getMatchingNameValuePairs());

        List<SplitReaderCreator> creators = getMockSplitReaderCreators(10, 1);
        OperatorContext ctx = getCtx();
        PrefetchingIterator it = new PrefetchingIterator(createSplitReaderCreatorIterator(creators));
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());

            it.addRuntimeFilter(filter);

            InputStreamProvider inputStreamProvider = null;
            MutableParquetMetadata prevFooter = null;
            for (int i = 0; i < creators.size(); i++) {
                SplitReaderCreator insertedCreator = creators.get(i);
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

    public void testIteratorWithFilterAllSkipped(boolean fromRowGroupSplits) throws Exception {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
                .thenReturn(getNonMatchingNameValuePairs());

        OperatorContext ctx = getCtx();
        SplitReaderCreatorIterator creators = createSplitReaderCreator(ctx, fromRowGroupSplits, true, 1, 10, readerConfig);
        PrefetchingIterator it = new PrefetchingIterator(creators);
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());

            it.addRuntimeFilter(filter);
            assertTrue(it.hasNext());
            RecordReader recordReader = it.next();
            assertEquals(0, recordReader.next());
            assertFalse(it.hasNext());
            assertEquals(10L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIteratorWithFilterAllSkipped() throws Exception {
      testIteratorWithFilterAllSkipped(true);
      testIteratorWithFilterAllSkipped(false);
    }

    private void testIteratorWithFilterSomeSkipped(boolean fromRowGroupSplits) throws Exception {
        Predicate<Integer> isSelectedSplit = i -> i==1 || i==3 || i==9;
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);

        OperatorContext ctx = getCtx();
        SplitReaderCreatorIterator creators = createSplitReaderCreator(ctx, fromRowGroupSplits, true, 1, 10, readerConfig, isSelectedSplit, null);
        PrefetchingIterator it = new PrefetchingIterator(creators);
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());

            it.addRuntimeFilter(filter);
            it.next();

            while (it.hasNext()) {
                it.next();
            }
            assertEquals(7L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIteratorWithFilterSomeSkipped() throws Exception {
      testIteratorWithFilterSomeSkipped(true);
      testIteratorWithFilterSomeSkipped(false);
    }

    private void testIteratorWithFilterSomeSkippedPrefetch(boolean fromRowGroupSplits, int numPrefetch) throws Exception {
      Predicate<Integer> isSelectedSplit = i -> i==1 || i==3 || i==9;
      CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);

      OperatorContext ctx = getCtx();
      SplitReaderCreatorIterator creators = createSplitReaderCreator(ctx, fromRowGroupSplits, true, numPrefetch, 10, readerConfig, isSelectedSplit, null);

      PrefetchingIterator it = new PrefetchingIterator(creators);
      try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
        RuntimeFilter filter = prepareRuntimeFilter();
        closer.add(filter.getPartitionColumnFilter().getBloomFilter());

        it.addRuntimeFilter(filter);
        it.next();

        while (it.hasNext()) {
          it.next();
        }
        assertEquals(7L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      it.close();
    }

    @Test
    public void testIteratorWithFilterSomeSkippedPrefetchAll() throws Exception {
      for(int i = 1; i < 10; i++) {
        testIteratorWithFilterSomeSkippedPrefetch(true, i);
      }
      for(int i = 1; i < 10; i++) {
        testIteratorWithFilterSomeSkippedPrefetch(false, i);
      }
    }

  private void testMultipleFilters(boolean fromRowGroupSplits) throws Exception {
    Predicate<Integer> isSelectedSplit1 = i -> i==1 || i==2 || i==3 || i==9;
    Predicate<Integer> isSelectedSplit2 = i -> i==3 || i==5 || i==9;

    CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);

    OperatorContext ctx = getCtx();
    SplitReaderCreatorIterator creators = createSplitReaderCreator(ctx, fromRowGroupSplits, true, 1, 10, readerConfig, isSelectedSplit1, isSelectedSplit2);

    PrefetchingIterator it = new PrefetchingIterator(creators);
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter1 = prepareRuntimeFilter();
            closer.add(filter1.getPartitionColumnFilter().getBloomFilter());
            it.addRuntimeFilter(filter1);

            RuntimeFilter filter2 = prepareRuntimeFilter("partitionCol2", 1, 2);
            closer.add(filter2.getPartitionColumnFilter().getBloomFilter());
            it.addRuntimeFilter(filter2);

            while (it.hasNext()) {
              it.next();
            }
            assertEquals(8L , ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testMultipleFilters() throws Exception {
      testMultipleFilters(true);
      testMultipleFilters(false);
    }

    @Test
    public void testIteratorEmpty() {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
                .thenReturn(getMatchingNameValuePairs());

        List<SplitReaderCreator> creators = Collections.EMPTY_LIST;
        OperatorContext ctx = getCtx();
        PrefetchingIterator it = new PrefetchingIterator(createSplitReaderCreatorIterator(creators));
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

    private List<SplitReaderCreator> getMockSplitReaderCreators(int size, int numPrefetch) {
        final List<SplitReaderCreator> readerCreators = new ArrayList<>(size);
        SplitReaderCreator prev = null;

        for (int i = 0; i < size; i++) {
          SplitReaderCreator current = new SplitReaderCreatorTest(i, numPrefetch);
          if (prev != null) {
            prev.setNext(current);
          }
          readerCreators.add(current);
          prev = current;
        }
        return readerCreators;
    }

    private SplitReaderCreatorIterator createSplitReaderCreatorIterator(List<SplitReaderCreator> creators) {
      Iterator<SplitReaderCreator> creatorIterator = creators.iterator();
      return new SplitReaderCreatorIterator() {
        @Override
        public void addRuntimeFilter(RuntimeFilter runtimeFilter) { }

        @Override
        public void close() throws Exception { }

        @Override
        public boolean hasNext() {
          return creatorIterator.hasNext();
        }

        @Override
        public SplitReaderCreator next() {
          return creatorIterator.next();
        }
      };
    }

    private OperatorContext getCtx() {
        OpProfileDef prof = new OpProfileDef(1, 1, 1);
        final OperatorStats stats = new OperatorStats(prof, testAllocator);

        OperatorContext ctx = mock(OperatorContext.class);
        when(ctx.getStats()).thenReturn(stats);

        when(ctx.getAllocator()).thenReturn(testAllocator);
        return ctx;
    }

    class SplitReaderCreatorTest extends SplitReaderCreator {
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

  private ParquetSplitReaderCreatorIterator createSplitReaderCreator(OperatorContext context, boolean fromRowGroupSplits, boolean prefetch, long numPrefetch, int numSplits, CompositeReaderConfig readerConfig) throws Exception {
      return createSplitReaderCreator(context, fromRowGroupSplits, prefetch, numPrefetch, numSplits, readerConfig, null, null);
  }

  private ParquetSplitReaderCreatorIterator createSplitReaderCreator(OperatorContext context, boolean fromRowGroupSplits, boolean prefetch, long numPrefetch, int numSplits, CompositeReaderConfig readerConfig, Predicate<Integer> isSelectedSplit1, Predicate<Integer> isSelectedSplit2) throws Exception {
    FragmentExecutionContext fragmentExecutionContext = mock(FragmentExecutionContext.class);
    ParquetSubScan config = mock(ParquetSubScan.class);

    SabotConfig sabotConfig = mock(SabotConfig.class);
    InputStreamProviderFactory inputStreamProviderFactory = mock(InputStreamProviderFactory.class);
    OptionManager optionManager = mock(OptionManager.class);
    FileSystemPlugin fileSystemPlugin = BaseTestQuery.getMockedFileSystemPlugin();
    FileSystem fs = mock(FileSystem.class);
    StoragePluginId storagePluginId = mock(StoragePluginId.class);
    OpProps opProps = mock(OpProps.class);
    InputStreamProvider inputStreamProvider = mock(InputStreamProvider.class);
    MutableParquetMetadata footer = mock(MutableParquetMetadata.class);

    when(sabotConfig.getInstance(InputStreamProviderFactory.KEY, InputStreamProviderFactory.class, InputStreamProviderFactory.DEFAULT)).thenReturn(inputStreamProviderFactory);
    when(sabotConfig.getInstance("dremio.plugins.parquet.factory", ParquetReaderFactory.class, ParquetReaderFactory.NONE)).thenReturn(ParquetReaderFactory.NONE);
    when(context.getConfig()).thenReturn(sabotConfig);
    when(context.getOptions()).thenReturn(optionManager);
    when(optionManager.getOption(PREFETCH_READER)).thenReturn(prefetch);
    when(optionManager.getOption(NUM_SPLITS_TO_PREFETCH)).thenReturn(numPrefetch);
    when(optionManager.getOption(PARQUET_CACHED_ENTITY_SET_FILE_SIZE)).thenReturn(true);
    when(optionManager.getOption(ExecConstants.RUNTIME_FILTER_KEY_MAX_SIZE)).thenReturn(32L);
    when(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS)).thenReturn("");
    when(optionManager.getOption(QUERY_EXEC_OPTION_KEY)).thenReturn(OptionValue.createString(OptionValue.OptionType.SYSTEM,QUERY_EXEC_OPTION_KEY,"Gandiva"));
    when(fragmentExecutionContext.getStoragePlugin(any())).thenReturn(fileSystemPlugin);
    when(fileSystemPlugin.createFS(any(), any(), any())).thenReturn(fs);
    when(fs.supportsPath(any())).thenReturn(true);
    when(fs.supportsAsync()).thenReturn(true);
    when(config.getPluginId()).thenReturn(storagePluginId);
    when(storagePluginId.getName()).thenReturn("");
    when(config.getProps()).thenReturn(opProps);
    when(config.getFullSchema()).thenReturn(new BatchSchema(Collections.emptyList()));
    when(opProps.getUserName()).thenReturn("");
    when(config.getColumns()).thenReturn(Collections.singletonList(SchemaPath.getSimplePath("*")));
    when(config.getFormatSettings()).thenReturn(FileConfig.getDefaultInstance());
    when(optionManager.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR)).thenReturn("dir");
    when(inputStreamProviderFactory.create(any(),any(),any(),anyLong(),anyLong(),any(),any(),any(),any(),anyBoolean(),any(),anyLong(),anyBoolean(),anyBoolean())).thenReturn(inputStreamProvider);

    BlockMetaData blockMetaData = mock(BlockMetaData.class);
    when(footer.getBlocks()).thenReturn(Collections.singletonList(blockMetaData));
    ColumnChunkMetaData chunkMetaData = mock(ColumnChunkMetaData.class);
    when(blockMetaData.getColumns()).thenReturn(Collections.singletonList(chunkMetaData));
    when(chunkMetaData.getFirstDataPageOffset()).thenReturn(0L);
    when(inputStreamProvider.getFooter()).thenReturn(footer);
    when(footer.getFileMetaData()).thenReturn(new FileMetaData(new MessageType("", new ArrayList<>()), new HashMap<>(), ""));

    List<SplitAndPartitionInfo> splits = new ArrayList<>();
    IntStream.range(0, numSplits).forEach(i -> splits.add(createSplit(fromRowGroupSplits, i)));

    if (isSelectedSplit1 != null) {
      if (isSelectedSplit2 == null) {
        for (int i = 0; i < splits.size(); i++) {
          SplitAndPartitionInfo split = splits.get(i);
          if (isSelectedSplit1.test(i)) {
            when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), eq(split)))
              .thenReturn(getMatchingNameValuePairs());
          } else {
            when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), eq(split)))
              .thenReturn(getNonMatchingNameValuePairs());
          }
        }
      } else {
        final String secondColumnName = "partitionCol2";

        for (int i = 0; i < splits.size(); i++) {
          SplitAndPartitionInfo split = splits.get(i);
          List<NameValuePair<?>> nvp = new ArrayList<>();
          if (isSelectedSplit1.and(isSelectedSplit2).test(i)) {
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
      }

    }

    when(config.getSplits()).thenReturn(splits);
    return new ParquetSplitReaderCreatorIterator(fragmentExecutionContext, context, config, readerConfig, fromRowGroupSplits);
  }

  private SplitAndPartitionInfo createSplit(boolean fromRowGroupSplits, int partitionId) {
    ByteString serializedSplitXAttr;
    if (fromRowGroupSplits) {
      ParquetProtobuf.ParquetDatasetSplitScanXAttr splitXAttr = ParquetProtobuf.ParquetDatasetSplitScanXAttr.newBuilder()
        .setStart(0)
        .setLength(10)
        .setPath("/tmp/path/" + partitionId)
        .setFileLength(10)
        .setLastModificationTime(0)
        .build();

      serializedSplitXAttr = splitXAttr.toByteString();
    } else {
      ParquetProtobuf.ParquetBlockBasedSplitXAttr splitXAttr = ParquetProtobuf.ParquetBlockBasedSplitXAttr.newBuilder()
        .setStart(0)
        .setLength(10)
        .setPath("/tmp/path/" + partitionId)
        .setFileLength(10)
        .setLastModificationTime(0)
        .build();

      serializedSplitXAttr = splitXAttr.toByteString();
    }

    PartitionProtobuf.NormalizedPartitionInfo partitionInfo = PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
      .setId(Integer.toString(partitionId))
      .build();

    PartitionProtobuf.NormalizedDatasetSplitInfo datasetSplitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
      .setPartitionId(Integer.toString(partitionId))
      .setExtendedProperty(serializedSplitXAttr)
      .build();

    return new SplitAndPartitionInfo(partitionInfo, datasetSplitInfo);
  }
}

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

package com.dremio.exec.store.hive.exec;

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
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.store.CompositeColumnFilter;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.PrefetchingIterator;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.util.BloomFilter;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.test.AllocatorRule;

/**
 * Tests for {@link HiveRecordReaderIterator}
 */
public class TestHiveRecordReaderIterator {
    private final static String TEST_NAME = "TestHiveRecordReaderIterator";

    private BufferAllocator testAllocator;

    @Rule
    public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

    @Before
    public void setupBeforeTest() {
        testAllocator = allocatorRule.newAllocator("test-hive-iterator", 0, Long.MAX_VALUE);
    }

    @After
    public void cleanupAfterTest() {
        testAllocator.close();
    }

    @Test
    public void testIteratorWithoutFilter() {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        List<Pair<SplitAndPartitionInfo, RecordReader>> readers = getMockSplitReaders(10);
        HiveRecordReaderIterator it = new HiveRecordReaderIterator(getCtx(), readerConfig, withSupplier(readers));

        for (int i = 0; i < readers.size(); i++) {
            RecordReader reader = readers.get(i).getRight();
            assertTrue(it.hasNext());
            assertEquals(reader, it.next());
        }
        assertFalse(it.hasNext());
    }

    @Test
    public void testIteratorWithFilterAddedInBetween() {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
                .thenReturn(getNonMatchingNameValuePairs());

        List<Pair<SplitAndPartitionInfo, RecordReader>> readers = getMockSplitReaders(10);
        OperatorContext ctx = getCtx();
        HiveRecordReaderIterator it = new HiveRecordReaderIterator(ctx, readerConfig, withSupplier(readers));

        for (int i = 0; i < 5; i++) {
            RecordReader reader = readers.get(i).getRight();
            assertTrue(it.hasNext());
            assertEquals(reader, it.next());
        }

        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());

            it.addRuntimeFilter(filter);
            assertFalse(it.hasNext());
            assertEquals(5L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIteratorWithFilterNothingSkipped() {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
                .thenReturn(getMatchingNameValuePairs());

        List<Pair<SplitAndPartitionInfo, RecordReader>> readers = getMockSplitReaders(10);
        OperatorContext ctx = getCtx();
        HiveRecordReaderIterator it = new HiveRecordReaderIterator(getCtx(), readerConfig, withSupplier(readers));
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());
            it.addRuntimeFilter(filter);

            for (int i = 0; i < readers.size(); i++) {
                RecordReader reader = readers.get(i).getRight();
                assertTrue(it.hasNext());
                assertEquals(reader, it.next());
            }
            assertEquals(0L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIteratorWithFilterAllSkipped() {
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), any(SplitAndPartitionInfo.class)))
                .thenReturn(getNonMatchingNameValuePairs());

        List<Pair<SplitAndPartitionInfo, RecordReader>> readers = getMockSplitReaders(10);
        OperatorContext ctx = getCtx();
        HiveRecordReaderIterator it = new HiveRecordReaderIterator(ctx, readerConfig, withSupplier(readers));
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());

            it.addRuntimeFilter(filter);
            assertFalse(it.hasNext());
            assertEquals(10L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIteratorWithFilterSomeSkipped() {
        Predicate<Integer> isSelectedSplit = i -> i==1 || i==3 || i==9;
        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        List<Pair<SplitAndPartitionInfo, RecordReader>> readers = getMockSplitReaders(10);
        List<RecordReader> selectedReaders = new ArrayList<>();

        for (int i = 0; i < readers.size(); i++) {
            SplitAndPartitionInfo split = readers.get(i).getLeft();
            if (isSelectedSplit.test(i)) {
                when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), eq(split)))
                        .thenReturn(getMatchingNameValuePairs());
                selectedReaders.add(readers.get(i).getRight());
            } else {
                when(readerConfig.getPartitionNVPairs(any(BufferAllocator.class), eq(split)))
                        .thenReturn(getNonMatchingNameValuePairs());
            }
        }

        OperatorContext ctx = getCtx();
        HiveRecordReaderIterator it = new HiveRecordReaderIterator(ctx, readerConfig, withSupplier(readers));
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter = prepareRuntimeFilter();
            closer.add(filter.getPartitionColumnFilter().getBloomFilter());
            it.addRuntimeFilter(filter);

            for (int i = 0; i < selectedReaders.size(); i++) {
                RecordReader reader = selectedReaders.get(i);
                assertTrue(it.hasNext());
                assertEquals(reader, it.next());
            }
            assertEquals(7L, ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }


    @Test
    public void testMultipleFilters() {
        Predicate<Integer> isSelectedSplit1 = i -> i==1 || i==2 || i==3 || i==9;
        Predicate<Integer> isSelectedSplit2 = i -> i==3 || i==5 || i==9;

        CompositeReaderConfig readerConfig = mock(CompositeReaderConfig.class);
        List<Pair<SplitAndPartitionInfo, RecordReader>> readers = getMockSplitReaders(10);
        List<RecordReader> selectedReaders = new ArrayList<>();
        final String secondColumnName = "partitionCol2";

        for (int i = 0; i < readers.size(); i++) {
            SplitAndPartitionInfo split = readers.get(i).getLeft();
            List<NameValuePair<?>> nvp = new ArrayList<>();
            if (isSelectedSplit1.and(isSelectedSplit2).test(i)) {
                selectedReaders.add(readers.get(i).getRight());
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

        HiveRecordReaderIterator it = new HiveRecordReaderIterator(ctx, readerConfig, withSupplier(readers));
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
            RuntimeFilter filter1 = prepareRuntimeFilter();
            closer.add(filter1.getPartitionColumnFilter().getBloomFilter());
            it.addRuntimeFilter(filter1);

            RuntimeFilter filter2 = prepareRuntimeFilter(secondColumnName, 1, 2);
            closer.add(filter2.getPartitionColumnFilter().getBloomFilter());
            it.addRuntimeFilter(filter2);

            for (int i = 0; i < selectedReaders.size(); i++) {
                RecordReader recordReader = selectedReaders.get(i);
                assertTrue(it.hasNext());
                assertEquals(recordReader, it.next());
            }
            assertEquals(8L , ctx.getStats().getLongStat(ScanOperator.Metric.NUM_PARTITIONS_PRUNED));
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

        List<SplitReaderCreator> creators = Collections.EMPTY_LIST;
        OperatorContext ctx = getCtx();
        PrefetchingIterator<SplitReaderCreator> it = new PrefetchingIterator<>(ctx, readerConfig, creators, 1);
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

    private List<Pair<SplitAndPartitionInfo, RecordReader>> getMockSplitReaders(int size) {
        final List<Pair<SplitAndPartitionInfo, RecordReader>> readerCreators = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            SplitAndPartitionInfo split = mock(SplitAndPartitionInfo.class);
            PartitionProtobuf.NormalizedPartitionInfo partitionInfo = PartitionProtobuf
                    .NormalizedPartitionInfo.newBuilder().build();
            when(split.getPartitionInfo()).thenReturn(partitionInfo);
            RecordReader reader = mock(RecordReader.class);
            readerCreators.add(Pair.of(split, reader));
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

    private List<Pair<SplitAndPartitionInfo, Supplier<RecordReader>>> withSupplier(List<Pair<SplitAndPartitionInfo, RecordReader>> readers) {
        return readers.stream().map(p -> Pair.of(p.getLeft(), (Supplier<RecordReader>)(() -> p.getRight()))).collect(Collectors.toList());
    }
}

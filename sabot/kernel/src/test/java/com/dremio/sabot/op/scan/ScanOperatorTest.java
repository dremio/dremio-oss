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

package com.dremio.sabot.op.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.proto.ExecProtos.CompositeColumnFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilterType;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.util.RuntimeFilterTestUtils;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.Lists;

/**
 * Tests for {@link ScanOperator}
 */
public class ScanOperatorTest {
    private RuntimeFilterTestUtils utils;
    private BufferAllocator testAllocator;

    @Rule
    public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

    @Before
    public void setupBeforeTest() {
        testAllocator = allocatorRule.newAllocator("test-scan_operator", 0, Long.MAX_VALUE);
        utils = new RuntimeFilterTestUtils(testAllocator);
    }

    @After
    public void cleanupAfterTest() {
        testAllocator.close();
    }

    @Test
    public void testWorkOnOOBRuntimeFilter() {
        try {
            // Send 3 messages, two are redundant filters coming from different sending minor fragments.
            // Finally, we expect two runtime filters to be added.

            int buildMinorFragment1 = 2;
            int buildMinorFragment2 = 1;
            ArrowBuf bloomFilterBuf = testAllocator.buffer(64);
            bloomFilterBuf.setZero(0, bloomFilterBuf.capacity());
            List<String> m1PtCols = Lists.newArrayList("pCol1", "pCol2");
            ValueListFilter m1Vlf1 = utils.prepareNewValueListFilter("npCol1", false, 1, 2, 3);
            ValueListFilter m1Vlf2 = utils.prepareNewValueListFilter("npCol2", false, 11, 12, 13);

            // START: re-use for other messages
            m1Vlf1.buf().retain(); // msg3
            m1Vlf2.buf().retain(); // msg3
            bloomFilterBuf.retain(2); // msg2, msg3
            // END
            OutOfBandMessage msg1 = utils.newOOB(buildMinorFragment1, m1PtCols, bloomFilterBuf, m1Vlf1, m1Vlf2);

            RecordReader mockReader = mock(RecordReader.class);
            ScanOperator scanOp = new ScanOperator(mock(SubScan.class), getMockContext(), RecordReaderIterator.from(mockReader), null, null, null);

            scanOp.workOnOOB(msg1);
            Arrays.stream(msg1.getBuffers()).forEach(ArrowBuf::release);
            assertEquals(1, scanOp.getRuntimeFilters().size());
            ArgumentCaptor<com.dremio.exec.store.RuntimeFilter> addedFilter = ArgumentCaptor.forClass(com.dremio.exec.store.RuntimeFilter.class);

            List<String> m2PtCols = Lists.newArrayList("pCol3", "pCol4");
            ValueListFilter m2Vlf1 = utils.prepareNewValueListBooleanFilter("npCol3", false, false, true);
            ValueListFilter m2Vlf2 = utils.prepareNewValueListBooleanFilter("npCol4", true, true, false);

            OutOfBandMessage msg2 = utils.newOOB(buildMinorFragment2, m2PtCols, bloomFilterBuf, m2Vlf1, m2Vlf2);
            scanOp.workOnOOB(msg2);
            Arrays.stream(msg2.getBuffers()).forEach(ArrowBuf::release);
            assertEquals(2, scanOp.getRuntimeFilters().size());

            OutOfBandMessage msg3 = utils.newOOB(buildMinorFragment1, m1PtCols, bloomFilterBuf, m1Vlf1, m1Vlf2);
            scanOp.workOnOOB(msg3); // should get skipped
            Arrays.stream(msg3.getBuffers()).forEach(ArrowBuf::release);
            assertEquals(2, scanOp.getRuntimeFilters().size());

            verify(mockReader, times(2)).addRuntimeFilter(addedFilter.capture());
            com.dremio.exec.store.RuntimeFilter filter1 = addedFilter.getAllValues().get(0);
            assertEquals(Lists.newArrayList("pCol1", "pCol2"), filter1.getPartitionColumnFilter().getColumnsList());
            assertEquals("BLOOM_FILTER", filter1.getPartitionColumnFilter().getFilterType().name());
            assertEquals(2, filter1.getNonPartitionColumnFilters().size());
            assertEquals(Lists.newArrayList("npCol1"), filter1.getNonPartitionColumnFilters().get(0).getColumnsList());
            assertEquals(Lists.newArrayList(1, 2, 3), utils.getValues(filter1.getNonPartitionColumnFilters().get(0).getValueList()));
            assertEquals("VALUE_LIST", filter1.getNonPartitionColumnFilters().get(0).getFilterType().name());
            assertEquals(Lists.newArrayList("npCol2"), filter1.getNonPartitionColumnFilters().get(1).getColumnsList());
            assertEquals(Lists.newArrayList(11, 12, 13), utils.getValues(filter1.getNonPartitionColumnFilters().get(1).getValueList()));
            assertEquals("VALUE_LIST", filter1.getNonPartitionColumnFilters().get(1).getFilterType().name());

            com.dremio.exec.store.RuntimeFilter filter2 = addedFilter.getAllValues().get(1);
            assertEquals(Lists.newArrayList("pCol3", "pCol4"), filter2.getPartitionColumnFilter().getColumnsList());
            assertEquals(Lists.newArrayList("pCol3", "pCol4"), filter2.getPartitionColumnFilter().getColumnsList());
            assertEquals("BLOOM_FILTER", filter2.getPartitionColumnFilter().getFilterType().name());
            assertEquals(2, filter2.getNonPartitionColumnFilters().size());
            assertEquals(Lists.newArrayList("npCol3"), filter2.getNonPartitionColumnFilters().get(0).getColumnsList());
            assertEquals("VALUE_LIST", filter2.getNonPartitionColumnFilters().get(0).getFilterType().name());
            ValueListFilter col1Filter = filter2.getNonPartitionColumnFilters().get(0).getValueList();
            assertFalse(col1Filter.isContainsNull());
            assertFalse(col1Filter.isContainsFalse());
            assertTrue(col1Filter.isContainsTrue());
            assertTrue(col1Filter.isBoolField());

            assertEquals(Lists.newArrayList("npCol4"), filter2.getNonPartitionColumnFilters().get(1).getColumnsList());
            assertEquals("VALUE_LIST", filter2.getNonPartitionColumnFilters().get(1).getFilterType().name());
            ValueListFilter col2Filter = filter2.getNonPartitionColumnFilters().get(1).getValueList();
            assertTrue(col2Filter.isContainsNull());
            assertTrue(col2Filter.isContainsFalse());
            assertFalse(col2Filter.isContainsTrue());
            assertTrue(col2Filter.isBoolField());

            AutoCloseables.close(scanOp.getRuntimeFilters());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testWorkOnOOBRuntimeFilterInvalidFilterSize() {
        int buildMinorFragment1 = 2;
        int buildMajorFragment1 = 1;
        try (ArrowBuf oobMessageBuf = testAllocator.buffer(128)) {
            RuntimeFilter filter1 = newRuntimeFilter(512, "col1", "col2"); // mismatched size
            OutOfBandMessage msg1 = newOOBMessage(filter1, oobMessageBuf, buildMajorFragment1, buildMinorFragment1);
            RecordReader mockReader = mock(RecordReader.class);
            ScanOperator scanOp = new ScanOperator(mock(SubScan.class), getMockContext(), RecordReaderIterator.from(mockReader), null, null, null);
            scanOp.workOnOOB(msg1);
            Arrays.stream(msg1.getBuffers()).forEach(ArrowBuf::release);
            verify(mockReader, never()).addRuntimeFilter(any(com.dremio.exec.store.RuntimeFilter.class));
        }
    }

    private OperatorContext getMockContext() {
        OperatorContext context = mock(OperatorContext.class);
        OperatorStats stats = mock(OperatorStats.class);
        when(context.getStats()).thenReturn(stats);
        doNothing().when(stats).startProcessing();
        doNothing().when(stats).addLongStat(eq(ScanOperator.Metric.NUM_READERS), eq(1));
        return context;
    }

    private OutOfBandMessage newOOBMessage(RuntimeFilter filter, ArrowBuf oobMessageBuf, int buildMajorFragment, int buildMinorFragment) {
        int probeScanId = 2;
        int probeOpId = 131074;
        int buildOpId = 65541;
        ArrowBuf[] bufs = new ArrowBuf[] {oobMessageBuf};
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
                false);
    }

    private RuntimeFilter newRuntimeFilter(int sizeBytes, String... cols) {
        int probeScanId = 2;
        int probeOpId = 131074;
        return RuntimeFilter.newBuilder()
                .setProbeScanMajorFragmentId(probeScanId)
                .setProbeScanOperatorId(probeOpId)
                .setPartitionColumnFilter(CompositeColumnFilter.newBuilder()
                        .addAllColumns(Lists.newArrayList(cols))
                        .setFilterType(RuntimeFilterType.BLOOM_FILTER)
                        .setSizeBytes(sizeBytes).build())
                .build();
    }
}

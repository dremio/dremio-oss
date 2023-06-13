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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.proto.ExecProtos.CompositeColumnFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilterType;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.util.RuntimeFilterTestUtils;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.options.OptionManager;
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
    public void testWorkOnOOBRuntimeFilter() throws Exception {
            // Send 6 messages. 1/2 are independent filters, 3 is dup of 1 from a different minor frag and should be dropped, 4 comes from
            // a different sender but filter structure is similar to 2/3, 5 comes from same sender as 4 but has one extra column.
            // 6th comes from same sender as 4 but with one less non-partition column.

            // Finally, we expect four runtime filters to be added.

            int sendingMajorFragment1 = 11;
            int sendingMajorFragment2 = 12;

            int sendingMinorFragment1 = 2;
            int sendingMinorFragment2 = 1;

            int sendingOp1 = 101;
            int sendingOp2 = 102;
            int sendingOp3 = 103;

            ArrowBuf bloomFilterBuf = testAllocator.buffer(64);
            bloomFilterBuf.setZero(0, bloomFilterBuf.capacity());
            List<String> m1PtCols = Lists.newArrayList("pCol1", "pCol2");
            List<String> m2PtCols = Lists.newArrayList("pCol3", "pCol4");
            List<String> m5PtCols = new ArrayList<>(m2PtCols);
            m5PtCols.add("pCol5"); //Extra partition col, so this won't be considered as a duplicate.

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
            RecordReader mockReader = mock(RecordReader.class);
            ScanOperator scanOp = new ScanOperator(getConfig(), getMockContext(), RecordReaderIterator.from(mockReader), null, null, null);

            OutOfBandMessage msg1 = utils.newOOB(sendingMajorFragment1, sendingOp1, sendingMinorFragment1, m1PtCols, bloomFilterBuf, m1Vlf1, m1Vlf2);
            scanOp.workOnOOB(msg1);
            Arrays.stream(msg1.getBuffers()).forEach(ArrowBuf::close);
            assertEquals(1, scanOp.getRuntimeFilters().size());
            ArgumentCaptor<com.dremio.exec.store.RuntimeFilter> addedFilter = ArgumentCaptor.forClass(com.dremio.exec.store.RuntimeFilter.class);

            OutOfBandMessage msg2 = utils.newOOB(sendingMajorFragment2, sendingOp2, sendingMinorFragment1, m2PtCols, bloomFilterBuf, m2Vlf1, m2Vlf2);
            scanOp.workOnOOB(msg2);
            Arrays.stream(msg2.getBuffers()).forEach(ArrowBuf::close);
            assertEquals(2, scanOp.getRuntimeFilters().size());

            OutOfBandMessage msg3 = utils.newOOB(sendingMajorFragment1, sendingOp1, sendingMinorFragment2, m1PtCols, bloomFilterBuf, m1Vlf1, m1Vlf2);
            scanOp.workOnOOB(msg3); // should get skipped
            Arrays.stream(msg3.getBuffers()).forEach(ArrowBuf::close);
            assertEquals(2, scanOp.getRuntimeFilters().size());

            OutOfBandMessage msg4 = utils.newOOB(sendingMajorFragment2, sendingOp3, sendingMinorFragment1, m2PtCols, bloomFilterBuf, m2Vlf1, m2Vlf2);
            scanOp.workOnOOB(msg4); // shouldn't be considered as duplicate because of different sending operator.
            Arrays.stream(msg4.getBuffers()).forEach(ArrowBuf::close);
            assertEquals(3, scanOp.getRuntimeFilters().size());

            OutOfBandMessage msg5 = utils.newOOB(sendingMajorFragment2, sendingOp3, sendingMinorFragment1, m5PtCols, bloomFilterBuf, m2Vlf1, m2Vlf2);
            scanOp.workOnOOB(msg5);
            Arrays.stream(msg5.getBuffers()).forEach(ArrowBuf::close);
            assertEquals(4, scanOp.getRuntimeFilters().size());

            // With one less non partition col filter - m2vlf2
            OutOfBandMessage msg6 = utils.newOOB(sendingMajorFragment2, sendingOp3, sendingMinorFragment1, m2PtCols, bloomFilterBuf, m2Vlf1);
            scanOp.workOnOOB(msg6);
            Arrays.stream(msg6.getBuffers()).forEach(ArrowBuf::close);
            assertEquals(5, scanOp.getRuntimeFilters().size());

            verify(mockReader, times(5)).addRuntimeFilter(addedFilter.capture());

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

            com.dremio.exec.store.RuntimeFilter filter3 = addedFilter.getAllValues().get(2);
            assertEquals(Lists.newArrayList("pCol3", "pCol4"), filter3.getPartitionColumnFilter().getColumnsList());
            List<String> f3NonPartitionCols = filter3.getNonPartitionColumnFilters().stream().map(f -> f.getColumnsList().get(0)).collect(Collectors.toList());
            assertEquals(Lists.newArrayList("npCol3", "npCol4"), f3NonPartitionCols);

            com.dremio.exec.store.RuntimeFilter filter4 = addedFilter.getAllValues().get(3);
            assertEquals(Lists.newArrayList("pCol3", "pCol4", "pCol5"), filter4.getPartitionColumnFilter().getColumnsList());
            List<String> f4NonPartitionCols = filter4.getNonPartitionColumnFilters().stream().map(f -> f.getColumnsList().get(0)).collect(Collectors.toList());
            assertEquals(Lists.newArrayList("npCol3", "npCol4"), f4NonPartitionCols);

            com.dremio.exec.store.RuntimeFilter filter5 = addedFilter.getAllValues().get(4);
            assertEquals(Lists.newArrayList("pCol3", "pCol4"), filter5.getPartitionColumnFilter().getColumnsList());
            List<String> f5NonPartitionCols = filter5.getNonPartitionColumnFilters().stream().map(f -> f.getColumnsList().get(0)).collect(Collectors.toList());
            assertEquals(Lists.newArrayList("npCol3"), f5NonPartitionCols);

            AutoCloseables.close(scanOp.getRuntimeFilters());
    }

    @Test
    public void testWorkOnOOBRuntimeFilterInvalidFilterSize() {
        int buildMinorFragment1 = 2;
        int buildMajorFragment1 = 1;
        try (ArrowBuf oobMessageBuf = testAllocator.buffer(128)) {
            RuntimeFilter filter1 = newRuntimeFilter(512, "col1", "col2"); // mismatched size
            OutOfBandMessage msg1 = newOOBMessage(filter1, oobMessageBuf, buildMajorFragment1, buildMinorFragment1);
            RecordReader mockReader = mock(RecordReader.class);
            ScanOperator scanOp = new ScanOperator(getConfig(), getMockContext(), RecordReaderIterator.from(mockReader), null, null, null);
            scanOp.workOnOOB(msg1);
            Arrays.stream(msg1.getBuffers()).forEach(ArrowBuf::close);
            verify(mockReader, never()).addRuntimeFilter(any(com.dremio.exec.store.RuntimeFilter.class));
        }
    }

    private OperatorContext getMockContext() {
        OperatorContext context = mock(OperatorContext.class);
        OperatorStats stats = mock(OperatorStats.class);
        when(context.getStats()).thenReturn(stats);
        doNothing().when(stats).startProcessing();
        doNothing().when(stats).addLongStat(eq(ScanOperator.Metric.NUM_READERS), eq(1));

        OptionManager options = mock(OptionManager.class);
        when(options.getOption(ExecConstants.ENABLE_ROW_LEVEL_RUNTIME_FILTERING)).thenReturn(false);
        when(context.getOptions()).thenReturn(options);
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

    private SubScan getConfig() {
        SubScan config = mock(SubScan.class);
        OpProps props = mock(OpProps.class);
        when(props.getOperatorId()).thenReturn(123);
        when(config.getProps()).thenReturn(props);
        return config;
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

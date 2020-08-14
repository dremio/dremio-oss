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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.proto.ExecProtos.CompositeColumnFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilterType;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.RecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.Lists;

/**
 * Tests for {@link ScanOperator}
 */
public class ScanOperatorTest {
    private BufferAllocator testAllocator;

    @Rule
    public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

    @Before
    public void setupBeforeTest() {
        testAllocator = allocatorRule.newAllocator("test-scan_operator", 0, Long.MAX_VALUE);
    }

    @After
    public void cleanupAfterTest() {
        testAllocator.close();
    }

    @Test
    public void testWorkOnOOBRuntimeFilter() {
        // Send 3 messages, two are redundant filters coming from different sending minor fragments.
        // Finally, we expect two runtime filters to be added.

        int buildMinorFragment1 = 2;
        int buildMinorFragment2 = 1;
        int buildMajorFragment1 = 1;
        int buildMajorFragment2 = 3;
        try (ArrowBuf oobMessageBuf = testAllocator.buffer(64)) {
            RuntimeFilter filter1 = newRuntimeFilter(64,"col1", "col2");
            OutOfBandMessage msg1 = newOOBMessage(filter1, oobMessageBuf, buildMajorFragment1, buildMinorFragment1);

            RecordReader mockReader = mock(RecordReader.class);
            ScanOperator scanOp = new ScanOperator(mock(SubScan.class), getMockContext(), Lists.newArrayList(mockReader).iterator(), null, null, null);

            scanOp.workOnOOB(msg1);
            msg1.getBuffer().release();
            assertEquals(1, scanOp.getRuntimeFilters().size());
            ArgumentCaptor<com.dremio.exec.store.RuntimeFilter> addedFilter1 = ArgumentCaptor.forClass(com.dremio.exec.store.RuntimeFilter.class);

            RuntimeFilter filter2 = newRuntimeFilter(64,"col2", "col3");
            OutOfBandMessage msg2 = newOOBMessage(filter2, oobMessageBuf, buildMajorFragment2, buildMinorFragment2);
            scanOp.workOnOOB(msg2);
            msg2.getBuffer().release();
            assertEquals(2, scanOp.getRuntimeFilters().size());

            OutOfBandMessage msg3 = newOOBMessage(filter1, oobMessageBuf, buildMajorFragment1, buildMinorFragment2);
            scanOp.workOnOOB(msg3); // should get skipped
            msg3.getBuffer().release();
            assertEquals(2, scanOp.getRuntimeFilters().size());

            verify(mockReader, times(2)).addRuntimeFilter(addedFilter1.capture());
            assertEquals(Lists.newArrayList("col1", "col2"), addedFilter1.getAllValues().get(0).getPartitionColumnFilter().getColumnsList());
            assertEquals(Lists.newArrayList("col2", "col3"), addedFilter1.getAllValues().get(1).getPartitionColumnFilter().getColumnsList());

            oobMessageBuf.release(2); // close extra buffer
        }
    }

    @Test
    public void testWorkOnOOBRuntimeFilterInvalidFilterSize() {
        int buildMinorFragment1 = 2;
        int buildMajorFragment1 = 1;
        try (ArrowBuf oobMessageBuf = testAllocator.buffer(128)) {
            RuntimeFilter filter1 = newRuntimeFilter(32, "col1", "col2"); // mismatched size
            OutOfBandMessage msg1 = newOOBMessage(filter1, oobMessageBuf, buildMajorFragment1, buildMinorFragment1);
            RecordReader mockReader = mock(RecordReader.class);
            ScanOperator scanOp = new ScanOperator(mock(SubScan.class), getMockContext(), Lists.newArrayList(mockReader).iterator(), null, null, null);
            scanOp.workOnOOB(msg1);
            msg1.getBuffer().release();
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
        return new OutOfBandMessage(
                UserBitShared.QueryId.newBuilder().build(),
                probeScanId,
                Lists.newArrayList(0, 3),
                probeOpId,
                buildMajorFragment,
                buildMinorFragment,
                buildOpId,
                new OutOfBandMessage.Payload(filter),
                oobMessageBuf.asNettyBuffer(),
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

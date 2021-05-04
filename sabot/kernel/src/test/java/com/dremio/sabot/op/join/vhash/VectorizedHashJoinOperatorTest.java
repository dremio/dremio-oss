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

package com.dremio.sabot.op.join.vhash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.planner.physical.filter.RuntimeFilterEntry;
import com.dremio.exec.planner.physical.filter.RuntimeFilterInfo;
import com.dremio.exec.proto.CoordExecRPC.FragmentAssignment;
import com.dremio.exec.proto.CoordExecRPC.MajorFragmentAssignment;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecProtos.RuntimeFilter;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.RuntimeFilterTestUtils;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.exec.rpc.AccountingExecTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.Lists;

/**
 * Tests for {@link VectorizedHashJoinOperator}
 */
public class VectorizedHashJoinOperatorTest {
    private RuntimeFilterTestUtils utils;
    private BufferAllocator testAllocator;

    @Rule
    public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

    @Before
    public void setupBeforeTest() {
        testAllocator = allocatorRule.newAllocator("test-vectorized_hash_join_operator", 0, Long.MAX_VALUE);
        utils = new RuntimeFilterTestUtils(testAllocator);
    }

    @After
    public void cleanupAfterTest() {
        testAllocator.close();
    }

    @Test
    public void testTryPushRuntimeFilterNonSendingFragmentBroadcastJoin() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(4).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(true, "col1"), fh));
        JoinTable joinTable = mock(JoinTable.class);
        when(joinTable.prepareBloomFilter(any(List.class), eq(true), anyInt())).thenReturn(Optional.empty());
        joinOp.setTable(joinTable);

        joinOp.tryPushRuntimeFilter();
        verify(joinOp, never()).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class), anyList());
        verify(joinOp, never()).sendRuntimeFilterToProbeScan(any(RuntimeFilter.class), any(Optional.class), anyList());
    }

    @Test
    public void testTryPushRuntimeFilterPartitionColOnlyBroadcastJoin() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(true, "col1"), fh));

        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());

        JoinTable joinTable = mock(JoinTable.class);
        Optional<BloomFilter> bloomFilter = mockedBloom();
        when(joinTable.prepareBloomFilter(any(List.class), eq(true), anyInt())).thenReturn(bloomFilter);
        joinOp.setTable(joinTable);

        joinOp.tryPushRuntimeFilter();
        verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());
        verify(joinOp, never()).sendRuntimeFilterAtMergePoints(valCaptor.capture(), any(Optional.class), anyList());

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals("col1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
        assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());
    }

    @Test
    public void testTryPushRuntimeFilterNonPartitionColOnlyBroadcastJoin() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(true, Collections.EMPTY_LIST, Lists.newArrayList("col1")), fh));

        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());

        JoinTable joinTable = mock(JoinTable.class);
        ValueListFilter valueListFilter = mockedValueListFilter();
        when(joinTable.prepareBloomFilter(any(List.class), eq(true), anyInt())).thenReturn(Optional.empty());
        when(joinTable.prepareValueListFilter(any(String.class), anyInt())).thenReturn(Optional.of(valueListFilter));
        joinOp.setTable(joinTable);

        joinOp.tryPushRuntimeFilter();
        verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());
        verify(joinOp, never()).sendRuntimeFilterAtMergePoints(valCaptor.capture(), any(Optional.class), anyList());

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(0, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals(1, filterVal.getNonPartitionColumnFilterCount());
        assertEquals("col1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
        assertEquals(valueListFilter.getSizeInBytes(), filterVal.getNonPartitionColumnFilter(0).getSizeBytes());
        assertEquals(valueListFilter.getValueCount(), filterVal.getNonPartitionColumnFilter(0).getValueCount());
    }

    @Test
    public void testTryPushRuntimeFilterBothColsBroadcastJoin() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(true,
                Lists.newArrayList("pCol1", "pCol2"), Lists.newArrayList("npCol1", "npCol2")), fh));

        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());

        JoinTable joinTable = mock(JoinTable.class);
        ValueListFilter valueListFilter = mockedValueListFilter();
        Optional<BloomFilter> bloomFilter = mockedBloom();
        when(joinTable.prepareBloomFilter(any(List.class), eq(true), anyInt())).thenReturn(bloomFilter);
        when(joinTable.prepareValueListFilter(any(String.class), anyInt())).thenReturn(Optional.of(valueListFilter));
        joinOp.setTable(joinTable);

        joinOp.tryPushRuntimeFilter();
        verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());
        verify(joinOp, never()).sendRuntimeFilterAtMergePoints(valCaptor.capture(), any(Optional.class), anyList());

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(2, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals("pCol1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
        assertEquals("pCol2_probe", filterVal.getPartitionColumnFilter().getColumns(1));
        assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());

        assertEquals(2, filterVal.getNonPartitionColumnFilterCount());
        assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
        assertEquals(1, filterVal.getNonPartitionColumnFilter(1).getColumnsCount());
        assertEquals("npCol1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
        assertEquals("npCol2_probe", filterVal.getNonPartitionColumnFilter(1).getColumns(0));
        assertEquals(valueListFilter.getSizeInBytes(), filterVal.getNonPartitionColumnFilter(0).getSizeBytes());
        assertEquals(valueListFilter.getValueCount(), filterVal.getNonPartitionColumnFilter(1).getValueCount());
    }

    @Test
    public void testTryPushRuntimeFilterAllColsDropped() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(true,
                Lists.newArrayList("pCol1", "pCol2"), Lists.newArrayList("npCol1", "npCol2")), fh));

        JoinTable joinTable = mock(JoinTable.class);
        when(joinTable.prepareBloomFilter(any(List.class), eq(true), anyInt())).thenReturn(Optional.empty());
        when(joinTable.prepareValueListFilter(any(String.class), anyInt())).thenReturn(Optional.empty());
        joinOp.setTable(joinTable);

        joinOp.tryPushRuntimeFilter();
        verify(joinOp, never()).sendRuntimeFilterToProbeScan(any(RuntimeFilter.class), any(Optional.class), anyList());
    }

    @Test
    public void testTryPushRuntimeFilterNonPartitionColFlagDisabled1() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(true,
                Lists.newArrayList("pCol1", "pCol2"), Lists.newArrayList("npCol1", "npCol2")), fh));
        when(joinOp.isRuntimeFilterEnabledForNonPartitionedCols()).thenReturn(false);

        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());

        JoinTable joinTable = mock(JoinTable.class);
        ValueListFilter valueListFilter = mockedValueListFilter();
        Optional<BloomFilter> bloomFilter = mockedBloom();
        when(joinTable.prepareBloomFilter(any(List.class), eq(true), anyInt())).thenReturn(bloomFilter);
        when(joinTable.prepareValueListFilter(any(String.class), anyInt())).thenReturn(Optional.of(valueListFilter));
        joinOp.setTable(joinTable);

        joinOp.tryPushRuntimeFilter();
        verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());
        verify(joinOp, never()).sendRuntimeFilterAtMergePoints(valCaptor.capture(), any(Optional.class), anyList());

        verify(joinTable, never()).prepareValueListFilter(anyString(), anyInt());

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(2, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals("pCol1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
        assertEquals("pCol2_probe", filterVal.getPartitionColumnFilter().getColumns(1));
        assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());

        assertEquals(0, filterVal.getNonPartitionColumnFilterCount());
    }

    @Test
    public void testTryPushRuntimeFilterNonPartitionColFlagDisabled2() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(true,
                Collections.EMPTY_LIST, Lists.newArrayList("npCol1", "npCol2")), fh));
        when(joinOp.isRuntimeFilterEnabledForNonPartitionedCols()).thenReturn(false);

        JoinTable joinTable = mock(JoinTable.class);
        ValueListFilter valueListFilter = mockedValueListFilter();
        when(joinTable.prepareBloomFilter(anyList(), anyBoolean(), anyInt())).thenReturn(Optional.empty());
        when(joinTable.prepareValueListFilter(anyString(), anyInt())).thenReturn(Optional.of(valueListFilter));
        joinOp.setTable(joinTable);

        joinOp.tryPushRuntimeFilter();
        verify(joinTable, never()).prepareValueListFilter(anyString(), anyInt());
        verify(joinOp, never()).sendRuntimeFilterToProbeScan(any(RuntimeFilter.class), any(Optional.class), anyList());
    }

    @Test
    public void testTryPushRuntimeFilterNonSendingFragmentShuffleJoin() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(4).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false,
                Lists.newArrayList("pCol1", "pCol2"), Lists.newArrayList("npCol1", "npCol2")), fh));
        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(valCaptor.capture(), any(Optional.class), anyList());

        JoinTable joinTable = mock(JoinTable.class);
        ValueListFilter valueListFilter = mockedValueListFilter();
        Optional<BloomFilter> bloomFilter = mockedBloom();
        when(joinTable.prepareBloomFilter(any(List.class), eq(false), anyInt())).thenReturn(bloomFilter);
        when(joinTable.prepareValueListFilter(any(String.class), anyInt())).thenReturn(Optional.of(valueListFilter));
        joinOp.setTable(joinTable);

        joinOp.tryPushRuntimeFilter();
        verify(joinOp, times(1)).sendRuntimeFilterAtMergePoints(valCaptor.capture(), any(Optional.class), anyList());

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(2, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals("pCol1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
        assertEquals("pCol2_probe", filterVal.getPartitionColumnFilter().getColumns(1));
        assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());

        assertEquals(2, filterVal.getNonPartitionColumnFilterCount());
        assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
        assertEquals(1, filterVal.getNonPartitionColumnFilter(1).getColumnsCount());
        assertEquals("npCol1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
        assertEquals("npCol2_probe", filterVal.getNonPartitionColumnFilter(1).getColumns(0));
        assertEquals(valueListFilter.getSizeInBytes(), filterVal.getNonPartitionColumnFilter(0).getSizeBytes());
        assertEquals(valueListFilter.getValueCount(), filterVal.getNonPartitionColumnFilter(1).getValueCount());
    }

    @Test
    public void testTryPushRuntimeFilterPartitionColOnlyShuffleJoin() throws Exception {
        try (ArrowBuf recvBuffer = testAllocator.buffer(64)) {
            recvBuffer.setZero(0, 64); // set all bytes to zero
            FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
            VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false, "col1"), fh));

            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class), anyList());
            doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());

            JoinTable joinTable = mock(JoinTable.class);
            BloomFilter bloomFilter = mockedBloom().get();
            when(joinTable.prepareBloomFilter(any(List.class), eq(false), anyInt())).thenReturn(Optional.of(bloomFilter));
            joinOp.setTable(joinTable);

            joinOp.tryPushRuntimeFilter();

            // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
            for (int sendingFragment = 2; sendingFragment <= 4; sendingFragment++) {
                OutOfBandMessage oobMsg = runtimeFilterOOBFromMinorFragment(sendingFragment, recvBuffer, "col1");
                joinOp.workOnOOB(oobMsg);
                Arrays.stream(oobMsg.getBuffers()).forEach(ArrowBuf::close);
            }

            // verify filter sent to probe scan.
            verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());

            RuntimeFilter filterVal = valCaptor.getValue();
            assertEquals(1, filterVal.getProbeScanMajorFragmentId());
            assertEquals(101, filterVal.getProbeScanOperatorId());
            assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
            assertEquals("col1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
            assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());
        }
    }

    @Test
    public void testTryPushRuntimeFilterNonPartitionColOnlyShuffleJoin() throws Exception {
        ValueListFilter valueListFilter1 = utils.prepareNewValueListFilter("col1_probe", false, 1, 2, 3);
        ValueListFilter valueListFilter2 = utils.prepareNewValueListFilter("col2_probe", false, 11, 12, 13);
        ValueListFilter valueListFilter3 = utils.prepareNewValueListFilter("col1_probe", false, 3, 4, 5);
        ValueListFilter valueListFilter4 = utils.prepareNewValueListFilter("col2_probe", false, 13, 14, 15);
        ValueListFilter valueListFilter5 = utils.prepareNewValueListFilter("col1_probe", true, 2, 5, 6);
        ValueListFilter valueListFilter6 = utils.prepareNewValueListFilter("col2_probe", false, 12, 15, 16);
        ValueListFilter valueListFilter7 = utils.prepareNewValueListFilter("col1_probe", false, 7, 8, 9);
        ValueListFilter valueListFilter8 = utils.prepareNewValueListFilter("col2_probe", false, 17, 18, 19);

        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false,
                Collections.EMPTY_LIST, Lists.newArrayList("col1", "col2")), fh));

        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        ArgumentCaptor<List> valListCaptor = ArgumentCaptor.forClass(List.class);

        doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class), anyList());
        doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), valListCaptor.capture());

        JoinTable joinTable = mock(JoinTable.class);
        when(joinTable.prepareBloomFilter(anyList(), anyBoolean(), anyInt())).thenReturn(Optional.empty());
        when(joinTable.prepareValueListFilter(anyString(), anyInt())).thenReturn(Optional.empty());
        when(joinTable.prepareValueListFilter(eq("col1_build"), anyInt())).thenReturn(Optional.of(valueListFilter1));
        when(joinTable.prepareValueListFilter(eq("col2_build"), anyInt())).thenReturn(Optional.of(valueListFilter2));


        joinOp.setTable(joinTable);
        joinOp.tryPushRuntimeFilter();
        AutoCloseables.close(valueListFilter1, valueListFilter2); // sendFilterToMergePoints is expected to close; mocked here.
        // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan

        OutOfBandMessage oobMsg2 = utils.newOOB(11, 101, 2, Collections.EMPTY_LIST, null, valueListFilter3, valueListFilter4);
        OutOfBandMessage oobMsg3 = utils.newOOB(11, 101, 3, Collections.EMPTY_LIST, null, valueListFilter5, valueListFilter6);
        OutOfBandMessage oobMsg4 = utils.newOOB(11, 101, 4, Collections.EMPTY_LIST, null, valueListFilter7, valueListFilter8);

        joinOp.workOnOOB(oobMsg2);
        joinOp.workOnOOB(oobMsg3);
        joinOp.workOnOOB(oobMsg4);

        Arrays.stream(oobMsg2.getBuffers()).forEach(ArrowBuf::close);
        Arrays.stream(oobMsg3.getBuffers()).forEach(ArrowBuf::close);
        Arrays.stream(oobMsg4.getBuffers()).forEach(ArrowBuf::close);

        // verify filter sent to probe scan.
        verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), valListCaptor.capture());

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(0, filterVal.getPartitionColumnFilter().getColumnsCount());

        assertEquals(2, filterVal.getNonPartitionColumnFilterCount());
        assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
        assertEquals("col1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
        assertEquals(1, filterVal.getNonPartitionColumnFilter(1).getColumnsCount());
        assertEquals("col2_probe", filterVal.getNonPartitionColumnFilter(1).getColumns(0));

        List<ValueListFilter> valLists = valListCaptor.getValue();
        assertEquals(2, valLists.size());
        assertTrue(valLists.get(0).isContainsNull());
        assertEquals(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9), utils.getValues(valLists.get(0)));
        assertFalse(valLists.get(1).isContainsNull());
        assertEquals(Lists.newArrayList(11, 12, 13, 14, 15, 16, 17, 18, 19), utils.getValues(valLists.get(1)));
        AutoCloseables.close(valLists);
    }

    @Test
    public void testTryPushRuntimeFilterBothColsShuffleJoin() throws Exception {
        try (ArrowBuf recvBuffer = testAllocator.buffer(64)) {
            recvBuffer.setZero(0, 64); // set all bytes to zero

            ValueListFilter valueListFilter1 = utils.prepareNewValueListFilter("col1_probe", false, 1, 2, 3);
            ValueListFilter valueListFilter2 = utils.prepareNewValueListFilter("col2_probe", false, 11, 12, 13);
            ValueListFilter valueListFilter3 = utils.prepareNewValueListFilter("col1_probe", false, 3, 4, 5);
            ValueListFilter valueListFilter4 = utils.prepareNewValueListFilter("col2_probe", false, 13, 14, 15);
            ValueListFilter valueListFilter5 = utils.prepareNewValueListFilter("col1_probe", true, 2, 5, 6);
            ValueListFilter valueListFilter6 = utils.prepareNewValueListFilter("col2_probe", false, 12, 15, 16);
            ValueListFilter valueListFilter7 = utils.prepareNewValueListFilter("col1_probe", false, 7, 8, 9);
            ValueListFilter valueListFilter8 = utils.prepareNewValueListFilter("col2_probe", false, 17, 18, 19);

            List<String> partitionColNames = Lists.newArrayList("pCol1");
            FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
            VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false,
                    partitionColNames, Lists.newArrayList("col1", "col2")), fh));

            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            ArgumentCaptor<List> valListCaptor = ArgumentCaptor.forClass(List.class);

            doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class), anyList());
            doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), valListCaptor.capture());

            JoinTable joinTable = mock(JoinTable.class);
            BloomFilter bloomFilter = mockedBloom().get();
            when(joinTable.prepareBloomFilter(any(List.class), eq(false), anyInt())).thenReturn(Optional.of(bloomFilter));
            when(joinTable.prepareValueListFilter(anyString(), anyInt())).thenReturn(Optional.empty());
            when(joinTable.prepareValueListFilter(eq("col1_build"), anyInt())).thenReturn(Optional.of(valueListFilter1));
            when(joinTable.prepareValueListFilter(eq("col2_build"), anyInt())).thenReturn(Optional.of(valueListFilter2));

            joinOp.setTable(joinTable);
            joinOp.tryPushRuntimeFilter();
            AutoCloseables.close(valueListFilter1, valueListFilter2); // sendRuntimeFilterAtMergePoints is mocked, which would have closed these.

            // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
            recvBuffer.retain(3);
            OutOfBandMessage oobMsg2 = utils.newOOB(11, 101, 2, partitionColNames, recvBuffer, valueListFilter3, valueListFilter4);
            OutOfBandMessage oobMsg3 = utils.newOOB(11, 101, 3, partitionColNames, recvBuffer, valueListFilter5, valueListFilter6);
            OutOfBandMessage oobMsg4 = utils.newOOB(11, 101, 4, partitionColNames, recvBuffer, valueListFilter7, valueListFilter8);

            joinOp.workOnOOB(oobMsg2);
            joinOp.workOnOOB(oobMsg3);
            joinOp.workOnOOB(oobMsg4);

            Arrays.stream(oobMsg2.getBuffers()).forEach(ArrowBuf::close);
            Arrays.stream(oobMsg3.getBuffers()).forEach(ArrowBuf::close);
            Arrays.stream(oobMsg4.getBuffers()).forEach(ArrowBuf::close);

            // verify filter sent to probe scan.
            verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), valListCaptor.capture());

            RuntimeFilter filterVal = valCaptor.getValue();
            assertEquals(1, filterVal.getProbeScanMajorFragmentId());
            assertEquals(101, filterVal.getProbeScanOperatorId());
            assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
            assertEquals("pCol1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
            assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());

            assertEquals(2, filterVal.getNonPartitionColumnFilterCount());
            assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
            assertEquals("col1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
            assertEquals(1, filterVal.getNonPartitionColumnFilter(1).getColumnsCount());
            assertEquals("col2_probe", filterVal.getNonPartitionColumnFilter(1).getColumns(0));

            List<ValueListFilter> valLists = valListCaptor.getValue();
            assertEquals(2, valLists.size());
            assertTrue(valLists.get(0).isContainsNull());
            assertEquals(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9), utils.getValues(valLists.get(0)));
            assertFalse(valLists.get(1).isContainsNull());
            assertEquals(Lists.newArrayList(11, 12, 13, 14, 15, 16, 17, 18, 19), utils.getValues(valLists.get(1)));
            AutoCloseables.close(valLists);
        }
    }

    @Test
    public void testTryPushRuntimeFilterBooleanNonPartitionShuffleJoin() throws Exception {
        try (ArrowBuf recvBuffer = testAllocator.buffer(64)) {
            recvBuffer.setZero(0, 64); // set all bytes to zero

            ValueListFilter valueListFilter1 = utils.prepareNewValueListBooleanFilter("col1_probe", false, false,true);
            ValueListFilter valueListFilter2 = utils.prepareNewValueListBooleanFilter("col2_probe", false, true,false);
            ValueListFilter valueListFilter3 = utils.prepareNewValueListBooleanFilter("col1_probe", true, false,true);
            ValueListFilter valueListFilter4 = utils.prepareNewValueListBooleanFilter("col2_probe", false, true,false);
            ValueListFilter valueListFilter5 = utils.prepareNewValueListBooleanFilter("col1_probe", false, false,true);
            ValueListFilter valueListFilter6 = utils.prepareNewValueListBooleanFilter("col2_probe", false, true,false);
            ValueListFilter valueListFilter7 = utils.prepareNewValueListBooleanFilter("col1_probe", false, false,true);
            ValueListFilter valueListFilter8 = utils.prepareNewValueListBooleanFilter("col2_probe", false, true,false);

            List<String> partitionColNames = Lists.newArrayList("pCol1");
            FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
            VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false,
                    partitionColNames, Lists.newArrayList("col1", "col2")), fh));

            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            ArgumentCaptor<List> valListCaptor = ArgumentCaptor.forClass(List.class);

            doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class), anyList());
            doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());

            JoinTable joinTable = mock(JoinTable.class);
            BloomFilter bloomFilter = mockedBloom().get();
            when(joinTable.prepareBloomFilter(any(List.class), eq(false), anyInt())).thenReturn(Optional.of(bloomFilter));
            when(joinTable.prepareValueListFilter(anyString(), anyInt())).thenReturn(Optional.empty());
            when(joinTable.prepareValueListFilter(eq("col1_build"), anyInt())).thenReturn(Optional.of(valueListFilter1));
            when(joinTable.prepareValueListFilter(eq("col2_build"), anyInt())).thenReturn(Optional.of(valueListFilter2));

            joinOp.setTable(joinTable);
            joinOp.tryPushRuntimeFilter();
            AutoCloseables.close(valueListFilter1, valueListFilter2); // sendRuntimeFilterAtMergePoints is mocked, which would have closed these.

            // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
            recvBuffer.retain(3);
            OutOfBandMessage oobMsg2 = utils.newOOB(11, 101, 2, partitionColNames, recvBuffer, valueListFilter3, valueListFilter4);
            OutOfBandMessage oobMsg3 = utils.newOOB(11, 101, 3, partitionColNames, recvBuffer, valueListFilter5, valueListFilter6);
            OutOfBandMessage oobMsg4 = utils.newOOB(11, 101, 4, partitionColNames, recvBuffer, valueListFilter7, valueListFilter8);

            joinOp.workOnOOB(oobMsg2);
            joinOp.workOnOOB(oobMsg3);
            joinOp.workOnOOB(oobMsg4);

            Arrays.stream(oobMsg2.getBuffers()).forEach(ArrowBuf::close);
            Arrays.stream(oobMsg3.getBuffers()).forEach(ArrowBuf::close);
            Arrays.stream(oobMsg4.getBuffers()).forEach(ArrowBuf::close);

            // verify filter sent to probe scan.
            verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), valListCaptor.capture());

            RuntimeFilter filterVal = valCaptor.getValue();
            assertEquals(2, filterVal.getNonPartitionColumnFilterCount());
            assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
            assertEquals("col1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
            assertEquals(1, filterVal.getNonPartitionColumnFilter(1).getColumnsCount());
            assertEquals("col2_probe", filterVal.getNonPartitionColumnFilter(1).getColumns(0));

            List<ValueListFilter> valLists = valListCaptor.getValue();
            assertEquals(2, valLists.size());
            assertTrue(valLists.get(0).isContainsNull());
            assertTrue(valLists.get(0).isContainsTrue());
            assertFalse(valLists.get(0).isContainsFalse());

            assertFalse(valLists.get(1).isContainsNull());
            assertFalse(valLists.get(1).isContainsTrue());
            assertTrue(valLists.get(1).isContainsFalse());
            AutoCloseables.close(valLists);
        }
    }

    @Test
    public void testTryPushRuntimeFilterBooleanColumnDrop() throws Exception {
        try (ArrowBuf recvBuffer = testAllocator.buffer(64)) {
            recvBuffer.setZero(0, 64); // set all bytes to zero

            ValueListFilter valueListFilter1 = utils.prepareNewValueListBooleanFilter("col1_probe", false, false,true);
            ValueListFilter valueListFilter2 = utils.prepareNewValueListBooleanFilter("col2_probe", false, true,false);
            ValueListFilter valueListFilter3 = utils.prepareNewValueListBooleanFilter("col1_probe", true, false,true);
            ValueListFilter valueListFilter4 = utils.prepareNewValueListBooleanFilter("col2_probe", false, true,false);
            ValueListFilter valueListFilter5 = utils.prepareNewValueListBooleanFilter("col1_probe", false, true,false);
            ValueListFilter valueListFilter6 = utils.prepareNewValueListBooleanFilter("col2_probe", false, true,false);
            ValueListFilter valueListFilter7 = utils.prepareNewValueListBooleanFilter("col1_probe", false, false,true);
            ValueListFilter valueListFilter8 = utils.prepareNewValueListBooleanFilter("col2_probe", false, true,false);

            List<String> partitionColNames = Lists.newArrayList("pCol1");
            FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
            VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false,
                    partitionColNames, Lists.newArrayList("col1", "col2")), fh));

            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            ArgumentCaptor<List> valListCaptor = ArgumentCaptor.forClass(List.class);
            doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class), anyList());
            doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());

            JoinTable joinTable = mock(JoinTable.class);
            BloomFilter bloomFilterOrg = mockedBloom().get();
            BloomFilter bloomFilter = mockedBloom().get();
            when(bloomFilterOrg.createCopy(eq(testAllocator))).thenReturn(bloomFilter);

            when(joinTable.prepareBloomFilter(any(List.class), eq(false), anyInt())).thenReturn(Optional.of(bloomFilterOrg));
            when(joinTable.prepareValueListFilter(anyString(), anyInt())).thenReturn(Optional.empty());
            when(joinTable.prepareValueListFilter(eq("col1_build"), anyInt())).thenReturn(Optional.of(valueListFilter1));
            when(joinTable.prepareValueListFilter(eq("col2_build"), anyInt())).thenReturn(Optional.of(valueListFilter2));

            joinOp.setTable(joinTable);
            joinOp.tryPushRuntimeFilter();
            AutoCloseables.close(valueListFilter1, valueListFilter2); // sendRuntimeFilterAtMergePoints is mocked, which would have closed these.

            // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
            recvBuffer.retain(3);
            OutOfBandMessage oobMsg2 = utils.newOOB(11, 101, 2, partitionColNames, recvBuffer, valueListFilter3, valueListFilter4);
            OutOfBandMessage oobMsg3 = utils.newOOB(11, 101, 3, partitionColNames, recvBuffer, valueListFilter5, valueListFilter6);
            OutOfBandMessage oobMsg4 = utils.newOOB(11, 101, 4, partitionColNames, recvBuffer, valueListFilter7, valueListFilter8);

            joinOp.workOnOOB(oobMsg2);
            joinOp.workOnOOB(oobMsg3);
            joinOp.workOnOOB(oobMsg4);

            Arrays.stream(oobMsg2.getBuffers()).forEach(ArrowBuf::close);
            Arrays.stream(oobMsg3.getBuffers()).forEach(ArrowBuf::close);
            Arrays.stream(oobMsg4.getBuffers()).forEach(ArrowBuf::close);

            // verify filter sent to probe scan.
            verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), valListCaptor.capture());

            RuntimeFilter filterVal = valCaptor.getValue();
            assertEquals(1, filterVal.getNonPartitionColumnFilterCount());
            assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
            assertEquals("col2_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));

            List<ValueListFilter> valLists = valListCaptor.getValue();
            assertEquals(1, valLists.size());
            assertFalse(valLists.get(0).isContainsNull());
            assertFalse(valLists.get(0).isContainsTrue());
            assertTrue(valLists.get(0).isContainsFalse());
            AutoCloseables.close(valLists);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testTryPushRuntimeFilterShuffleJoinAllDroppedAtMerge() throws Exception {
        try (ArrowBuf recvBuffer = testAllocator.buffer(64)) {
            recvBuffer.setZero(0, 64); // set all bytes to zero

            ValueListFilter valueListFilter1 = utils.prepareNewValueListFilter("col1_probe", false, 1, 2, 3);
            ValueListFilter valueListFilter2 = utils.prepareNewValueListFilter("col2_probe", false, 11, 12, 13);
            ValueListFilter valueListFilter3 = utils.prepareNewValueListFilter("col1_probe", false, IntStream.range(1000, 2000).toArray());
            ValueListFilter valueListFilter4 = utils.prepareNewValueListFilter("col2_probe", false, 13, 14, 15);
            ValueListFilter valueListFilter5 = utils.prepareNewValueListFilter("col1_probe", true, 2, 5, 6);
            ValueListFilter valueListFilter6 = utils.prepareNewValueListFilter("col2_probe", false, IntStream.range(2000, 3000).toArray());
            ValueListFilter valueListFilter7 = utils.prepareNewValueListFilter("col1_probe", false, 7, 8, 9);
            ValueListFilter valueListFilter8 = utils.prepareNewValueListFilter("col2_probe", false, 17, 18, 19);

            List<String> partitionColNames = Lists.newArrayList("pCol1");
            FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
            VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false,
                    partitionColNames, Lists.newArrayList("col1", "col2")), fh));

            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            ArgumentCaptor<List> valListCaptor = ArgumentCaptor.forClass(List.class);

            doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class), anyList());
            doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), valListCaptor.capture());

            JoinTable joinTable = mock(JoinTable.class);
            BloomFilter bloomFilter = mockedBloom().get();
            when(joinTable.prepareBloomFilter(any(List.class), eq(false), anyInt())).thenReturn(Optional.of(bloomFilter));
            when(joinTable.prepareValueListFilter(anyString(), anyInt())).thenReturn(Optional.empty());
            when(joinTable.prepareValueListFilter(eq("col1_build"), anyInt())).thenReturn(Optional.of(valueListFilter1));
            when(joinTable.prepareValueListFilter(eq("col2_build"), anyInt())).thenReturn(Optional.of(valueListFilter2));

            joinOp.setTable(joinTable);
            joinOp.tryPushRuntimeFilter();
            AutoCloseables.close(valueListFilter1, valueListFilter2); // sendRuntimeFilterAtMergePoints is mocked, which would have closed these.
            when(bloomFilter.isCrossingMaxFPP()).thenReturn(true); // To force drop

            // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
            recvBuffer.retain(3);
            OutOfBandMessage oobMsg2 = utils.newOOB(11, 101, 2, partitionColNames, recvBuffer, valueListFilter3, valueListFilter4);
            OutOfBandMessage oobMsg3 = utils.newOOB(11, 101, 3, partitionColNames, recvBuffer, valueListFilter5, valueListFilter6);
            OutOfBandMessage oobMsg4 = utils.newOOB(11, 101, 4, partitionColNames, recvBuffer, valueListFilter7, valueListFilter8);

            joinOp.workOnOOB(oobMsg2);
            joinOp.workOnOOB(oobMsg3);
            joinOp.workOnOOB(oobMsg4);

            Arrays.stream(oobMsg2.getBuffers()).forEach(ArrowBuf::close);
            Arrays.stream(oobMsg3.getBuffers()).forEach(ArrowBuf::close);
            Arrays.stream(oobMsg4.getBuffers()).forEach(ArrowBuf::close);

            verify(joinOp, never()).sendRuntimeFilterToProbeScan(any(RuntimeFilter.class), any(Optional.class), anyList());
        }
    }

    @Test
    public void testTryPushRuntimeFilterShuffleJoinIsLastPiece() throws Exception {
        ArrowBuf recvBuffer = testAllocator.buffer(64);
        recvBuffer.setBytes(0, new byte[64]); // set all bytes to zero
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false, "col1"), fh));

        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        ArgumentCaptor<Optional> bfCaptor = ArgumentCaptor.forClass(Optional.class);

        doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class), anyList());
        doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());

        JoinTable joinTable = mock(JoinTable.class);
        BloomFilter bloomFilter = BloomFilter.prepareFrom(recvBuffer);
        when(joinTable.prepareBloomFilter(any(List.class), eq(false), anyInt())).thenReturn(Optional.of(bloomFilter));
        joinOp.setTable(joinTable);

        for (int sendingFragment = 2; sendingFragment <= 4; sendingFragment++) {
            OutOfBandMessage oobMsg = runtimeFilterOOBFromMinorFragment(sendingFragment, recvBuffer, "col1");
            joinOp.workOnOOB(oobMsg);
            Arrays.stream(oobMsg.getBuffers()).forEach(ArrowBuf::release);
        }
        joinOp.tryPushRuntimeFilter();

        // verify filter sent to probe scan.
        verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), bfCaptor.capture(), anyList());

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals("col1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
        assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());
        bfCaptor.getValue().ifPresent(c -> ((BloomFilter) c).close());
    }

    @Test
    public void testTryPushRuntimeFilterBothColsShuffleJoinIsLastPiece() throws Exception {
        try (ArrowBuf recvBuffer = testAllocator.buffer(64)) {
            recvBuffer.setZero(0, 64); // set all bytes to zero

            ValueListFilter valueListFilter1 = utils.prepareNewValueListFilter("col1_probe", false, 1, 2, 3);
            ValueListFilter valueListFilter2 = utils.prepareNewValueListFilter("col2_probe", false, 11, 12, 13);
            ValueListFilter valueListFilter3 = utils.prepareNewValueListFilter("col1_probe", false, 3, 4, 5);
            ValueListFilter valueListFilter4 = utils.prepareNewValueListFilter("col2_probe", false, 13, 14, 15);
            ValueListFilter valueListFilter5 = utils.prepareNewValueListFilter("col1_probe", true, 2, 5, 6);
            ValueListFilter valueListFilter6 = utils.prepareNewValueListFilter("col2_probe", false, 12, 15, 16);
            ValueListFilter valueListFilter7 = utils.prepareNewValueListFilter("col1_probe", false, 7, 8, 9);
            ValueListFilter valueListFilter8 = utils.prepareNewValueListFilter("col2_probe", false, 17, 18, 19);

            List<String> partitionColNames = Lists.newArrayList("pCol1");
            FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
            VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false,
                    partitionColNames, Lists.newArrayList("col1", "col2")), fh));

            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            ArgumentCaptor<List> valListCaptor = ArgumentCaptor.forClass(List.class);

            doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class), anyList());
            doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), anyList());

            JoinTable joinTable = mock(JoinTable.class);
            BloomFilter bloomFilter = mockedBloom().get();
            when(bloomFilter.getDataBuffer()).thenReturn(recvBuffer);
            when(joinTable.prepareBloomFilter(any(List.class), eq(false), anyInt())).thenReturn(Optional.of(bloomFilter));
            when(joinTable.prepareValueListFilter(anyString(), anyInt())).thenReturn(Optional.empty());
            when(joinTable.prepareValueListFilter(eq("col1_build"), anyInt())).thenReturn(Optional.of(valueListFilter1));
            when(joinTable.prepareValueListFilter(eq("col2_build"), anyInt())).thenReturn(Optional.of(valueListFilter2));

            joinOp.setTable(joinTable);
            // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
            recvBuffer.retain(3);
            OutOfBandMessage oobMsg2 = utils.newOOB(11, 101, 2, partitionColNames, recvBuffer, valueListFilter3, valueListFilter4);
            OutOfBandMessage oobMsg3 = utils.newOOB(11, 101, 3, partitionColNames, recvBuffer, valueListFilter5, valueListFilter6);
            OutOfBandMessage oobMsg4 = utils.newOOB(11, 101, 4, partitionColNames, recvBuffer, valueListFilter7, valueListFilter8);

            joinOp.workOnOOB(oobMsg2);
            joinOp.workOnOOB(oobMsg3);
            joinOp.workOnOOB(oobMsg4);

            Arrays.stream(oobMsg2.getBuffers()).forEach(ArrowBuf::close);
            Arrays.stream(oobMsg3.getBuffers()).forEach(ArrowBuf::close);
            Arrays.stream(oobMsg4.getBuffers()).forEach(ArrowBuf::close);

            joinOp.tryPushRuntimeFilter();

            // verify filter sent to probe scan.
            verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class), valListCaptor.capture());

            RuntimeFilter filterVal = valCaptor.getValue();
            assertEquals(1, filterVal.getProbeScanMajorFragmentId());
            assertEquals(101, filterVal.getProbeScanOperatorId());
            assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
            assertEquals("pCol1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
            assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());

            assertEquals(2, filterVal.getNonPartitionColumnFilterCount());
            assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
            assertEquals("col1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
            assertEquals(1, filterVal.getNonPartitionColumnFilter(1).getColumnsCount());
            assertEquals("col2_probe", filterVal.getNonPartitionColumnFilter(1).getColumns(0));

            List<ValueListFilter> valLists = valListCaptor.getValue();
            assertEquals(2, valLists.size());
            assertTrue(valLists.get(0).isContainsNull());
            assertEquals(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9), utils.getValues(valLists.get(0)));
            assertFalse(valLists.get(1).isContainsNull());
            assertEquals(Lists.newArrayList(11, 12, 13, 14, 15, 16, 17, 18, 19), utils.getValues(valLists.get(1)));
            AutoCloseables.close(valLists);
        }
    }

    @Test
    public void testSendRuntimeFilterToProbeScan() throws Exception{
        int probeScanId = 2;
        int probeOpId = 131074;
        int buildMajorFragment = 1;
        int buildMinorFragment = 2;
        int buildOpId = 65541;
        QueryId queryId = QueryId.newBuilder().build();
        try (ArrowBuf recvBuffer = testAllocator.buffer(64)) {
            recvBuffer.setBytes(0, new byte[64]); // set all bytes to zero
            FragmentHandle fh = FragmentHandle.newBuilder()
                    .setQueryId(queryId).setMajorFragmentId(buildMajorFragment).setMinorFragmentId(buildMinorFragment).build();

            TunnelProvider tunnelProvider = mock(TunnelProvider.class);
            AccountingExecTunnel tunnel = mock(AccountingExecTunnel.class);
            doNothing().when(tunnel).sendOOBMessage(any(OutOfBandMessage.class));

            when(tunnelProvider.getExecTunnel(any(NodeEndpoint.class))).thenReturn(tunnel);
            OperatorContext opCtx = mockOpContext(fh);
            EndpointsIndex ei = mock(EndpointsIndex.class);
            NodeEndpoint node1 = NodeEndpoint.newBuilder().build();
            when(ei.getNodeEndpoint(any(Integer.class))).thenReturn(node1);
            when(opCtx.getEndpointsIndex()).thenReturn(ei);
            when(opCtx.getTunnelProvider()).thenReturn(tunnelProvider);

            HashJoinPOP popConfig = mockPopConfig(newRuntimeFilterInfo(false, "col1"));
            OpProps props = mock(OpProps.class);
            when(props.getOperatorId()).thenReturn(buildOpId);
            when(popConfig.getProps()).thenReturn(props);
            VectorizedHashJoinOperator joinOp = spy(new VectorizedHashJoinOperator(opCtx, popConfig));

            BloomFilter bloomFilter = mockedBloom().get();
            when(bloomFilter.getDataBuffer()).thenReturn(recvBuffer);
            when(bloomFilter.getExpectedFPP()).thenReturn(0.001D);

            RuntimeFilter filter = RuntimeFilter.newBuilder()
                    .setProbeScanMajorFragmentId(probeScanId)
                    .setProbeScanOperatorId(probeOpId)
                    .setPartitionColumnFilter(ExecProtos.CompositeColumnFilter.newBuilder()
                            .addColumns("col1")
                            .setFilterType(ExecProtos.RuntimeFilterType.BLOOM_FILTER)
                            .setSizeBytes(64).build())
                    .build();
            FragmentAssignment assignment1 = FragmentAssignment.newBuilder()
                    .addAllMinorFragmentId(Lists.newArrayList(1, 3, 5)).setAssignmentIndex(1).build();
            FragmentAssignment assignment2 = FragmentAssignment.newBuilder()
                    .addAllMinorFragmentId(Lists.newArrayList(0, 2, 4)).setAssignmentIndex(2).build();
            MajorFragmentAssignment majorFragmentAssignment = MajorFragmentAssignment.newBuilder()
                    .setMajorFragmentId(probeScanId)
                    .addAllAllAssignment(Lists.newArrayList(assignment1, assignment2))
                    .build();
            when(opCtx.getExtMajorFragmentAssignments(eq(probeScanId))).thenReturn(majorFragmentAssignment);
            ArgumentCaptor<OutOfBandMessage> oobMessageCaptor = ArgumentCaptor.forClass(OutOfBandMessage.class);
            joinOp.sendRuntimeFilterToProbeScan(filter, Optional.of(bloomFilter), new ArrayList<>());

            verify(tunnel, times(2)).sendOOBMessage(oobMessageCaptor.capture());
            for (int assignment = 0; assignment <= 1; assignment++) {
                assertEquals(queryId, oobMessageCaptor.getAllValues().get(assignment).getQueryId());
                assertEquals(probeScanId, oobMessageCaptor.getAllValues().get(assignment).getMajorFragmentId());
                assertEquals(probeOpId, oobMessageCaptor.getAllValues().get(assignment).getOperatorId());
                assertEquals(buildMajorFragment, oobMessageCaptor.getAllValues().get(assignment).getSendingMajorFragmentId());
                assertEquals(buildMinorFragment, oobMessageCaptor.getAllValues().get(assignment).getSendingMinorFragmentId());
            }

            assertEquals(Lists.newArrayList(1,3,5), oobMessageCaptor.getAllValues().get(0).getTargetMinorFragmentIds());
            assertEquals(Lists.newArrayList(0,2,4), oobMessageCaptor.getAllValues().get(1).getTargetMinorFragmentIds());
            assertEquals(2, recvBuffer.refCnt()); // Each message retains separately

            recvBuffer.close(); // close extra buffer
        }
    }

    @Test
    public void testSendRuntimeFilterToProbeScanMajorFragmentNotPresent() throws Exception {
        int probeScanId = 2; // no major fragment present on this id. Expect send to be silently skipped.
        int differentProbeScanId = 5; // major fragment will be present only for this ID
        int probeOpId = 131074;
        int buildMajorFragment = 1;
        int buildMinorFragment = 2;
        int buildOpId = 65541;
        QueryId queryId = QueryId.newBuilder().build();
        ArrowBuf recvBuffer = testAllocator.buffer(64);
        FragmentHandle fh = FragmentHandle.newBuilder()
                .setQueryId(queryId).setMajorFragmentId(buildMajorFragment).setMinorFragmentId(buildMinorFragment).build();

        TunnelProvider tunnelProvider = mock(TunnelProvider.class);
        AccountingExecTunnel tunnel = mock(AccountingExecTunnel.class);
        doNothing().when(tunnel).sendOOBMessage(any(OutOfBandMessage.class));

        when(tunnelProvider.getExecTunnel(any(NodeEndpoint.class))).thenReturn(tunnel);
        OperatorContext opCtx = mockOpContext(fh);
        EndpointsIndex ei = mock(EndpointsIndex.class);
        NodeEndpoint node1 = NodeEndpoint.newBuilder().build();
        when(ei.getNodeEndpoint(any(Integer.class))).thenReturn(node1);
        when(opCtx.getEndpointsIndex()).thenReturn(ei);
        when(opCtx.getTunnelProvider()).thenReturn(tunnelProvider);

        HashJoinPOP popConfig = mockPopConfig(newRuntimeFilterInfo(false, "col1"));
        OpProps props = mock(OpProps.class);
        when(props.getOperatorId()).thenReturn(buildOpId);
        when(popConfig.getProps()).thenReturn(props);
        VectorizedHashJoinOperator joinOp = spy(new VectorizedHashJoinOperator(opCtx, popConfig));

        BloomFilter bloomFilter = mockedBloom().get();
        when(bloomFilter.getDataBuffer()).thenReturn(recvBuffer);
        when(bloomFilter.getExpectedFPP()).thenReturn(0.001D);

        RuntimeFilter filter = RuntimeFilter.newBuilder()
                .setProbeScanMajorFragmentId(probeScanId)
                .setProbeScanOperatorId(probeOpId)
                .setPartitionColumnFilter(ExecProtos.CompositeColumnFilter.newBuilder()
                        .addColumns("col1")
                        .setFilterType(ExecProtos.RuntimeFilterType.BLOOM_FILTER)
                        .setSizeBytes(64).build())
                .build();
        FragmentAssignment assignment1 = FragmentAssignment.newBuilder()
                .addAllMinorFragmentId(Lists.newArrayList(1, 3, 5)).setAssignmentIndex(1).build();
        FragmentAssignment assignment2 = FragmentAssignment.newBuilder()
                .addAllMinorFragmentId(Lists.newArrayList(0, 2, 4)).setAssignmentIndex(2).build();
        MajorFragmentAssignment majorFragmentAssignment = MajorFragmentAssignment.newBuilder()
                .setMajorFragmentId(differentProbeScanId)
                .addAllAllAssignment(Lists.newArrayList(assignment1, assignment2))
                .build();
        when(opCtx.getExtMajorFragmentAssignments(eq(differentProbeScanId))).thenReturn(majorFragmentAssignment);
        when(opCtx.getExtMajorFragmentAssignments(eq(probeScanId))).thenReturn(null);
        ArgumentCaptor<OutOfBandMessage> oobMessageCaptor = ArgumentCaptor.forClass(OutOfBandMessage.class);
        joinOp.sendRuntimeFilterToProbeScan(filter, Optional.of(bloomFilter), new ArrayList<>());

        verify(tunnel, never()).sendOOBMessage(oobMessageCaptor.capture());
    }

    private OutOfBandMessage runtimeFilterOOBFromMinorFragment(int sendingMinorFragment, ArrowBuf buf, String... col) {
        List<Integer> allFragments = Lists.newArrayList(1,2,3,4);
        allFragments.removeIf(val -> val == sendingMinorFragment);
        RuntimeFilter filter = RuntimeFilter.newBuilder().setProbeScanOperatorId(101).setProbeScanMajorFragmentId(1)
                .setPartitionColumnFilter(ExecProtos.CompositeColumnFilter.newBuilder().setSizeBytes(64).addAllColumns(Lists.newArrayList(col)).build())
                .build();
        ArrowBuf[] bufs = new ArrowBuf[]{buf};
        return new OutOfBandMessage(null, 1, allFragments, 101, 1, sendingMinorFragment,
                101, new OutOfBandMessage.Payload(filter), bufs, true);
    }

    private Optional<BloomFilter> mockedBloom() {
        BloomFilter bloom = mock(BloomFilter.class);
        when(bloom.isCrossingMaxFPP()).thenReturn(Boolean.FALSE);
        when(bloom.getSizeInBytes()).thenReturn(64L);
        doNothing().when(bloom).merge(any(BloomFilter.class));
        return Optional.of(bloom);
    }

    private ValueListFilter mockedValueListFilter() {
        ValueListFilter valueListFilter = mock(ValueListFilter.class);
        when(valueListFilter.getValueCount()).thenReturn(21);
        when(valueListFilter.getSizeInBytes()).thenReturn(115L);
        when(valueListFilter.getBlockSize()).thenReturn((byte) 4);
        doNothing().when(valueListFilter).setFieldName(anyString());
        return valueListFilter;
    }

    private RuntimeFilterInfo newRuntimeFilterInfo(boolean isBroadcast, String... partitionCols) {
        return newRuntimeFilterInfo(isBroadcast, Arrays.asList(partitionCols), Collections.EMPTY_LIST);
    }

    private RuntimeFilterInfo newRuntimeFilterInfo(boolean isBroadcast, List<String> partitionCols, List<String> nonPartitionCols) {
        final List<RuntimeFilterEntry> partitionColEntries = partitionCols.stream()
                .map(c -> new RuntimeFilterEntry(c + "_probe", c + "_build", 1, 101))
                .collect(Collectors.toList());
        final List<RuntimeFilterEntry> nonPartitionColEntries = nonPartitionCols.stream()
                .map(c -> new RuntimeFilterEntry(c + "_probe", c + "_build", 1, 101))
                .collect(Collectors.toList());
        return new RuntimeFilterInfo.Builder().isBroadcastJoin(isBroadcast)
                .partitionJoinColumns(partitionColEntries).nonPartitionJoinColumns(nonPartitionColEntries).build();
    }

    private OperatorContext mockOpContext(FragmentHandle fragmentHandle) {
        OptionManager optionManager = mock(OptionManager.class);
        when(optionManager.getOption(eq(ExecConstants.RUNTIME_FILTER_VALUE_FILTER_MAX_SIZE))).thenReturn(1000l);
        when(optionManager.getOption(eq(ExecConstants.ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET))).thenReturn(true);

        OperatorContext context = mock(OperatorContext.class);
        when(context.getFragmentHandle()).thenReturn(fragmentHandle);
        when(context.getAllocator()).thenReturn(testAllocator);
        when(context.getOptions()).thenReturn(optionManager);
        FragmentAssignment assignment = FragmentAssignment.newBuilder().addAllMinorFragmentId(Lists.newArrayList(1,2,3,4)).build();
        when(context.getAssignments()).thenReturn(Lists.newArrayList(assignment));
        return context;
    }

    private HashJoinPOP mockPopConfig(RuntimeFilterInfo filterInfo) {
        HashJoinPOP popConfig = mock(HashJoinPOP.class);
        when(popConfig.getRuntimeFilterInfo()).thenReturn(filterInfo);
        return popConfig;
    }

    private VectorizedHashJoinOperator newVecHashJoinOp(RuntimeFilterInfo filterInfo, FragmentHandle fragmentHandle) {
        return new VectorizedHashJoinOperator(mockOpContext(fragmentHandle), mockPopConfig(filterInfo));
    }
}

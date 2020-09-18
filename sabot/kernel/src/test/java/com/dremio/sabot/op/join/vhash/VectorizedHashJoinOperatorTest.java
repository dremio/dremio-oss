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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

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
    private BufferAllocator bfTestAllocator;

    @Rule
    public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

    @Before
    public void setupBeforeTest() {
        bfTestAllocator = allocatorRule.newAllocator("test-vectorized_hash_join_operator", 0, Long.MAX_VALUE);
    }

    @After
    public void cleanupAfterTest() {
        bfTestAllocator.close();
    }


    @Test
    public void testTryPushRuntimeFilterNonSendingFragmentBroadcastJoin() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(4).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(true, "col1"), fh));
        JoinTable joinTable = mock(JoinTable.class);
        when(joinTable.prepareBloomFilter(any(List.class), eq(true))).thenReturn(Optional.empty());
        joinOp.setTable(joinTable);

        joinOp.tryPushRuntimeFilter();
        verify(joinOp, never()).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class));
        verify(joinOp, never()).sendRuntimeFilterToProbeScan(any(RuntimeFilter.class), any(Optional.class));
    }

    @Test
    public void testTryPushRuntimeFilterBroadcastJoin() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(true, "col1"), fh));

        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class));

        JoinTable joinTable = mock(JoinTable.class);
        Optional<BloomFilter> bloomFilter = mockedBloom();
        when(joinTable.prepareBloomFilter(any(List.class), eq(true))).thenReturn(bloomFilter);
        joinOp.setTable(joinTable);

        joinOp.tryPushRuntimeFilter();
        verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class));
        verify(joinOp, never()).sendRuntimeFilterAtMergePoints(valCaptor.capture(), any(Optional.class));

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals("col1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
        assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());
    }

    @Test
    public void testTryPushRuntimeFilterNonSendingFragmentShuffleJoin() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(4).build();

        VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false, "col1"), fh));
        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(valCaptor.capture(), any(Optional.class));

        JoinTable joinTable = mock(JoinTable.class);
        Optional<BloomFilter> bloomFilter = mockedBloom();
        when(joinTable.prepareBloomFilter(any(List.class), eq(false))).thenReturn(bloomFilter);
        joinOp.setTable(joinTable);

        joinOp.tryPushRuntimeFilter();
        verify(joinOp, times(1)).sendRuntimeFilterAtMergePoints(valCaptor.capture(), any(Optional.class));

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals("col1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
        assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());
    }

    @Test
    public void testTryPushRuntimeFilterShuffleJoin() throws Exception {
        try (ArrowBuf recvBuffer = bfTestAllocator.buffer(64)) {
            recvBuffer.setBytes(0, new byte[64]); // set all bytes to zero
            FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
            VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false, "col1"), fh));

            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class));
            doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class));

            JoinTable joinTable = mock(JoinTable.class);
            BloomFilter bloomFilter = mockedBloom().get();
            when(bloomFilter.getDataBuffer()).thenReturn(recvBuffer);
            when(joinTable.prepareBloomFilter(any(List.class), eq(false))).thenReturn(Optional.of(bloomFilter));
            joinOp.setTable(joinTable);

            joinOp.tryPushRuntimeFilter();

            // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
            for (int sendingFragment = 2; sendingFragment <= 4; sendingFragment++) {
                OutOfBandMessage oobMsg = runtimeFilterOOBFromMinorFragment(sendingFragment, recvBuffer, "col1");
                joinOp.workOnOOB(oobMsg);
                oobMsg.getBuffer().release();
            }
            recvBuffer.release();  // tryPushRuntimeFilter() didn't close its copy as sendRuntimeFilterAtMergePoints() is mocked.

            // verify filter sent to probe scan.
            verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class));

            RuntimeFilter filterVal = valCaptor.getValue();
            assertEquals(1, filterVal.getProbeScanMajorFragmentId());
            assertEquals(101, filterVal.getProbeScanOperatorId());
            assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
            assertEquals("col1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
            assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());
        }
    }

    @Test
    public void testTryPushRuntimeFilterShuffleJoinIsLastPiece() throws Exception {
        try (ArrowBuf recvBuffer = bfTestAllocator.buffer(64)) {
            recvBuffer.setBytes(0, new byte[64]); // set all bytes to zero
            FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
            VectorizedHashJoinOperator joinOp = spy(newVecHashJoinOp(newRuntimeFilterInfo(false, "col1"), fh));

            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            doNothing().when(joinOp).sendRuntimeFilterAtMergePoints(any(RuntimeFilter.class), any(Optional.class));
            doNothing().when(joinOp).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class));

            JoinTable joinTable = mock(JoinTable.class);
            BloomFilter bloomFilter = BloomFilter.prepareFrom(recvBuffer);
            when(joinTable.prepareBloomFilter(any(List.class), eq(false))).thenReturn(Optional.of(bloomFilter));
            joinOp.setTable(joinTable);

            for (int sendingFragment = 2; sendingFragment <= 4; sendingFragment++) {
                OutOfBandMessage oobMsg = runtimeFilterOOBFromMinorFragment(sendingFragment, recvBuffer, "col1");
                joinOp.workOnOOB(oobMsg);
                oobMsg.getBuffer().release();
            }

            joinOp.tryPushRuntimeFilter();

            // verify filter sent to probe scan.
            verify(joinOp, times(1)).sendRuntimeFilterToProbeScan(valCaptor.capture(), any(Optional.class));

            RuntimeFilter filterVal = valCaptor.getValue();
            assertEquals(1, filterVal.getProbeScanMajorFragmentId());
            assertEquals(101, filterVal.getProbeScanOperatorId());
            assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
            assertEquals("col1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
            assertEquals(64, filterVal.getPartitionColumnFilter().getSizeBytes());
        }
    }

    @Test
    public void testSendRuntimeFilterToProbeScan() {
        int probeScanId = 2;
        int probeOpId = 131074;
        int buildMajorFragment = 1;
        int buildMinorFragment = 2;
        int buildOpId = 65541;
        QueryId queryId = QueryId.newBuilder().build();
        try (ArrowBuf recvBuffer = bfTestAllocator.buffer(64)) {
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
            joinOp.sendRuntimeFilterToProbeScan(filter, Optional.of(bloomFilter));

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
    public void testSendRuntimeFilterToProbeScanMajorFragmentNotPresent() {
        int probeScanId = 2; // no major fragment present on this id. Expect send to be silently skipped.
        int differentProbeScanId = 5; // major fragment will be present only for this ID
        int probeOpId = 131074;
        int buildMajorFragment = 1;
        int buildMinorFragment = 2;
        int buildOpId = 65541;
        QueryId queryId = QueryId.newBuilder().build();
        ArrowBuf recvBuffer = bfTestAllocator.buffer(64);
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
        joinOp.sendRuntimeFilterToProbeScan(filter, Optional.of(bloomFilter));

        verify(tunnel, never()).sendOOBMessage(oobMessageCaptor.capture());
    }

    private OutOfBandMessage runtimeFilterOOBFromMinorFragment(int sendingMinorFragment, ArrowBuf buf, String... col) {
        List<Integer> allFragments = Lists.newArrayList(1,2,3,4);
        allFragments.removeIf(val -> val == sendingMinorFragment);
        RuntimeFilter filter = RuntimeFilter.newBuilder().setProbeScanOperatorId(101).setProbeScanMajorFragmentId(1)
                .setPartitionColumnFilter(ExecProtos.CompositeColumnFilter.newBuilder().setSizeBytes(64).addAllColumns(Lists.newArrayList(col)).build())
                .build();
        return new OutOfBandMessage(null, 1, allFragments, 101, 1, sendingMinorFragment,
                101, new OutOfBandMessage.Payload(filter), buf, true);
    }

    private Optional<BloomFilter> mockedBloom() {
        BloomFilter bloom = mock(BloomFilter.class);
        when(bloom.isCrossingMaxFPP()).thenReturn(Boolean.FALSE);
        when(bloom.getSizeInBytes()).thenReturn(64L);
        doNothing().when(bloom).merge(any(BloomFilter.class));
        return Optional.of(bloom);
    }

    private RuntimeFilterInfo newRuntimeFilterInfo(boolean isBroadcast, String... cols) {
        List<RuntimeFilterEntry> partitionCols = new ArrayList<>();
        for (String col : cols) {
            partitionCols.add(new RuntimeFilterEntry(col + "_probe", col + "_build", 1, 101));
        }
        return new RuntimeFilterInfo.Builder().isBroadcastJoin(isBroadcast)
                .partitionJoinColumns(partitionCols).build();
    }

    private OperatorContext mockOpContext(FragmentHandle fragmentHandle) {
        OperatorContext context = mock(OperatorContext.class);

        when(context.getFragmentHandle()).thenReturn(fragmentHandle);
        when(context.getAllocator()).thenReturn(bfTestAllocator);
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

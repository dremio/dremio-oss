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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.physical.config.RuntimeFilterProbeTarget;
import com.dremio.exec.planner.fragment.EndpointsIndex;
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
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VectorPivotDef;
import com.dremio.test.AllocatorRule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests for {@link VectorizedHashJoinOperator}
 */
public class VectorizedHashJoinOperatorTest {
    private RuntimeFilterTestUtils utils;
    private BufferAllocator testAllocator;
    private List<ValueListFilter> valueListFiltersCopy;

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
    public void testTryPushRuntimeFilterPartitionColOnlyBroadcastJoin() throws Exception {
      FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
      ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
      RuntimeFilterInfo runtimeFilterInfo = newRuntimeFilterInfo(true, "col1");
      try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(runtimeFilterInfo, fh);
           MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {
        joinOp.tryPushRuntimeFilter();

        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), times(1));
        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterAtMergePoints(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), never());

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals("col1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
        assertEquals(256, filterVal.getPartitionColumnFilter().getSizeBytes());
      }
    }

    @Test
    public void testTryPushRuntimeFilterNonPartitionColOnlyBroadcastJoin() throws Exception {
      FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
      RuntimeFilterInfo runtimeFilterInfo = newRuntimeFilterInfo(true, Collections.EMPTY_LIST,
        Lists.newArrayList("col1"));
      ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
      try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(runtimeFilterInfo, fh);
           MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {
        copyValueListFiltersFromProbeScan(runtimeFilterUtilMockedStatic, valCaptor);

        joinOp.tryPushRuntimeFilter();

        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), times(1));
        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterAtMergePoints(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), never());

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(0, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals(1, filterVal.getNonPartitionColumnFilterCount());
        assertEquals("col1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
        assertEquals(valueListFiltersCopy.get(0).getSizeInBytes(), filterVal.getNonPartitionColumnFilter(0).getSizeBytes());
        assertEquals(valueListFiltersCopy.get(0).getValueCount(), filterVal.getNonPartitionColumnFilter(0).getValueCount());
        AutoCloseables.close(valueListFiltersCopy);
      }
    }

    @Test
    public void testTryPushRuntimeFilterBothColsBroadcastJoin() throws Exception {
      FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
      ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
      RuntimeFilterInfo runtimeFilterInfo = newRuntimeFilterInfo(true,
        Lists.newArrayList("pCol1", "pCol2"), Lists.newArrayList("npCol1", "npCol2"));

      try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(runtimeFilterInfo, fh);
           MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {
        copyValueListFiltersFromProbeScan(runtimeFilterUtilMockedStatic, valCaptor);

        joinOp.tryPushRuntimeFilter();

        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), times(1));
        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterAtMergePoints(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), never());

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(2, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals("pCol1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
        assertEquals("pCol2_probe", filterVal.getPartitionColumnFilter().getColumns(1));
        assertEquals(256, filterVal.getPartitionColumnFilter().getSizeBytes());

        assertEquals(2, filterVal.getNonPartitionColumnFilterCount());
        assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
        assertEquals(1, filterVal.getNonPartitionColumnFilter(1).getColumnsCount());
        assertEquals("npCol1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
        assertEquals("npCol2_probe", filterVal.getNonPartitionColumnFilter(1).getColumns(0));

        assertEquals(valueListFiltersCopy.get(1).getSizeInBytes(), filterVal.getNonPartitionColumnFilter(0).getSizeBytes());
        assertEquals(valueListFiltersCopy.get(0).getValueCount(), filterVal.getNonPartitionColumnFilter(1).getValueCount());
        AutoCloseables.close(valueListFiltersCopy);
      }
    }

    @Test
    public void testTryPushRuntimeFilterAllColsDropped() throws Exception {
      FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
      ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
      RuntimeFilterInfo runtimeFilterInfo = newRuntimeFilterInfo(true,
        Lists.newArrayList("pCol1", "pCol2"), Lists.newArrayList("npCol1", "npCol2"));
      try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(runtimeFilterInfo, fh);
           MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {

        PartitionColFilters partitionColFilters = mock(PartitionColFilters.class);
        doReturn(partitionColFilters).when(joinOp).createPartitionColFilters();
        when(partitionColFilters.getBloomFilter(anyInt(), any(RuntimeFilterProbeTarget.class)))
          .thenReturn(Optional.empty());

        NonPartitionColFilters nonPartitionColFilters = mock(NonPartitionColFilters.class);
        doReturn(nonPartitionColFilters).when(joinOp).createNonPartitionColFilters();
        doNothing().when(nonPartitionColFilters).finalizeValueListFilters();
        when(nonPartitionColFilters.getValueListFilters(anyInt(), any(RuntimeFilterProbeTarget.class)))
          .thenReturn(Collections.emptyList());

        joinOp.tryPushRuntimeFilter();

        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), never());
        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterAtMergePoints(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), never());
      }
    }

    @Test
    public void testTryPushRuntimeFilterNonPartitionColFlagDisabled1() throws Exception {
      FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
      ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
      RuntimeFilterInfo runtimeFilterInfo = newRuntimeFilterInfo(true,
        Lists.newArrayList("pCol1", "pCol2"),
        Lists.newArrayList("npCol1", "npCol2"));
      try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(runtimeFilterInfo, fh);
           MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {

        runtimeFilterUtilMockedStatic.when(() -> RuntimeFilterUtil.isRuntimeFilterEnabledForNonPartitionedCols(
          any(OperatorContext.class))).thenReturn(false);

        joinOp.tryPushRuntimeFilter();

        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), times(1));
        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterAtMergePoints(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), never());

        verify(joinOp.getTable(), never()).prepareValueListFilters(any(NonPartitionColFilters.class));

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(2, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals("pCol1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
        assertEquals("pCol2_probe", filterVal.getPartitionColumnFilter().getColumns(1));
        assertEquals(256, filterVal.getPartitionColumnFilter().getSizeBytes());
        assertEquals(0, filterVal.getNonPartitionColumnFilterCount());
      }
    }

    @Test
    public void testTryPushRuntimeFilterNonPartitionColFlagDisabled2() throws Exception {
      FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
      ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
      RuntimeFilterInfo runtimeFilterInfo = newRuntimeFilterInfo(true,Collections.EMPTY_LIST,
      Lists.newArrayList("npCol1", "npCol2"));
      try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(runtimeFilterInfo, fh);
           MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {

        runtimeFilterUtilMockedStatic.when(() -> RuntimeFilterUtil.isRuntimeFilterEnabledForNonPartitionedCols(
          any(OperatorContext.class))).thenReturn(false);

        joinOp.tryPushRuntimeFilter();

        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), never());
        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterAtMergePoints(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), never());

        verify(joinOp.getTable(), never()).prepareValueListFilters(any(NonPartitionColFilters.class));
      }
    }

    @Test
    public void testTryPushRuntimeFilterNonSendingFragmentShuffleJoin() throws Exception {
      FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(4).build();
      RuntimeFilterInfo runtimeFilterInfo = newRuntimeFilterInfo(false,
        Lists.newArrayList("pCol1", "pCol2"), Lists.newArrayList("npCol1", "npCol2"));

      ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
      try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(runtimeFilterInfo, fh);
           MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {
        copyValueListFiltersAtMergePoints(runtimeFilterUtilMockedStatic, valCaptor);

        joinOp.tryPushRuntimeFilter();

        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterAtMergePoints(
          any(RuntimeFilter.class), any(Optional.class), anyList(),
          any(OperatorContext.class),any(HashJoinPOP.class)), times(1));

        RuntimeFilter filterVal = valCaptor.getValue();
        assertEquals(1, filterVal.getProbeScanMajorFragmentId());
        assertEquals(101, filterVal.getProbeScanOperatorId());
        assertEquals(2, filterVal.getPartitionColumnFilter().getColumnsCount());
        assertEquals("pCol1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
        assertEquals("pCol2_probe", filterVal.getPartitionColumnFilter().getColumns(1));
        assertEquals(2097152, filterVal.getPartitionColumnFilter().getSizeBytes());

        assertEquals(2, filterVal.getNonPartitionColumnFilterCount());
        assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
        assertEquals(1, filterVal.getNonPartitionColumnFilter(1).getColumnsCount());
        assertEquals("npCol1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
        assertEquals("npCol2_probe", filterVal.getNonPartitionColumnFilter(1).getColumns(0));
        assertEquals(valueListFiltersCopy.get(0).getSizeInBytes(), filterVal.getNonPartitionColumnFilter(0).getSizeBytes());
        assertEquals(valueListFiltersCopy.get(1).getValueCount(), filterVal.getNonPartitionColumnFilter(1).getValueCount());
        AutoCloseables.close(valueListFiltersCopy);
      }
    }

    @Test
    public void testTryPushRuntimeFilterPartitionColOnlyShuffleJoin() throws Exception {
        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();
        RuntimeFilterInfo runtimeFilterInfo = newRuntimeFilterInfo(false, "col1");
        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        try (ArrowBuf recvBuffer = testAllocator.buffer(2 * 1024 * 1024);
             VectorizedHashJoinOperator joinOp = newVecHashJoinOp(runtimeFilterInfo, fh);
             MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {
            recvBuffer.setZero(0, recvBuffer.capacity()); // set all bytes to zero

            joinOp.tryPushRuntimeFilter();

            // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
            for (int sendingFragment = 2; sendingFragment <= 4; sendingFragment++) {
                OutOfBandMessage oobMsg = runtimeFilterOOBFromMinorFragment(sendingFragment, recvBuffer, "col1");
                joinOp.workOnOOB(oobMsg);
                Arrays.stream(oobMsg.getBuffers()).forEach(ArrowBuf::close);
            }

            // verify filter sent to probe scan.
            runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
              any(RuntimeFilter.class), any(Optional.class), anyList(),
              any(OperatorContext.class),any(HashJoinPOP.class)), times(1));

            RuntimeFilter filterVal = valCaptor.getValue();
            assertEquals(1, filterVal.getProbeScanMajorFragmentId());
            assertEquals(101, filterVal.getProbeScanOperatorId());
            assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
            assertEquals("col1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
            assertEquals(2097152, filterVal.getPartitionColumnFilter().getSizeBytes());
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
        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(newRuntimeFilterInfo(false,
                Collections.EMPTY_LIST, Lists.newArrayList("col1", "col2")), fh);
             MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {

            NonPartitionColFilters nonPartitionColFilters = mock(NonPartitionColFilters.class);
            doReturn(nonPartitionColFilters).when(joinOp).createNonPartitionColFilters();
            doNothing().when(nonPartitionColFilters).finalizeValueListFilters();
            List<ValueListFilter> valueListFilterList = Arrays.asList(valueListFilter1, valueListFilter2);
            when(nonPartitionColFilters.getValueListFilters(anyInt(), any(RuntimeFilterProbeTarget.class)))
              .thenReturn(valueListFilterList);

            copyValueListFiltersFromProbeScan(runtimeFilterUtilMockedStatic, valCaptor);

            joinOp.tryPushRuntimeFilter();

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

            AutoCloseables.close(valueListFilter1, valueListFilter2); // sendFilterToMergePoints is expected to close; mocked here.o

            // verify filter sent to probe scan.
            runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
              any(RuntimeFilter.class), any(Optional.class), anyList(),
              any(OperatorContext.class),any(HashJoinPOP.class)),
              times(1));

            RuntimeFilter filterVal = valCaptor.getValue();
            assertEquals(1, filterVal.getProbeScanMajorFragmentId());
            assertEquals(101, filterVal.getProbeScanOperatorId());
            assertEquals(0, filterVal.getPartitionColumnFilter().getColumnsCount());

            assertEquals(2, filterVal.getNonPartitionColumnFilterCount());
            assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
            assertEquals("col1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
            assertEquals(1, filterVal.getNonPartitionColumnFilter(1).getColumnsCount());
            assertEquals("col2_probe", filterVal.getNonPartitionColumnFilter(1).getColumns(0));

            assertEquals(2, valueListFiltersCopy.size());
            assertTrue(valueListFiltersCopy.get(0).isContainsNull());
            assertEquals(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9), utils.getValues(valueListFiltersCopy.get(0)));
            assertFalse(valueListFiltersCopy.get(1).isContainsNull());
            assertEquals(Lists.newArrayList(11, 12, 13, 14, 15, 16, 17, 18, 19), utils.getValues(valueListFiltersCopy.get(1)));
            AutoCloseables.close(valueListFiltersCopy);
        }
    }

    @Test
    public void testTryPushRuntimeFilterBothColsShuffleJoin() throws Exception {
        try (ArrowBuf recvBuffer = testAllocator.buffer(2 * 1024 * 1024)) {
            recvBuffer.setZero(0, recvBuffer.capacity()); // set all bytes to zero

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
            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(newRuntimeFilterInfo(false,
                    partitionColNames, Lists.newArrayList("col1", "col2")), fh);

                 MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {

                NonPartitionColFilters nonPartitionColFilters = mock(NonPartitionColFilters.class);
                doReturn(nonPartitionColFilters).when(joinOp).createNonPartitionColFilters();
                doNothing().when(nonPartitionColFilters).finalizeValueListFilters();
                List<ValueListFilter> valueListFilterList = Arrays.asList(valueListFilter1, valueListFilter2);
                when(nonPartitionColFilters.getValueListFilters(anyInt(), any(RuntimeFilterProbeTarget.class)))
                  .thenReturn(valueListFilterList);

                copyValueListFiltersFromProbeScan(runtimeFilterUtilMockedStatic, valCaptor);

                joinOp.tryPushRuntimeFilter();

                // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
                recvBuffer.getReferenceManager().retain(3);
                OutOfBandMessage oobMsg2 = utils.newOOB(11, 101, 2, partitionColNames, recvBuffer, valueListFilter3, valueListFilter4);
                OutOfBandMessage oobMsg3 = utils.newOOB(11, 101, 3, partitionColNames, recvBuffer, valueListFilter5, valueListFilter6);
                OutOfBandMessage oobMsg4 = utils.newOOB(11, 101, 4, partitionColNames, recvBuffer, valueListFilter7, valueListFilter8);

                joinOp.workOnOOB(oobMsg2);
                joinOp.workOnOOB(oobMsg3);
                joinOp.workOnOOB(oobMsg4);

                Arrays.stream(oobMsg2.getBuffers()).forEach(ArrowBuf::close);
                Arrays.stream(oobMsg3.getBuffers()).forEach(ArrowBuf::close);
                Arrays.stream(oobMsg4.getBuffers()).forEach(ArrowBuf::close);

                AutoCloseables.close(valueListFilter1, valueListFilter2); // sendRuntimeFilterAtMergePoints is mocked, which would have closed these.

                // verify filter sent to probe scan.
                runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
                  any(RuntimeFilter.class), any(Optional.class), anyList(),
                  any(OperatorContext.class),any(HashJoinPOP.class)),
                  times(1));

                RuntimeFilter filterVal = valCaptor.getValue();
                assertEquals(1, filterVal.getProbeScanMajorFragmentId());
                assertEquals(101, filterVal.getProbeScanOperatorId());
                assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
                assertEquals("pCol1_probe", filterVal.getPartitionColumnFilter().getColumns(0));
                assertEquals(2097152, filterVal.getPartitionColumnFilter().getSizeBytes());

                assertEquals(2, filterVal.getNonPartitionColumnFilterCount());
                assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
                assertEquals("col1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
                assertEquals(1, filterVal.getNonPartitionColumnFilter(1).getColumnsCount());
                assertEquals("col2_probe", filterVal.getNonPartitionColumnFilter(1).getColumns(0));

                assertEquals(2, valueListFiltersCopy.size());
                assertTrue(valueListFiltersCopy.get(0).isContainsNull());
                assertEquals(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9), utils.getValues(valueListFiltersCopy.get(0)));
                assertFalse(valueListFiltersCopy.get(1).isContainsNull());
                assertEquals(Lists.newArrayList(11, 12, 13, 14, 15, 16, 17, 18, 19), utils.getValues(valueListFiltersCopy.get(1)));
                AutoCloseables.close(valueListFiltersCopy);
            }
        }
    }

    @Test
    public void testTryPushRuntimeFilterBooleanNonPartitionShuffleJoin() throws Exception {
        try (ArrowBuf recvBuffer = testAllocator.buffer(2 * 1024 * 1024)) {
            recvBuffer.setZero(0, recvBuffer.capacity()); // set all bytes to zero

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

            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);

            try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(newRuntimeFilterInfo(false,
                    partitionColNames, Lists.newArrayList("col1", "col2")), fh);

                 MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {

                NonPartitionColFilters nonPartitionColFilters = mock(NonPartitionColFilters.class);
                doReturn(nonPartitionColFilters).when(joinOp).createNonPartitionColFilters();
                doNothing().when(nonPartitionColFilters).finalizeValueListFilters();
                List<ValueListFilter> valueListFilterList = Arrays.asList(valueListFilter1, valueListFilter2);
                when(nonPartitionColFilters.getValueListFilters(anyInt(), any(RuntimeFilterProbeTarget.class)))
                  .thenReturn(valueListFilterList);

                joinOp.tryPushRuntimeFilter();

                AutoCloseables.close(valueListFilter1, valueListFilter2); // sendRuntimeFilterAtMergePoints is mocked, which would have closed these.

                // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
                recvBuffer.getReferenceManager().retain(3);
                OutOfBandMessage oobMsg2 = utils.newOOB(11, 101, 2, partitionColNames, recvBuffer, valueListFilter3, valueListFilter4);
                OutOfBandMessage oobMsg3 = utils.newOOB(11, 101, 3, partitionColNames, recvBuffer, valueListFilter5, valueListFilter6);
                OutOfBandMessage oobMsg4 = utils.newOOB(11, 101, 4, partitionColNames, recvBuffer, valueListFilter7, valueListFilter8);

                joinOp.workOnOOB(oobMsg2);
                joinOp.workOnOOB(oobMsg3);
                joinOp.workOnOOB(oobMsg4);

                Arrays.stream(oobMsg2.getBuffers()).forEach(ArrowBuf::close);
                Arrays.stream(oobMsg3.getBuffers()).forEach(ArrowBuf::close);
                Arrays.stream(oobMsg4.getBuffers()).forEach(ArrowBuf::close);

                ArgumentCaptor<List> valListCaptor = ArgumentCaptor.forClass(List.class);
                // verify filter sent to probe scan.
                runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
                  any(RuntimeFilter.class), any(Optional.class), valListCaptor.capture(),
                  any(OperatorContext.class),any(HashJoinPOP.class)),
                  times(1));

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
            }
        }
    }

    @Test
    public void testTryPushRuntimeFilterBooleanColumnDrop() throws Exception {
        try (ArrowBuf recvBuffer = testAllocator.buffer(2 * 1024 * 1024)) {
            recvBuffer.setZero(0, recvBuffer.capacity()); // set all bytes to zero

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
            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(newRuntimeFilterInfo(false,
                    partitionColNames, Lists.newArrayList("col1", "col2")), fh);
                 MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {

                 NonPartitionColFilters nonPartitionColFilters = mock(NonPartitionColFilters.class);
                 doReturn(nonPartitionColFilters).when(joinOp).createNonPartitionColFilters();
                 doNothing().when(nonPartitionColFilters).finalizeValueListFilters();
                 List<ValueListFilter> valueListFilterList = Arrays.asList(valueListFilter1, valueListFilter2);
                 when(nonPartitionColFilters.getValueListFilters(anyInt(), any(RuntimeFilterProbeTarget.class)))
                   .thenReturn(valueListFilterList);

                 copyValueListFiltersFromProbeScan(runtimeFilterUtilMockedStatic, valCaptor);

                 joinOp.tryPushRuntimeFilter();

                 AutoCloseables.close(valueListFilter1, valueListFilter2); // sendRuntimeFilterAtMergePoints is mocked, which would have closed these.

                 // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
                 recvBuffer.getReferenceManager().retain(3);
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
                 runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
                   any(RuntimeFilter.class), any(Optional.class), anyList(),
                   any(OperatorContext.class),any(HashJoinPOP.class)),
                   times(1));

                 RuntimeFilter filterVal = valCaptor.getValue();
                 assertEquals(1, filterVal.getNonPartitionColumnFilterCount());
                 assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
                 assertEquals("col2_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));

                 assertEquals(1, valueListFiltersCopy.size());
                 assertFalse(valueListFiltersCopy.get(0).isContainsNull());
                 assertFalse(valueListFiltersCopy.get(0).isContainsTrue());
                 assertTrue(valueListFiltersCopy.get(0).isContainsFalse());
                 AutoCloseables.close(valueListFiltersCopy);
            }
        }
    }

    @Test
    public void testTryPushRuntimeFilterShuffleJoinAllDroppedAtMerge() throws Exception {
        try (ArrowBuf recvBuffer = testAllocator.buffer(2 * 1024 * 1024)) {
            recvBuffer.setZero(0, recvBuffer.capacity()); // set all bytes to zero

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
            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(newRuntimeFilterInfo(false,
                    partitionColNames, Lists.newArrayList("col1", "col2")), fh);

                MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {

                PartitionColFilters partitionColFilters = mock(PartitionColFilters.class);
                doReturn(partitionColFilters).when(joinOp).createPartitionColFilters();
                BloomFilter bloomFilter = mockedBloom().get();
                when(partitionColFilters.getBloomFilter(anyInt(), any(RuntimeFilterProbeTarget.class)))
                  .thenReturn(Optional.of(bloomFilter));
                when(bloomFilter.isCrossingMaxFPP()).thenReturn(true); // To force drop

                NonPartitionColFilters nonPartitionColFilters = mock(NonPartitionColFilters.class);
                doReturn(nonPartitionColFilters).when(joinOp).createNonPartitionColFilters();
                doNothing().when(nonPartitionColFilters).finalizeValueListFilters();
                List<ValueListFilter> valueListFilterList = Arrays.asList(valueListFilter1, valueListFilter2);
                when(nonPartitionColFilters.getValueListFilters(anyInt(), any(RuntimeFilterProbeTarget.class))).
                  thenReturn(valueListFilterList);

                joinOp.tryPushRuntimeFilter();

                AutoCloseables.close(valueListFilter1, valueListFilter2); // sendRuntimeFilterAtMergePoints is mocked, which would have closed these.

                // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
                recvBuffer.getReferenceManager().retain(3);
                OutOfBandMessage oobMsg2 = utils.newOOB(11, 101, 2, partitionColNames, recvBuffer, valueListFilter3, valueListFilter4);
                OutOfBandMessage oobMsg3 = utils.newOOB(11, 101, 3, partitionColNames, recvBuffer, valueListFilter5, valueListFilter6);
                OutOfBandMessage oobMsg4 = utils.newOOB(11, 101, 4, partitionColNames, recvBuffer, valueListFilter7, valueListFilter8);

                joinOp.workOnOOB(oobMsg2);
                joinOp.workOnOOB(oobMsg3);
                joinOp.workOnOOB(oobMsg4);

                Arrays.stream(oobMsg2.getBuffers()).forEach(ArrowBuf::close);
                Arrays.stream(oobMsg3.getBuffers()).forEach(ArrowBuf::close);
                Arrays.stream(oobMsg4.getBuffers()).forEach(ArrowBuf::close);

                runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
                  any(RuntimeFilter.class), any(Optional.class), anyList(),
                  any(OperatorContext.class),any(HashJoinPOP.class)),
                  never());
            }
        }
    }

  @Test
  public void testTryPushRuntimeFilterShuffleJoinAllDroppedAtMergeBecauseOfZeroRefCount() throws Exception {
    try (ArrowBuf recvBuffer = testAllocator.buffer(2 * 1024 * 1024)) {
      recvBuffer.setZero(0, recvBuffer.capacity()); // set all bytes to zero

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
      ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
      try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(newRuntimeFilterInfo(false,
        partitionColNames, Lists.newArrayList("col1", "col2")), fh);

        MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {

        PartitionColFilters partitionColFilters = mock(PartitionColFilters.class);
        doReturn(partitionColFilters).when(joinOp).createPartitionColFilters();
        BloomFilter bloomFilter = mockedBloom().get();
        when(partitionColFilters.getBloomFilter(anyInt(), any(RuntimeFilterProbeTarget.class)))
          .thenReturn(Optional.of(bloomFilter));
        ArrowBuf mockBuf = bloomFilter.getDataBuffer();
        when(mockBuf.refCnt()).thenReturn(0); // To force drop

        NonPartitionColFilters nonPartitionColFilters = mock(NonPartitionColFilters.class);
        doReturn(nonPartitionColFilters).when(joinOp).createNonPartitionColFilters();
        doNothing().when(nonPartitionColFilters).finalizeValueListFilters();
        List<ValueListFilter> valueListFilterList = Arrays.asList(valueListFilter1, valueListFilter2);
        when(nonPartitionColFilters.getValueListFilters(anyInt(), any(RuntimeFilterProbeTarget.class))).
          thenReturn(valueListFilterList);

        joinOp.tryPushRuntimeFilter();

        AutoCloseables.close(valueListFilter1, valueListFilter2); // sendRuntimeFilterAtMergePoints is mocked, which would have closed these.

        // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
        recvBuffer.getReferenceManager().retain(3);
        OutOfBandMessage oobMsg2 = utils.newOOB(11, 101, 2, partitionColNames, recvBuffer, valueListFilter3, valueListFilter4);
        OutOfBandMessage oobMsg3 = utils.newOOB(11, 101, 3, partitionColNames, recvBuffer, valueListFilter5, valueListFilter6);
        OutOfBandMessage oobMsg4 = utils.newOOB(11, 101, 4, partitionColNames, recvBuffer, valueListFilter7, valueListFilter8);

        joinOp.workOnOOB(oobMsg2);
        joinOp.workOnOOB(oobMsg3);
        joinOp.workOnOOB(oobMsg4);

        Arrays.stream(oobMsg2.getBuffers()).forEach(ArrowBuf::close);
        Arrays.stream(oobMsg3.getBuffers()).forEach(ArrowBuf::close);
        Arrays.stream(oobMsg4.getBuffers()).forEach(ArrowBuf::close);

        runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
            any(RuntimeFilter.class), any(Optional.class), anyList(),
            any(OperatorContext.class),any(HashJoinPOP.class)),
          never());
      }
    }
  }

    @Test
    public void testTryPushRuntimeFilterShuffleJoinIsNotOnlyLastPiece() throws Exception {
        ArrowBuf recvBuffer = testAllocator.buffer(2 * 1024 * 1024);
        recvBuffer.setZero(0, recvBuffer.capacity()); // set all bytes to zero

        FragmentHandle fh = FragmentHandle.newBuilder().setMinorFragmentId(1).build();

        ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
        //Note: newRuntimeFilterInfo will change column name "col1" to "col1_probe", serve as last RF piece below.
        try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(newRuntimeFilterInfo(false, "col1"), fh);
             MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {

            for (int sendingFragment = 2; sendingFragment <= 4; sendingFragment++) {
                //Simulate receiving 3 message (from minor fragment 2,3,4) and each of them send partition column "col1"
                OutOfBandMessage oobMsg = runtimeFilterOOBFromMinorFragment(sendingFragment, recvBuffer, "col1");
                joinOp.workOnOOB(oobMsg);
                Arrays.stream(oobMsg.getBuffers()).forEach(ArrowBuf::close);
            }

            // apply the last RF piece which has column name "col1_probe".
            // RuntimeFilterManagerEntry::merge won't use "col1_probe" (last piece) to override existing name "col1".
            joinOp.tryPushRuntimeFilter();

            // verify filter sent to probe scan.
            runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
              any(RuntimeFilter.class), any(Optional.class), anyList(),
              any(OperatorContext.class),any(HashJoinPOP.class)),
              times(1));

            RuntimeFilter filterVal = valCaptor.getValue();
            assertEquals(1, filterVal.getProbeScanMajorFragmentId());
            assertEquals(101, filterVal.getProbeScanOperatorId());
            assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
            assertEquals("col1", filterVal.getPartitionColumnFilter().getColumns(0));
            assertEquals(2097152, filterVal.getPartitionColumnFilter().getSizeBytes());

            recvBuffer.close();
        }
  }

  @Test
  public void testTryPushRuntimeFilterBothColsShuffleJoinIsNotOnlyLastPiece() throws Exception {
        try (ArrowBuf recvBuffer = testAllocator.buffer(2 * 1024 * 1024)) {
            recvBuffer.setZero(0, recvBuffer.capacity()); // set all bytes to zero

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
            ArgumentCaptor<RuntimeFilter> valCaptor = ArgumentCaptor.forClass(RuntimeFilter.class);
            try (VectorizedHashJoinOperator joinOp = newVecHashJoinOp(newRuntimeFilterInfo(false,
                    partitionColNames, Lists.newArrayList("col1", "col2")), fh);
                 MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = getRuntimeFilterUtilClassMocked(valCaptor)) {

                NonPartitionColFilters nonPartitionColFilters = mock(NonPartitionColFilters.class);
                doReturn(nonPartitionColFilters).when(joinOp).createNonPartitionColFilters();
                doNothing().when(nonPartitionColFilters).finalizeValueListFilters();
                List<ValueListFilter> valueListFilterList = Arrays.asList(valueListFilter1, valueListFilter2);
                when(nonPartitionColFilters.getValueListFilters(anyInt(), any(RuntimeFilterProbeTarget.class)))
                  .thenReturn(valueListFilterList);

                // Get pieces from all other fragments. At last piece's merge, filter is sent to probe scan
                recvBuffer.getReferenceManager().retain(3);
                OutOfBandMessage oobMsg2 = utils.newOOB(11, 101, 2, partitionColNames, recvBuffer, valueListFilter3, valueListFilter4);
                OutOfBandMessage oobMsg3 = utils.newOOB(11, 101, 3, partitionColNames, recvBuffer, valueListFilter5, valueListFilter6);
                OutOfBandMessage oobMsg4 = utils.newOOB(11, 101, 4, partitionColNames, recvBuffer, valueListFilter7, valueListFilter8);

                joinOp.workOnOOB(oobMsg2);
                joinOp.workOnOOB(oobMsg3);
                joinOp.workOnOOB(oobMsg4);

                Arrays.stream(oobMsg2.getBuffers()).forEach(ArrowBuf::close);
                Arrays.stream(oobMsg3.getBuffers()).forEach(ArrowBuf::close);
                Arrays.stream(oobMsg4.getBuffers()).forEach(ArrowBuf::close);

                copyValueListFiltersFromProbeScan(runtimeFilterUtilMockedStatic, valCaptor);

                joinOp.tryPushRuntimeFilter();

                // verify filter sent to probe scan.
                runtimeFilterUtilMockedStatic.verify(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
                  any(RuntimeFilter.class), any(Optional.class), anyList(),
                  any(OperatorContext.class),any(HashJoinPOP.class)),
                  times(1));

                AutoCloseables.close(valueListFilter1, valueListFilter2);

                RuntimeFilter filterVal = valCaptor.getValue();
                assertEquals(1, filterVal.getProbeScanMajorFragmentId());
                assertEquals(101, filterVal.getProbeScanOperatorId());
                assertEquals(1, filterVal.getPartitionColumnFilter().getColumnsCount());
                assertEquals("pCol1", filterVal.getPartitionColumnFilter().getColumns(0));
                assertEquals(2097152, filterVal.getPartitionColumnFilter().getSizeBytes());

                assertEquals(2, filterVal.getNonPartitionColumnFilterCount());
                assertEquals(1, filterVal.getNonPartitionColumnFilter(0).getColumnsCount());
                assertEquals("col1_probe", filterVal.getNonPartitionColumnFilter(0).getColumns(0));
                assertEquals(1, filterVal.getNonPartitionColumnFilter(1).getColumnsCount());
                assertEquals("col2_probe", filterVal.getNonPartitionColumnFilter(1).getColumns(0));

                assertEquals(2, valueListFiltersCopy.size());
                assertTrue(valueListFiltersCopy.get(0).isContainsNull());
                assertEquals(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9), utils.getValues(valueListFiltersCopy.get(0)));
                assertFalse(valueListFiltersCopy.get(1).isContainsNull());
                assertEquals(Lists.newArrayList(11, 12, 13, 14, 15, 16, 17, 18, 19), utils.getValues(valueListFiltersCopy.get(1)));
                AutoCloseables.close(valueListFiltersCopy);
            }
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
            when(ei.getNodeEndpoint(anyInt())).thenReturn(node1);
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
            RuntimeFilterUtil.sendRuntimeFilterToProbeScan(filter, Optional.of(bloomFilter), new ArrayList<>(),
              opCtx, popConfig);

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
            assertEquals(3, recvBuffer.refCnt()); // Each message retains separately

            recvBuffer.close(); // close extra buffer
            recvBuffer.close(); // close extra buffer
            doNothing().when(joinOp).updateStats();
            joinOp.close();
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
        when(ei.getNodeEndpoint(anyInt())).thenReturn(node1);
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
        RuntimeFilterUtil.sendRuntimeFilterToProbeScan(filter, Optional.of(bloomFilter), new ArrayList<>(),
          opCtx, popConfig);

        verify(tunnel, never()).sendOOBMessage(oobMessageCaptor.capture());

        recvBuffer.close();
        doNothing().when(joinOp).updateStats();
        joinOp.close();
    }

    private OutOfBandMessage runtimeFilterOOBFromMinorFragment(int sendingMinorFragment, ArrowBuf buf, String... col) {
        List<Integer> allFragments = Lists.newArrayList(1,2,3,4);
        allFragments.removeIf(val -> val == sendingMinorFragment);
        RuntimeFilter filter = RuntimeFilter.newBuilder().setProbeScanOperatorId(101).setProbeScanMajorFragmentId(1)
                .setPartitionColumnFilter(ExecProtos.CompositeColumnFilter.newBuilder().setSizeBytes(buf.capacity())
                                            .addAllColumns(Lists.newArrayList(col)).build())
                .build();
        ArrowBuf[] bufs = new ArrowBuf[]{buf};
        List<Integer> bufferLengths = new ArrayList<>();
        bufferLengths.add((int)filter.getPartitionColumnFilter().getSizeBytes());
        return new OutOfBandMessage(null, 1, allFragments, 101, 1, sendingMinorFragment,
                101, new OutOfBandMessage.Payload(filter), bufs, bufferLengths,true);
    }

    private Optional<BloomFilter> mockedBloom() {
        BloomFilter bloom = mock(BloomFilter.class);
        when(bloom.isCrossingMaxFPP()).thenReturn(Boolean.FALSE);
        when(bloom.getSizeInBytes()).thenReturn(64L);
        ArrowBuf buf = mock(ArrowBuf.class);
        when(bloom.getDataBuffer()).thenReturn(buf);
        when(buf.refCnt()).thenReturn(1);
        doNothing().when(bloom).merge(any(BloomFilter.class));
        return Optional.of(bloom);
    }

    private ValueListFilter mockedValueListFilter(RuntimeFilterInfo runtimeFilterInfo, int index) {
        ValueListFilter valueListFilter = mock(ValueListFilter.class);
        when(valueListFilter.getValueCount()).thenReturn(21);
        when(valueListFilter.getSizeInBytes()).thenReturn(115L);
        when(valueListFilter.getBlockSize()).thenReturn((byte) 4);
        when(valueListFilter.getFieldName()).thenReturn(runtimeFilterInfo.getRuntimeFilterProbeTargets().get(0).getNonPartitionProbeTableKeys().get(index));
        doNothing().when(valueListFilter).setFieldName(anyString());
        return valueListFilter;
    }

    private RuntimeFilterInfo newRuntimeFilterInfo(boolean isBroadcast, String... partitionCols) {
        return newRuntimeFilterInfo(isBroadcast, Arrays.asList(partitionCols), Collections.EMPTY_LIST);
    }

    private RuntimeFilterInfo newRuntimeFilterInfo(boolean isBroadcast, List<String> partitionCols, List<String> nonPartitionCols) {
      RuntimeFilterProbeTarget.Builder builder =
        new RuntimeFilterProbeTarget.Builder(1, 101);
      for (String partitionCol: partitionCols) {
        builder.addPartitionKey(partitionCol + "_build", partitionCol + "_probe");
      }
      for (String nonPartitionCol: nonPartitionCols) {
        builder.addNonPartitionKey(nonPartitionCol + "_build", nonPartitionCol + "_probe");
      }
      return new RuntimeFilterInfo.Builder().isBroadcastJoin(isBroadcast)
          .setRuntimeFilterProbeTargets(ImmutableList.of(builder.build()))
          .build();
    }

    private OperatorContext mockOpContext(FragmentHandle fragmentHandle) {
        OptionManager optionManager = mock(OptionManager.class);
        when(optionManager.getOption(eq(ExecConstants.RUNTIME_FILTER_VALUE_FILTER_MAX_SIZE))).thenReturn(1000L);
        when(optionManager.getOption(eq(ExecConstants.ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET))).thenReturn(true);
        when(optionManager.getOption(eq(ExecConstants.RUNTIME_FILTER_KEY_MAX_SIZE))).thenReturn(32L);

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

    private VectorPivotDef createVectorPivotDef(String name) {
      ArrowType arrowType = new ArrowType.Int(32, true);
        /*
        ArrowType arrowType = mock(ArrowType.class);
        when(arrowType.isComplex()).thenReturn(false);
         */

      Field field = mock(Field.class);
      when(field.getType()).thenReturn(arrowType);
      when(field.getName()).thenReturn(name);

      FieldVector fieldVector = mock(FieldVector.class);
      when(fieldVector.getField()).thenReturn(field);

      VectorPivotDef vectorPivotDef = mock(VectorPivotDef.class);
      when(vectorPivotDef.getOffset()).thenReturn(0);
      when(vectorPivotDef.getNullByteOffset()).thenReturn(5);
      when(vectorPivotDef.getNullBitOffset()).thenReturn(5);
      when(vectorPivotDef.getIncomingVector()).thenReturn(fieldVector);
      when(vectorPivotDef.getMode()).thenReturn(PivotBuilder.FieldMode.FIXED);
      when(vectorPivotDef.getByteSize()).thenReturn(4);

      return vectorPivotDef;
    }

    private VectorizedHashJoinOperator newVecHashJoinOp(RuntimeFilterInfo filterInfo, FragmentHandle fragmentHandle) throws Exception {
      VectorizedHashJoinOperator joinOp = spy(new VectorizedHashJoinOperator(
        mockOpContext(fragmentHandle), mockPopConfig(filterInfo)));

      JoinTable joinTable = mock(JoinTable.class);
      when(joinTable.size()).thenReturn(100);
      doNothing().when(joinTable).prepareBloomFilters(any(PartitionColFilters.class), eq(true));
      doNothing().when(joinTable).prepareValueListFilters(any(NonPartitionColFilters.class));
      joinOp.setTable(joinTable);

      List<RuntimeFilterProbeTarget> probeTargets = filterInfo.getRuntimeFilterProbeTargets();
      Preconditions.checkState(probeTargets.size() == 1);
      List<String> partBuildTableKeys = probeTargets.get(0).getPartitionBuildTableKeys();
      List<String> nonPartBuildTableKeys = probeTargets.get(0).getNonPartitionBuildTableKeys();

      List<VectorPivotDef> vectorPivotDefList = new ArrayList<>();

      for (int i = 0; i < partBuildTableKeys.size(); i++) {
        VectorPivotDef vectorPivotDef = createVectorPivotDef(partBuildTableKeys.get(i));
        vectorPivotDefList.add(vectorPivotDef);
      }

      for (int i = 0; i < nonPartBuildTableKeys.size(); i++) {
        VectorPivotDef vectorPivotDef = createVectorPivotDef(nonPartBuildTableKeys.get(i));
        vectorPivotDefList.add(vectorPivotDef);
      }

      ImmutableList<VectorPivotDef> vectorPivotDefs = ImmutableList.copyOf(vectorPivotDefList);

      PivotDef pivot = mock(PivotDef.class);
      when(pivot.getFixedPivots()).thenReturn(vectorPivotDefs);
      joinOp.setBuildPivot(pivot);

      /* For close */
      doNothing().when(joinOp).updateStats();

      return joinOp;
    }

    private MockedStatic<RuntimeFilterUtil> getRuntimeFilterUtilClassMocked(ArgumentCaptor<RuntimeFilter> valCaptor) {
      MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic = mockStatic(RuntimeFilterUtil.class,
        withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));

      runtimeFilterUtilMockedStatic.when(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
        valCaptor.capture(), any(Optional.class), anyList(),
        any(OperatorContext.class), any(HashJoinPOP.class)))
        .thenAnswer((Answer<Void>) invocation -> null);
      runtimeFilterUtilMockedStatic.when(() -> RuntimeFilterUtil.addRunTimeFilterInfosToProfileDetails(
        any(OperatorContext.class), anyList()))
        .thenAnswer((Answer<Void>) invocation -> null);
      runtimeFilterUtilMockedStatic.when(() -> RuntimeFilterUtil.sendRuntimeFilterAtMergePoints(
        valCaptor.capture(), any(Optional.class), anyList(),
        any(OperatorContext.class), any(HashJoinPOP.class)))
        .thenAnswer((Answer<Void>) invocation -> null);

      return runtimeFilterUtilMockedStatic;
    }

    private void copyValueListFiltersFromProbeScan(MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic,
                                                   ArgumentCaptor<RuntimeFilter> runtimeFilterArgumentCaptor) {
      ArgumentCaptor<List> valListCaptor = ArgumentCaptor.forClass(List.class);
      runtimeFilterUtilMockedStatic.when(() -> RuntimeFilterUtil.sendRuntimeFilterToProbeScan(
      runtimeFilterArgumentCaptor.capture(), any(Optional.class), valListCaptor.capture(),
      any(OperatorContext.class), any(HashJoinPOP.class)))
      .thenAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) {
          List<ValueListFilter> nonPartitionColFilters = valListCaptor.getValue();
          valueListFiltersCopy = new ArrayList<>();
          nonPartitionColFilters.forEach(v -> valueListFiltersCopy.add(v.createCopy(testAllocator)));
          return null;
        }
      });
  }

  private void copyValueListFiltersAtMergePoints(MockedStatic<RuntimeFilterUtil> runtimeFilterUtilMockedStatic,
                                                 ArgumentCaptor<RuntimeFilter> runtimeFilterArgumentCaptor) {
    ArgumentCaptor<List> valListCaptor = ArgumentCaptor.forClass(List.class);
    runtimeFilterUtilMockedStatic.when(() -> RuntimeFilterUtil.sendRuntimeFilterAtMergePoints(
      runtimeFilterArgumentCaptor.capture(), any(Optional.class), valListCaptor.capture(),
      any(OperatorContext.class), any(HashJoinPOP.class)))
      .thenAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) {
          List<ValueListFilter> nonPartitionColFilters = valListCaptor.getValue();
          valueListFiltersCopy = new ArrayList<>();
          nonPartitionColFilters.forEach(v -> valueListFiltersCopy.add(v.createCopy(testAllocator)));
          return null;
        }
      });
  }
}

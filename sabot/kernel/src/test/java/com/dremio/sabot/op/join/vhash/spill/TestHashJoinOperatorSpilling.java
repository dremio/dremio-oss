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
package com.dremio.sabot.op.join.vhash.spill;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.common.AutoCloseables;
import com.dremio.common.SuppressForbidden;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.physical.config.RuntimeFilterProbeTarget;
import com.dremio.exec.planner.physical.filter.RuntimeFilterInfo;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Fixtures.DataRow;
import com.dremio.sabot.Generator;
import com.dremio.sabot.exec.heap.HeapLowMemParticipant;
import com.dremio.sabot.join.hash.TestVHashJoinSpill;
import com.dremio.sabot.op.join.vhash.NonPartitionColFilters;
import com.dremio.sabot.op.join.vhash.PartitionColFilters;
import com.dremio.sabot.op.join.vhash.spill.partition.MultiPartition;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Test;

@SuppressForbidden
public class TestHashJoinOperatorSpilling extends TestVHashJoinSpill {

  @Test
  public void testSpillWithPartFilters() throws Exception {
    RuntimeFilterInfo runtimeFilterInfo = newRuntimeFilterInfoPart(false, "b", null);
    JoinInfo joinInfo =
        getJoinInfo(
            Arrays.asList(new JoinCondition("EQUALS", f("a"), f("b"))),
            JoinRelType.INNER,
            runtimeFilterInfo);
    final int batchSize = 1096;
    final int expectedRowsCount = 1;
    final Fixtures.DataRow[] leftRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] rightRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] expectedRows = new Fixtures.DataRow[expectedRowsCount];
    generateTestData(batchSize, leftRows, rightRows, expectedRows);

    final Fixtures.Table left = t(th("a", "va"), leftRows);
    final Fixtures.Table right = t(th("b", "vb"), rightRows);
    final Fixtures.Table expected = t(th("b", "vb", "a", "va"), expectedRows).orderInsensitive();
    VectorizedSpillingHashJoinOperator op =
        newOperator(VectorizedSpillingHashJoinOperator.class, joinInfo.operator, batchSize);
    final List<RecordBatchData> data = new ArrayList<>();
    try (Generator leftGen = left.toGenerator(getTestAllocator());
        Generator rightGen = right.toGenerator(getTestAllocator())) {
      int rightCount = rightGen.next(batchSize);
      int leftCount = leftGen.next(batchSize);

      VectorAccessible output = op.setup(leftGen.getOutput(), rightGen.getOutput());
      spillPartitions(op);
      op.consumeDataRight(rightCount);
      while (op.getState() == DualInputOperator.State.CAN_PRODUCE) {
        int outputCount = op.outputData();
        if (outputCount > 0 || (outputCount == 0 && expected.isExpectZero())) {
          data.add(new RecordBatchData(output, getTestAllocator()));
        }
      }
      op.noMoreToConsumeRight();

      op.consumeDataLeft(leftCount);
      while (op.getState() == DualInputOperator.State.CAN_PRODUCE) {
        int outputCount = op.outputData();
        if (outputCount > 0 || (outputCount == 0 && expected.isExpectZero())) {
          data.add(new RecordBatchData(output, getTestAllocator()));
        }
      }

      op.noMoreToConsumeLeft();
      while (op.getState() == DualInputOperator.State.CAN_PRODUCE) {
        int outputCount = op.outputData();
        if (outputCount > 0 || (outputCount == 0 && expected.isExpectZero())) {
          data.add(new RecordBatchData(output, getTestAllocator()));
        }
      }
      assertState(op, SingleInputOperator.State.DONE);
      Field partitionColFiltersField = op.getClass().getDeclaredField("partitionColFilters");
      partitionColFiltersField.setAccessible(true);
      Field nonPartitionColFiltersField = op.getClass().getDeclaredField("nonPartitionColFilters");
      nonPartitionColFiltersField.setAccessible(true);
      PartitionColFilters partitionColFilters =
          (PartitionColFilters) partitionColFiltersField.get(op);
      List<RuntimeFilterProbeTarget> probeTargets = partitionColFilters.getProbeTargets();
      assertEquals(probeTargets.size(), 1);
      Optional<BloomFilter> bloomFilter =
          partitionColFilters.getBloomFilter(0, probeTargets.get(0));
      assertTrue(bloomFilter.isPresent());
      NonPartitionColFilters nonPartitionColFilters =
          (NonPartitionColFilters) nonPartitionColFiltersField.get(op);
      List<ValueListFilter> valueListFilters =
          nonPartitionColFilters.getValueListFilters(0, probeTargets.get(0));
      assertTrue(valueListFilters.isEmpty());
      SpillStats spillStats = getSpillStats(op);
      assertEquals(spillStats.getHeapSpillCount(), 4);
      expected.checkValid(data);
    } finally {
      AutoCloseables.close(data);
    }
  }

  @Test
  public void testSpillWithNonPartFilters() throws Exception {
    RuntimeFilterInfo runtimeFilterInfo = newRuntimeFilterInfoPart(false, null, "b");
    JoinInfo joinInfo =
        getJoinInfo(
            Arrays.asList(new JoinCondition("EQUALS", f("a"), f("b"))),
            JoinRelType.INNER,
            runtimeFilterInfo);
    final int batchSize = 1096;
    final int expectedRowsCount = 1;
    final Fixtures.DataRow[] leftRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] rightRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] expectedRows = new Fixtures.DataRow[expectedRowsCount];
    generateTestData(batchSize, leftRows, rightRows, expectedRows);

    final Fixtures.Table left = t(th("a", "va"), leftRows);
    final Fixtures.Table right = t(th("b", "vb"), rightRows);
    final Fixtures.Table expected = t(th("b", "vb", "a", "va"), expectedRows).orderInsensitive();
    VectorizedSpillingHashJoinOperator op =
        newOperator(VectorizedSpillingHashJoinOperator.class, joinInfo.operator, batchSize);
    final List<RecordBatchData> data = new ArrayList<>();
    try (Generator leftGen = left.toGenerator(getTestAllocator());
        Generator rightGen = right.toGenerator(getTestAllocator())) {
      int rightCount = rightGen.next(batchSize);
      int leftCount = leftGen.next(batchSize);

      VectorAccessible output = op.setup(leftGen.getOutput(), rightGen.getOutput());
      spillPartitions(op);
      op.consumeDataRight(rightCount);
      while (op.getState() == DualInputOperator.State.CAN_PRODUCE) {
        int outputCount = op.outputData();
        if (outputCount > 0 || (outputCount == 0 && expected.isExpectZero())) {
          data.add(new RecordBatchData(output, getTestAllocator()));
        }
      }
      op.noMoreToConsumeRight();

      op.consumeDataLeft(leftCount);
      while (op.getState() == DualInputOperator.State.CAN_PRODUCE) {
        int outputCount = op.outputData();
        if (outputCount > 0 || (outputCount == 0 && expected.isExpectZero())) {
          data.add(new RecordBatchData(output, getTestAllocator()));
        }
      }

      op.noMoreToConsumeLeft();
      while (op.getState() == DualInputOperator.State.CAN_PRODUCE) {
        int outputCount = op.outputData();
        if (outputCount > 0 || (outputCount == 0 && expected.isExpectZero())) {
          data.add(new RecordBatchData(output, getTestAllocator()));
        }
      }
      assertState(op, SingleInputOperator.State.DONE);
      Field partitionColFiltersField = op.getClass().getDeclaredField("partitionColFilters");
      partitionColFiltersField.setAccessible(true);
      Field nonPartitionColFiltersField = op.getClass().getDeclaredField("nonPartitionColFilters");
      nonPartitionColFiltersField.setAccessible(true);
      PartitionColFilters partitionColFilters =
          (PartitionColFilters) partitionColFiltersField.get(op);
      List<RuntimeFilterProbeTarget> probeTargets = partitionColFilters.getProbeTargets();
      assertEquals(probeTargets.size(), 1);
      Optional<BloomFilter> bloomFilter =
          partitionColFilters.getBloomFilter(0, probeTargets.get(0));
      assertFalse(bloomFilter.isPresent());
      NonPartitionColFilters nonPartitionColFilters =
          (NonPartitionColFilters) nonPartitionColFiltersField.get(op);
      List<ValueListFilter> valueListFilters =
          nonPartitionColFilters.getValueListFilters(0, probeTargets.get(0));
      assertEquals(1, valueListFilters.size());
      ValueListFilter valueListFilter = valueListFilters.get(0);
      assertEquals("b", valueListFilter.getFieldName());
      SpillStats spillStats = getSpillStats(op);
      assertEquals(spillStats.getHeapSpillCount(), 4);
      expected.checkValid(data);
    } finally {
      AutoCloseables.close(data);
    }
  }

  @Test
  public void testSpillWithPartAndNonPartFilters() throws Exception {
    RuntimeFilterInfo runtimeFilterInfo = newRuntimeFilterInfoPart(false, "b", "vb");
    JoinInfo joinInfo =
        getJoinInfo(
            Arrays.asList(
                new JoinCondition("EQUALS", f("a"), f("b")),
                new JoinCondition("EQUALS", f("va"), f("vb"))),
            JoinRelType.INNER,
            runtimeFilterInfo);
    final int batchSize = 1096;
    final int expectedRowsCount = 1;
    final Fixtures.DataRow[] leftRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] rightRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] expectedRows = new Fixtures.DataRow[expectedRowsCount];
    generateTestData(batchSize, leftRows, rightRows, expectedRows);

    final Fixtures.Table left = t(th("a", "va"), leftRows);
    final Fixtures.Table right = t(th("b", "vb"), rightRows);
    final Fixtures.Table expected = t(th("b", "vb", "a", "va"), expectedRows).orderInsensitive();
    VectorizedSpillingHashJoinOperator op =
        newOperator(VectorizedSpillingHashJoinOperator.class, joinInfo.operator, batchSize);
    final List<RecordBatchData> data = new ArrayList<>();
    try (Generator leftGen = left.toGenerator(getTestAllocator());
        Generator rightGen = right.toGenerator(getTestAllocator())) {
      int rightCount = rightGen.next(batchSize);
      int leftCount = leftGen.next(batchSize);

      VectorAccessible output = op.setup(leftGen.getOutput(), rightGen.getOutput());
      spillPartitions(op);
      op.consumeDataRight(rightCount);
      while (op.getState() == DualInputOperator.State.CAN_PRODUCE) {
        int outputCount = op.outputData();
        if (outputCount > 0 || (outputCount == 0 && expected.isExpectZero())) {
          data.add(new RecordBatchData(output, getTestAllocator()));
        }
      }
      op.noMoreToConsumeRight();
      op.consumeDataLeft(leftCount);

      while (op.getState() == DualInputOperator.State.CAN_PRODUCE) {
        int outputCount = op.outputData();
        if (outputCount > 0 || (outputCount == 0 && expected.isExpectZero())) {
          data.add(new RecordBatchData(output, getTestAllocator()));
        }
      }
      op.noMoreToConsumeLeft();

      while (op.getState() == DualInputOperator.State.CAN_PRODUCE) {
        int outputCount = op.outputData();
        if (outputCount > 0 || (outputCount == 0 && expected.isExpectZero())) {
          data.add(new RecordBatchData(output, getTestAllocator()));
        }
      }
      assertState(op, SingleInputOperator.State.DONE);
      Field partitionColFiltersField = op.getClass().getDeclaredField("partitionColFilters");
      partitionColFiltersField.setAccessible(true);
      Field nonPartitionColFiltersField = op.getClass().getDeclaredField("nonPartitionColFilters");
      nonPartitionColFiltersField.setAccessible(true);
      PartitionColFilters partitionColFilters =
          (PartitionColFilters) partitionColFiltersField.get(op);
      List<RuntimeFilterProbeTarget> probeTargets = partitionColFilters.getProbeTargets();
      assertEquals(probeTargets.size(), 1);
      Optional<BloomFilter> bloomFilter =
          partitionColFilters.getBloomFilter(0, probeTargets.get(0));
      assertTrue(bloomFilter.isPresent());
      NonPartitionColFilters nonPartitionColFilters =
          (NonPartitionColFilters) nonPartitionColFiltersField.get(op);
      List<ValueListFilter> valueListFilters =
          nonPartitionColFilters.getValueListFilters(0, probeTargets.get(0));
      assertEquals(1, valueListFilters.size());
      ValueListFilter valueListFilter = valueListFilters.get(0);
      assertEquals("vb", valueListFilter.getFieldName());
      SpillStats spillStats = getSpillStats(op);
      assertEquals(spillStats.getHeapSpillCount(), 4);
      expected.checkValid(data);
    } finally {
      AutoCloseables.close(data);
    }
  }

  private void generateTestData(
      int batchSize, DataRow[] leftRows, DataRow[] rightRows, DataRow[] expectedRows) {
    for (int i = 0; i < batchSize; i++) {
      if (i < batchSize / 2) {
        leftRows[i] = tr(String.valueOf(1 + i), String.valueOf(i));
        rightRows[i] = tr(String.valueOf(600 + i), String.valueOf(i));
      }
      if (i == batchSize / 2) {
        leftRows[i] = tr("2000", String.valueOf(i));
        rightRows[i] = tr("2000", String.valueOf(i));
        expectedRows[0] = tr("2000", String.valueOf(i), "2000", String.valueOf(i));
      }
      if (i > batchSize / 2) {
        leftRows[i] = tr(String.valueOf(3000 + i), String.valueOf(i));
        rightRows[i] = tr(String.valueOf(4000 + i), String.valueOf(i));
      }
    }
  }

  private SpillStats getSpillStats(VectorizedSpillingHashJoinOperator op)
      throws NoSuchFieldException, IllegalAccessException {
    Field setupParamsField = op.getClass().getDeclaredField("joinSetupParams");
    setupParamsField.setAccessible(true);
    JoinSetupParams setupParams = (JoinSetupParams) setupParamsField.get(op);
    return setupParams.getSpillStats();
  }

  private void spillPartitions(VectorizedSpillingHashJoinOperator op)
      throws NoSuchFieldException, IllegalAccessException {
    Field partitionField = op.getClass().getDeclaredField("partition");
    partitionField.setAccessible(true);
    MultiPartition mp = (MultiPartition) partitionField.get(op);

    Field childWrappersField = mp.getClass().getDeclaredField("childWrappers");
    childWrappersField.setAccessible(true);
    Object arr = childWrappersField.get(mp);
    int len = Array.getLength(arr);
    for (int i = 0; i < len; i++) {
      // set every second partition as victim for spilling
      if (i % 2 == 0) {
        Object el = Array.get(arr, i);
        Field memPartitionField = el.getClass().getDeclaredField("partition");
        memPartitionField.setAccessible(true);
        Object memPartition = memPartitionField.get(el);
        Field overhead = memPartition.getClass().getDeclaredField("overheadParticipant");
        overhead.setAccessible(true);
        ParticipantTest participantTest = new ParticipantTest();
        participantTest.setAsVictim();
        overhead.set(memPartition, participantTest);
      }
    }
  }

  private JoinInfo getJoinInfo(
      List<JoinCondition> conditions, JoinRelType type, RuntimeFilterInfo runtimeFilterInfo) {
    return new JoinInfo(
        VectorizedSpillingHashJoinOperator.class,
        new HashJoinPOP(PROPS, null, null, conditions, null, type, true, true, runtimeFilterInfo));
  }

  private RuntimeFilterInfo newRuntimeFilterInfoPart(
      boolean isBroadcast, String partitionCols, String nonPartitionCols) {
    return newRuntimeFilterInfo(
        isBroadcast,
        partitionCols != null ? Arrays.asList(partitionCols) : Collections.EMPTY_LIST,
        nonPartitionCols != null ? Arrays.asList(nonPartitionCols) : Collections.EMPTY_LIST);
  }

  private RuntimeFilterInfo newRuntimeFilterInfo(
      boolean isBroadcast, List<String> partitionCols, List<String> nonPartitionCols) {
    RuntimeFilterProbeTarget.Builder builder = new RuntimeFilterProbeTarget.Builder(1, 101);
    for (String partitionCol : partitionCols) {
      builder.addPartitionKey(partitionCol, partitionCol);
    }
    for (String nonPartitionCol : nonPartitionCols) {
      builder.addNonPartitionKey(nonPartitionCol, nonPartitionCol);
    }
    return new RuntimeFilterInfo.Builder()
        .isBroadcastJoin(isBroadcast)
        .setRuntimeFilterProbeTargets(ImmutableList.of(builder.build()))
        .build();
  }

  private class ParticipantTest implements HeapLowMemParticipant {
    private boolean victim;

    @Override
    public void addBatches(int numBatches) {}

    @Override
    public boolean isVictim() {
      return victim;
    }

    public void setAsVictim() {
      victim = true;
    }
  }
}

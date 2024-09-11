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
package com.dremio.sabot.op.sort.external;

import static com.dremio.sabot.CustomGenerator.ID;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.CustomGenerator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Generator;
import com.dremio.sabot.op.sort.external.VectorSorter.SortState;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.spi.SingleInputOperator.State;
import io.airlift.tpch.GenerationDefinition;
import io.airlift.tpch.TpchGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.Test;

/** Test behavior of ExternalSortOperator when asked to spill in various internal states. */
public class TestExternalSortOperatorShrinkableStates extends BaseTestOperator {

  private static final GenerationDefinition.TpchTable TABLE =
      GenerationDefinition.TpchTable.CUSTOMER;
  private static final double SCALE = 1;

  private ExternalSort getRegionSort() {
    return new ExternalSort(
        PROPS,
        null,
        Collections.singletonList(
            ordering(
                "c_custkey",
                RelFieldCollation.Direction.ASCENDING,
                RelFieldCollation.NullDirection.FIRST)),
        false);
  }

  @Test
  public void checkShrinkableMemory() throws Exception {
    // Two types of sort operators - Soring with Splay Tree or Quick Sort
    for (int sortOperatorType = 0; sortOperatorType < 2; sortOperatorType++) {

      try (AutoCloseable option =
              with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, sortOperatorType != 0);
          ExternalSortOperator sortOperator =
              newOperator(ExternalSortOperator.class, getRegionSort(), 4095);
          TpchGenerator tableGenerator =
              TpchGenerator.singleGenerator(TABLE, SCALE, getTestAllocator())) {

        sortOperator.setup(tableGenerator.getOutput());

        final int count = tableGenerator.next(4095);

        // Consume a batch
        sortOperator.consumeData(count);

        sortOperator.setStates(SingleInputOperator.State.CAN_PRODUCE, SortState.SPILL_IN_PROGRESS);
        long shrinkableMemory = sortOperator.shrinkableMemory();
        // Shrinkable memory is returned > 0 as operator is allowed to shrink in SPILL_IN_PROGRESS
        // state
        assertTrue(shrinkableMemory > 0L);

        sortOperator.setStates(SingleInputOperator.State.CAN_PRODUCE, SortState.COPY_FROM_DISK);
        shrinkableMemory = sortOperator.shrinkableMemory();
        // Shrinkable memory is returned as 0 as operator is not allowed to shrink in COPY_FROM_DISK
        // state
        assertEquals(0L, shrinkableMemory);

        sortOperator.setStates(SingleInputOperator.State.CAN_PRODUCE, SortState.COPY_FROM_MEMORY);
        shrinkableMemory = sortOperator.shrinkableMemory();
        // Shrinkable memory is returned > 0 as operator is allowed to shrink in COPY_FROM_MEMORY
        // state
        assertTrue(shrinkableMemory > 0L);

        // Ask operator to shrink
        sortOperator.setStateToCanConsume();
        sortOperator.shrinkMemory(count);

        sortOperator.setStates(SingleInputOperator.State.CAN_PRODUCE, SortState.CONSOLIDATE);
        shrinkableMemory = sortOperator.shrinkableMemory();
        // Consolidate states is not shrinkable
        assertEquals(0L, shrinkableMemory);
      }
    }
  }

  @Test
  public void checkShrinkMemoryCommandInVariousStates() throws Exception {
    for (int sortOperatorType = 0; sortOperatorType < 2; sortOperatorType++) {
      try (AutoCloseable option =
              with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, sortOperatorType != 0);
          ExternalSortOperator o = newOperator(ExternalSortOperator.class, getRegionSort(), 4095);
          TpchGenerator generator =
              TpchGenerator.singleGenerator(TABLE, SCALE, getTestAllocator())) {

        o.setup(generator.getOutput());

        int count = generator.next(1096);
        o.consumeData(count);
        assertTrue(o.shrinkableMemory() > 0);

        o.setStateToCanConsume();
        count = generator.next(1096);
        o.consumeData(count);

        o.setStates(SingleInputOperator.State.CAN_PRODUCE, SortState.COPY_FROM_DISK);
        assertEquals(0L, o.shrinkableMemory());
        // Cannot shrink in COPY_FROM_DISK state but function still returns 0 as shrinkable memory
        // is returned 0 in previous state
        assertTrue(o.shrinkMemory(count));
      }
    }
  }

  @Test
  public void testSorterShrinkMemoryFromCopier() throws Exception {
    // use DetectHowManyRecordFitIntoMemory() to detect maximum records can fit into
    // sorter's memory. Since test case sorterShrinkMemoryFromCopier rely on that max value.
    int total = detectHowManyRecordFitIntoMemory();
    for (int recordInCopier = 0; recordInCopier <= total; recordInCopier += 1000) {
      sorterShrinkMemoryFromCopier(recordInCopier, total - recordInCopier);
    }
  }

  private int detectHowManyRecordFitIntoMemory() throws Exception {
    int totalRecordCount = 0;
    CustomGenerator generator = new CustomGenerator(10000, getTestAllocator());
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false)) {
      ExternalSort sort =
          new ExternalSort(
              PROPS.cloneWithNewReserve(1_000_000),
              null,
              singletonList(ordering(ID.getName(), ASCENDING, FIRST)),
              false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      int batchSize = 1000;
      try (ExternalSortOperator sorter = newOperator(ExternalSortOperator.class, sort, batchSize);
          Generator closeable = generator; ) {
        final VectorAccessible output = sorter.setup(generator.getOutput());
        int count = 0;

        // try to read all 10K record into sorter's memory. 10 batches, each has 1000 records.
        // until reach memory limit
        while ((count = generator.next(batchSize)) != 0) {
          sorter.consumeData(count);
          if (sorter.getState() != State.CAN_CONSUME) {
            break;
          }
          totalRecordCount += count;
        }
      }
    }
    return totalRecordCount;
  }

  private void sorterShrinkMemoryFromCopier(int recordInCopier, int recordOnDisk) throws Exception {
    int total = recordInCopier + recordOnDisk;
    CustomGenerator generator = new CustomGenerator(total, getTestAllocator());
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false)) {
      ExternalSort sort =
          new ExternalSort(
              PROPS.cloneWithNewReserve(1_000_000),
              null,
              singletonList(ordering(ID.getName(), ASCENDING, FIRST)),
              false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table validator = generator.getExpectedSortedTable();

      final List<RecordBatchData> data = new ArrayList<>();
      long actualRecordCount = 0;
      int batchSize = 1000;
      long memoryToShrink = 1000000;

      try (ExternalSortOperator sorter = newOperator(ExternalSortOperator.class, sort, batchSize);
          Generator closeable = generator; ) {
        final VectorAccessible output = sorter.setup(generator.getOutput());
        int count = 0;

        // read all records into sorter's memory. each batch has 1000 records.
        while ((count = generator.next(batchSize)) != 0) {
          assertState(sorter, State.CAN_CONSUME);
          sorter.consumeData(count);
        }

        // no more input. sorter's state should be COPY_FROM_MEMORY
        sorter.noMoreToConsume();
        assertEquals(sorter.getOperatorStateToPrint(), "CAN_PRODUCE COPY_FROM_MEMORY");

        // output data directly from memory (sorter's copier)
        int recordsOutput = 0;
        while (actualRecordCount < recordInCopier) {
          recordsOutput = sorter.outputData();
          assertEquals(recordsOutput, batchSize);
          actualRecordCount += recordsOutput;
          data.add(new RecordBatchData(output, getTestAllocator()));
        }

        // shrink memory in the middle, this will trigger spill from copier to disk
        sorter.shrinkMemory(memoryToShrink);
        if (actualRecordCount < total) {
          // if all records are already output, then 0 record left in copier.
          // shrinkMemory at this moment will actually let sorter stay in current state, never enter
          // COPIER_SPILL_IN_PROGRESS state.
          // so below assert only check 'actualRecordCount < total' case.
          assertEquals(sorter.getOperatorStateToPrint(), "CAN_PRODUCE COPIER_SPILL_IN_PROGRESS");
        }

        while (sorter.getState() == State.CAN_PRODUCE) {
          recordsOutput = sorter.outputData();
          actualRecordCount += recordsOutput;
          if (recordsOutput > 0) {
            data.add(new RecordBatchData(output, getTestAllocator()));
          }
        }

        assertEquals(actualRecordCount, total);
        assertState(sorter, State.DONE);
        validator.checkValid(data);

      } finally {
        AutoCloseables.close(data);
      }
    }
  }

  @Test
  public void testSorterShrinkMemory() throws Exception {
    // two scenarios - input records are 2000 - 10000 (small input) and 11000 - 21000 (large input)
    // one batch is 1000 record, memoryRun can hold up to 10 batches in memory.
    // without 'shrink memory' request, 'small input' will not trigger disk spill but 'large input'
    // will.
    // below for loop will inject 'shrink memory' request at different places for those two
    // scenarios.
    // 2000 / 1000 = 2 batches, 3000 / 1000 = 3 batches, etc.
    // choose odd and even number of batches
    for (int expectedRecordCount : new int[] {2000, 3000, 10000, 11000, 20000, 21000}) {
      boolean[] pos = new boolean[5];

      // inject 'shrink memory' request at one specific place
      for (int i = 0; i < 5; i++) {
        Arrays.fill(pos, false);
        pos[i] = true;
        sorterShrinkMemory(expectedRecordCount, pos);
      }

      // inject 'shrink memory' at all 5 places.
      Arrays.fill(pos, true);
      sorterShrinkMemory(expectedRecordCount, pos);
    }
  }

  private void sorterShrinkMemory(int expectedRecordCount, boolean[] pos) throws Exception {
    class Simulator {
      // simulate sorter operator receive 'shrink memory' request from MA thread.
      // It's possible that fragment receives multiple such requests before doing pump, so
      // we invoke shrinkMemory twice to simulate such scenario - sorter react on 'shrink memory'
      // twice in a row.
      void InsertShrinkMemoryRequest(ExternalSortOperator sorter, int cur, boolean[] pos)
          throws Exception {
        if (!pos[cur]) {
          return;
        }
        sorter.shrinkMemory(1000000); // 1MB
        sorter.shrinkMemory(1000000);
      }
    }

    Simulator s = new Simulator();
    CustomGenerator generator = new CustomGenerator(expectedRecordCount, getTestAllocator());
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false)) {
      ExternalSort sort =
          new ExternalSort(
              PROPS.cloneWithNewReserve(1_000_000),
              null,
              singletonList(ordering(ID.getName(), ASCENDING, FIRST)),
              false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table validator = generator.getExpectedSortedTable();

      final List<RecordBatchData> data = new ArrayList<>();
      long actualRecordCount = 0;
      int batchSize = 1000;
      long memoryToShrink = 1000000;

      try (ExternalSortOperator sorter = newOperator(ExternalSortOperator.class, sort, batchSize);
          Generator closeable = generator; ) {
        final VectorAccessible output = sorter.setup(generator.getOutput());

        int count;
        while (sorter.getState() != State.DONE && (count = generator.next(batchSize)) != 0) {
          // keep feeding input records to sorter
          assertState(sorter, State.CAN_CONSUME);

          // PLACE 1 - 'shrink memory' before consume any input data.
          s.InsertShrinkMemoryRequest(sorter, 0, pos);
          sorter.consumeData(count);
          // if spill happens, sorter's state becomes CAN_PRODUCE (plus internal sortState is
          // SPILL_IN_PROGRESS)
          while (sorter.getState() == State.CAN_PRODUCE) {
            // PLACE 2 - 'shrink memory' after some data spilled onto disk.
            s.InsertShrinkMemoryRequest(sorter, 1, pos);
            if (sorter.getState() != State.CAN_PRODUCE) {
              break;
            }
            int recordsOutput = sorter.outputData();
            actualRecordCount += recordsOutput;
            if (recordsOutput > 0) {
              data.add(new RecordBatchData(output, getTestAllocator()));
            }
          }
        }

        while (actualRecordCount < expectedRecordCount) {
          // PLACE 3 - 'shrink memory' just before consuming all input records
          s.InsertShrinkMemoryRequest(sorter, 2, pos);
          if (sorter.getState() == State.CAN_CONSUME) {
            sorter.noMoreToConsume();
          }

          while (sorter.getState() == State.CAN_PRODUCE) {
            // PLACE 4 - 'shrink memory' when outputting records
            s.InsertShrinkMemoryRequest(sorter, 3, pos);
            if (sorter.getState() != State.CAN_PRODUCE) {
              break;
            }
            int recordsOutput = sorter.outputData();
            actualRecordCount += recordsOutput;
            if (recordsOutput > 0) {
              data.add(new RecordBatchData(output, getTestAllocator()));
            }
          }
        }

        // PLACE 5 - 'shrink memory' after 'done' status
        s.InsertShrinkMemoryRequest(sorter, 4, pos);
        assertState(sorter, State.DONE);
        validator.checkValid(data);
      } finally {
        AutoCloseables.close(data);
      }
    }
  }
}

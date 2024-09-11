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
package com.dremio.sabot.op.aggregate.vectorized;

import static org.junit.Assert.assertTrue;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.CustomHashAggDataGenerator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.aggregate.hash.ITSpillingHashAgg;
import com.dremio.sabot.op.spi.SingleInputOperator;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class ITVectorizedHashAggShrinkableStates extends ITSpillingHashAgg {

  /**
   * Test spilling in OUTPUT_INMEMORY_PARTITIONS state
   *
   * @throws Exception
   */
  @Test
  public void testOutputSpilling() throws Exception {
    HashAggregate agg = getHashAggregate(1_000_000, Integer.MAX_VALUE);
    final int shortLen = (120 * 1024);
    final List<RecordBatchData> data = new ArrayList<>();

    try (AutoCloseable useSpillingAgg =
            with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true);
        CustomHashAggDataGenerator generator =
            new CustomHashAggDataGenerator(10000, getTestAllocator(), shortLen);
        VectorizedHashAggOperator op = newOperator(VectorizedHashAggOperator.class, agg, 1000)) {
      Fixtures.Table table = generator.getExpectedGroupsAndAggregations();

      final VectorAccessible output = op.setup(generator.getOutput());

      int count;
      int batchSize = 1000;
      // operator consumes data
      while (op.getState() != SingleInputOperator.State.DONE
          && (count = generator.next(batchSize)) != 0) {
        assertState(op, SingleInputOperator.State.CAN_CONSUME);
        op.consumeData(count);
        // if ran out of memory while consuming, it will be in CAN_PRODUCE, SPILL_NEXT_BATCH state
        while (op.getState() == SingleInputOperator.State.CAN_PRODUCE) {
          op.outputData();
        }
      }
      if (op.getState() == SingleInputOperator.State.CAN_CONSUME) {
        op.noMoreToConsume();
      }
      // process until operator reaches OUTPUT_INMEMORY_PARTITIONS state
      while (op.getInternalStateMachine()
          != VectorizedHashAggOperator.InternalState.OUTPUT_INMEMORY_PARTITIONS) {
        op.outputData();
      }

      // shrink in OUTPUT_INMEMORY_PARTITIONS state
      long shrinkableMemory = op.shrinkableMemory();

      op.shrinkMemory(1000);

      while (op.getInternalStateMachine()
          == VectorizedHashAggOperator.InternalState.SPILL_NEXT_BATCH) {
        op.outputData();
      }

      long newShrinkable = op.shrinkableMemory();

      // some memory was shrinked
      assertTrue(shrinkableMemory > newShrinkable);

      // generate output batches
      while (op.getState() != SingleInputOperator.State.DONE) {
        int outputCount = op.outputData();
        if (outputCount > 0) {
          data.add(new RecordBatchData(output, getTestAllocator()));
        }
      }

      // check output validity
      table.checkValid(data);

    } finally {
      AutoCloseables.close(data);
    }
  }
}

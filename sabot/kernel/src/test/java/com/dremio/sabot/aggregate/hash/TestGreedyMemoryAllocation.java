/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.aggregate.hash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.junit.Test;

import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.op.aggregate.vectorized.AccumulatorSet;
import com.dremio.sabot.op.aggregate.vectorized.SumAccumulators;

public class  TestGreedyMemoryAllocation {

  private static final int MAX_VALUES_PER_BATCH = 1024;
  private static final int JOINT_ALLOCATION_MIN = 4*1024;
  private static final int JOINT_ALLOCATION_MAX = 64*1024;

  @Test
  public void testMemoryAllocation() throws Exception {

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

      BigIntVector in1 = new BigIntVector("in1", allocator);
      BigIntVector in2 = new BigIntVector("in2", allocator);
      BigIntVector in3 = new BigIntVector("in3", allocator);
      BigIntVector in4 = new BigIntVector("in4", allocator);

      BigIntVector out1 = new BigIntVector("in1-sum", allocator);
      BigIntVector out2 = new BigIntVector("in2-sum", allocator);
      BigIntVector out3 = new BigIntVector("in3-sum", allocator);
      BigIntVector out4 = new BigIntVector("in4-sum", allocator);

      final SumAccumulators.BigIntSumAccumulator ac1 = new SumAccumulators.BigIntSumAccumulator(in1, out1, out1, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.BigIntSumAccumulator ac2 = new SumAccumulators.BigIntSumAccumulator(in2, out2, out2, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.BigIntSumAccumulator ac3 = new SumAccumulators.BigIntSumAccumulator(in3, out3, out3, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.BigIntSumAccumulator ac4 = new SumAccumulators.BigIntSumAccumulator(in4, out4, out4, MAX_VALUES_PER_BATCH, allocator);

      final AccumulatorSet accumulatorSet = new AccumulatorSet(JOINT_ALLOCATION_MIN, JOINT_ALLOCATION_MAX, allocator, ac1, ac2, ac3, ac4);

      accumulatorSet.addBatch();
      Map<Integer, List<AccumulatorSet.AccumulatorRange>> allocationMapping = accumulatorSet.getMapping();
      assertEquals(1, allocationMapping.size());
      assertEquals(3, allocationMapping.keySet().iterator().next().intValue()); // 32KB bucket
      List<AccumulatorSet.AccumulatorRange> ranges = allocationMapping.get(3);
      assertEquals(1, ranges.size());
      assertEquals(0, ranges.get(0).getStart());
      assertEquals(3, ranges.get(0).getEnd());
      //32KB for data buffers of all accumulators and 128bytes*4 for validity buffer of all accumulators
      final int allocatedMemory = 8 * MAX_VALUES_PER_BATCH * 4 + getValidityBufferSizeFromCount(MAX_VALUES_PER_BATCH) * 4;
      assertEquals(allocatedMemory, allocator.getAllocatedMemory());
      accumulatorSet.close();
    }
  }

  @Test
  public void testMemoryAllocation1() throws Exception {

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final VectorContainer c = new VectorContainer()) {

      IntVector in1 = new IntVector("in1", allocator);
      c.add(in1);

      BigIntVector in2 = new BigIntVector("in2", allocator);
      c.add(in2);

      IntVector in3 = new IntVector("in3", allocator);
      c.add(in3);

      IntVector in4 = new IntVector("in4", allocator);
      c.add(in4);

      BigIntVector in5 = new BigIntVector("in5", allocator);
      c.add(in5);

      DecimalVector in6 = new DecimalVector("in6", allocator, 38, 9);
      c.add(in6);

      BigIntVector in7 = new BigIntVector("in7", allocator);
      c.add(in7);

      DecimalVector in8 = new DecimalVector("in8", allocator, 38, 9);
      c.add(in8);

      IntVector in9 = new IntVector("in9", allocator);
      c.add(in9);

      IntVector in10 = new IntVector("in10", allocator);
      c.add(in10);

      BigIntVector out1 = new BigIntVector("in1-sum", allocator);
      BigIntVector out2 = new BigIntVector("in2-sum", allocator);
      BigIntVector out3 = new BigIntVector("in3-sum", allocator);
      BigIntVector out4 = new BigIntVector("in4-sum", allocator);
      BigIntVector out5 = new BigIntVector("in5-sum", allocator);
      Float8Vector out6 = new Float8Vector("in6-sum", allocator);
      BigIntVector out7 = new BigIntVector("in7-sum", allocator);
      Float8Vector out8 = new Float8Vector("in8-sum", allocator);
      BigIntVector out9 = new BigIntVector("in9-sum", allocator);
      BigIntVector out10 = new BigIntVector("in10-sum", allocator);

      final SumAccumulators.BigIntSumAccumulator ac1 = new SumAccumulators.BigIntSumAccumulator(in1, out1, out1, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.BigIntSumAccumulator ac2 = new SumAccumulators.BigIntSumAccumulator(in2, out2, out2, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.BigIntSumAccumulator ac3 = new SumAccumulators.BigIntSumAccumulator(in3, out3, out3, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.BigIntSumAccumulator ac4 = new SumAccumulators.BigIntSumAccumulator(in4, out4, out4, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.BigIntSumAccumulator ac5 = new SumAccumulators.BigIntSumAccumulator(in5, out5, out5, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.DoubleSumAccumulator ac6 = new SumAccumulators.DoubleSumAccumulator(in6, out6, out6, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.BigIntSumAccumulator ac7 = new SumAccumulators.BigIntSumAccumulator(in7, out7, out7, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.DoubleSumAccumulator ac8 = new SumAccumulators.DoubleSumAccumulator(in8, out8, out8, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.BigIntSumAccumulator ac9 = new SumAccumulators.BigIntSumAccumulator(in9, out9, out9, MAX_VALUES_PER_BATCH, allocator);
      final SumAccumulators.BigIntSumAccumulator ac10 = new SumAccumulators.BigIntSumAccumulator(in10, out10, out10, MAX_VALUES_PER_BATCH, allocator);

      final AccumulatorSet accumulatorSet = new AccumulatorSet(JOINT_ALLOCATION_MIN, JOINT_ALLOCATION_MAX, allocator,
                                                               ac1, ac2, ac3, ac4, ac5, ac6, ac7, ac8, ac9, ac10);

      accumulatorSet.addBatch();
      Map<Integer, List<AccumulatorSet.AccumulatorRange>> allocationMapping = accumulatorSet.getMapping();
      assertEquals(2, allocationMapping.size());
      assertTrue(allocationMapping.containsKey(4)); // ac1 ..... ac8 go to 64KB bucket
      assertTrue(allocationMapping.containsKey(2)); // ac9 .... ac10 go to 16KB bucket

      List<AccumulatorSet.AccumulatorRange> ranges = allocationMapping.get(4);
      assertEquals(1, ranges.size());
      assertEquals(0, ranges.get(0).getStart());
      assertEquals(7, ranges.get(0).getEnd());

      ranges = allocationMapping.get(2);
      assertEquals(1, ranges.size());
      assertEquals(8, ranges.get(0).getStart());
      assertEquals(9, ranges.get(0).getEnd());
      // joint allocation of data buffer for first 8 vectors of size 64KB +
      // joint allocation of validity buffer for first 8 vectors of size (128 * 8) +
      // joint allocation of data buffer for last 2 vectors of size 16KB +
      // joint allocation of validity buffer for last 2 vectors of size (128 * 2)
      final int allocatedMemory = (8 * MAX_VALUES_PER_BATCH * 8) + (getValidityBufferSizeFromCount(MAX_VALUES_PER_BATCH) * 8) + (8 * MAX_VALUES_PER_BATCH * 2) + (getValidityBufferSizeFromCount(MAX_VALUES_PER_BATCH) * 2);
      assertEquals(allocatedMemory, allocator.getAllocatedMemory());
      accumulatorSet.close();
    }
  }

  private static int getValidityBufferSizeFromCount(final int valueCount) {
    return (int) Math.ceil(valueCount / 8.0);
  }
}

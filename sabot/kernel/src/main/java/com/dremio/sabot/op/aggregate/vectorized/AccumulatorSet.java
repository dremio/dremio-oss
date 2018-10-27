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
package com.dremio.sabot.op.aggregate.vectorized;

import com.dremio.common.util.Numbers;
import com.dremio.sabot.op.common.ht2.ResizeListener;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

/**
 * Represents the set of accumulators of type {@link BaseSingleAccumulator}.
 * {@link VectorizedHashAggOperator} contains an AccumulatorSet that has a 1:1
 * mapping between an agg operation (SUM, MIN, MAX) and {@link BaseSingleAccumulator}.
 * Every operation done by {@link VectorizedHashAggOperator} and a
 * {@link com.dremio.sabot.op.common.ht2.LBlockHashTable}
 * on accumulator(s) go through the interfaces provided by AccumulatorSet
 */
public class AccumulatorSet implements ResizeListener, AutoCloseable {
  private final int jointAllocationMin;
  private final int jointAllocationLimit;
  private final BufferAllocator allocator;
  private final Accumulator[] children;
  private final int[] sizes;
  private final int[] allocationLevels;
  private final boolean[] visited;
  private final Map<Integer, List<AccumulatorRange>> map;
  private final List<Integer> singleAccumulatorIndexes;

  public AccumulatorSet(final long jointAllocationMin, final long jointAllocationLimit,
                        final BufferAllocator allocator, final Accumulator... children) {
    super();
    this.jointAllocationMin = (int)jointAllocationMin;
    this.jointAllocationLimit = (int)jointAllocationLimit;
    this.allocator = allocator;
    this.children = children;
    this.sizes = new int[children.length];
    this.allocationLevels = new int[children.length];
    this.visited = new boolean[children.length];
    final int numBoundaries = Long.numberOfTrailingZeros(jointAllocationLimit) - Long.numberOfTrailingZeros(jointAllocationMin);
    this.map = new HashMap<>(numBoundaries);
    this.singleAccumulatorIndexes = new ArrayList<>();
    if (jointAllocationLimit > 0) {
      computeAllocationBoundaries(0);
    }
  }

  /**
   * Represents a contiguous range of accumulators that will
   * be jointly allocated as a single ArrowBuf. Both start
   * and end represent indices (inclusive) in the accumulator
   * array.
   */
  private static class AccumulatorRange {
    private final int start;
    private final int end;
    AccumulatorRange(final int start, final int end) {
      this.start = start;
      this.end = end;
    }
  }

  @Override
  public void addBatch() {
    if (jointAllocationLimit == 0) {
      addBatchWithoutLimitOptimizedForHeap();
    } else {
      addBatchWithLimitOptimizedForDirect();
    }
  }

  /**
   * Add a new batch to accumulators. We first compute the total
   * buffer size needed for vectors across all accumulators.
   * We then slice this ArrowBuf and hand over buffers to
   * individual accumulators. This significantly
   * reduces the heap overhead associated with large number
   * of ArrowBufs.
   *
   * The problems with this approach are:
   *
   * (1) Since we initially allocate a single large buffer,
   * we may hit OOM too frequently.
   *
   * (2) Direct memory wastage due to power of 2 nature of allocations
   * in our allocator -- this is also the reason why we will hit
   * OOM if the total size of buffer is very large
   */
  private void addBatchWithoutLimitOptimizedForHeap() {
    int size = 0;
    for(Accumulator a : children){
      size += a.getTotalBufferSize();
    }
    /* if we succeed with the following allocation, it means we
     * have allocated memory for next batch for all accumulators.
     * however, if we fail (OOM), then the hash table will still
     * go ahead and revert the new memory allocations on each
     * and that will be a NO-OP since as far as the individual
     * accumulators are concerned, no memory allocation was attempted
     * on them
     */
    final ArrowBuf bufferForAllAccumulators = allocator.buffer(size);
    int offset = 0;
    for(Accumulator a : children){
      final int bufferSizeForAccumulator = a.getTotalBufferSize();
      final ArrowBuf bufferForAccumulator = bufferForAllAccumulators.slice(offset, bufferSizeForAccumulator);
      offset += bufferSizeForAccumulator;
      a.addBatch(bufferForAccumulator);
    }
    bufferForAllAccumulators.close();
  }

  /**
   * Alternative version of previous algorithm where
   * we do joint allocations for accumulator vectors
   * but only upto a fixed power of 2 threshold.
   * This helps to minimize cases where we are requesting
   * very large chunks of memory and also does a good
   * job at reducing heap overhead of ArrowBufs.
   */
  private void addBatchWithLimitOptimizedForHeapAndDirect() {
    int size = 0;
    int start = 0;
    int i;

    for (i = 0; i < children.length; ++i) {
      final Accumulator accumulator = children[i];
      final int bufferSize = accumulator.getTotalBufferSize();
      size += bufferSize;
      if (size == jointAllocationLimit) {
        /* reached exact power of 2 request size, so allocate */
        allocatePowerOfTwoOrLessAndSlice(size, start, i);
        size = 0;
        start = i + 1;
      } else if (size > jointAllocationLimit) {
        /* just crossed power of 2 request size, so backoff to go
         * slightly less than power of 2 and allocate
         */
        size -= bufferSize;
        if (size == 0) {
          /* this can happen if joint allocation limit
           * (which is always a power of 2) is set to a low number
           * and HT batch size is a power of 2
           */
          allocatePowerOfTwoOrLessAndSlice(bufferSize, i, i);
        } else {
          --i;
          allocatePowerOfTwoOrLessAndSlice(size, start, i);
        }
        size = 0;
        start = i + 1;
      }
    }

    if (size < jointAllocationLimit && size != 0) {
      allocatePowerOfTwoOrLessAndSlice(size, start, i - 1);
    }
  }

  /**
   * We do this computation exactly once. For a given
   * set of accumulators and their buffer sizes,
   * we try to group accumulators into different allocation
   * buckets (size equal to some power of 2).
   * @param start starting accumulator index
   */
  private void computeAllocationBoundaries(int start) {
    int size = 0;
    int i;

    if (start >= children.length) {
      return;
    }

    if (start == children.length - 1) {
      /* we will be here if there is only one accumulator in the set or
       * if it turns out that last accumulator goes into
       * its own bucket and we weren't able to put it into
       * one of the allocation levels with other accumulator(s)
       *
       * at this point we don't have an option
       * to consider grouping into one of allocation levels.
       *
       * secondly,
       * this accumulator's required buffer size could very well be beyond
       * the joint allocation limit threshold. so we will separately allocate
       * memory for this lone accumulator whenever there is a need to add batch
       */
      singleAccumulatorIndexes.add(start);
      return;
    }

    for (i = start; i < children.length; ++i) {
      /* compute cumulative actual buffer sizes for accumulators */
      final Accumulator accumulator = children[i];
      size += accumulator.getTotalBufferSize();
      sizes[i] = size;
    }

    /* use the cumulative sizes computed above to decide the allocation
     * level and group accumulators into an allocation level by adding
     * a mapping from level (power of 2 size bucket) to a contiguous of range
     * of accumulators.
     */
    for (i = start; i < children.length; ++i) {
      final int cumulativeSize = sizes[i];
      if (cumulativeSize > jointAllocationLimit) {
        if (i >= 1 && !visited[i - 1]) {
          final AccumulatorRange range = new AccumulatorRange(start, i - 1);
          addMapping(allocationLevels[i - 1], range);
          visited[i - 1] = true;
          /* restart packing from next one */
          computeAllocationBoundaries(i);
          return;
        } else {
          ++start;
          singleAccumulatorIndexes.add(i);
        }
      } else {
        allocationLevels[i] = getAllocationLevelForSize(cumulativeSize);
      }
    }

    final AccumulatorRange range = new AccumulatorRange(start, i - 1);
    addMapping(allocationLevels[i - 1], range);
  }

  private void addMapping(final int allocationLevel, AccumulatorRange range) {
    if (map.containsKey(allocationLevel)) {
      map.get(allocationLevel).add(range);
    } else {
      List<AccumulatorRange> ranges = new ArrayList<>();
      ranges.add(range);
      map.put(allocationLevel, ranges);
    }
  }

  private int getAllocationLevelForSize(int size) {
    final int powerOfTwoSize = Numbers.nextPowerOfTwo(size);
    return Integer.numberOfTrailingZeros(powerOfTwoSize) - Integer.numberOfTrailingZeros(jointAllocationMin);
  }

  /**
   * This algorithm is mainly aimed for using direct memory optimally
   * with minimizing wastage (due to power of 2 rounding) as much as
   * possible. We still do joint allocations, but the reduction in
   * heap overhead is least compared to other two algorithms.
   */
  private void addBatchWithLimitOptimizedForDirect() {
    Set<Map.Entry<Integer, List<AccumulatorRange>>> levelToAccumulatorsMapping = map.entrySet();
    for (Map.Entry<Integer, List<AccumulatorRange>> mapping : levelToAccumulatorsMapping) {
      final int allocationLevel = mapping.getKey();
      final List<AccumulatorRange> ranges = mapping.getValue();
      for (AccumulatorRange range : ranges) {
        allocatePowerOfTwoOrLessAndSlice(jointAllocationMin << allocationLevel, range.start, range.end);
      }
    }
    for (int singleAccumulatorIndex : singleAccumulatorIndexes) {
      allocatePowerOfTwoOrLessAndSlice(children[singleAccumulatorIndex].getTotalBufferSize(), singleAccumulatorIndex, singleAccumulatorIndex);
    }
  }

  private void allocatePowerOfTwoOrLessAndSlice(final int size, final int start, int end) {
    final ArrowBuf bufferForAllAccumulators = allocator.buffer(size);
    int offset = 0;
    for(int i = start; i <= end; ++i) {
      final Accumulator accumulator = children[i];
      final int bufferSize = accumulator.getTotalBufferSize();
      final ArrowBuf bufferForSingleAccumulator = bufferForAllAccumulators.slice(offset, bufferSize);
      offset += bufferSize;
      accumulator.addBatch(bufferForSingleAccumulator);
    }
    bufferForAllAccumulators.close();
  }

  public void accumulate(final long memoryAddr, final int count) {
    for(Accumulator a : children){
      a.accumulate(memoryAddr, count);
    }
  }

  public void accumulateNoSpill(final long memoryAddr, final int count) {
    for(Accumulator a : children){
      a.accumulateNoSpill(memoryAddr, count);
    }
  }

  public void output(int batchIndex) {
    for(Accumulator a : children){
      a.output(batchIndex);
    }
  }

  public Accumulator[] getChildren() {
    return children;
  }

  /**
   * Get accumulator vector size (in bytes).
   * {@link VectorizedHashAggOperator} works with a NestedAccumulator
   * in each {@link VectorizedHashAggPartition} and there is a need
   * to compute size of partition's data structures. We use this
   * method to get total size across all the accumulators the operator
   * is working with.
   *
   * @return total size (in bytes) of all accumulator vectors.
   */
  public long getSizeInBytes() {
    long size = 0;
    for(Accumulator a : children){
      size += a.getSizeInBytes();
    }
    return size;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(children);
  }

  @Override
  public void revertResize() {
    for(Accumulator a : children){
      a.revertResize();
    }
  }

  @Override
  public void verifyBatchCount(int batches) {
    for (Accumulator a : children) {
      a.verifyBatchCount(batches);
    }
  }

  @Override
  public void commitResize() {
    for(Accumulator a : children){
      a.commitResize();
    }
  }

  @Override
  public void resetToMinimumSize() throws Exception {
    for(Accumulator a : children){
      a.resetToMinimumSize();
    }
  }
}

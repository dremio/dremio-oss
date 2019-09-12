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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.Numbers;
import com.dremio.sabot.op.common.ht2.ResizeListener;
import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ArrowBuf;

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
  private final Map<Integer, List<List<Integer>>> combinedAccumulators;
  private final List<Integer> singleAccumulators;
  private final int validitySizeForSingleAccumulator;

  public AccumulatorSet(final long jointAllocationMin, final long jointAllocationLimit,
                        final BufferAllocator allocator, final Accumulator... children) {
    super();
    this.jointAllocationMin = (int)jointAllocationMin;
    this.jointAllocationLimit = (int)jointAllocationLimit;
    this.allocator = allocator;
    this.children = children;
    this.validitySizeForSingleAccumulator = children.length > 0 ? children[0].getValidityBufferSize() : 0;
    final int numAllocationBuckets = Long.numberOfTrailingZeros(jointAllocationLimit) - Long.numberOfTrailingZeros(jointAllocationMin);
    this.combinedAccumulators = new HashMap<>(numAllocationBuckets);
    this.singleAccumulators = new ArrayList<>();

    // sort the children in descending order of their sizes.
    final List<Integer> sortedChildIndices = new ArrayList<>();
    for (int i = 0; i < children.length; ++i) {
      sortedChildIndices.add(i);
    }
    sortedChildIndices.sort((a, b) -> (computeAccumulatorSize(b) - computeAccumulatorSize(a)));

    // compute the allocation boundaries.
    computeAllocationBoundaries(sortedChildIndices, 0);
  }

  @Override
  public void addBatch() throws Exception {
    addBatchWithLimitOptimizedForDirect();
  }

  private int computeAccumulatorSize(int index) {
    final Accumulator accumulator = children[index];
    return Numbers.nextMultipleOfEight(accumulator.getDataBufferSize()) +
      Numbers.nextMultipleOfEight(validitySizeForSingleAccumulator);
  }

  /**
   * Memory Allocation - Algorithm 3 (currently in use)
   *
   * We do this computation exactly once. For a given
   * set of accumulators and their buffer sizes,
   * we try to group accumulators into different allocation
   * buckets (size equal to some power of 2).
   * @param start starting accumulator index
   */
  private void computeAllocationBoundaries(List<Integer> accumulatorIndices, int start) {
    List<Integer> combined = new ArrayList<>();

    /* use the cumulative sizes to decide the allocation
     * level and group accumulators into an allocation level by adding
     * a mapping from level (power of 2 size bucket) to a contiguous of range
     * of accumulators.
     */
    int cumulativeSize = 0;
    while (start < accumulatorIndices.size()) {

      /*
       * Compute the size of the current child.
       */
      int childIndex = accumulatorIndices.get(start);
      final int childSize = computeAccumulatorSize(childIndex);

      if (cumulativeSize + childSize > jointAllocationLimit) {
        if (cumulativeSize > 0) {
          // one or more previous accumulators can be combined.
          addMapping(getAllocationLevelForSize(cumulativeSize), combined);

          /* restart packing from 'start' */
          combined.clear();
          cumulativeSize = 0;
        } else {
          // single accumulator is large enough. so, skip combining for this one.
          ++start;
          singleAccumulators.add(childIndex);
        }
      } else {
        // one accumulator can be combined.
        cumulativeSize += childSize;
        combined.add(childIndex);
        ++start;
      }
    }

    if (cumulativeSize > 0) {
      addMapping(getAllocationLevelForSize(cumulativeSize), combined);
    }
  }

  private void addMapping(final int allocationLevel, List<Integer> indexList) {
    assert indexList.size() > 0;

    if (combinedAccumulators.containsKey(allocationLevel)) {
      combinedAccumulators.get(allocationLevel).add(new ArrayList<>(indexList));
    } else {
      List<List<Integer>> listOfLists = new ArrayList<>();
      listOfLists.add(new ArrayList<>(indexList));

      combinedAccumulators.put(allocationLevel, listOfLists);
    }
  }

  private int getAllocationLevelForSize(int size) {
    if (size < jointAllocationMin) {
      return 0;
    } else {
      final int powerOfTwoSize = Numbers.nextPowerOfTwo(size);
      return Integer.numberOfTrailingZeros(powerOfTwoSize)
          - Integer.numberOfTrailingZeros(jointAllocationMin);
    }
  }

  /**
   * Memory Allocation - Algorithm 3 (currently in use)
   *
   * This algorithm is mainly aimed for using direct memory optimally
   * with minimizing wastage (due to power of 2 rounding) as much as
   * possible. We still do joint allocations, but the reduction in
   * heap overhead is least compared to other two algorithms.
   */
  private void addBatchWithLimitOptimizedForDirect() throws Exception {
    for (Map.Entry<Integer, List<List<Integer>>> mapping : combinedAccumulators.entrySet()) {
      final int allocationLevel = mapping.getKey();
      for (final List<Integer> childIndices : mapping.getValue()) {
        allocatePowerOfTwoOrLessAndSlice(jointAllocationMin << allocationLevel, childIndices);
      }
    }

    for (final Integer childIndex : singleAccumulators) {
      int totalSize = computeAccumulatorSize(childIndex);
      allocatePowerOfTwoOrLessAndSlice(totalSize, Collections.singletonList(childIndex));
    }
  }

  private void allocatePowerOfTwoOrLessAndSlice(final int totalSize, List<Integer> childIndices) throws Exception {
    final int validitySize = this.validitySizeForSingleAccumulator;
    try(AutoCloseables.RollbackCloseable rollbackable = new AutoCloseables.RollbackCloseable()) {
      final ArrowBuf bufferForAllAccumulators = allocator.buffer(totalSize);
      rollbackable.add(bufferForAllAccumulators);
      int offset = 0;
      for (final Integer index : childIndices) {
        Accumulator accumulator = children[index];
        // slice validity buffer from the combined buffer.
        final ArrowBuf validityBuffer = bufferForAllAccumulators.slice(offset, validitySize);
        offset += Numbers.nextMultipleOfEight(validitySize);

        // slice data buffer from the combined buffer.
        final int dataSize = accumulator.getDataBufferSize();
        final ArrowBuf dataBuffer = bufferForAllAccumulators.slice(offset, dataSize);
        offset += Numbers.nextMultipleOfEight(dataSize);

        accumulator.addBatch(dataBuffer, validityBuffer);
      }
      bufferForAllAccumulators.close();
      rollbackable.commit();
    } // hashtable/operator will handle the exception
  }

  public void accumulate(final long memoryAddr, final int count,
                         final int bitsInChunk, final int chunkOffsetMask) {
    for(Accumulator a : children){
      a.accumulate(memoryAddr, count, bitsInChunk, chunkOffsetMask);
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

  @Override
  public void releaseBatch(final int batchIdx)
  {
    for(Accumulator a : children){
      a.releaseBatch(batchIdx);
    }
  }

  @VisibleForTesting
  public Map<Integer, List<List<Integer>>> getMapping() {
    return combinedAccumulators;
  }
}
